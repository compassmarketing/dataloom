package com.dataloom.er


import com.dataloom.er
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector, Vectors}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasSeed}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql._
import org.apache.hadoop.fs.Path

import scala.util.Random

/**
  * Params for ER Model.
  */
trait ERParams extends HasInputCol with HasOutputCol {
  /**
    * Param for the number of hash tables used in LSH OR-amplification.
    *
    * LSH OR-amplification can be used to reduce the false negative rate. Higher values for this
    * param lead to a reduced false negative rate, at the expense of added computational complexity.
    *
    * @group param
    */
  final val numHashTables: IntParam = new IntParam(this, "numHashTables", "number of hash " +
    "tables, where increasing number of hash tables lowers the false negative rate, and " +
    "decreasing it improves the running performance", ParamValidators.gt(0))

  /** @group getParam */
  final def getNumHashTables: Int = $(numHashTables)

  setDefault(numHashTables -> 1)

  final val numHashFunctions: IntParam = new IntParam(this, "numHashFunctions", "number of hash " +
    "tables, where increasing number of hash tables lowers the false negative rate, and " +
    "decreasing it improves the running performance", ParamValidators.gt(0))


  /** @group getParam */
  final def getNumHashFunctions: Int = $(numHashFunctions)

  setDefault(numHashFunctions -> 1)

  /**
    * Param for input column names.
    *
    * @group param
    */
  final val uniqueCol: Param[String] = new Param[String](this, "uniqueCol", "unique feature input column name")

  /** @group getParam */
  final def getUniqueCol: String = $(uniqueCol)

  setDefault(uniqueCol -> "")


  /**
    * Transform the Schema for LSH
    *
    * @param schema The schema of the input dataset without [[outputCol]].
    * @return A derived schema with [[outputCol]] added.
    */
  protected[this] final def validateAndTransformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), DataTypes.createArrayType(SQLDataTypes.VectorType))
  }
}

class ERModel(
               override val uid: String,
               private val randCoefficients: Array[(Int, Int)]) extends Model[ERModel] with ERParams with DefaultParamsWritable {

  def this(uid: String) {
    this(uid, Array())
  }

  type Entity = (Long, Array[Int])

  def candidatePairs(
                       dataset: Dataset[Row],
                       idCol: String,
                       pairNames: (String, String) = ("A", "B")): Dataset[_] = {

    import dataset.sparkSession.implicits._
    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
      transform(dataset)
    } else {
      dataset.toDF()
    }

    val transformed = modelDataset.rdd.flatMap[((Int, Seq[Double]), Long)] { row =>
      val hashes = row.getAs[Seq[Seq[Double]]]($(outputCol))
      hashes.zipWithIndex.map { case (hash: Seq[Double], i: Int) =>
        val entry = if (i >= $(numHashTables)) $(numHashTables) else i
        ((entry, hash), row.getAs[Long](idCol))
      }
    }

    val blocks = transformed.combineByKey(
      (v: Long) => Set(v),
      (acc: Set[Long], v: Long) => acc + v,
      (x: Set[Long], y: Set[Long]) => x.union(y),
      transformed.getNumPartitions * 5
    )

    val pairs = blocks.flatMap[((Long, Long), Boolean)] { case (key: (Int, Seq[Double]), rows: Set[Long]) =>
      if (rows.size <= 50) {
        rows.subsets(2)
          .map { p => ((p.head, p.last), key._1 == $(numHashTables)) }
      } else {
        Iterator.empty.asInstanceOf[Iterator[((Long, Long), Boolean)]]
      }
    }.reduceByKey(_ || _).map(p => (p._1._1, p._1._2, p._2))

    pairs.toDF(pairNames._1, pairNames._2, "unique")
  }

  protected val hashFunction: Seq[Int] => Seq[Seq[Double]] = {
    elems: Seq[Int] => {
      val hashValues = randCoefficients.map { case (a, b) =>
        elems.map { elem: Int =>
          ((1 + elem) * a + b) % ERMinHashLSH.HASH_PRIME
        }.min.toDouble
      }
      hashValues.toSeq.grouped($(numHashFunctions)).toSeq
    }
  }

  protected val uniqueFunction: (Seq[Seq[Double]], Seq[Double]) => Seq[Seq[Double]] = {
    (hashes: Seq[Seq[Double]], uniques: Seq[Double]) => {
      if (uniques.nonEmpty) {
        hashes ++ uniques.grouped(1)
      }
      else {
        hashes
      }
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction(_: Seq[Int]), DataTypes.createArrayType(DataTypes.createArrayType(DoubleType)))
    val uniquesUDF = udf(uniqueFunction(_: Seq[Seq[Double]], _: Seq[Double]), DataTypes.createArrayType(DataTypes.createArrayType(DoubleType)))

    if (!$(uniqueCol).isEmpty) {
      dataset
        .withColumn($(outputCol), uniquesUDF(transformUDF(col($(inputCol))), col($(uniqueCol))))
    }
    else {
      dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ERModel = {
    val copied = new ERModel(uid, randCoefficients).setParent(parent)
    copyValues(copied, extra)
  }

  override def save(path: String): Unit = this.save(path, false)

  def save(path: String, overwrite: Boolean): Unit = {

    if (overwrite) {
      this.write.overwrite()
    }
    else {
      this.write
    }.save(path) // Write params

    val erWriter = new er.ERModel.ERModelWriter(this)
    val dataPath = new Path(path, "data").toString

    if (overwrite) {
      erWriter.overwrite()
    }
    else {
      erWriter
    }.save(dataPath) // Write random coefficients
  }

}

class ERMinHashLSH(override val uid: String) extends Estimator[ERModel] with ERParams with HasSeed {
  self: Estimator[ERModel] =>

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNumHashTables(value: Int): this.type = set(numHashTables, value)

  def setNumHashFunctions(value: Int): this.type = set(numHashFunctions, value)

  def setUniqueCol(value: String): this.type = set(uniqueCol, value)

  def this() = {
    this(Identifiable.randomUID("er-mh-lsh"))
  }


  def setSeed(value: Long): this.type = set(seed, value)

  def createRawLSHModel(inputDim: Int): ERModel = {
    require(inputDim <= ERMinHashLSH.HASH_PRIME,
      s"The input vector dimension $inputDim exceeds the threshold ${ERMinHashLSH.HASH_PRIME}.")


    val rand = new Random($(seed))
    val randCoefs: Array[(Int, Int)] = Array.fill($(numHashTables) * $(numHashFunctions)) {
      (1 + rand.nextInt(ERMinHashLSH.HASH_PRIME - 1), rand.nextInt(ERMinHashLSH.HASH_PRIME - 1))
    }
    val model = new ERModel(uid, randCoefs)
    copyValues(model)
  }

  override def fit(dataset: Dataset[_]): ERModel = {
    transformSchema(dataset.schema, logging = true)
    val inputDim = dataset.select(col($(inputCol))).head().get(0).asInstanceOf[Seq[Int]].length
    createRawLSHModel(inputDim).setParent(this)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.equals(DataTypes.createArrayType(IntegerType)), s"Input column must be a Vector data type.")

    if (!$(uniqueCol).isEmpty) {
      require(schema($(uniqueCol)).dataType.equals(DataTypes.createArrayType(DoubleType)), s"Unique column must be a Vector data type.")
    }

    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}


object ERMinHashLSH {
  // A large prime smaller than sqrt(2^63 − 1)
  private[er] val HASH_PRIME = 2147483647
}


object ERModel extends DefaultParamsReadable[ERModel] {

  override def load(path: String): ERModel = {
    val model = this.read.load(path)

    val erReader = new ERModelReader()
    val erModel = erReader.load(path)

    model.copyValues(erModel)
  }

  private[ERModel] class ERModelWriter(instance: ERModel)
    extends MLWriter {

    private case class Data(randCoefficients: Array[Int])

    override def saveImpl(path: String): Unit = {
      val data = Data(instance.randCoefficients.flatMap(tuple => Array(tuple._1, tuple._2)))
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(path)
    }
  }

  private class ERModelReader extends MLReader[ERModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ERModel].getName

    override def load(path: String): ERModel = {
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("randCoefficients").head()
      val randCoefficients = data.getAs[Seq[Int]](0).grouped(2)
        .map(tuple => (tuple(0), tuple(1))).toArray

      new ERModel("rand", randCoefficients)
    }
  }

}