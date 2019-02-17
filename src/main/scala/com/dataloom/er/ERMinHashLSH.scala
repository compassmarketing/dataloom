package com.dataloom.er

import org.apache.spark.ml.linalg.{Vector, SQLDataTypes, Vectors}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasSeed}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Encoders, _}

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
    * @group param
    */
  final val uniqueCol: Param[String] = new Param[String](this, "uniqueCol", "unique feature input column name")

  /** @group getParam */
  final def getUniqueCol: String = $(uniqueCol)

  setDefault(uniqueCol -> "")

  /**
    * Param for pair column names.
    * @group param
    */
  final val pairNames: Param[(String, String)] = new Param[(String, String)](this, "pairNames", "The pair names for each set of canidate pairs")

  /** @group getParam */
  final def getPairNames: (String, String) = $(pairNames)

  setDefault(pairNames -> ("A", "B"))

  /**
    * Transform the Schema for LSH
    * @param schema The schema of the input dataset without [[outputCol]].
    * @return A derived schema with [[outputCol]] added.
    */
  protected[this] final def validateAndTransformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), DataTypes.createArrayType(SQLDataTypes.VectorType))
  }
}

class ERModel(
                           override val uid: String,
                           private val randCoefficients: Array[(Int, Int)]) extends Model[ERModel] with ERParams {


  private def newRowFromSchema(row: Row, newSchema: StructType): Row = {
    val fields = newSchema.fieldNames.map(n => row.get(row.schema.fieldIndex(n)))
    Row.fromSeq(fields)
  }

  def candidatePairs(dataset: Dataset[Row]): Dataset[_] = {

    val schema = dataset.schema


    val modelDataset: DataFrame = if (!dataset.columns.contains($(outputCol))) {
      transform(dataset)
    } else {
      dataset.toDF()
    }
    val transformed = modelDataset
      .select(col("*"), posexplode(col($(outputCol))).as(Seq("entry", "hashValue")))
      .withColumn("actualEntry", when(col("entry") >= $(numHashTables), $(numHashTables)).otherwise(col("entry")))

    val blocks =
      transformed.groupByKey(hr => (hr.getAs[Int]("actualEntry"), hr.getAs[Vector]("hashValue")))(Encoders.product)

    val (pairA, pairB) = $(pairNames)
    val pairs = blocks.flatMapGroups { case (_, rows) =>
      val set = rows.toSet
      if (set.size <= 50) {
        set.subsets(2).map { p =>
          (newRowFromSchema(p.head, schema), newRowFromSchema(p.last, schema))
        }
      } else {
        Iterator.empty.asInstanceOf[Iterator[(Row, Row)]]
      }
    }(Encoders.tuple(RowEncoder.apply(schema), RowEncoder.apply(schema)))
      .toDF(pairA, pairB)

    pairs.distinct()
  }

  protected val hashFunction: Vector => Array[Vector] = {
    elems: Vector => {
      require(elems.numNonzeros > 0, "Must have at least 1 non zero entry.")

      val elemsList = elems.toSparse.indices.toList
      val hashValues = randCoefficients.map { case (a, b) =>
        elemsList.map { elem: Int =>
          ((1 + elem) * a + b) % ERMinHashLSH.HASH_PRIME
        }.min.toDouble
      }
      hashValues.grouped($(numHashFunctions)).map(Vectors.dense).toArray
    }
  }

  protected val uniqueFunction: (Seq[Vector], Vector) => Array[Vector] = {
    (hashes:Seq[Vector], uniques: Vector) => {
      if (uniques.numNonzeros > 0) {
        hashes.toArray ++ uniques.toArray.grouped(1).map(Vectors.dense)
      }
      else {
        hashes.toArray
      }
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(hashFunction(_: Vector), DataTypes.createArrayType(SQLDataTypes.VectorType))
    val uniquesUDF = udf(uniqueFunction(_:Seq[Vector], _: Vector), DataTypes.createArrayType(SQLDataTypes.VectorType))

    if (!$(uniqueCol).isEmpty) {
      // TODO: Require uniqueCol to be array column of DenseVector[Int]

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
    new ERModel(uid, randCoefs)
  }

  override def fit(dataset: Dataset[_]): ERModel = {
    transformSchema(dataset.schema, logging = true)
    val inputDim = dataset.select(col($(inputCol))).head().get(0).asInstanceOf[Vector].size
    val model = createRawLSHModel(inputDim).setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.equals(SQLDataTypes.VectorType), s"Input column must be a Vector data type.")

    if (!$(uniqueCol).isEmpty) {
      require(schema($(uniqueCol)).dataType.equals(SQLDataTypes.VectorType), s"Unique column must be a Vector data type.")
    }

    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}


object ERMinHashLSH {
  // A large prime smaller than sqrt(2^63 − 1)
  private[er] val HASH_PRIME = 2147483647
}
