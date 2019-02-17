package com.dataloom.er

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, OneHotEncoder}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Feature hashing projects a set of categorical or numerical features into a feature vector of
  * specified dimension (typically substantially smaller than that of the original feature
  * space). This is done using the hashing trick (https://en.wikipedia.org/wiki/Feature_hashing)
  * to map features to indices in the feature vector.
  *
  * The [[FeatureHasher]] transformer operates on multiple columns. Each column may contain either
  * numeric or categorical features. Behavior and handling of column data types is as follows:
  * -Numeric columns: For numeric features, the hash value of the column name is used to map the
  * feature value to its index in the feature vector. By default, numeric features
  * are not treated as categorical (even when they are integers). To treat them
  * as categorical, specify the relevant columns in `categoricalCols`.
  * -String columns: For categorical features, the hash value of the string "column_name=value"
  * is used to map to the vector index, with an indicator value of `1.0`.
  * Thus, categorical features are "one-hot" encoded
  * (similarly to using [[OneHotEncoder]] with `dropLast=false`).
  * -Boolean columns: Boolean values are treated in the same way as string columns. That is,
  * boolean features are represented as "column_name=true" or "column_name=false",
  * with an indicator value of `1.0`.
  *
  * Null (missing) values are ignored (implicitly zero in the resulting feature vector).
  *
  * The hash function used here is also the MurmurHash 3 used in [[HashingTF]]. Since a simple modulo
  * on the hashed value is used to determine the vector index, it is advisable to use a power of two
  * as the numFeatures parameter; otherwise the features will not be mapped evenly to the vector
  * indices.
  *
  * {{{
  *   val df = Seq(
  *    (2.0, true, "1", "foo"),
  *    (3.0, false, "2", "bar")
  *   ).toDF("real", "bool", "stringNum", "string")
  *
  *   val hasher = new FeatureHasher()
  *    .setInputCols("real", "bool", "stringNum", "string")
  *    .setOutputCol("features")
  *
  *   hasher.transform(df).show(false)
  *
  *   +----+-----+---------+------+------------------------------------------------------+
  *   |real|bool |stringNum|string|features                                              |
  *   +----+-----+---------+------+------------------------------------------------------+
  *   |2.0 |true |1        |foo   |(262144,[51871,63643,174475,253195],[1.0,1.0,2.0,1.0])|
  *   |3.0 |false|2        |bar   |(262144,[6031,80619,140467,174475],[1.0,1.0,1.0,3.0]) |
  *   +----+-----+---------+------+------------------------------------------------------+
  * }}}
  */
class ERUniqueHasher(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("re-unique-hasher"))

  def setInputCols(values: String*): this.type = setInputCols(values.toArray)

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val localInputCols = $(inputCols)

    val outputSchema = transformSchema(dataset.schema)

    val hashFeatures = udf { row: Row =>
      val hashSet = localInputCols
        .filter(c => !row.isNullAt(row.fieldIndex(c)))
        .map { c =>
          val value = row.get(row.fieldIndex(c)).toString
          value.hashCode.toDouble
        }

      Vectors.dense(hashSet)
    }

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(
      col("*"),
      hashFeatures(struct($(inputCols).map(col): _*)).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): ERFeatureHasher = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    fields.foreach { fieldSchema =>
      val dataType = fieldSchema.dataType
      val fieldName = fieldSchema.name
      require(dataType.isInstanceOf[NumericType] ||
        dataType.isInstanceOf[StringType] ||
        dataType.isInstanceOf[BooleanType],
        s"ERUniqueHasher requires columns to be of NumericType, BooleanType or StringType. " +
          s"Column $fieldName was $dataType")
    }
    schema.add($(outputCol), DataTypes.createArrayType(SQLDataTypes.VectorType))
  }
}