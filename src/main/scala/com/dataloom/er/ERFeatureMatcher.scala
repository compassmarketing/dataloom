package com.dataloom.er

import com.dataloom.er.functions.{doubleMetaBinary, familyNameScore, genderClassification, givenNameScore}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class ERFeatureMatcher(override val uid: String) extends Transformer
  with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("er-feature-matcher"))

  /**
    * Param for pair column names.
    * @group param
    */
  final val pairNames: Param[(String, String)] = new Param[(String, String)](this, "pairNames", "The pair names for each set of canidate pairs")

  /** @group getParam */
  final def getPairNames: (String, String) = $(pairNames)

  setDefault(pairNames -> ("A", "B"))

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    /**
      * Performs a waterfall match on name scores
      */
    val matcher = udf {
      (aFN:String, bFN:String, aLN:String, bLN:String, aG:String, bG:String) => {
        val gnScore = givenNameScore(aFN, bFN)
        val revGnScore = givenNameScore(aFN, bLN)
        val famScore = familyNameScore(aLN, bLN)
        val revFamScore = familyNameScore(aLN, bFN)
        val dblmetaScore = doubleMetaBinary(aFN, bFN)
        val genderClass = genderClassification(aG, bG)

        if (revGnScore >= 0.95 && revFamScore >= 0.95 && genderClass != 3.0) {
          true
        }
        else if (gnScore >= 0.85 && famScore >= 0.85 && genderClass != 3.0) {
          true
        }
        else if (gnScore >= 0.95 && genderClass == 1.0) {
          true // TOOD: Need to look at more than first name here. e.g. 4 Mary's in an APT complex
        }
        else if (dblmetaScore >= 1.0 && famScore >= 0.90 && genderClass != 3.0) {
          true
        }
        else {
          false
        }
      }
    }

    val (pairA, pairB) = $(pairNames)
    val metadata = outputSchema($(outputCol)).metadata
    val matchCol = matcher(
      col(s"$pairA.$DL_FNAME"),
      col(s"$pairB.$DL_FNAME"),
      col(s"$pairA.$DL_LNAME"),
      col(s"$pairB.$DL_LNAME"),
      col(s"$pairA.$DL_GENDER"),
      col(s"$pairB.$DL_GENDER")
    )
    dataset.select(col("*"), matchCol.as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): ERFeatureMatcher = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add($(outputCol), DataTypes.BooleanType)
  }
}
