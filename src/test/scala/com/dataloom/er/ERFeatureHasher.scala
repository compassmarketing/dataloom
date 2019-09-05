package com.dataloom.er

import com.dataloom.helper.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class ERFeatureHasherTests extends FunSpec with SparkSessionTestWrapper with DatasetComparer {

  it("should handle empty feature attrs") {

    import spark.implicits._

    val df = Seq(
      (8, "bat", Seq(30881)),
      (64, null, Seq[Int]()),
      (64, "mouse", Seq(8096, 26684, 7965)),
      (-27, "horse", Seq(27411, 12167, 22869))
    ).toDF("number", "word", "expected")

    val tf = new ERFeatureHasher()
      .setInputCols("word")
      .setOutputCol("expected")
    val featurized = tf.transform(df.drop("expected"))

    assertSmallDataFrameEquality(df, featurized)
  }

}
