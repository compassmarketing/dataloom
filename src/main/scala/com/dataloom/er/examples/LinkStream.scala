package com.dataloom.er.examples

import com.dataloom.er.{ERFeatureHasher, ERModel, ERUniqueHasher}
import com.dataloom.utils.SchemaUtils
import com.dataloom.er.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LinkStream {

  case class Group(qID:Int, eId:Long)

  final val REQUIRED = Array(
    "DL_FirstName"
//    "DL_LastName",
//    "DL_Gender"
  )

  final val FEATURES = Array(
    "^DL_FirstName$",
    "^DL_LastName$",
    "^DL_Gender$",
    "^DL_Street1$",
    "^DL_Street2$",
    "^DL_PostalCode$",
    "^DL_Title$",
    "^DL_Firm$"
  )

  final val UNIQUES = Array(
    "^DL_Email\\d+$",
    "^DL_Phone\\d+$",
    "^DL_MD5Email\\d+$",
    "^DL_SHA1Email\\d+$",
    "^DL_SHA2Email\\d+$",
    "^DL_IP\\d+$"
  )

  def isMatch(a:Row, b:Row, uniqueFlag:Boolean):Boolean = {
    val aFN = SchemaUtils.getAsOrElse[String](a, "DL_FirstName", "").toUpperCase
    val bFN = SchemaUtils.getAsOrElse[String](b, "DL_FirstName", "").toUpperCase
    givenNameScore(aFN, bFN) >= 0.85
  }

  def main(args: Array[String]) {
    // Context
    val DEV = sys.env.keySet.contains("DEV")

    val session = SparkSession.builder()
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions", "2")

    val spark = session.config(conf).getOrCreate()
    import spark.implicits._

    val cacheDF = spark
      .read.parquet("./data/cache")

    //spark.sparkContext.broadcast(cacheDF)

    val linkSchema = new StructType()
      .add("id", LongType)
      .add("DL_FirstName", StringType)
      .add("DL_LastName", StringType)
      .add("DL_Street1", StringType)
      .add("DL_Street2", StringType)
      .add("DL_Locality", StringType)
      .add("DL_Region", StringType)
      .add("DL_PostalCode", StringType)
      .add("DL_Phone1", StringType)
      .add("DL_Gender", StringType)
      .add("DL_Email1", StringType)
      .add("DL_Email2", StringType)
      .add("DL_Email3", StringType)

    val streamDS = spark
      .readStream
      .option("header", "true")
      .option("sep", "|")
      .schema(linkSchema)
      .csv("./data/stream_in")

    val hashingTF = new ERFeatureHasher()
      .setInputCols(SchemaUtils.getCols(streamDS.schema, FEATURES): _*)
      .setOutputCol("features")

    val featurizedDS = hashingTF.transform(streamDS)

    val uniqueTF = new ERUniqueHasher()
      .setInputCols(SchemaUtils.getCols(streamDS.schema, UNIQUES): _*)
      .setOutputCol("uniques")
    val uniqueDS = uniqueTF.transform(featurizedDS)

    val model = ERModel.load("./data/model")

    val lshDS = model
      .transform(uniqueDS)
      .select(col("*"), hash(streamDS("*")).as("QID"), posexplode(col(model.getOutputCol)).as(Seq("entry", "hashValue")))
      .withColumn("entry", when(col("entry") >= model.getNumHashTables, model.getNumHashTables).otherwise(col("entry")))

    // TODO: Join to an ignite Dataframe where the key (entry, hashValue) are equal.
    val reqCols = SchemaUtils.getCols(streamDS.schema, REQUIRED)
    val joined = lshDS
      .join(cacheDF, lshDS("entry") === cacheDF("entry") && lshDS("hashValue") === cacheDF("hashValue"), "left")
      .select(
        lshDS("entry").equalTo(model.getNumHashTables).as("unique"),
        struct(reqCols.map(lshDS(_)) :+ lshDS("QID"):_*).as("Q"),
        struct(reqCols.map(cacheDF(_)) :+ cacheDF("DL_ID"):_*).as("E"))

    // TODO: ReduceByKey to limit the number of comparisons (ID, IdvId)
    val reduced = joined
      .groupByKey { r =>
        val qId = r.getAs[Row]("Q").getAs[Int]("QID")
        val eID = r.getAs[Row]("E").getAs[Long]("DL_ID")

        Group(qId, eID)
      }
      .flatMapGroups {(_, ri) =>
        Iterator(ri.reduce((r1, r2) => if (r1.getAs[Boolean]("unique")) r1 else r2))
      }(RowEncoder.apply(joined.schema))

    /* Matched Pairs */
    // TODO: Match pairs
    val matcher = udf((a:Row, b:Row, unique:Boolean) => isMatch(a, b, unique), BooleanType)
    val matches = reduced.withColumn("match", matcher(col("Q"), col("E"), col("unique")))

    /* Matched Pairs */
    // TODO: Filter by matched pairs
    // TODO: Upsert Enrichment Cache with serialized data. (Specific attr update is governed by source owner. Non-Blank, Newest, or Never)

    /* Non-matched Pairs */
    // TODO: Filter by non-matched pairs



    val query = matches.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
