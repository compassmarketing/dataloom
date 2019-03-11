package com.dataloom.er.examples

import com.dataloom.er.{ERFeatureHasher, ERMinHashLSH, ERUniqueHasher}

import com.dataloom.utils.{FileUtils, SchemaUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IdentityCacheBuild {

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

  case class ArgsConfig(
                         inputLoc: String = "",
                         outputLoc: String = "",
                         delimiter: String = "|",
                         surnameChange: Boolean = false)

  def parseArgs(args: Array[String]): ArgsConfig = {
    val parser = new scopt.OptionParser[ArgsConfig]("Dedupe") {
      head("Dedupe", "1.0")

      arg[String]("<input>")
        .required()
        .valueName("inputLoc")
        .action((x, c) => c.copy(inputLoc = x))
        .text("input path to file")
      arg[String]("<output>")
        .required()
        .valueName("outputLoc")
        .action((x, c) => c.copy(outputLoc = x))
        .text("output path of results")
      opt[String]('d', "delimiter")
        .valueName("delimiter")
        .validate {
          case "PIPE" => success
          case "COMMA" => success
          case "TAB" => success
          case _ => failure("Value <delimiter> is invalid")
        }
        .action { (x, c) =>
          x match {
            case "PIPE" => c.copy(delimiter = "|")
            case "COMMA" => c.copy(delimiter = ",")
            case "TAB" => c.copy(delimiter = "\t")
          }
        }
        .text("csv column delimiter")
      opt[String]('s', "surname")
        .valueName("surnameChange")
        .validate {
          case "yes" => success
          case "no" => success
          case _ => failure("Value <diif_surname> must be 'yes' or 'no'")
        }
        .action( (x, c) => c.copy(surnameChange = x == "yes") )
        .text("allow surname change, (yes|no)")
    }

    parser.parse(args, ArgsConfig()) match {
      case Some(config) => config
      case None => throw new Error("Invalid arguments")
    }
  }

  def main(args: Array[String]) {
    // Context
    val DEV = sys.env.keySet.contains("DEV")

    val options = parseArgs(args)

    val inputLoc = options.inputLoc
    val outputLoc = options.outputLoc
    val sep = options.delimiter

    val session = SparkSession.builder()
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = session.config(conf).getOrCreate()
    import spark.implicits._

    val numOfMB = FileUtils.getSparkFileSize(inputLoc, spark.sparkContext) / 1048576.0
    val parts = Math.ceil(1 + numOfMB / 64).toInt
    spark.conf.set("spark.sql.shuffle.partitions", parts.toString)

    val lines = spark.sqlContext.sparkContext.textFile(inputLoc, parts)
    val sourceDF = spark.read
      .option("header", "true")
      .option("sep", sep)
      .option("MODE", "DROPMALFORMED")
      .csv(lines.toDS()).limit(10000)

    val profiles = sourceDF.withColumn("DL_ID", monotonically_increasing_id.cast(LongType))

    /**
      * Transform only the Dataloom columns into hashes for LSH. Then return the
      * candidate pairs from ERMinHashLSH that we will perform pairwise comparisons
      * for.
      *
      * By hashing unique "joinable" columns separately, we can preserve the relationship
      * through LSH and be sure that these attributes will create a candidate pair. We hash
      * the values to reduce the memory requirements, but also to conform to the feature hash
      * data type.
      */
    val dataloomDF = profiles.select(SchemaUtils.getCols(profiles.schema, "^DL_*").map(col): _*)

    val hashingTF = new ERFeatureHasher()
      .setInputCols(SchemaUtils.getCols(dataloomDF.schema, FEATURES): _*)
      .setOutputCol("features")
    val featurized = hashingTF.transform(dataloomDF)

    val uniqueTF = new ERUniqueHasher()
      .setInputCols(SchemaUtils.getCols(dataloomDF.schema, UNIQUES): _*)
      .setOutputCol("uniques")
    val unique = uniqueTF.transform(featurized)

    val mh = new ERMinHashLSH()
      .setNumHashFunctions(8) // r
      .setNumHashTables(8) // b
      .setInputCol("features")
      .setOutputCol("hashes")
      .setUniqueCol("uniques")

    val model = mh.fit(unique)

    val lshDS = model
      .transform(unique)
      .select($"DL_ID", $"DL_FirstName", posexplode(col(mh.getOutputCol)).as(Seq("entry", "hashValue")))
      .withColumn("entry", when(col("entry") >= mh.getNumHashTables, mh.getNumHashTables).otherwise(col("entry")))


    lshDS.show(false)

    lshDS.write.mode("overwrite").parquet(outputLoc)
    model.save("./data/model", true)

  }
}
