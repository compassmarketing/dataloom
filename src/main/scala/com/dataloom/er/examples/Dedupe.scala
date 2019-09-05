package com.dataloom.er.examples

import com.dataloom.er.{ConnectedComponents, ERFeatureHasher, ERMinHashLSH, ERUniqueHasher}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import com.dataloom.er.functions._
import com.dataloom.utils.{FileUtils, SchemaUtils}
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.Path
import com.databricks.spark.avro._

/**
  * Dataloom - Dedupe
  *
  * This spark program uses state of the art entity resolution techniques to group
  * similar individual identities together. It works by using Local Sensitivity Hashing
  * to bucket similar individuals together that could be considered "candidate pairs" and then performing
  * name-wise comparisons using a few custom name similarity functions.
  *
  * Matching individuals are then grouped together to form "cliques". These cliques are individuals that
  * Dataloom deems the same individual, by using the transitive nature of pair matches. i.e. A = B, B = C then A = C.
  *
  * Dedupe does not merge data into one best profile, that task is left the the developer after running Dedupe. In practice,
  * different methods of merging are used for different use cases.
  */
object Dedupe {

  final val VERSION = "0.0.1"

  final val DL_ID = "DL_Id"
  final val DL_BREAKGROUP = "DL_BreakGroup"

  final val FEATURE_ATTRIBUTES = Array(
    "^DL_FirstName$",
    "^DL_LastName$",
    "^DL_Gender$",
    "^DL_Street1$",
    "^DL_Street2$",
    "^DL_PostalCode$",
    "^DL_Title$",
    "^DL_Firm$",
    "^DL_DOB_Year$"
  )

  final val UNIQUE_ATTRIBUTES = Array(
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
                         outParts: Int = 10)

  def parseArgs(args: Array[String]): ArgsConfig = {
    val parser = new scopt.OptionParser[ArgsConfig]("Dedupe") {
      head("Dedupe", VERSION)

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
      opt[Int]('p', "parts")
        .valueName("outParts")
        .action((x, c) => c.copy(outParts = x))
        .text("number of output partitions")
    }

    parser.parse(args, ArgsConfig()) match {
      case Some(config) => config
      case None => throw new Error("Invalid arguments")
    }
  }

  /**
    * Show the resulting duplicate pairs for testing.
    *
    * @param ds - Dataset that has been ran through Dedupe.
    */
  def getMatchSize(ds: Dataset[_], lvlname: String): Dataset[_] = {
    val byClique = Window.partitionBy(col(lvlname))
    ds.withColumn("cliqueSize", count(col(DL_ID)).over(byClique)).where(col("cliqueSize") > 1)
  }


  /**
    * Stage 1 - This stage reads the input location and does the following:
    *
    * 1. Repartition the input data.
    * 2. Assigns unique id's across all records
    * 3. Generates feature hashes for location attributes.
    * 4. Generates unique hashes for all unique attributes.
    * 5. Serializes the data to disk as parquet to avoid storing in memory, but keeping it local for faster access.
    *
    * @param spark        - The spark session
    * @param numOfParts   - The number of partitions to repartition to.
    * @param options      - The arguments map
    */
  def stage1(spark: SparkSession, numOfParts: Int, options: ArgsConfig): Unit = {
    val inputLoc = options.inputLoc
    val sep = options.delimiter

    val stage1Path = new Path(options.outputLoc, "stage1").toString

    val sourceDF = spark.read
      .option("header", "true")
      .option("sep", sep)
      .option("MODE", "DROPMALFORMED")
      .csv(inputLoc)

    if (SchemaUtils.getCols(sourceDF.schema, "^DL_*").length == 0) {
      throw new Exception("No Dataloom columns found")
    }

    // Generate positive unique ID's across the input data set.
    val profiles = sourceDF
      .repartition(numOfParts)
      .withColumn(DL_ID, monotonically_increasing_id.cast(LongType))

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
    val hashingTF = new ERFeatureHasher()
      .setInputCols(SchemaUtils.getCols(profiles.schema, FEATURE_ATTRIBUTES): _*)
      .setOutputCol("features")
    val featurized = hashingTF.transform(profiles)

    val uniqueTF = new ERUniqueHasher()
      .setInputCols(SchemaUtils.getCols(profiles.schema, UNIQUE_ATTRIBUTES): _*)
      .setOutputCol("uniques")
    val unique = uniqueTF.transform(featurized)

    unique.write.mode("overwrite").parquet(stage1Path)

  }

  /**
    * Stage 2 - This stage preforms the candidate selection by utilizing LSH to bucket similar records. It then
    * matches each candidate pair and builds the personID and householdId from the connected components
    *
    * @param spark
    * @param options
    */
  def stage2(spark: SparkSession, options: ArgsConfig): Unit = {
    import spark.implicits._

    val stage1Path = new Path(options.outputLoc, "stage1").toString
    val stage2Path = new Path(options.outputLoc, "stage2").toString

    // Get need cols for matching
    val records = spark.read.parquet(stage1Path).select(DL_ID, "features", "uniques")
    records.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val mh = new ERMinHashLSH()
      .setNumHashFunctions(6) // r
      .setNumHashTables(6) // b
      .setInputCol("features")
      .setUniqueCol("uniques")
      .setOutputCol("hashes")

    val model = mh.fit(records)

    val req = records.select(DL_ID, "features")
    val (pairA, pairB) = ("A", "B")
    val pairs = model
      .candidatePairs(records, DL_ID, (pairA, pairB))
      .join(req, col(pairA) === req(DL_ID)).select(struct(req("*")).as(pairA), col(pairB), col("unique"))
      .join(req, col(pairB) === req(DL_ID)).select(col(pairA), struct(req("*")).as(pairB),  col("unique"))

    /**
      * Generate the match level for each pair.
      */
    val jaccardDis = udf((a: Seq[Int], b: Seq[Int]) => jaccardDistance(a,b), DoubleType)
    val matches = pairs
      .filter($"unique".equalTo(true).or(jaccardDis($"$pairA.features", $"$pairB.features") <= 0.3))
      .select($"$pairA.$DL_ID".as(pairA), $"$pairB.$DL_ID".as(pairB))
      .map(r => (r.getAs[Long](0), r.getAs[Long](1)))

    val cliques = ConnectedComponents.groupEdges(matches.rdd, 5).toDF(DL_ID, DL_BREAKGROUP)

    cliques.write.mode("overwrite").parquet(stage2Path)
    records.unpersist()
  }

  /**
    * Stage 3 - This stage joins the results back to the original data set and writes it back out.
    *
    * @param spark
    * @param options
    */
  def stage3(spark: SparkSession, options: ArgsConfig): Unit = {
    val stage1Path = new Path(options.outputLoc, "stage1").toString
    val stage2Path = new Path(options.outputLoc, "stage2").toString
    val stage3Path = new Path(options.outputLoc, "stage3").toString

    val original = spark.read.parquet(stage1Path).drop("features", "uniques")
    val result = spark.read.parquet(stage2Path)

    val full = original
      .join(result, original(DL_ID) === result(DL_ID))
      .select(original("*"), result(DL_BREAKGROUP))

    full
      .sortWithinPartitions(DL_BREAKGROUP)
      .coalesce(options.outParts)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("compression", "gzip")
      .csv(stage3Path)
  }

  def main(args: Array[String]) {
    // Context
    val options = parseArgs(args)

    val session = SparkSession.builder()
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = session.config(conf).getOrCreate()

    // Estimate the file size to determine number of partitions.
    val numOfMB = FileUtils.getSparkFileSize(options.inputLoc, spark.sparkContext) / 1048576.0
    val parts = Math.ceil(1 + numOfMB / 64).toInt
    spark.conf.set("spark.sql.shuffle.partitions", parts.toString)

    stage1(spark, parts, options)
    stage2(spark, options)
    stage3(spark, options)

//    val pairs = spark.read.option("header","true").csv(options.outputLoc.concat("/stage3"))
//    getMatchSize(pairs, DL_BREAKGROUP).show(100, false)
    //System.in.read

  }

}
