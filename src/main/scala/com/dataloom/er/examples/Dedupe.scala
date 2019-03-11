package com.dataloom.er.examples

import com.dataloom.er.{ConnectedComponents, ERFeatureHasher, ERMinHashLSH, ERUniqueHasher}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, CharType, IntegerType, LongType}
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
  final val DL_PERSONID = "DL_PersonId"
  final val DL_HOUSEHOLDID = "DL_HouseholdId"
  final val DL_FNAME = "DL_FirstName"
  final val DL_LNAME = "DL_LastName"
  final val DL_GENDER = "DL_Gender"

  final val REQUIRED_ATTRIBUTES = Array(
    DL_FNAME,
    DL_LNAME,
    DL_GENDER
  )

  final val FEATURE_ATTRIBUTES = Array(
    s"^$DL_LNAME$$",
    "^DL_Street1$",
    "^DL_Street2$",
    "^DL_PostalCode$",
    "^DL_Title$",
    "^DL_Firm$"
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
                         surnameChange: Boolean = false)

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
      opt[String]('s', "surname")
        .valueName("surnameChange")
        .validate {
          case "yes" => success
          case "no" => success
          case _ => failure("Value <diif_surname> must be 'yes' or 'no'")
        }
        .action((x, c) => c.copy(surnameChange = x == "yes"))
        .text("allow surname change, (yes|no)")
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
    * Determine the match level between two candidate pairs.
    *
    * 3 = Individual Match - This estimates that the records are the same individual.
    * 2 = Family Match - This estimates that the records belong to the same family.
    * 1 = Location Match - This estimates that the records exist at the same location.
    * 0 = No Match - This indicates the records linked by a unique column, but were
    * not considered a match.
    *
    * @param a - First candidate pair.
    * @param b - Second candidate pair.
    */
  def matchLevel(a: Row, b: Row): Int = {
    val aFN = SchemaUtils.getAsOrElse[String](a, DL_FNAME, "").toUpperCase
    val bFN = SchemaUtils.getAsOrElse[String](b, DL_FNAME, "").toUpperCase
    val aLN = SchemaUtils.getAsOrElse[String](a, DL_LNAME, "").toUpperCase
    val bLN = SchemaUtils.getAsOrElse[String](b, DL_LNAME, "").toUpperCase
    val aG = SchemaUtils.getAsOrElse[String](a, DL_GENDER, "").toUpperCase
    val bG = SchemaUtils.getAsOrElse[String](b, DL_GENDER, "").toUpperCase

    val aFeatures = a.getAs[Seq[Int]]("features")
    val bFeatures = b.getAs[Seq[Int]]("features")

    val aUniques = a.getAs[Seq[Double]]("uniques")
    val bUniques = b.getAs[Seq[Double]]("uniques")

    val gnScore = givenNameScore(aFN, bFN)
    val revGnScore = givenNameScore(aFN, bLN)
    val famScore = familyNameScore(aLN, bLN)
    val revFamScore = familyNameScore(aLN, bFN)
    val dblmetaScore = doubleMetaBinary(aFN, bFN)
    val genderClass = genderClassification(aG, bG)
    val featureScore = jaccardDistance(aFeatures, bFeatures)
    val uniqueScore = aUniques.intersect(bUniques).length

    val householdMatch = featureScore <= 0.30 || uniqueScore > 0

    if (householdMatch && revGnScore >= 0.95 && revFamScore >= 0.95 && genderClass != 2) { // Flipped Names
      3
    }
    else if (householdMatch && gnScore >= 0.85 && famScore >= 0.90 && genderClass != 2) { // Name Match
      3
    }
    else if (householdMatch && dblmetaScore >= 1.0 && famScore >= 0.90 && genderClass == 1) { // Nickname Match
      3
    }
    else if (householdMatch && famScore >= 0.90) { // Family Match
      2
    } // Family Match
    else if (householdMatch) {
      1
    } // Location Match
    else {
      0
    } // Unique Match e.g. Phone, Email, IP, etc...
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

    SchemaUtils.validateInputSchema(sourceDF.schema, REQUIRED_ATTRIBUTES)

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
    val dataloomDF = profiles.select(SchemaUtils.getCols(profiles.schema, "^DL_*").map(col): _*)

    val hashingTF = new ERFeatureHasher()
      .setInputCols(SchemaUtils.getCols(dataloomDF.schema, FEATURE_ATTRIBUTES): _*)
      .setOutputCol("features")
    val featurized = hashingTF.transform(dataloomDF)

    val uniqueTF = new ERUniqueHasher()
      .setInputCols(SchemaUtils.getCols(dataloomDF.schema, UNIQUE_ATTRIBUTES): _*)
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
    val reqCols = REQUIRED_ATTRIBUTES.union(Array(DL_ID, "features", "uniques")).map(col)
    val records = spark.read.parquet(stage1Path).select(reqCols: _*)
    records.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val mh = new ERMinHashLSH()
      .setNumHashFunctions(6) // r
      .setNumHashTables(6) // b
      .setInputCol("features")
      .setOutputCol("hashes")
      .setUniqueCol("uniques")

    val model = mh.fit(records)

    val (pairA, pairB) = ("A", "B")
    val pairs = model
      .candidatePairs(records, DL_ID, (pairA, pairB))
      .join(records, col(pairA) === records(DL_ID)).select(struct(records("*")).as(pairA), col(pairB))
      .join(records, col(pairB) === records(DL_ID)).select(col(pairA), struct(records("*")).as(pairB))

    /**
      * Generate the match level for each pair.
      */
    val matcher = udf((a: Row, b: Row) => matchLevel(a, b), IntegerType)
    val matches = pairs.select($"$pairA.$DL_ID".as(pairA), $"$pairB.$DL_ID".as(pairB), matcher(col(pairA), col(pairB)).as("level"))
    matches.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /**
      * Gather cliques for the Person Match Level.
      */
    val lvl1Edges = matches.filter($"level" >= 3).rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1)))
    val lvl1Cliques = ConnectedComponents.groupEdges(lvl1Edges, 5).toDF(DL_ID, DL_PERSONID)

    /**
      * Gather cliques for the Household Match Level.
      */
    val lvl2Edges = matches.filter($"level" >= 2).rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1)))
    val lvl2Cliques = ConnectedComponents.groupEdges(lvl2Edges, 5).toDF(DL_ID, DL_HOUSEHOLDID)

    /**
      * Left join back to records
      */
    val cc1 = records
      .join(lvl1Cliques, records(DL_ID) === lvl1Cliques(DL_ID), "left")
      .select(records(DL_ID), when(isnull(lvl1Cliques(DL_PERSONID)), records(DL_ID)).otherwise(lvl1Cliques(DL_PERSONID)).as(DL_PERSONID))

    val cc2 = cc1
      .join(lvl2Cliques, cc1(DL_ID) === lvl2Cliques(DL_ID), "left")
      .select(cc1("*"), when(isnull(lvl2Cliques(DL_HOUSEHOLDID)), cc1(DL_ID)).otherwise(lvl2Cliques(DL_HOUSEHOLDID)).as(DL_HOUSEHOLDID))

    cc2.write.mode("overwrite").parquet(stage2Path)
    records.unpersist()
  }

  /**
    * Stage 3 - This stage joins the results back to the original data set and writes it back out.
    *
    * @param spark
    * @param options
    */
  def stage3(spark: SparkSession, numOfMB:Double, options: ArgsConfig): Unit = {
    val stage1Path = new Path(options.outputLoc, "stage1").toString
    val stage2Path = new Path(options.outputLoc, "stage2").toString
    val stage3Path = new Path(options.outputLoc, "stage3").toString

    val original = spark.read.parquet(stage1Path).drop("features", "uniques")
    val result = spark.read.parquet(stage2Path)

    val readableParts = Math.ceil(1 + numOfMB / 1024).toInt

    val full = original
      .join(result, original(DL_ID) === result(DL_ID))
      .select(original("*"), result(DL_PERSONID), result(DL_HOUSEHOLDID))

    full
      .coalesce(readableParts)
      .write
      .mode("overwrite")
      .avro(stage3Path)
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
    var parts = Math.ceil(1 + numOfMB / 64).toInt

    if (options.inputLoc.endsWith(".gz")) {
      parts = parts * 2
    } // Double the amount of partitions if gzip
    spark.conf.set("spark.sql.shuffle.partitions", parts.toString)

    stage1(spark, parts, options)
    stage2(spark, options)
    stage3(spark, numOfMB, options)

//    val pairs = spark.read.parquet(options.outputLoc.concat("/stage2"))
//    println(getMatchSize(pairs, DL_PERSONID).count())
    //System.in.read

  }

}
