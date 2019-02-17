package com.dataloom.er.examples

import com.dataloom.er.{ConnectedComponents, ERFeatureHasher, ERFeatureMatcher, ERMinHashLSH, ERUniqueHasher}
import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel
import com.dataloom.er.{DL_CLIQUE_ID, DL_ID}
import com.dataloom.er.validation._
import org.apache.spark.sql.expressions.Window

object Dedupe {

  def main(args: Array[String]) {

    if (args.length != 2) {
      throw new Exception("Invalid arguments")
    }

    args.map(println)

    val inputLoc = args(0)
    val outputLoc = args(1)

    val session = SparkSession.builder()
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = session.config(conf).getOrCreate()
    import spark.implicits._

//    val lines = spark.sqlContext.sparkContext.textFile(inputLoc)
    val sourceDF = spark.read
      .option("header", "true")
      .option("sep","|")
      .option("MODE", "DROPMALFORMED")
      .csv(inputLoc)

    val profiles = sourceDF.withColumn(DL_ID, monotonically_increasing_id.cast(LongType))
    profiles.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(profiles.rdd.getNumPartitions)

    // Validate the input schema
    validateInputSchema(profiles.schema)

    val hashingTF = new ERFeatureHasher()
      .setInputCols(getFeatureCols(profiles.schema):_*)
      .setOutputCol("features")
    val featurized = hashingTF.transform(profiles)

    val uniqueTF = new ERUniqueHasher()
      .setInputCols(getUniqueCols(profiles.schema):_*)
      .setOutputCol("uniques")
    val unique = uniqueTF.transform(featurized)

    val requiredCols = (getRequiredCols(profiles.schema) ++ Seq("features", "uniques")).map(col)
    val erReadyDF = unique.select(requiredCols:_*)
    val mh = new ERMinHashLSH()
      .setNumHashFunctions(9) // r
      .setNumHashTables(15) // b
      .setInputCol("features")
      .setOutputCol("hashes")
      .setUniqueCol("uniques")

    val model = mh.fit(erReadyDF)
    val (pairA, pairB) = model.getPairNames
    val pairs = model.candidatePairs(erReadyDF)

    val matcher = new ERFeatureMatcher()
      .setOutputCol("match")


    val matches = matcher.transform(pairs).filter($"match")
    val edges = matches.select($"$pairA.$DL_ID", $"$pairB.$DL_ID").rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1)))

    val cliques = ConnectedComponents.groupEdges(edges, 7).toDF(DL_ID, DL_CLIQUE_ID)

    val output = profiles
      .join(cliques, profiles(DL_ID) === cliques(DL_ID), "left")
      .select(profiles("*"), when(isnull(cliques(DL_CLIQUE_ID)), profiles(DL_ID)).otherwise(cliques(DL_CLIQUE_ID)).as(DL_CLIQUE_ID))

    output.write.mode("overwrite").avro(outputLoc)

    // Show dups
//    val byClique = Window.partitionBy(col(DL_CLIQUE_ID))
//    output.withColumn("cliqueSize", count(col(DL_ID)).over(byClique)).where($"cliqueSize" > 1)
//      .write.mode("overwrite").avro(outputLoc)
    System.in.read() // Pause for analysis
  }

}