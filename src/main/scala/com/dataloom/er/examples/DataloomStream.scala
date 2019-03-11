//package com.dataloom.er.examples
//
//import com.dataloom.er.{ERFeatureHasher, ERModel, ERUniqueHasher}
//import com.dataloom.utils.SchemaUtils
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.internal.Logging
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.{LongType, StringType, StructType}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.ml.linalg.{SQLDataTypes, Vector, Vectors}
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//
//object DataloomStream extends Logging {
//
//  final val REQUIRED = Array(
//    "DL_FirstName"
//    //    "DL_LastName",
//    //    "DL_Gender"
//  )
//
//  final val FEATURES = Array(
//    "^DL_LastName$",
//    "^DL_Gender$",
//    "^DL_Street1$",
//    "^DL_Street2$",
//    "^DL_PostalCode$",
//    "^DL_Title$",
//    "^DL_Firm$"
//  )
//
//  final val UNIQUES = Array(
//    "^DL_Email\\d+$",
//    "^DL_Phone\\d+$",
//    "^DL_MD5Email\\d+$",
//    "^DL_SHA1Email\\d+$",
//    "^DL_SHA2Email\\d+$",
//    "^DL_IP\\d+$"
//  )
//
//  // Internal models
//  case class Query(fname:String, lname:String, features:Set[Int], hashes:Seq[Vector])
//  case class Result(person:Person, unique:Boolean)
//  case class MatchInfo(pid:Long, unique:Boolean, hashes:Seq[Vector], level:Int)
//
//  // Cassandra Models
//  case class HLookup(hash_id: Int, entry:Int, id:Long)
//  case class PLookup(pid:Long, sid:Long, unique:Boolean)
//
//  case class Hash(hash_id: Int, entry:Int, pid:Long)
//  case class Person(pid:Long, fname:String, lname:String, features:Set[Set[Int]])
//
//  def setStreamingLogLevels() {
//    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
//    if (!log4jInitialized) {
//      // We first log something to initialize Spark's default logging, then we override the
//      // logging level.
//      logInfo("Setting log level to [WARN] for streaming example." +
//        " To override add a custom log4j.properties to the classpath.")
//      Logger.getRootLogger.setLevel(Level.WARN)
//    }
//  }
//
//  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: HdfsWordCount <directory>")
//      System.exit(1)
//    }
//
//    val sparkConf = new SparkConf()
//      .setAppName("HdfsWordCount")
//      .set("spark.cassandra.connection.host", "0.0.0.0")
//      .set("spark.cassandra.input.join.throughput_query_per_sec", "500")
//    // Create the context
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//    ssc.sparkContext.setLogLevel("WARN")
//
//    val linkSchema = new StructType()
//      .add("id", LongType)
//      .add("DL_FirstName", StringType)
//      .add("DL_LastName", StringType)
//      .add("DL_Street1", StringType)
//      .add("DL_Street2", StringType)
//      .add("DL_Locality", StringType)
//      .add("DL_Region", StringType)
//      .add("DL_PostalCode", StringType)
//      .add("DL_Phone1", StringType)
//      .add("DL_Gender", StringType)
//      .add("DL_Email1", StringType)
//      .add("DL_Email2", StringType)
//      .add("DL_Email3", StringType)
//
//    val hashingTF = new ERFeatureHasher()
//      .setInputCols(SchemaUtils.getCols(linkSchema, FEATURES): _*)
//      .setOutputCol("features")
//    val uniqueTF = new ERUniqueHasher()
//      .setInputCols(SchemaUtils.getCols(linkSchema, UNIQUES): _*)
//      .setOutputCol("uniques")
//    val lshTF = ERModel.load("./data/model")
//
//    val numHashTables = lshTF.getNumHashTables
//
//    // Create the FileInputDStream on the directory and use the
//    // stream to count words in new files created
//    val lines = ssc.textFileStream(args(0))
//
//    // Transform into features
//    // TODO: Really bad and needs to be moved into pure RDDs
//    // TODO: parse JSON blob
//    // TODO: Pull out location attrs from JSON
//    // TODO: Pull out unique attrs from JSON
//    // TODO: Featurize location and unique attrs
//    // TODO: MinHash vector of attrs
//    val recs = lines.transform { rdd =>
//      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
//      import spark.implicits._
//      val rows = spark.read
//        .option("header", "true")
//        .option("sep", "|")
//        .option("MODE", "DROPMALFORMED")
//        .schema(linkSchema)
//        .csv(rdd.toDS())
//
//      lshTF.transform(uniqueTF.transform(hashingTF.transform(rows))).rdd
//    }
//
//    // TODO: mapWithState to build unique record ids!!
//    val qCache = recs.map { row =>
//      (row.getAs[Long]("id"), row)
//    }
//    qCache.cache()
//
//    // Flat map out the features to be joined to Cassandra.
//    val hashes = qCache.flatMap { case (sid:Long, row:Row) =>
//      val hashes = row.getAs[Seq[Vector]]("hashes")
//      hashes.zipWithIndex.map { case (hash: Vector, i: Int) =>
//        val entry = if (i >= numHashTables) numHashTables else i
//        HLookup(hash.hashCode(), entry, sid)
//      }
//    }
//
//    // Repartition to Cassandra HashTable partitions (e.g. hash_id PRIMARY KEY)
//    // TODO: !!! NOT FAST ENOUGH !!! - Will probably need to use mapWithState and store it in memory. Will have to load init hashes from storage
//    val joined = hashes
//      .repartitionByCassandraReplica("test", "kv", 10)
//      .joinWithCassandraTable[Hash]("test","kv")
//      .map{ case (lookup:HLookup, hash:Hash) =>
//        ((lookup.id, hash.pid), lookup.entry >= numHashTables)
//      }
//
//    // Reduce duplicate candidate pairs into single MatchInfo result.
//    val candidatePairs = joined.reduceByKey(_ || _).map(cp => PLookup(cp._1._2, cp._1._1, cp._2))
//
//    // Repartition and join to the Persons table in Cassandra
//    val people = candidatePairs
//      .repartitionByCassandraReplica("test", "people", 10)
//      .joinWithCassandraTable[Person]("test","people")
//      .map{ case (lookup:PLookup, person:Person) =>
//        (lookup.sid, Result(person, lookup.unique))
//      }
//
//    // Join back to cache and set match lvl
//    val matches = qCache.leftOuterJoin(people).map { case (_, pair:(Row, Option[Result])) =>
//      val (row, result) = pair
//
//      if (result.nonEmpty) {
//        val isUnique = result.get.unique
//        val lvl = 2 // TODO: DO MATCH HERE!! getMatchLvl(row, result.get.person, isUnique)
//        (row, MatchInfo(result.get.person.pid, isUnique, Seq(), lvl))
//      } else {
//        val hashes = row.getAs[Seq[Vector]]("hashes")
//        (row, MatchInfo(0, unique=false, hashes, 0)) // No Match.
//      }
//    }
//
//    // Reduce matches into single match info set. We do this to handle convergence across people.
//    val results = matches.reduceByKey((a,_) => a).map(r => (r._1.getAs[Long]("id"), r._2.pid))
//
//    // TODO: Foreach RDD, update cassandra and write out avro for bigquery load.
//
//    results.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//  /** Lazily instantiated singleton instance of SparkSession */
//  object SparkSessionSingleton {
//
//    @transient  private var instance: SparkSession = _
//
//    def getInstance(sparkConf: SparkConf): SparkSession = {
//      if (instance == null) {
//        instance = SparkSession
//          .builder
//          .config(sparkConf)
//          .getOrCreate()
//      }
//      instance
//    }
//  }
//}
