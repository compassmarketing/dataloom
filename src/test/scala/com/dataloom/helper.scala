package com.dataloom

import org.apache.spark.sql.SparkSession

object helper {

  trait SparkSessionTestWrapper {

    lazy val spark: SparkSession = {
      SparkSession.builder().master("local[4]")
        .config("spark.ui.enabled", true)
        //        .config("spark.default.parallelism", 1)
        //        .config("spark.sql.shuffle.partitions", 1)
        //        .config("spark.executor.memory", "1g")
        //        .config("spark.executor.memoryOverhead", "4g")
        //        .config("spark.driver.memory", "2g")
        //        .config("spark.memory.fraction", 0.8)
        .appName("spark session").getOrCreate()
    }

  }
}