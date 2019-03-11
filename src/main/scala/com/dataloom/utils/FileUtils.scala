package com.dataloom.utils

import java.net.URI
import org.apache.spark.SparkContext

object FileUtils {


  def getSparkFileSize(path: String, context: SparkContext): Long = {
    val uri = new URI(path)
    val fs = org.apache.hadoop.fs.FileSystem.get(uri, context.hadoopConfiguration)
    val file = new org.apache.hadoop.fs.Path(uri)
    if (fs.isDirectory(file)) {
      val files = fs.listFiles(file, false)
      var size = 0L
      while(files.hasNext) {
        size += files.next().getLen
      }
      size
    }
    else {
      fs.getFileStatus(file).getLen
    }
  }

}
