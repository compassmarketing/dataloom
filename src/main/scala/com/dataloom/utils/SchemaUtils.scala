package com.dataloom.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object SchemaUtils {

  def validateInputSchema(schema: StructType, colNames:Array[String]): Boolean = {
    getCols(schema, colNames).length == colNames.length
  }

  def getCols(schema: StructType, regexes:Array[String]): Array[String] = {
    schema.fieldNames.filter { f =>
      regexes.exists(_.r.findFirstIn(f).nonEmpty)
    }
  }

  def getCols(schema: StructType, regex:String): Array[String] = {
    schema.fieldNames.filter { f =>
      regex.r.findFirstIn(f).nonEmpty
    }
  }

  def getAsOrElse[T](row:Row, name:String, default:T):T = {
    val x = row.getAs[T](name)
    if (x == null) default else x
  }

}
