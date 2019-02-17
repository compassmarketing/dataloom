package com.dataloom.er

import org.apache.spark.sql.types.StructType

object validation {

  def validateInputSchema(schema: StructType): Boolean = {
    getRequiredCols(schema).length == REQUIRED_ATTRIBUTES.length
  }

  def getRequiredCols(schema: StructType): Array[String] = {
    schema.fieldNames.filter { f =>
      (REQUIRED_ATTRIBUTES :+ DL_ID).contains(f)
    }
  }

  def getFeatureCols(schema: StructType): Array[String] = {
    schema.fieldNames.filter { f =>
      FEATURE_ATTRIBUTES.exists(_.r.findFirstIn(f).nonEmpty)
    }
  }

  def getUniqueCols(schema: StructType): Array[String] = {
    schema.fieldNames.filter { f =>
      UNIQUE_ATTRIBUTES.exists(_.r.findFirstIn(f).nonEmpty)
    }
  }

}
