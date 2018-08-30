package com.figmd.janus.master

import com.figmd.janus.util.PostgreUtility
import org.apache.spark.sql.{DataFrame, SparkSession}

class MasterCode {

  def getMasterCode(spark: SparkSession, tableName: String): DataFrame = {
    return new PostgreUtility().getPostgresTable(spark, tableName)
  }

  def broadcast(spark: SparkSession): Unit = {

  }

}
