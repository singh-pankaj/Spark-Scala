package com.figmd.janus.util

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastMaster extends App {


  def getBrodcastMaster(spark: SparkSession): Broadcast[masterList] = {

    import spark.implicits._

    val emptyList: util.List[String] = new util.ArrayList[String]

    val medCodeCrosswalk_2018 = new PostgreUtility().getPostgresTable(spark, "MedCodeCrosswalk_2018")

    val medicationList = medCodeCrosswalk_2018.select("medication", "shortname").groupBy("medication").agg(collect_set($"shortname") as "shortNameList").rdd
      .map(x => if (!x.isNullAt(0)) (x.getString(0), x.getList[String](1)) else ("", emptyList)).distinct().collect().toMap

    val codeList = medCodeCrosswalk_2018.select("code", "shortname").groupBy("code").agg(collect_set($"shortname") as "shortNameList").rdd
      .map(x => if (!x.isNullAt(0)) (x.getString(0), x.getList[String](1)) else ("", emptyList)).distinct().collect().toMap

    val code1List = medCodeCrosswalk_2018.select("code1", "shortname").groupBy("code1").agg(collect_set($"shortname") as "shortNameList").rdd
      .map(x => if (!x.isNullAt(0)) (x.getDouble(0), x.getList[String](1)) else (0.0, emptyList)).distinct().collect().toMap


    code1List.foreach(x => println("*****Double******" + x._1 + "\n*****List******" + x._2))

    var lst = masterList(medicationList, codeList, code1List)


    return spark.sparkContext.broadcast(lst)

  }


}