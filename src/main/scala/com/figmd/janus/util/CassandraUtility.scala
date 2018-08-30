package com.figmd.janus.util

import java.text.SimpleDateFormat
import java.util.Date


import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.sql.SparkSession
import com.figmd.janus.DataMartCreator.prop


class CassandraUtility extends Serializable {



  @transient lazy val fileUtility = new FileUtility();
  //var keyspace = fileUtility.getProperty("cassandra.keyspace.name");
  //var CASHOSTNAME = fileUtility.getProperty("cassandra.host.name");
  var location = fileUtility.getProperty("cassandra.wdm.table.name.location");
  var practice = fileUtility.getProperty("cassandra.wdm.table.name.practice");
  var provider = fileUtility.getProperty("cassandra.wdm.table.name.provider");
  var webdatamart_keyspace = prop.getProperty("keyspace_webdatamart")

  val SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  var quarterStartDate = SIMPLE_DATE_FORMAT.parse(prop.getProperty("quarterStartDate"))
  var quarterEndDate = SIMPLE_DATE_FORMAT.parse(prop.getProperty("quarterEndDate"))




  def deleteFromWebdatamart(spark:SparkSession,MEASURE_NAME:String): Unit =
  {
    val rdd1 = spark.sparkContext.cassandraTable(s"$webdatamart_keyspace", s"$practice")
    rdd1.filter(r=>chkDateRangeBetween(r,"visit_dt",quarterStartDate,quarterEndDate)).deleteFromCassandra(s"$webdatamart_keyspace",s"$practice")

    new PostgreUtility().insertIntoProcessDetails(MEASURE_NAME,"","","Data deletion From Cassandra table "+practice+" Date range from "+quarterStartDate+" to "+quarterEndDate,"PASS")

    val rdd2 = spark.sparkContext.cassandraTable(s"$webdatamart_keyspace", s"$provider")
    rdd2.filter(r=>chkDateRangeBetween(r,"visit_dt",quarterStartDate,quarterEndDate)).deleteFromCassandra(s"$webdatamart_keyspace",s"$provider")

    new PostgreUtility().insertIntoProcessDetails(MEASURE_NAME,"","","Data deletion From Cassandra table "+provider+" Date range from "+quarterStartDate+" to "+quarterEndDate,"PASS")

    val rdd3 = spark.sparkContext.cassandraTable(s"$webdatamart_keyspace", s"$location")
    rdd3.filter(r=>chkDateRangeBetween(r,"visit_dt",quarterStartDate,quarterEndDate)).deleteFromCassandra(s"$webdatamart_keyspace",s"$location")

    new PostgreUtility().insertIntoProcessDetails(MEASURE_NAME,"","","Data deletion From Cassandra table "+location+" Date range from "+quarterStartDate+" to "+quarterEndDate,"PASS")

  }

  def chkDateRangeBetween(r: CassandraRow,checkDate:String, startDate: Date, endDate: Date): Boolean = {
     val isExist = !r.isNullAt(checkDate) &&
      (r.getDate(checkDate).after(startDate) || r.getDate(checkDate).equals(startDate)) &&
      (r.getDate(checkDate).before(endDate) || r.getDate(checkDate).equals(endDate))
     return isExist;
  }

  def getCassandraRDD(sparkSession: SparkSession): CassandraTableScanRDD[CassandraRow] = {
    var rdd = sparkSession.sparkContext.cassandraTable(prop.getProperty("keyspace_datamart"), prop.getProperty("keyspace_datamart_table_name"))
    return rdd
  }






}
