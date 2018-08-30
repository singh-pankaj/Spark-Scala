package com.figmd.janus

import java.util.{Calendar, Properties}

import com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder
import com.figmd.janus.Sections.PatientMedication.PatientMedication
import com.figmd.janus.Sections.PlanOfCare.PlanOfCare
import com.figmd.janus.Sections.ResultObservation.ResultObservation
import com.figmd.janus.Sections.SocialHistoryObservation._
import com.figmd.janus.Sections.VitalSigns.VitalSigns
import com.figmd.janus.util._
import com.google.common.base.Throwables
import org.apache.log4j.{Level, Logger}


object DataMartCreator extends Serializable {

  var prop = new Properties
  var action_name="CDRtoDataMart"
  val cdr_db_name="acep_visit"
  //val cdr_db_name="acepfigmdhqiprodcdr_01"

  def main(args: Array[String]) {
/*
    prop.setProperty("wf_id", args(2))
    prop.setProperty("postgresHostName",args(3))
    prop.setProperty("postgresHostPort",args(4))
    prop.setProperty("postgresConfigDatabaseName",args(5))
    prop.setProperty("postgresHostUserName",args(6))
    prop.setProperty("postgresUserPass",args(7))
    prop.setProperty("num_executors",args(9))
    prop.setProperty("executor_cores",args(10))
    prop.setProperty("executor_memory",args(11))
    prop.setProperty("keyspace_datamart",args(12))
    prop.setProperty("keyspace_datamart_table_name",args(13))
    prop.setProperty("keyspace_webdatamart",args(14))
    prop.setProperty("cassandra_host",args(15))
    prop.setProperty("cassandra_port",args(16))
    prop.setProperty("spark_master_url",args(17))
    prop.setProperty("mode",args(19))
*/

    prop.setProperty("wf_id", "WF_TEST_CDR_TO_DM")
    prop.setProperty("postgresHostName","10.20.201.36")
    prop.setProperty("postgresHostPort","5432")
    prop.setProperty("postgresConfigDatabaseName","config_db")
    prop.setProperty("postgresManagementDatabaseName","acepmgt")
    prop.setProperty("postgresHostUserName","postgres")
    prop.setProperty("postgresUserPass","Janus@123")
    prop.setProperty("num_executors","2")
    prop.setProperty("executor_cores","5")
    prop.setProperty("executor_memory","7G")
    prop.setProperty("keyspace_datamart","acep")
    prop.setProperty("keyspace_datamart_table_name","tblencounter_mat")//tblencounter_nonqpp
    prop.setProperty("cassandra_host","localhost") // 10.20.201.152
    prop.setProperty("cassandra_port","9042")
    prop.setProperty("spark_master_url","local[8]")
    prop.setProperty("mode","client")
//    prop.setProperty("spark_master_url","yarn")
//    prop.setProperty("mode","cluster")
    prop.setProperty("hive_metastore_uris", "thrift://10.20.201.36:9083")
    prop.setProperty("awsAccessKeyId","AKIAKZYJUVVNDMCO6PMQ")
    prop.setProperty("awsSecretAccessKey","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")
    prop.setProperty("data_mart_output_path","/tmp/DataMart/ACEP")



    prop.setProperty("postgresHostName","10.20.201.103")
    prop.setProperty("hive_metastore_uris", "thrift://10.20.201.103:9083")



    val start_date = Calendar.getInstance.getTime

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("myLogger").setLevel(Level.OFF)


    try {

      var sparkUtility = new SparkUtility()
      var spark = sparkUtility.getSparkSession()
      val dateUtility = new DateUtility()


      val visit_df = spark.sql("select visituid,from_unixtime(unix_timestamp(visitdate,\"yyyy-mm-dd'T'HH:mm:ss.SSS'Z'\"),'yyyy-mm-dd HH:mm:ss.S') as encounterdate,patientuid,practiceuid from " + cdr_db_name + ".visit");

      val resultObservation = ResultObservation.createDM(spark)
      resultObservation.cache()
      resultObservation.show(false)
      println(">>count<<"+resultObservation.count())

      /* val patientLabOrder = PatientLabOrder.createDM(spark)
       patientLabOrder.cache()
       //patientLabOrder.show(false)

       val patientMedication = PatientMedication.createDM(spark)
       patientMedication.cache()
       //patientMedication.show(false)

       val vitalSign = VitalSigns.createDM(spark)
       vitalSign.cache()
       //vitalSign.show(false)

       val socialHistoryObservation = SocialHistoryObservation.createDM(spark)
       socialHistoryObservation.cache()
       //socialHistoryObservation.show(false)

       val planOfCare = PlanOfCare.createDM(spark)
       planOfCare.cache
       //planOfCare,show(false)

       val datamart = visit_df.join(patientMedication, Seq("visituid"), "left")
         .join(vitalSign, Seq("visituid"), "left")
         .join(socialHistoryObservation, Seq("visituid"), "left")
         .join(planOfCare, Seq("visituid"), "left")


       datamart.show(10, false)
       datamart.printSchema();
       datamart.createOrReplaceTempView("tblencounter")
       spark.sql("select count(1) from tblencounter").show()
       spark.sql("select count(distinct visituid) from tblencounter").show()*/


      //datamart.show(false)

      //new ConditionsUtil().saveToWebDM(datamart)

      //BroadcastMaster.getBrodcastMaster(spark)


      println("Start Time  : " + start_date)
      println("End Time    : " + Calendar.getInstance.getTime)


    }
    catch {
      case e: Exception => {
        println(e.printStackTrace())
        new com.figmd.janus.util.PostgreUtility().insertIntoProcessDetails("", "W0001", "CRITICAL", Throwables.getStackTraceAsString(e), "FAIL")
        System.exit(-1)
      }
    }

  }
}

