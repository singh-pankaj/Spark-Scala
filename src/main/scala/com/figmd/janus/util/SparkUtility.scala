package com.figmd.janus.util

import org.apache.spark.sql.SparkSession
import com.figmd.janus.DataMartCreator.prop

class SparkUtility extends Serializable {

  val fileUtility = new FileUtility();

  var appName = fileUtility.getProperty("spark.app.name");



  var num_executors       =prop.getProperty("num_executors")
  var executor_cores      =prop.getProperty("executor_cores")
  var executor_memory     =prop.getProperty("executor_memory")
  var cassHostName        = prop.getProperty("cassandra_host");
  var sparkMasterUrl      = prop.getProperty("spark_master_url");
  var port                = prop.getProperty("cassandra_port");
  var mode                = prop.getProperty("mode");
  var hive_metastore_uris = prop.getProperty("hive_metastore_uris");
  var awsAccessKeyId      = prop.getProperty("awsAccessKeyId");
  var awsSecretAccessKey  = prop.getProperty("awsSecretAccessKey");

  def getSparkSession(): SparkSession =
  {
    val spark = SparkSession.builder.master(sparkMasterUrl).appName(appName)
      .config("spark.cassandra.connection.host",cassHostName)
      //.config("spark.closure.serializer","org.apache.spark.serializer.JavaSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lzf")
      .config("executor-memory",executor_memory)
      .config("spark.submit.deployMode", mode)
      .config("executor-cores", executor_cores)
      //.config("spark.driver.memory", "0g")
      .config("num-executors",num_executors)
      .config("hive.metastore.uris", hive_metastore_uris)
      .config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      .config("fs.s3n.awsAccessKeyId", awsAccessKeyId)
      .config("fs.s3n.awsSecretAccessKey",awsSecretAccessKey)
      .enableHiveSupport()
      .getOrCreate()
    return spark;
  }

/*  def getRDD(): Any =
  {
    var sprkSession=getSparkContext();
    var rdd = sprkSession.sparkContext.cassandraTable("webdatamart", "tblencounter_small")
    return rdd.getClass;
  }*/
}
