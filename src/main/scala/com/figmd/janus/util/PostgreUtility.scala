package com.figmd.janus.util

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.figmd.janus.DataMartCreator
import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgreUtility extends Serializable {

  var properties = new Properties()

  var wf_id=DataMartCreator.prop.getProperty("wf_id")

  def getPostgresConnection(): Object =
  {
       return null;
  }
  var connection: Connection = _

  def postgresConnect(): Connection ={
    var prop=new FileUtility().getProperty()
    val url = "jdbc:postgresql://"+DataMartCreator.prop.getProperty("postgresHostName")+":"+DataMartCreator.prop.getProperty("postgresHostPort")+"/"+DataMartCreator.prop.getProperty("postgresConfigDatabaseName")
    val driver = "org.postgresql.Driver"
    val username = DataMartCreator.prop.getProperty("postgresHostUserName")
    val password = DataMartCreator.prop.getProperty("postgresUserPass")

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    return connection

 }

  def getPostgresTable(spark:SparkSession,tableName:String): DataFrame ={
    return spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://"+DataMartCreator.prop.getProperty("postgresHostName")+":"+DataMartCreator.prop.getProperty("postgresHostPort")+"/"+DataMartCreator.prop.getProperty("postgresManagementDatabaseName"))
      .option("dbtable", tableName)
      .option("user", DataMartCreator.prop.getProperty("postgresHostUserName"))
      .option("password", DataMartCreator.prop.getProperty("postgresUserPass"))
      .load()
  }

//CREATE TABLE IF NOT EXISTS process_details(id serial primary key,wf_id varchar(50) NOT NULL,measure_name varchar(200),action_name varchar(200),date_time timestamp NOT NULL DEFAULT 'now'::timestamp,
// error_code  varchar(50),error_type  varchar(50),error_message text,status varchar(50))
  // Auding table for detailed worflow action
  def insertIntoProcessDetails(measure_name:String,error_code:String,error_type:String,error_message:String,status:String ): Unit ={
    connection = postgresConnect();
      try {


        val pstatement = connection.prepareStatement("Insert into process_details(wf_id,measure_name,action_name,error_code,error_type,error_message,status)values(?,?,?,?,?,?,?)");
        pstatement.setString(1, wf_id)
        pstatement.setString(2, measure_name)
        pstatement.setString(3, com.figmd.janus.DataMartCreator.action_name)
        pstatement.setString(4, error_code)
        pstatement.setString(5, error_type)
        pstatement.setString(6, error_message)
        pstatement.setString(7, status)

        pstatement.executeUpdate()
      }
    catch
      {
        case e:Exception => print("ERROR..."+e.printStackTrace())
      }
    finally {
      connection.close()
    }
  }
  //CREATE TABLE IF NOT EXISTS central_log(id serial primary key,wf_id varchar(50) NOT NULL,action_name varchar(150),date_time timestamp NOT NULL DEFAULT 'now'::timestamp,
  // error_code varchar(50),error_message text,status varchar(100))
  // Auding table for each worflow action
  def insertIntoCentralLog(measure_name:String,error_code:String,error_type:String,error_message:String,status:String ): Unit ={
    //var wf_id=DataMartCreator.prop.getProperty("wf_id")
    connection = postgresConnect();
    try {

      val pstatement = connection.prepareStatement("Insert into central_log(wf_id ,action_name,status,error_code,error_message)values(?,?,?,?,?)");
      pstatement.setString(1, wf_id)
      pstatement.setString(2, com.figmd.janus.DataMartCreator.action_name)
      pstatement.setString(3, status)
      pstatement.setString(4, error_code)
      pstatement.setString(5, error_message)

      pstatement.executeUpdate()
    }
    catch
      {
        case e:Exception => print("ERROR..."+e.printStackTrace())
      }
    finally {
      connection.close()
    }
  }
}
