package com.figmd.janus.util

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util

import com.datastax.spark.connector.CassandraRow
import com.figmd.janus.DataMartCreator.prop
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import javax.script.ScriptEngineManager
import javax.script.ScriptException
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import scala.util.matching.Regex

class ConditionsUtil extends Serializable {

  def InCondition(r: Row, columnName: String, matchValue: String): Boolean = {

    val matchList = matchValue.split(",")

    return !r.isNullAt(r.fieldIndex(columnName)) && matchList.contains(r.getAs[String](columnName))

  }

  def EqualCondition(r: Row, columnName: String, matchValue: String): Boolean = {

    return !r.isNullAt(r.fieldIndex(columnName)) && (matchValue.equals(r.getAs[String](columnName)) || matchValue == (r.getAs[String](columnName)))

  }

  def LikeCondition(r: Row, columnName: String, matchValue: String): Boolean = {
    val pattern: Regex = matchValue.replace("%", ".*").r
    val re: Boolean = !r.isNullAt(r.fieldIndex(columnName)) && (r.getAs[String](columnName) match {
      case `pattern`() => true
      case _ => false
    })

    return re
  }

  def CheckNumeric(r: Row, columnName: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(columnName)) && r.getString(r.fieldIndex(columnName)).forall(_.isDigit)
  }

  def CheckNull(r: Row, columnName: String): Boolean = {
    return r.isNullAt(r.fieldIndex(columnName))
  }

  def CheckStringEqual(r: Row, columnName: String, equalsTo: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(columnName)) && (r.getString(r.fieldIndex(columnName)) == equalsTo || r.getString(r.fieldIndex(columnName)).equals(equalsTo))
  }

  def CheckNumGreaterOrEqual(r: Row, columnName: String, checkValue: Int): Boolean = {
    return !r.isNullAt(r.fieldIndex(columnName)) && r.getInt(r.fieldIndex(columnName)) >= checkValue;
  }

  def CheckNumLessOrEqual(r: Row, columnName: String, checkValue: Int): Boolean = {
    return !r.isNullAt(r.fieldIndex(columnName)) && r.getInt(r.fieldIndex(columnName)) <= checkValue;
  }


  def checkElementDateCaseUtil(r: Row, sectionDateColumn: String, visitStartDateColumn: String, visitEndDateColumn: String): Boolean = {

    /*CASE
    When cast(cast(PM.StartDate as datetime) as time)= '00:00:00.000'
    and ((cast(PM.StartDate as date) >= cast(TEMP.ArrivalDate as Date)) AND (cast(PM.StartDate as date) <= cast(Coalesce(temp.EndDate,TEMP.ArrivalDate) as Date)))
    then TEMP.ArrivalDate
    When PM.StartDate >= TEMP.ArrivalDate and PM.StartDate <= temp.EndDate then PM.StartDate
    When PM.StartDate >= Coalesce(temp.EndDate,TEMP.ArrivalDate) then TEMP.ArrivalDate
    else PM.StartDate
    END AS StartDate,*/

    if (CheckDateIfTimeZero(r, sectionDateColumn) && CheckCastDateLessThanOrEqual(r, sectionDateColumn, visitStartDateColumn) &&
      CheckCastDateGreaterThanOrEqual(r, sectionDateColumn, Coalesce(r, visitEndDateColumn, visitStartDateColumn)))
      return true;
    else if (CheckCastDateGreaterThanOrEqual(r, sectionDateColumn, visitStartDateColumn) && CheckCastDateLessThanOrEqual(r, sectionDateColumn, visitEndDateColumn))
      return false;
    else if (CheckCastDateGreaterThanOrEqual(r, sectionDateColumn, Coalesce(r, visitEndDateColumn, visitStartDateColumn)))
      return true;
    else
      return false
  }

  //cast(DocumentationDate as date) < cast(ArrivalDate as date)
  def CheckCastDateLessThanOrEqual(r: Row, startDateColumn: String, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(startDateColumn)) && !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      (castTimesatampToDate(r.getTimestamp(r.fieldIndex(startDateColumn))).before(castTimesatampToDate(r.getTimestamp(r.fieldIndex(checkDateColumn))))
        || castTimesatampToDate(r.getTimestamp(r.fieldIndex(startDateColumn))).equals(castTimesatampToDate(r.getTimestamp(r.fieldIndex(checkDateColumn)))))
  }

  def castTimesatampToDate(timestamp: Timestamp): Date = {
    return new Date(timestamp.getTime)
  }

  //cast(DocumentationDate as date) > cast(ArrivalDate as date)
  def CheckCastDateGreaterThanOrEqual(r: Row, startDateColumn: String, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(startDateColumn)) && !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      (castTimesatampToDate(r.getTimestamp(r.fieldIndex(startDateColumn))).after(castTimesatampToDate(r.getTimestamp(r.fieldIndex(checkDateColumn))))
        || castTimesatampToDate(r.getTimestamp(r.fieldIndex(startDateColumn))).equals(castTimesatampToDate(r.getTimestamp(r.fieldIndex(checkDateColumn)))))
  }

  //cast(DocumentationDate as date) = cast(ArrivalDate as date)
  def CheckCastDateEqual(r: Row, firstDateColumn: String, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(firstDateColumn)) && !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      castTimesatampToDate(r.getTimestamp(r.fieldIndex(firstDateColumn))).equals(castTimesatampToDate(r.getTimestamp(r.fieldIndex(checkDateColumn))))
  }

  //cast(cast(DocumentationDate as datetime)as time)='00:00:00.000'
  def CheckDateIfTimeZero(r: Row, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      (r.getTimestamp(r.fieldIndex(checkDateColumn)).getHours == 0 &&
        r.getTimestamp(r.fieldIndex(checkDateColumn)).getMinutes == 0 &&
        r.getTimestamp(r.fieldIndex(checkDateColumn)).getSeconds == 0)
  }

  def Coalesce(r: Row, firstColumn: String, secondColumn: String): String = {
    if (!r.isNullAt(r.fieldIndex(firstColumn)))
      return firstColumn
    else
      return secondColumn
  }

  //DocumentationDate < ArrivalDate
  def CheckDateLessThanOrEqual(r: Row, startDateColumn: String, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(startDateColumn)) && !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      (r.getTimestamp(r.fieldIndex(startDateColumn)).before(r.getTimestamp(r.fieldIndex(checkDateColumn))) || r.getTimestamp(r.fieldIndex(startDateColumn)).equals(r.getTimestamp(r.fieldIndex(checkDateColumn))))
  }

  //DocumentationDate > ArrivalDate
  def CheckDateGreaterThanOrEqual(r: Row, startDateColumn: String, checkDateColumn: String): Boolean = {
    return !r.isNullAt(r.fieldIndex(startDateColumn)) && !r.isNullAt(r.fieldIndex(checkDateColumn)) &&
      (r.getTimestamp(r.fieldIndex(startDateColumn)).after(r.getTimestamp(r.fieldIndex(checkDateColumn))) || r.getTimestamp(r.fieldIndex(startDateColumn)).equals(r.getTimestamp(r.fieldIndex(checkDateColumn))))
  }

  def toDate(timestamp: Long) = {
    val date = Instant.ofEpochMilli(timestamp * 1000).atZone(ZoneId.systemDefault).toLocalDate
    date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }

  def convertToCassandraDate(inDate: String): String = {
    import java.text.DateFormat
    import java.text.SimpleDateFormat
    val targetFormat = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS'Z'")

    val outDate = if (inDate.isEmpty || inDate.equals("null") || inDate == "null")
      "0000-00-00T00:00:00.000Z"
    else
      targetFormat.format(new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.S").parse(inDate))
    //LocalDateTime.parse(inDate.substring(0,inDate.length-2), DateTimeFormatter.ofPattern("yyyy-mm-dd'T'HH:mm:ss.SSS'Z'")).toString


    return outDate;
  }

  def saveToWebDM(datamart: DataFrame): Unit = {
    val hdfs = FileSystem.get(new SparkUtility().getSparkSession().sparkContext.hadoopConfiguration)

    if (hdfs.isDirectory(new org.apache.hadoop.fs.Path(prop.getProperty("data_mart_output_path"))))
      hdfs.delete(new org.apache.hadoop.fs.Path(prop.getProperty("data_mart_output_path")), true)


    datamart.write.mode(SaveMode.Overwrite).option("delimiter", "~").csv(prop.getProperty("data_mart_output_path"))

    hdfs.close()
  }

}
