package com.figmd.janus.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, Properties}

import com.figmd.janus.DataMartCreator.prop
import com.datastax.spark.connector.{CassandraRow, _}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.rdd.RDD

/*object Log{

  var logArray:List[String]=Nil
  var fileUtility=new FileUtility()
}*/

class MeasureUtility extends Serializable {


  @transient lazy val dateUtility = new DateUtility();
  // @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
  @transient lazy val fileUtility = new FileUtility();

  //@transient lazy val hdfs= FileSystem.get(new SparkUtility().getSparkContext().sparkContext.hadoopConfiguration)
  @transient lazy val logger = Logger.getLogger("customlog4j")
  val p = new Properties()
  p.setProperty("log4j.rootLogger", "DEBUG, customlog4j")
  p.setProperty("log4j.appender.customlog4j", "org.apache.log4j.FileAppender")
  //p.setProperty("log4j.appender.customlog4j.File","/tmp/acep.log")
  p.setProperty("log4j.appender.customlog4j.layout", "org.apache.log4j.PatternLayout")
  p.setProperty("log4j.appender.customlog4j.layout.ConversionPattern", "%m%n")
  p.setProperty("log4j.appender.customlog4j.filter.a", "org.apache.log4j.varia.LevelRangeFilter")
  p.setProperty("log4j.appender.customlog4j.filter.a.LevelMin", "INFO")
  p.setProperty("log4j.appender.customlog4j.filter.a.LevelMax", "INFO")
  p.setProperty("log4j.appender.CA", "org.apache.log4j.ConsoleAppender")
  p.setProperty("log4j.appender.CA.layout", "org.apache.log4j.PatternLayout")
  p.setProperty("log4j.appender.CA.layout.ConversionPattern", " %-4r [%t] %-5p %c %x - %m%n")


  //println(":::::::::::::::::::::::::::::::::::::"+p.getProperty("log4j.appender.customlog4j.File"))
  p.setProperty("log4j.appender.customlog4j.File", fileUtility.getProperty("file.output.path.log"))
  //println(":::::::::::::::::::::::::::::::::::::"+p.getProperty("log4j.appender.customlog4j.File"))
  PropertyConfigurator.configure(p)
  //val p = fileUtility.getLog4jProperty()
  //p.load(getClass.getResourceAsStream("/log4j.properties"))


  //var keyspace = fileUtility.getProperty("cassandra.keyspace.name");
  var logFile = prop.getProperty("measure_computation_output_path");
  final var IPP = "IPP"
  final var MET = "MET"
  final var EXCEPTION = "EXCEPTION"
  final var EXCLUSION = "EXCLUSION"
  final var ELIGIBLE = "ELIGIBLE"
  var message = "";

  var str = dateUtility.now + "," + "1" + ","

  // check element equal to 1    e.g.= chstpn=1
  def checkElementPresent(r: CassandraRow, conditionType: String, measureName: String, elementName: String): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getString(elementName).equalsIgnoreCase("1")
    if (isExist)
      message = elementName + " Found"
    else
      message = elementName + " not Found"
    measureLogger(r, measureName, conditionType, "checkElementPresent", elementName, isExist, message)
    return isExist;
  }

  //check element integer value e.g. chstpn=1
  def checkElementValue(r: CassandraRow, conditionType: String, measureName: String, elementName: String, value: Int): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getInt(elementName) == value
    if (isExist)
      message = elementName + " equals to " + value
    else
      message = elementName + " not equals to " + value
    measureLogger(r, measureName, conditionType, "checkElementValue", elementName, isExist, message)
    return isExist;
  }

  def chkDateRangeLessOrEqualendDate(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, endDate: Date): Boolean = {

    val isExist = !r.isNullAt(checkDate) &&
      (r.getDate(checkDate).before(endDate) || r.getDate(checkDate).equals(endDate))
    if (isExist)
      message = checkDate + " Found In Date Range less than equal to (" + endDate + ")"
    else
      message = checkDate + " Not Found In Date Range less then equal to (" + endDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeLessOrEqualendDate", checkDate, isExist, message)
    return isExist;
  }

  def chkDateYearDiffGreaterOrEqual(r: CassandraRow, conditionType: String, measureName: String, startDate: String, endDate: String, compareYears: Double): Boolean = {
    val isExist = !r.isNullAt(startDate) && !r.isNullAt(endDate) && new DateUtility().getAge(r.getString(startDate),r.getString(endDate)) >= compareYears
    if (isExist)
      message = startDate + " and " + endDate + " difference is greater than or equal to (" + compareYears + ") "
    else
      message = startDate + " and " + endDate + " difference is greater not than equal to (" + compareYears + ") "
    measureLogger(r, measureName, conditionType, "chkDateYearDiffGreaterOrEqual", startDate, isExist, message)
    return isExist;
  }

  def chkDateYearDiffLess(r: CassandraRow, conditionType: String, measureName: String, startDate: String, endDate: String, compareYears: Double): Boolean = {
    val isExist = !r.isNullAt(startDate) && !r.isNullAt(endDate) && new DateUtility().getAge(r.getString(startDate),r.getString(endDate)) < compareYears
    if (isExist)
      message = startDate + " and " + endDate + " diffrence is less than (" + compareYears + ") "
    else
      message = startDate + " and " + endDate + " diffrence is not less than (" + compareYears + ") "
    measureLogger(r, measureName, conditionType, "chkDateYearDiffLess", startDate, isExist, message)
    return isExist;
  }

  def checkElementValueDoubleGreater(r: CassandraRow, conditionType: String, measureName: String, elementName: String, checkValue: Double): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getDouble(elementName) > checkValue
    if (isExist)
      message = elementName + " is greater than (" + checkValue + ")"
    else
      message = elementName + " is not greater than (" + checkValue + ") "
    measureLogger(r, measureName, conditionType, "chkValueRangeGreater", elementName, isExist, message)
    return isExist;
  }

  def checkElementValueDoubleGreaterOrEqual(r: CassandraRow, conditionType: String, measureName: String, elementName: String, checkValue: Double): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getDouble(elementName) >= checkValue
    if (isExist)
      message = elementName + " is greater than or equal to (" + checkValue + ")"
    else
      message = elementName + " is not greater than or equal to (" + checkValue + ") "
    measureLogger(r, measureName, conditionType, "checkElementValueDoubleGreaterOrEqual", elementName, isExist, message)
    return isExist;
  }

  def checkElementValueDoubleLess(r: CassandraRow, conditionType: String, measureName: String, elementName: String, checkValue: Double): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getDouble(elementName) < checkValue
    if (isExist)
      message = elementName + " is less than (" + checkValue + ")"
    else
      message = elementName + " is not less than (" + checkValue + ") "
    measureLogger(r, measureName, conditionType, "checkElementValueDoubleLess", elementName, isExist, message)
    return isExist;
  }

  def checkElementValueDoubleLessOrEqual(r: CassandraRow, conditionType: String, measureName: String, elementName: String, checkValue: Double): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getDouble(elementName) <= checkValue
    if (isExist)
      message = elementName + " is less than equal to (" + checkValue + ")"
    else
      message = elementName + " is not less than equal to (" + checkValue + ") "
    measureLogger(r, measureName, conditionType, "checkElementValueDoubleLessOrEqual", elementName, isExist, message)
    return isExist;
  }

  def checknull(r: CassandraRow, conditionType: String, measureName: String, checkElement: String): Boolean = {
    val isExist = r.isNullAt(checkElement) || r.getString(checkElement).equalsIgnoreCase("null")
    if (isExist)
      message = checkElement + " null Found"
    else
      message = checkElement + " null not Found"
    measureLogger(r, measureName, conditionType, "checknull", checkElement, isExist, message)
    return isExist;
  }

  def chkDateRangeBetweenMinusHours(r: CassandraRow, conditionType: String, measureName: String, checkdate: String, compareDate: String, no_of_Hours: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkdate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkdate).toString)
    } else {
      chkDate = checkdate
    }

    val isExist = !r.isNullAt(checkdate) && !r.isNullAt(compareDate) && (
      (r.getDateTime(checkdate).isAfter(r.getDateTime(compareDate).minusHours(no_of_Hours)) || r.getDate(checkdate).equals(r.getDate(compareDate))) &&
        (r.getDate(checkdate).before(r.getDate(compareDate)) || r.getDate(checkdate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found in Date Range (" + cDate + ", " + cDate + " minus" + no_of_Hours + " hours)"
    else
      message = chkDate + " Not found in Date Range (" + cDate + " , " + cDate + "  minus " + no_of_Hours + " hours)"
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusHours", checkdate, isExist, message)

    return isExist
  }

  def chkDateRangeBetweenMinusSecondsFromQuarterEndDate(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: Date, endDate:Date, no_of_Seconds: Int): Boolean = {

    var cDate = convertDateToDDMMYYYY(startDate.toString)
    var eDate = convertDateToDDMMYYYY(endDate.toString)

    endDate.setTime(endDate.getTime-(no_of_Seconds*1000))
    val isExist = !r.isNullAt(checkDate) && (
      (r.getDate(checkDate).after(startDate) || r.getDate(checkDate).equals(startDate)) &&
        (r.getDate(checkDate).before(endDate) || r.getDate(checkDate).equals(endDate)))
    if (isExist)
      message = checkDate + " Found in Date Range (" + cDate + ", " + eDate + " minus" + no_of_Seconds + " Seconds)"
    else
      message = checkDate + " Not found in Date Range (" + cDate + " , " + eDate + "  minus " + no_of_Seconds + " Seconds)"
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusSecondsFromQuarterEndDate", checkDate, isExist, message)

    return isExist
  }

  // function for chkDateRangeLessOrEqualMinusHours
  def chkDateRangeLessOrEqualMinusHours(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: String, no_of_hours: Int): Boolean = {
    var sDate = ""
    if (!r.isNullAt(startDate)) {
      sDate = convertDateToDDMMYYYY(r.getDate(startDate).toString)
    } else {
      sDate = startDate
    }

    val isExist = !r.isNullAt(startDate) && !r.isNullAt(checkDate) && (r.getDateTime(checkDate).isBefore(r.getDateTime(startDate).minusHours(no_of_hours)) || r.getDateTime(checkDate).equals(r.getDateTime(startDate).minusHours(no_of_hours)))
    if (isExist)
      message = checkDate + " Found In Date Range Less Than Or Equal to (" + sDate + " - " + no_of_hours + " hours)"
    else
      message = checkDate + " Not Found In Date Range Less Than Or Equal to (" + sDate + "- " + no_of_hours + " hours) "
    measureLogger(r, measureName, conditionType, "chkDateRangeLessOrEqualMinusHours", checkDate, isExist, message)
    return isExist;
  }

  def convertDateToDDMMYYYY(dateString: String): String = {
    //println("::::::::::::::::::::::::::::::::"+dateString)
    return LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
  }

  def measureLogger(r: CassandraRow, measureName: String, conditionType: String, conditionName: String, elementName: String, status: Boolean, description: String): Unit = {
    var logStr = "" + r.getString("practiceuid") + "~" + measureName + "~" + r.getString("patientuid") + "~" + r.getString("visituid") + "~" + conditionType + "~" + conditionName + "~" + elementName + "~" + status + "~" + description + "\n"
    logger.info(logStr)


    /*   if(File(fileUtility.getProperty("file.output.path.log") ).exists)
    File(fileUtility.getProperty("file.output.path.log") ).appendAll(r.getString("practiceuid")+"~"+ measureName +"~"+r.getString("patientuid")+"~"+ r.getString("visituid") + "~" + conditionType + "~" + conditionName + "~" + elementName + "~" + status + "~" + description  + "\n")
    else {
      File(fileUtility.getProperty("file.output.path.log") ).createFile()
      File(fileUtility.getProperty("file.output.path.log") ).appendAll(r.getString("practiceuid")+"~"+ measureName +"~"+r.getString("patientuid")+"~"+ r.getString("visituid") + "~" + conditionType + "~" + conditionName + "~" + elementName + "~" + status + "~" + description  + "\n")
    }*/

    // val hdfs= FileSystem.get(new SparkUtility().getSparkContext().sparkContext.hadoopConfiguration)

    // log.info(r.getString("practiceuid") + "," + r.getString("patientuid") + "," + r.getString("visituid") + "," + measureName + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description)

    /*  val file=File(logFile)

       if (file.exists)
         file.appendAll(r.getString("practiceuid") + "~" +measureName + "~" +r.getString("patientuid") + "~" +r.getString("visituid") + "~" +conditionType + "~" +conditionName + "~" +elementName + "~" +status + "~" +description + "\n")
       else{
         file.createFile(true)
         file.appendAll(r.getString("practiceuid") + "~" +measureName + "~" +r.getString("patientuid") + "~" +r.getString("visituid") + "~" +conditionType + "~" +conditionName + "~" +elementName + "~" +status + "~" +description + "\n")
       }*/


    //hdfs.append(new org.apache.hadoop.fs.Path(fileUtility.getProperty("file.output.path")+"/"+measureName+"/"+measureName+".log")).writeBytes(r.getString("practiceuid")+","+ measureName +","+r.getString("patientuid")+","+ r.getString("visituid") + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description  + "\n")

    /*
        @transient lazy val path=new org.apache.hadoop.fs.Path(fileUtility.getProperty("file.output.path")+"/"+measureName+"/"+measureName+".log")
        @transient lazy val oStream=hdfs.create(path)
        oStream.writeUTF(r.getString("practiceuid")+","+ measureName +","+r.getString("patientuid")+","+ r.getString("visituid") + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description  + "\n")
        //logarray.add(r.getString("practiceuid")+","+ measureName +","+r.getString("patientuid")+","+ r.getString("visituid") + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description  + "\n")
        if(hdfs.isFile(path))

        else {
          hdfs.create(path)
          hdfs.append(path).writeBytes(r.getString("practiceuid") + "," + measureName + "," + r.getString("patientuid") + "," + r.getString("visituid") + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description + "\n")
        }
        oStream.close()
        hdfs.close()*/
    // var logstring = r.getString("practiceuid")+","+ measureName +","+r.getString("patientuid")+","+ r.getString("visituid") + "," + conditionType + "," + conditionName + "," + elementName + "," + status + "," + description  + "\n"

    //Log.logArray=Log.logArray.::(logstring)
  }

  // function for chkDateRangeBetweenMinusDaysForTwoArg
  def chkDateRangeBetweenMinusDaysForTwoArg(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Days1: Int, no_of_Days2: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isAfter(r.getDateTime(compareDate).minusDays(no_of_Days1)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusDays(no_of_Days1))) &&
        (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).minusDays(no_of_Days2)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusDays(no_of_Days2))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + " minus " + no_of_Days1 + ", " + cDate + " minus " + no_of_Days2 + " days)"
    else
      message = chkDate + " Not Found In Date Range (" + cDate + " minus " + no_of_Days1 + ", " + cDate + " minus " + no_of_Days2 + " days) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusDaysForTwoArg", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetweenMinusDaysForTwoArg1Minus2plus
  def chkDateRangeBetweenMinusDaysForTwoArg1Minus2plus(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Days1: Int, no_of_Days2: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isAfter(r.getDateTime(compareDate).plusDays(no_of_Days1)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).plusDays(no_of_Days1))) &&
        (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).minusDays(no_of_Days2)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusDays(no_of_Days2))))

    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + " plus " + no_of_Days1 + ", " + cDate + " minus " + no_of_Days2 + " days)"
    else
      message = chkDate + " Not Found In Date Range (" + cDate + " plus " + no_of_Days1 + ", " + cDate + " minus " + no_of_Days2 + " days) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusDaysForTwoArg1Minus2plus", checkDate, isExist, message)
    return isExist;
  }

  //function for chkDateRangeBetweenPlusMinutes
  def chkDateRangeBetweenPlusMinutes(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Mins: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).plusMinutes(no_of_Mins)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).plusMinutes(no_of_Mins))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Mins + " minutes) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Mins + " minutes) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenPlusMinutes", checkDate, isExist, message)
    return isExist;
  }

  //function for chkDateRangeBetweenMinusMinutes
  def chkDateRangeBetweenMinusMinutes(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Mins: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).minusMinutes(no_of_Mins)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusMinutes(no_of_Mins))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Mins + " minutes) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Mins + " minutes) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusMinutes", checkDate, isExist, message)
    return isExist;
  }

  //function for chkDateRangeBetweenMinusMonths
  def chkDateRangeBetweenMinusMonths(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Months: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).minusMonths(no_of_Months)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusMonths(no_of_Months))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Months + " Months) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Months + " Months) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusMonths", checkDate, isExist, message)
    return isExist;
  }

  //function for chkDateRangeBetweenPlusMonths
  def chkDateRangeBetweenPlusMonths(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Months: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).plusMonths(no_of_Months)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).plusMonths(no_of_Months))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Months + " Months) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Months + " Months) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenPlusMonths", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetweenMinusSeconds
  def chkDateRangeBetweenMinusSeconds(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Sec: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && (
      (r.getDateTime(checkDate).isAfter(r.getDateTime(compareDate).minusSeconds(no_of_Sec)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).minusSeconds(no_of_Sec))) &&
        (r.getDate(checkDate).before(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Sec + " Seconds) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Sec + " Seconds) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusSeconds", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetweenMinusSeconds
  def chkDateRangeBetweenPlusSeconds(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Sec: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var chkDate = ""
    if (!r.isNullAt(checkDate)) {
      chkDate = convertDateToDDMMYYYY(r.getDate(checkDate).toString)
    } else {
      chkDate = checkDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).plusSeconds(no_of_Sec)) || r.getDateTime(checkDate).equals(r.getDateTime(compareDate).plusSeconds(no_of_Sec))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = chkDate + " Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Sec + " Seconds) "
    else
      message = chkDate + " Not Found In Date Range (" + cDate + ", " + cDate + " plus " + no_of_Sec + " Seconds) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenPlusSeconds", checkDate, isExist, message)
    return isExist;
  }

  // function for chkValueRangeLess
  def chkValueRangeLess(r: CassandraRow, conditionType: String, measureName: String, checkValue: String, Value: Int): Boolean = {
    val isExist = !r.isNullAt(checkValue) && r.getInt(checkValue) < Value
    if (isExist)
      message = checkValue + " is less than (" + Value + ")"
    else
      message = checkValue + " is not less than (" + Value + ") "
    measureLogger(r, measureName, conditionType, "chkValueRangeLess", checkValue, isExist, message)
    return isExist;
  }

  // function for chkValueRangeLessorEqual
  def chkValueRangeLessorEqual(r: CassandraRow, conditionType: String, measureName: String, checkValue: String, Value: Int): Boolean = {
    val isExist = !r.isNullAt(checkValue) && r.getInt(checkValue) <= Value
    if (isExist)
      message = checkValue + " is less than or equal to (" + Value + ")"
    else
      message = checkValue + " is not less than or equal to (" + Value + ") "
    measureLogger(r, measureName, conditionType, "chkValueRangeLessorEqual", checkValue, isExist, message)
    return isExist;
  }

  // function for chkValueRangeGreater
  def chkValueRangeGreater(r: CassandraRow, conditionType: String, measureName: String, checkValue: String, Value: Int): Boolean = {
    val isExist = !r.isNullAt(checkValue) && r.getInt(checkValue) > Value
    if (isExist)
      message = checkValue + " is greater than (" + Value + ")"
    else
      message = checkValue + " is not greater than (" + Value + ") "
    measureLogger(r, measureName, conditionType, "chkValueRangeGreater", checkValue, isExist, message)
    return isExist;
  }

  // function for chkValueRangeGreaterOrEqual
  def chkValueRangeGreaterOrEqual(r: CassandraRow, conditionType: String, measureName: String, checkValue: String, Value: Int): Boolean = {
    val isExist = !r.isNullAt(checkValue) && r.getInt(checkValue) >= Value
    if (isExist)
      message = checkValue + " is greater than or equal to (" + Value + ")"
    else
      message = checkValue + " is not greater than or equal to (" + Value + ") "
    measureLogger(r, measureName, conditionType, "chkValueRangeGreaterOrEqual", checkValue, isExist, message)
    return isExist;
  }

  // function for chkDateRangeExist
  def chkDateRangeExist(r: CassandraRow, conditionType: String, measureName: String, elementName: String, startDate: Date, endDate: Date): Boolean = {
    var sDate = convertDateToDDMMYYYY(startDate.toString)
    var eDate = convertDateToDDMMYYYY(endDate.toString)

    val isExist = !r.isNullAt(elementName) && (
      (r.getDate(elementName).after(startDate) || r.getDate(elementName).equals(startDate)) &&
        (r.getDate(elementName).before(endDate) || r.getDate(elementName).equals(endDate)))
    if (isExist)
      message = elementName + " Found In Date Range (" + sDate + ", " + eDate + ")"
    else
      message = elementName + " Not Found In Date Range (" + sDate + ", " + eDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeExist", elementName, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetween
  def chkDateRangeBetween(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: String, endDate: String): Boolean = {
    var sDate = ""
    var eDate = ""
    if (!r.isNullAt(startDate)) {
      sDate = convertDateToDDMMYYYY(r.getDate(startDate).toString)
    } else {
      sDate = startDate
    }
    if (!r.isNullAt(endDate)) {
      eDate = convertDateToDDMMYYYY(r.getDate(endDate).toString)
    } else {
      eDate = endDate
    }

    val isExist = !r.isNullAt(startDate) && !r.isNullAt(endDate) && !r.isNullAt(checkDate) && (
      (r.getDate(checkDate).after(r.getDate(startDate)) || r.getDate(checkDate).equals(r.getDate(startDate))) &&
        (r.getDate(checkDate).before(r.getDate(endDate)) || r.getDate(checkDate).equals(r.getDate(endDate))))
    if (isExist)
      message = checkDate + " Found In Date Range (" + sDate + ", " + eDate + ")"
    else
      message = checkDate + " Not Found In Date Range (" + sDate + ", " + eDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetween", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeGreater
  def chkDateRangeGreater(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: String): Boolean = {
    var sDate = ""
    if (!r.isNullAt(startDate)) {
      sDate = convertDateToDDMMYYYY(r.getDate(startDate).toString)
    } else {
      sDate = startDate
    }


    val isExist = !r.isNullAt(startDate) && !r.isNullAt(checkDate) && r.getDate(checkDate).after(r.getDate(startDate))
    if (isExist)
      message = checkDate + " Found In Date Range Greater than (" + sDate + ")"
    else
      message = checkDate + " Not Found In Date Range Greater than (" + sDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeGreater", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetweenMinusDays
  def chkDateRangeBetweenMinusDays(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Days: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isAfter(r.getDateTime(compareDate).minusDays(no_of_Days)) || r.getDate(checkDate).equals(r.getDate(compareDate))) &&
        (r.getDate(checkDate).before(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = checkDate + " Found In Date Range (" + cDate + ", " + cDate + " minus " + no_of_Days + " days) "
    else
      message = checkDate + " Not Found In Date Range (" + cDate + "," + cDate + " minus " + no_of_Days + " days) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenMinusDays", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeBetweenPlusDays
  def chkDateRangeBetweenPlusDays(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, compareDate: String, no_of_Days: Int): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }

    val isExist = !r.isNullAt(checkDate) && !r.isNullAt(compareDate) && !r.isNullAt(checkDate) && (
      (r.getDateTime(checkDate).isBefore(r.getDateTime(compareDate).plusDays(no_of_Days)) || r.getDate(checkDate).equals(r.getDate(compareDate))) &&
        (r.getDate(checkDate).after(r.getDate(compareDate)) || r.getDate(checkDate).equals(r.getDate(compareDate))))
    if (isExist)
      message = checkDate + " Found In Date Range (" + cDate + ", plus " + no_of_Days + " days) "
    else
      message = checkDate + " Not Found In Date Range (" + cDate + ", plus " + no_of_Days + " days) "
    measureLogger(r, measureName, conditionType, "chkDateRangeBetweenPlusDays", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeLessOrEqual
  def chkDateRangeLessOrEqual(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: String): Boolean = {
    var sDate = ""
    if (!r.isNullAt(startDate)) {
      sDate = convertDateToDDMMYYYY(r.getDate(startDate).toString)
    } else {
      sDate = startDate
    }

    val isExist = !r.isNullAt(startDate) && !r.isNullAt(checkDate) && (r.getDate(checkDate).before(r.getDate(startDate)) || r.getDate(checkDate).equals(r.getDate(startDate)))
    if (isExist)
      message = checkDate + " Found In Date Range Less Than Or Equal to (" + sDate + ")"
    else
      message = checkDate + " Not Found In Date Range Less Than Or Equal to (" + sDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeLessOrEqual", checkDate, isExist, message)
    return isExist;
  }

  // function for chkDateRangeGreaterOrEqual
  def chkDateRangeGreaterOrEqual(r: CassandraRow, conditionType: String, measureName: String, checkDate: String, startDate: String): Boolean = {
    var sDate = ""
    if (!r.isNullAt(startDate)) {
      sDate = convertDateToDDMMYYYY(r.getDate(startDate).toString)
    } else {
      sDate = startDate
    }
    val isExist = !r.isNullAt(startDate) && !r.isNullAt(checkDate) && (r.getDate(checkDate).after(r.getDate(startDate)) || r.getDate(checkDate).equals(r.getDate(startDate)))
    if (isExist)
      message = checkDate + " Found In Date Range Greater Than Or Equal to (" + sDate + ")"
    else
      message = checkDate + " Not Found In Date Range Greater Than Or Equal to (" + sDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateRangeGreaterOrEqual", checkDate, isExist, message)
    return isExist;
  }

  // function for checkRange
  def checkRange(r: CassandraRow, conditionType: String, measureName: String, elementName: String, i: Integer, j: Integer): Boolean = {
    val isExist = !r.isNullAt(elementName) && r.getString(elementName).toInt >= i && r.getString(elementName).toInt <= j
    if (isExist)
      message = elementName + " Found In Date Range (" + i + ", " + j + ") "
    else
      message = elementName + " Not Found In Date Range (" + i + ", " + j + ") "
    measureLogger(r, measureName, conditionType, "checkRange", elementName, isExist, message)
    return isExist;
  }

  // function for chkDateEqual
  def chkDateEqual(r: CassandraRow, conditionType: String, measureName: String, firstDate: String, compareDate: String): Boolean = {
    var cDate = ""
    if (!r.isNullAt(compareDate)) {
      cDate = convertDateToDDMMYYYY(r.getDate(compareDate).toString)
    } else {
      cDate = compareDate
    }
    var fDate = ""
    if (!r.isNullAt(firstDate)) {
      fDate = convertDateToDDMMYYYY(r.getDate(firstDate).toString)
    } else {
      fDate = firstDate
    }

    val isExist = !r.isNullAt(firstDate) && !r.isNullAt(compareDate) && (r.getDate(firstDate).getDate.equals(r.getDate(compareDate).getDate) && r.getDate(firstDate).getMonth.equals(r.getDate(compareDate).getMonth) && r.getDate(firstDate).getYear.equals(r.getDate(compareDate).getYear))

    if (isExist)
      message = "(" + fDate + ") Found In Date equals to (" + cDate + ") "
    else
      message = "(" + fDate + ") Date Not equal to (" + cDate + ") "
    measureLogger(r, measureName, conditionType, "chkDateExsist", firstDate, isExist, message)
    return isExist;

  }

  def getFiledList(measureName: String): Array[String] = {
    var elements = fileUtility.getProperty("measure." + measureName + ".element.select");
    return elements.split(",")
  }

  def getNotMet(ippRDD: RDD[CassandraRow], metRDD: RDD[CassandraRow]): RDD[CassandraRow] = {
    return ippRDD.subtract(metRDD)
  }

  def getinterRDD(firstRDD: RDD[CassandraRow], secoundRDD: RDD[CassandraRow]): RDD[CassandraRow] = {
    return firstRDD.subtract(secoundRDD)
  }

  def getExceptionRdd(exceptionRDD: RDD[CassandraRow], notMetRDD: RDD[CassandraRow]): RDD[CassandraRow] = {
    return exceptionRDD.subtract(notMetRDD)
  }

  def getNotMetWithoutException(notMetRDD: RDD[CassandraRow], exceptionRDD: RDD[CassandraRow]): RDD[CassandraRow] = {
    return notMetRDD.subtract(exceptionRDD)
  }

  def isIppEqualsEligible: Boolean = {
    if (fileUtility.getProperty("isIPPequalsEligible").equalsIgnoreCase("true")) {
      return true;
    }
    else {
      return false;
    }
  }

  def getIPPString(): String = {
    return str + "1" + "," + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0"
  }

  def saveToWebDM(notEligibleRDD: RDD[CassandraRow], exclusionRDD: RDD[CassandraRow], metRDD: RDD[CassandraRow], exceptionRDD: RDD[CassandraRow], notMetRDD: RDD[CassandraRow], measureId: String): Unit = {
    val hdfs = FileSystem.get(new SparkUtility().getSparkSession().sparkContext.hadoopConfiguration)
    /*  if(hdfs.isDirectory(new org.apache.hadoop.fs.Path(fileUtility.getProperty("file.output.path.log")+"/"+measureId)))
        hdfs.delete(new org.apache.hadoop.fs.Path(fileUtility.getProperty("file.output.path.log")+"/"+measureId),true)*/
    if (hdfs.isDirectory(new org.apache.hadoop.fs.Path(prop.getProperty("measure_computation_output_path") + "/" + measureId)))
      hdfs.delete(new org.apache.hadoop.fs.Path(prop.getProperty("measure_computation_output_path") + "/" + measureId), true)


    notEligibleRDD.map(l => (if(l.isNullAt(0) ) "" else l.columnValues(0)) + "," + (if(l.isNullAt(1) ) "" else l.columnValues(1)) + "," + (if(l.isNullAt(2) ) "" else l.columnValues(2)) + "," + (if(l.isNullAt(3) ) "" else l.columnValues(3)) + "," + (if(l.isNullAt(4) ) "" else l.columnValues(4))  + "," + (if(l.isNullAt(5) ) "" else l.columnValues(5)) + "," + (if(l.isNullAt(6) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(6).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(7) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(7).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(8) ) "" else l.columnValues(8)) + "," + (if(l.isNullAt(9) ) "" else l.columnValues(9)) + "," + (if(l.isNullAt(10) ) "" else l.columnValues(10)) + "," + (if(l.isNullAt(11) ) "" else l.columnValues(11)) + "," + measureId + "," + getNotEligiblePopulationString()).cache().saveAsTextFile(prop.getProperty("measure_computation_output_path") + "/" + measureId + "/notEligible")
    exclusionRDD.map(l => (if(l.isNullAt(0) ) "" else l.columnValues(0)) + "," + (if(l.isNullAt(1) ) "" else l.columnValues(1)) + "," + (if(l.isNullAt(2) ) "" else l.columnValues(2)) + "," + (if(l.isNullAt(3) ) "" else l.columnValues(3)) + "," + (if(l.isNullAt(4) ) "" else l.columnValues(4))  + "," + (if(l.isNullAt(5) ) "" else l.columnValues(5)) + "," + (if(l.isNullAt(6) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(6).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(7) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(7).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(8) ) "" else l.columnValues(8)) + "," + (if(l.isNullAt(9) ) "" else l.columnValues(9)) + "," + (if(l.isNullAt(10) ) "" else l.columnValues(10)) + "," + (if(l.isNullAt(11) ) "" else l.columnValues(11)) + "," + measureId + "," + getExclusionString()).cache().saveAsTextFile(prop.getProperty("measure_computation_output_path") + "/" + measureId + "/exclusion")
    metRDD.map(l => (if(l.isNullAt(0) ) "" else l.columnValues(0)) + "," + (if(l.isNullAt(1) ) "" else l.columnValues(1)) + "," + (if(l.isNullAt(2) ) "" else l.columnValues(2)) + "," + (if(l.isNullAt(3) ) "" else l.columnValues(3)) + "," + (if(l.isNullAt(4) ) "" else l.columnValues(4))  + "," + (if(l.isNullAt(5) ) "" else l.columnValues(5)) + "," + (if(l.isNullAt(6) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(6).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(7) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(7).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(8) ) "" else l.columnValues(8)) + "," + (if(l.isNullAt(9) ) "" else l.columnValues(9)) + "," + (if(l.isNullAt(10) ) "" else l.columnValues(10)) + "," + (if(l.isNullAt(11) ) "" else l.columnValues(11)) + "," + measureId + "," + getMetString()).cache().saveAsTextFile(prop.getProperty("measure_computation_output_path") + "/" + measureId + "/met")
    exceptionRDD.map(l => (if(l.isNullAt(0) ) "" else l.columnValues(0)) + "," + (if(l.isNullAt(1) ) "" else l.columnValues(1)) + "," + (if(l.isNullAt(2) ) "" else l.columnValues(2)) + "," + (if(l.isNullAt(3) ) "" else l.columnValues(3)) + "," + (if(l.isNullAt(4) ) "" else l.columnValues(4))  + "," + (if(l.isNullAt(5) ) "" else l.columnValues(5)) + "," + (if(l.isNullAt(6) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(6).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(7) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(7).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(8) ) "" else l.columnValues(8)) + "," + (if(l.isNullAt(9) ) "" else l.columnValues(9)) + "," + (if(l.isNullAt(10) ) "" else l.columnValues(10)) + "," + (if(l.isNullAt(11) ) "" else l.columnValues(11)) + "," + measureId + "," + getExceptionString()).cache().saveAsTextFile(prop.getProperty("measure_computation_output_path") + "/" + measureId + "/exception")
    notMetRDD.map(l => (if(l.isNullAt(0) ) "" else l.columnValues(0)) + "," + (if(l.isNullAt(1) ) "" else l.columnValues(1)) + "," + (if(l.isNullAt(2) ) "" else l.columnValues(2)) + "," + (if(l.isNullAt(3) ) "" else l.columnValues(3)) + "," + (if(l.isNullAt(4) ) "" else l.columnValues(4))  + "," + (if(l.isNullAt(5) ) "" else l.columnValues(5)) + "," + (if(l.isNullAt(6) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(6).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(7) ) "0000-00-00 00:00:00.000000+0000" else LocalDateTime.parse(l.columnValues(7).toString, DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))+ "," + (if(l.isNullAt(8) ) "" else l.columnValues(8)) + "," + (if(l.isNullAt(9) ) "" else l.columnValues(9)) + "," + (if(l.isNullAt(10) ) "" else l.columnValues(10)) + "," + (if(l.isNullAt(11) ) "" else l.columnValues(11)) + "," + measureId + "," + getNotMetString()).cache().saveAsTextFile(prop.getProperty("measure_computation_output_path")+"/"+measureId+"/notMet")
    //new SparkUtility().getSparkContext().sparkContext.parallelize((Log.logArray)).saveAsTextFile(fileUtility.getProperty("file.output.path.log")+"/"+measureId+"/log")
    hdfs.close()
  }

  def getMetString(): String = {
    return str + "1" + "," + "0" + "," + "1" + "," + "0" + "," + "0" + "," + "0" + "," + "0"
  }

  def getNotMetString(): String = {
    return str + "1" + "," + "0" + "," + "0" + "," + "0" + "," + "1" + "," + "0" + "," + "0"
  }

  def getExceptionString(): String = {
    return str + "1" + "," + "0" + "," + "0" + "," + "1" + "," + "0" + "," + "0" + "," + "0"
  }

  def getNotEligiblePopulationString(): String = {
    return str + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0"
  }

  def getExclusionString(): String = {
    return str + "1" + "," + "1" + "," + "0" + "," + "0" + "," + "0" + "," + "0" + "," + "0"
  }
}