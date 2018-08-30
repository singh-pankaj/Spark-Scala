package com.figmd.janus.util

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.util.Date
import com.figmd.janus.DataMartCreator.prop


class DateUtility extends Serializable {


  val fileUtility = new FileUtility();
  var startDate = prop.getProperty("quarterStartDate")
  var endDate = prop.getProperty("quarterEndDate")
  var dateFormat = fileUtility.getProperty("date.format");
  var dateTimeFormat = fileUtility.getProperty("date.time.format");

  final var SIMPLE_DATE_FORMAT = new SimpleDateFormat(dateFormat)
  @transient lazy val DATE_FORMAT = DateTimeFormatter.ofPattern(dateFormat)
  @transient lazy val DATE_TIME_FORMAT = DateTimeFormatter.ofPattern(dateTimeFormat)
  val now = LocalDateTime.now().format(DATE_TIME_FORMAT)
  val today = LocalDateTime.now().format(DATE_FORMAT)




  def getStart(): Date = {
    var start_date = SIMPLE_DATE_FORMAT.parse(startDate);
    return start_date;
  }

  def dateParse(input : String): Date={
    return SIMPLE_DATE_FORMAT.parse(input)
  }

  def getEndDate(): Date = {
    var end_date = SIMPLE_DATE_FORMAT.parse(endDate);
    println(end_date)
    return end_date;
  }


  def getDateDiff(startDate: String, endDate: String): Boolean = {

    val s1 = LocalDate.parse(startDate);
    val s2 = LocalDate.parse(endDate)
    var dateDiff = ChronoUnit.DAYS.between(s1, s2)
    if (dateDiff >= 30) {
      return true;
    }
    else {
      return false;
    }
  }


  def getDateDiff1(startDate: String, endDate: String): Boolean = {


    val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
    val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
    import java.util.concurrent.TimeUnit
    val diffInMillies = Math.abs(s1.getTime - s2.getTime)
    val dateDiff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)
    if (dateDiff >= 30) {
      return true;
    }
    else {
      return false;
    }
  }




  def chkDateEqual(firstDate: String,compareDate :String): Boolean ={
    val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
    val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
    if (s1.equals(s2)) {
      return true;
    }
    else {
      return false;
    }


  }


  def getAge(birthDt: String): Long = {

    val dateString = birthDt
    val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy")
    val date = LocalDate.parse(dateString, formatter)

    var birthDate = LocalDate.of(date.getYear, date.getMonth, date.getDayOfMonth)
    var todayDate = LocalDate.now()
    var age = ChronoUnit.YEARS.between(birthDate, todayDate)

    return age

  }


  def getAge(dob: String,arrival_date :String): Double ={
    val s1 = SIMPLE_DATE_FORMAT.parse(dob);
    val s2 = SIMPLE_DATE_FORMAT.parse(arrival_date)
    import java.util.concurrent.TimeUnit
    val diffInMillies = Math.abs(s1.getTime - s2.getTime)
    return TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)/365.23076923074

  }


  def getDaysDiff(startDate: String,endDate :String): Long ={
    val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
    val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
    import java.util.concurrent.TimeUnit
    val diffInMillies = Math.abs(s1.getTime - s2.getTime)
    return TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS)

  }


  def getDateDiffGreaterOrEqualHours(startDate: String, endDate: String): Boolean = {


    val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
    val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
    import java.util.concurrent.TimeUnit
    val diffInMillies = Math.abs(s1.getTime - s2.getTime)
    val dateDiff = TimeUnit.HOURS.convert(diffInMillies, TimeUnit.MILLISECONDS)
    if (dateDiff >= 0) {
      return true;
    }
    else {
      return false;
    }
  }

    def getDateDiffHourslessthan(startDate: String, endDate: String): Boolean = {


      val s1 = SIMPLE_DATE_FORMAT.parse(startDate);
      val s2 = SIMPLE_DATE_FORMAT.parse(endDate)
      import java.util.concurrent.TimeUnit
      val diffInMillies = Math.abs(s1.getTime - s2.getTime)
      val dateDiff = TimeUnit.HOURS.convert(diffInMillies, TimeUnit.MILLISECONDS)
      if (dateDiff < 2) {
        return true;
      }
      else {
        return false;
      }
    }




}
