package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.util.{ConditionsUtil, Medications_M_CurrentMedication, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientMedication8 extends ConditionsUtil with Serializable {
//Medications_M_CurrentMedication

  def process(lookupDf: DataFrame,spark:SparkSession): Dataset[Medications_M_CurrentMedication] = {

    val multiTableExtend = new PostgreUtility().getPostgresTable(spark,"MultiTableExtend")

    lookupDf.createOrReplaceTempView("Medications_M2_specific_LessThanEquals")
    multiTableExtend.createOrReplaceTempView("MultiTableExtend")

    val df1 = spark.sql("select * from  Medications_M1_Crosswalk where cast(StartDate as Date) <= date_add(coalesce(cast(EndDate as date),cast(arrivaldate as date)),1) " +
      "AND (StopDate IS NULL OR cast(StopDate as date) >= coalesce(cast(EndDate as date),cast(arrivaldate as date))  OR StopDate IN ('1900-01-01','4700-01-01','2099-01-01','4700-12-31') ) " +
      "AND DrugName is not null and arrivaldate is not null")

    import spark.implicits._;
    val section = df1.map(r => {

      var Crntmed = 0;
      var Crntmed_Date = "";

      val shortname = checkElementValue(r);
      var flag = checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate");


      if (shortname) {
        Crntmed = 1;
        if (flag)
          Crntmed_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Crntmed_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      Medications_M_CurrentMedication(r.getString(r.fieldIndex("visituid")),Crntmed,Crntmed_Date)
    })

    return section;
  }

  def checkElementDate(r: Row): Boolean = {

    //DocumentationDate > TEMP.ArrivalDate or cast(cast(DocumentationDate as datetime)as time)='00:00:00.000'

    if (CheckDateGreaterThanOrEqual(r, "startdate", "arrivaldate") ||
      CheckDateIfTimeZero(r, "startdate"))
      return true;
    else
      return false;
  }


  def checkElementValue(r: Row): Boolean = {
    return !r.isNullAt(r.fieldIndex("drugname"));

  }

}
