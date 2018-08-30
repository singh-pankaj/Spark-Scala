package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.Sections.PatientMedication.PatientMedication5.{CheckDateIfTimeZero, CheckDateLessThanOrEqual}
import com.figmd.janus.util.{ConditionsUtil, Medications_M2_Specific_Equals_Replace, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientMedication6 extends ConditionsUtil with Serializable {
  //Medications_M2_Specific_Equals_Replace


  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Medications_M2_Specific_Equals_Replace] = {


    val multiTableExtend = new PostgreUtility().getPostgresTable(spark, "MultiTableExtend")

    lookupDf.createOrReplaceTempView("Medications_M2_Specific_Equals_Replace")
    multiTableExtend.createOrReplaceTempView("MultiTableExtend")

    val df1 = spark.sql("select * from Medications_M2_Specific_Equals_Replace MD " +
      "inner join multiTableExtend m " +
      "ON (" +
      "( (replace(replace(MD.DrugName,'[',''),']','') LIKE '' + replace(replace(m.element1,'[',''),']','') + '%' OR MD.MedicationGenericName LIKE '' + replace(replace(m.element1,'[',''),']','') + '%' ) " +
      "OR (replace(replace(MD.DrugName,'[',''),']','') LIKE '' + replace(replace(m.element2,'[',''),']','') + '%' OR MD.MedicationGenericName LIKE '' + replace(replace(m.element2,'[',''),']','') + '%' ) " +
      "OR (MD.medicationCode = m.element1 ) " +
      ")) " +
      "where " +
      "cast(MD.StartDate as Date) between cast(MD.startdate as date) and date_add(coalesce(cast(MD.EndDate as date),cast(MD.startdate as date)),1) " +
      "AND (MD.StopDate IS NULL OR MD.StopDate = '1900-01-01' OR MD.StopDate = '2099-01-01' OR MD.StopDate = '4700-12-31' OR cast(MD.StopDate as date) >= cast(MD.arrivalDate as date)) " +
      "AND m.Groupname in ('MedicationName','Cerner_Medication','medhost_medication','WhitePlains_Medications') " +
      "AND m.value in ('Anticoag','AntiPlatThrp','CrystSep','Epnephrne','IVAntibioticSep','NRTI')" +
      "")
      .select("visituid", "startdate", "arrivaldate", "enddate", "value")

    import spark.implicits._;
    val section = df1.map(r => {
      var CrystSep = 0;
      var CrystSep_Date = "";
      var IVAntibioticSep = 0;
      var IVAntibioticSep_Date = "";

      val shortname = checkElementValue(r);
      var flag = checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate");

      if (shortname == "CrystSep") {
        CrystSep = 1;
        if (flag)
          CrystSep_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CrystSep_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "IVAntibioticSep") {
        IVAntibioticSep = 1;
        if (flag)
          IVAntibioticSep_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          IVAntibioticSep_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }

      Medications_M2_Specific_Equals_Replace(r.getString(r.fieldIndex("visituid")),CrystSep,CrystSep_Date,IVAntibioticSep,IVAntibioticSep_Date)
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


  def checkElementValue(r: Row): String = {
    return r.getString(r.fieldIndex("value"));

  }

}
