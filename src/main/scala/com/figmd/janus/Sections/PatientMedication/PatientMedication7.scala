package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.Sections.PatientMedication.PatientMedication5.{CheckDateIfTimeZero, CheckDateLessThanOrEqual}
import com.figmd.janus.util.{ConditionsUtil, Medications_M2_specific_LessThanEquals, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientMedication7 extends ConditionsUtil with Serializable {
//Medications_M2_specific_LessThanEquals

  def process(lookupDf: DataFrame,spark:SparkSession): Dataset[Medications_M2_specific_LessThanEquals] = {

    val multiTableExtend = new PostgreUtility().getPostgresTable(spark,"MultiTableExtend")

    lookupDf.createOrReplaceTempView("Medications_M2_specific_LessThanEquals")
    multiTableExtend.createOrReplaceTempView("MultiTableExtend")

    val df1 = spark.sql("select * from Medications_M2_specific_LessThanEquals MD " +
      "inner join multiTableExtend m " +
      "ON  (\n((MD.DrugName LIKE '' + m.element1 + '%' OR MD.MedicationGenericName LIKE '' + m.element1 + '%' ) \nOR (MD.DrugName LIKE '' + m.element2 + '%' OR MD.MedicationGenericName LIKE '' + m.element2 + '%' ) \nOR (MD.medicationCode = m.element1 )\n)\n)"+
      "where " +
      "cast(MD.StartDate as Date) < date_add(coalesce(cast(MD.EndDate as date),cast(MD.ArrivalDate as date)),1) " +
      "AND (MD.StopDate IS NULL OR MD.StopDate = '1900-01-01' OR MD.StopDate = '2099-01-01' OR MD.StopDate = '4700-12-31' OR cast(MD.StopDate as date) >= cast(MD.arrivalDate as date)) "+
      "AND m.Groupname in ('MedicationName','Cerner_Medication','medhost_medication','WhitePlains_Medications') "+
      "AND m.value in ('Anticoag','AntiPlatThrp','CrystSep','Epnephrne','IVAntibioticSep','NRTI')")


    import spark.implicits._;
    val section = df1.map(r => {
      var AnMefoPh = 0;
      var AnMefoPh_Date = "";
      var AnPhTh = 0;
      var AnPhTh_Date = "";
      var Antibiotics_Dis = 0;
      var Antibiotics_Dis_Date = "";
      var Antibtc = 0;
      var Antibtc_Date = "";
      var AntimcThrp_Medc = 0;
      var AntimcThrp_Medc_Date = "";
      var IVtPA = 0;
      var IVtPA_Date = "";
      var prs_war_Med = 0;
      var prs_war_Med_Date = "";
      var TopPrepr = 0;
      var TopPrepr_Date = "";
      var ToUsCePh = 0;
      var ToUsCePh_Date = "";

      val shortname = checkElementValue(r);
      var flag = checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate");

      if (shortname == "AnMefoPh") {
        AnMefoPh = 1;
        if (flag)
          AnMefoPh_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AnMefoPh_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "AnPhTh") {
        AnPhTh = 1;
        if (flag)
          AnPhTh_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AnPhTh_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "Antibiotics_Dis") {
        Antibiotics_Dis = 1;
        if (flag)
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "Antibtc") {
        Antibtc = 1;
        if (flag)
          Antibtc_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Antibtc_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "AntimcThrp_Medc") {
        AntimcThrp_Medc = 1;
        if (flag)
          AntimcThrp_Medc_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AntimcThrp_Medc_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "IVtPA") {
        IVtPA = 1;
        if (flag)
          IVtPA_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          IVtPA_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "prs_war_Med") {
        prs_war_Med = 1;
        if (flag)
          prs_war_Med_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          prs_war_Med_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "TopPrepr") {
        TopPrepr = 1;
        if (flag)
          TopPrepr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TopPrepr_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "ToUsCePh") {
        ToUsCePh = 1;
        if (flag)
          ToUsCePh_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ToUsCePh_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }

      Medications_M2_specific_LessThanEquals(r.getString(r.fieldIndex("visituid")),AnMefoPh,AnMefoPh_Date,AnPhTh,AnPhTh_Date,Antibiotics_Dis,Antibiotics_Dis_Date,Antibtc,Antibtc_Date,AntimcThrp_Medc,AntimcThrp_Medc_Date,IVtPA,IVtPA_Date,prs_war_Med,prs_war_Med_Date,TopPrepr,TopPrepr_Date,ToUsCePh,ToUsCePh_Date)
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
