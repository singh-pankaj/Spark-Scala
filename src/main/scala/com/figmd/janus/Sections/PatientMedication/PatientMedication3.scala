package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.DataMartCreator
import com.figmd.janus.Sections.PatientMedication.PatientMedication2.{CheckDateIfTimeZero, CheckDateLessThanOrEqual}
import com.figmd.janus.util.{ConditionsUtil, Medications_M1_MedCodeCrosswalk, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit

object PatientMedication3 extends ConditionsUtil with Serializable {
  //Medications_M1_MedCodeCrosswalk

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Medications_M1_MedCodeCrosswalk] = {


    val medCodeCrosswalk_2018 = new PostgreUtility().getPostgresTable(spark, "MedCodeCrosswalk_2018")
    lookupDf.createOrReplaceTempView("Medications_M1_MedCodeCrosswalk")

    val df1 = spark.sql("select * from  Medications_M1_MedCodeCrosswalk where cast(startdate as Date) <= date_add(coalesce(cast(enddate as date),cast(arrivaldate as date)),1) " +
      "AND (stopdate IS NULL OR cast(stopdate as date) >= coalesce(cast(enddate as date),cast(arrivaldate as date))  OR stopdate LIKE '%1900%' )")

    val sectiondf1 = df1.join(medCodeCrosswalk_2018, df1("drugname") === medCodeCrosswalk_2018("medication"), "inner").select("visituid", "startdate", "arrivaldate", "enddate", "drugname", "shortname")
    val sectiondf2 = df1.join(medCodeCrosswalk_2018, df1("medicationcode") === medCodeCrosswalk_2018("code1"), "inner").select("visituid", "startdate", "arrivaldate", "enddate", "drugname", "shortname")
    val sectiondf3 = df1.join(medCodeCrosswalk_2018, df1("medicationcode") === medCodeCrosswalk_2018("code"), "inner").select("visituid", "startdate", "arrivaldate", "enddate", "drugname", "shortname")

    val sectionUnion = sectiondf1.union(sectiondf2).union(sectiondf3).distinct()

    import spark.implicits._;
    val section = sectionUnion.map(r => {

      var elements = collection.immutable.Map[Int, String]()

      var AnMefoPh = 0;
      var AnMefoPh_Date = "";
      var Antibiotics_Dis = 0;
      var Antibiotics_Dis_Date = "";
      var Anticoag = 0;
      var Anticoag_Date = "";
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

     /* if(!r.isNullAt(r.fieldIndex("visituid")) && r.getString(r.fieldIndex("visituid")).equals("6BA0B06A-8D74-4E12-AFB6-51622EEA2609")) {
        println(r.getString(r.fieldIndex("visituid"))+">>" + shortname)
      }*/

      if (shortname == "AnMefoPh") {
        AnMefoPh = 1;
        if (flag)
          AnMefoPh_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AnMefoPh_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "Antibiotics_Dis") {
        Antibiotics_Dis = 1;
        if (flag)
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "Anticoag") {
        Anticoag = 1;
        if (flag)
          Anticoag_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Anticoag_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
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

      Medications_M1_MedCodeCrosswalk(r.getString(r.fieldIndex("visituid")), AnMefoPh, AnMefoPh_Date, Antibiotics_Dis, Antibiotics_Dis_Date, Anticoag, Anticoag_Date, AntimcThrp_Medc, AntimcThrp_Medc_Date, IVtPA, IVtPA_Date, prs_war_Med, prs_war_Med_Date, TopPrepr, TopPrepr_Date, ToUsCePh, ToUsCePh_Date)

    }).distinct()


    return section;
  }

  def checkElementValue(r: Row): String = {
    return r.getString(r.fieldIndex("shortname"));

  }

}
