package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.Sections.PatientMedication.PatientMedication3.{CheckDateIfTimeZero, CheckDateLessThanOrEqual}
import com.figmd.janus.util.{ConditionsUtil, Medications_M1_MedCodeCrosswalk_Equals, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientMedication4 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Medications_M1_MedCodeCrosswalk_Equals] = {
    //Medications_M1_MedCodeCrosswalk_Equals

    val medCodeCrosswalk_2018 = new PostgreUtility().getPostgresTable(spark, "MedCodeCrosswalk_2018")
    lookupDf.createOrReplaceTempView("Medications_M1_MedCodeCrosswalk_Equals")

    val df1 = spark.sql("select * from Medications_M1_MedCodeCrosswalk_Equals " +
      "where cast(startdate as Date) <= date_add(coalesce(cast(enddate as date),cast(arrivaldate as date)),1) " +
      "AND (stopdate IS NULL OR cast(stopdate as date) >= coalesce(cast(enddate as date),cast(arrivaldate as date))  OR stopdate LIKE '%1900%' )")

    val sectiondf1 = df1.join(medCodeCrosswalk_2018, df1("drugname") === medCodeCrosswalk_2018("medication")
      or df1("medicationcode") === medCodeCrosswalk_2018("code")
      or df1("medicationcode") === medCodeCrosswalk_2018("code1"), "inner")
      .select("visituid", "startdate", "arrivaldate", "enddate", "drugname", "shortname")

    import spark.implicits._;
    val section = sectiondf1.map(r => {
      var AntiPlatThrp = 0;
      var AntiPlatThrp_Date = "";
      var CrystSep = 0;
      var CrystSep_Date = "";
      var Epnephrne = 0;
      var Epnephrne_Date = "";
      var IVAntibioticSep = 0;
      var IVAntibioticSep_Date = "";

      val shortname = checkElementValue(r);
      var flag = checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate");


      if (shortname == "AntiPlatThrp") {
        AntiPlatThrp = 1;
        if (flag)
          AntiPlatThrp_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AntiPlatThrp_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "CrystSep") {
        CrystSep = 1;
        if (flag)
          CrystSep_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CrystSep_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "Epnephrne") {
        Epnephrne = 1;
        if (flag)
          Epnephrne_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Epnephrne_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }
      if (shortname == "IVAntibioticSep") {
        IVAntibioticSep = 1;
        if (flag)
          IVAntibioticSep_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          IVAntibioticSep_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }

      Medications_M1_MedCodeCrosswalk_Equals(r.getString(r.fieldIndex("visituid")), AntiPlatThrp,AntiPlatThrp_Date,CrystSep,CrystSep_Date,Epnephrne,Epnephrne_Date,IVAntibioticSep,IVAntibioticSep_Date)

    }).distinct()


    return section;
  }

  def checkElementValue(r: Row): String = {
    return r.getString(r.fieldIndex("shortname"));

  }


}
