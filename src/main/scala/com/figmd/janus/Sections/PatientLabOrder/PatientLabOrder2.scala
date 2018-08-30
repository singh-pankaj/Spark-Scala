package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object PatientLabOrder2 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Patient_Lab_Order_M_LessThanEquals] = {

    val LonicToCPTCrosswalk = new PostgreUtility().getPostgresTable(spark, "LonicToCPTCrosswalk")
    val MasterCodeSet_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeSet_2018")


    val mastercodesystem = spark.sql("select * from "+DataMartCreator.cdr_db_name+".mastercodesystem")

    val lookup2 = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate, enddate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientProcedure a " +
      "inner join " + DataMartCreator.cdr_db_name + ".visit b on b.patientuid=a.patientuid " +
      "AND cast(a.effectivedate as Date) <= date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1) ")

    val sectionlookupDf = lookupDf.join(MasterCodeSet_2018, lookupDf("Practicecode") === MasterCodeSet_2018("value"), "inner")
      .join(mastercodesystem, MasterCodeSet_2018("codesystem") === mastercodesystem("externalid"), "left")
      .select(lookupDf("visituid"), lookupDf("orderdate")as("EffectiveDate"), lookupDf("arrivaldate"), lookupDf("enddate"), lookupDf("Practicecode") as("code"), MasterCodeSet_2018("name"))

    val sectiondf2 = lookup2.join(LonicToCPTCrosswalk, lookup2("PracticeCode") === LonicToCPTCrosswalk("cpt"), "inner")
      .join(MasterCodeSet_2018, MasterCodeSet_2018("value") === LonicToCPTCrosswalk("lonic"), "inner")
      .join(mastercodesystem, MasterCodeSet_2018("codesystem") === mastercodesystem("externalid"), "inner")
      .filter(r => CheckStringEqual(r, "ExternalID", "4"))
      .select(lookup2("visituid"), lookup2("EffectiveDate"), lookup2("arrivaldate"), lookup2("enddate"), LonicToCPTCrosswalk("lonic")as("code"), MasterCodeSet_2018("name"))

    val sectionUnion = sectionlookupDf.union(sectiondf2)


    // Elements defaut value declarations
    var CTScnPrnslSinsus = 0;
    var CTScnPrnslSinsus_Date = "";
    var D_Dimr = 0;
    var D_Dimr_Date = "";
    var EC12leorstor = 0;
    var EC12leorstor_Date = "";
    var GrAStTe = 0;
    var GrAStTe_Date = "";
    var INR = 0;
    var INR_Date = "";
    var LaTefoHy = 0;
    var LaTefoHy_Date = "";
    var PregUrnSrm = 0;
    var PregUrnSrm_Date = "";
    var PrtrmTme = 0;
    var PrtrmTme_Date = "";
    var Rh_immuneOdrd = 0;
    var Rh_immuneOdrd_Date = "";
    var Rh_immuneOrdr = 0;
    var Rh_immuneOrdr_Date = "";
    var TATV_performd = 0;
    var TATV_performd_Date = "";
    var TATV_prformd = 0;
    var TATV_prformd_Date = "";
    var TrnsVgUS = 0;
    var TrnsVgUS_Date = "";
    var UltrTransAb = 0;
    var UltrTransAb_Date = "";

    import spark.implicits._;

    val dmRdd = sectionUnion.map(r => {

      val name = checkElementValue_name(r)
      val code = checkElementValue_code(r)
      val flag = checkElementDateCaseUtil(r, "EffectiveDate", "arrivaldate", "enddate")


      if (name == "CTScnPrnslSinsus") {
        CTScnPrnslSinsus = 1;
        if (flag)
          CTScnPrnslSinsus_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTScnPrnslSinsus_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "PregUrnSrm") {
        PregUrnSrm = 1;
        if (flag)
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "TATV_performd") {
        TATV_performd = 1;
        if (flag)
          TATV_performd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TATV_performd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "TATV_prformd") {
        TATV_prformd = 1;
        if (flag)
          TATV_prformd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TATV_prformd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "TrnsVgUS") {
        TrnsVgUS = 1;
        if (flag)
          TrnsVgUS_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TrnsVgUS_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "UltrTransAb") {
        UltrTransAb = 1;
        if (flag)
          UltrTransAb_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          UltrTransAb_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "3246-6") {
        D_Dimr = 1;
        if (flag)
          D_Dimr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          D_Dimr_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "11524-6") {
        EC12leorstor = 1;
        if (flag)
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "11268-0") {
        GrAStTe = 1;
        if (flag)
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("6301-6","34714-6").contains(code)) {
        INR = 1;
        if (flag)
          INR_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          INR_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "24321-2") {
        LaTefoHy = 1;
        if (flag)
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("14979-9","46417-2","5964-2").contains(code)) {
        PrtrmTme = 1;
        if (flag)
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "10404-2") {
        Rh_immuneOdrd = 1;
        if (flag)
          Rh_immuneOdrd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Rh_immuneOdrd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "10404-2") {
        Rh_immuneOrdr = 1;
        if (flag)
          Rh_immuneOrdr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Rh_immuneOrdr_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }

      Patient_Lab_Order_M_LessThanEquals(r.getString(r.fieldIndex("visituid")),CTScnPrnslSinsus,CTScnPrnslSinsus_Date,D_Dimr,D_Dimr_Date,EC12leorstor,EC12leorstor_Date,GrAStTe,GrAStTe_Date,INR,INR_Date,LaTefoHy,LaTefoHy_Date,PregUrnSrm,PregUrnSrm_Date,PrtrmTme,PrtrmTme_Date,Rh_immuneOdrd,Rh_immuneOdrd_Date,Rh_immuneOrdr,Rh_immuneOrdr_Date,TATV_performd,TATV_performd_Date,TATV_prformd,TATV_prformd_Date,TrnsVgUS,TrnsVgUS_Date,UltrTransAb,UltrTransAb_Date)
    }).distinct();

    return dmRdd;
  }


  def checkElementValue_name(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("name")))
      return r.getString(r.fieldIndex("name"))
    else
      return ""

  }

  def checkElementValue_code(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("code")))
      return r.getString(r.fieldIndex("code"))
    else
      return ""

  }
}

