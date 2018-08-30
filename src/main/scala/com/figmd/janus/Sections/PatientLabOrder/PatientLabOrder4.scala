package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object PatientLabOrder4 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Patient_Lab_Order_M1_Crosswalk] = {

    val Master_ACEP_CE_Crosswalk = new PostgreUtility().getPostgresTable(spark, "Master_ACEP_CE_Crosswalk")


    val sectionlookupDf = lookupDf.join(Master_ACEP_CE_Crosswalk, lookupDf("PracticeCode") === Master_ACEP_CE_Crosswalk("Code"), "inner")


    // Elements defaut value declarations
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

    import spark.implicits._;
    val dmRdd = sectionlookupDf.map(r => {

      val name = checkElementValue_name(r)
      val code = checkElementValue_code(r)
      val flag = checkElementDateCaseUtil(r, "orderdate", "arrivaldate", "enddate")


      if (name == "D_Dimr") {
        D_Dimr = 1;
        if (flag)
          D_Dimr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          D_Dimr_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (code == "11524-6") {
        EC12leorstor = 1;
        if (flag)
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "GrAStTe") {
        GrAStTe = 1;
        if (flag)
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (List("6301-6", "34714-6").contains(code)) {
        INR = 1;
        if (flag)
          INR_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          INR_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "LaTefoHy") {
        LaTefoHy = 1;
        if (flag)
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "PregUrnSrm") {
        PregUrnSrm = 1;
        if (flag)
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "PrtrmTme") {
        PrtrmTme = 1;
        if (flag)
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }


      Patient_Lab_Order_M1_Crosswalk(r.getString(r.fieldIndex("visituid")), D_Dimr, D_Dimr_Date, EC12leorstor, EC12leorstor_Date, GrAStTe, GrAStTe_Date, INR, INR_Date, LaTefoHy, LaTefoHy_Date, PregUrnSrm, PregUrnSrm_Date, PrtrmTme, PrtrmTme_Date)
    }).distinct();

    return dmRdd;
  }


  def checkElementValue_name(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("shortname")))
      return r.getString(r.fieldIndex("shortname"))
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

