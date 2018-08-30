package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object PatientLabOrder5 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Patient_Lab_Order_M1_Crosswalk_Equals] = {

    val Master_ACEP_CE_Crosswalk = new PostgreUtility().getPostgresTable(spark, "Master_ACEP_CE_Crosswalk")


    val sectionlookupDf = lookupDf.join(Master_ACEP_CE_Crosswalk, lookupDf("PracticeCode") === Master_ACEP_CE_Crosswalk("Code"), "inner")


    // Elements defaut value declarations
    var BldCultre = 0;
    var BldCultre_Date = "";
    var CTScnPrnslSinsus = 0;
    var CTScnPrnslSinsus_Date = "";
    var CTTorso = 0;
    var CTTorso_Date = "";
    var SerLact = 0;
    var SerLact_Date = "";

    import spark.implicits._;
    val dmRdd = sectionlookupDf.map(r => {

      val name = checkElementValue_name(r)
      val flag = checkElementDateCaseUtil(r, "orderdate", "arrivaldate", "enddate")


      if (name == "BldCultre") {
        BldCultre = 1;
        if (flag)
          BldCultre_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          BldCultre_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "CTScnPrnslSinsus") {
        CTScnPrnslSinsus = 1;
        if (flag)
          CTScnPrnslSinsus_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTScnPrnslSinsus_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "CTTorso") {
        CTTorso = 1;
        if (flag)
          CTTorso_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTTorso_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }
      if (name == "SerLact") {
        SerLact = 1;
        if (flag)
          SerLact_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          SerLact_Date = r.getTimestamp(r.fieldIndex("orderdate")).toString
      }


      Patient_Lab_Order_M1_Crosswalk_Equals(r.getString(r.fieldIndex("visituid")), BldCultre, BldCultre_Date, CTScnPrnslSinsus, CTScnPrnslSinsus_Date, CTTorso, CTTorso_Date, SerLact, SerLact_Date)

    }).distinct();

    return dmRdd;
  }


  def checkElementValue_name(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("shortname")))
      return r.getString(r.fieldIndex("shortname"))
    else
      return ""

  }

}

