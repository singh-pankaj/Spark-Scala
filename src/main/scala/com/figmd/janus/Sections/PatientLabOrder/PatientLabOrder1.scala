package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object PatientLabOrder1 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Patient_Lab_Order_M_EqualsALTER] = {

    val LonicToCPTCrosswalk = new PostgreUtility().getPostgresTable(spark, "LonicToCPTCrosswalk")
    val MasterCodeSet_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeSet_2018")


    val mastercodesystem = spark.sql("select * from "+DataMartCreator.cdr_db_name+".mastercodesystem")

    val lookup2 = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate, enddate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientProcedure a " +
      "inner join " + DataMartCreator.cdr_db_name + ".visit b on b.patientuid=a.patientuid " +
      "AND cast(a.effectivedate as Date) between cast(b.startdate as date) and date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1) ")

    val sectionlookupDf = lookupDf.join(MasterCodeSet_2018, lookupDf("Practicecode") === MasterCodeSet_2018("value"), "inner")
      .join(mastercodesystem, MasterCodeSet_2018("codesystem") === mastercodesystem("externalid"), "inner")
      .select(lookupDf("visituid"), lookupDf("orderdate")as("EffectiveDate"), lookupDf("arrivaldate"), lookupDf("enddate"), lookupDf("Practicecode") as("code"), MasterCodeSet_2018("name"))

    val sectiondf2 = lookup2.join(LonicToCPTCrosswalk, lookup2("PracticeCode") === LonicToCPTCrosswalk("cpt"), "inner")
      .join(MasterCodeSet_2018, MasterCodeSet_2018("value") === LonicToCPTCrosswalk("lonic"), "inner")
      .join(mastercodesystem, MasterCodeSet_2018("codesystem") === mastercodesystem("externalid"), "inner")
      .filter(r => CheckStringEqual(r, "ExternalID", "4"))
      .select(lookup2("visituid"), lookup2("EffectiveDate"), lookup2("arrivaldate"), lookup2("enddate"), LonicToCPTCrosswalk("lonic")as("code"), MasterCodeSet_2018("name"))

    val sectionUnion = sectionlookupDf.union(sectiondf2)


    // Elements defaut value declarations
    var AbdCT = 0;
    var AbdCT_Date = "";
    var AdvBrnImging = 0;
    var AdvBrnImging_Date = "";
    var BldCultre = 0;
    var BldCultre_Date = "";
    var CT_Abd_Pel = 0;
    var CT_Abd_Pel_Date = "";
    var CT_E1 = 0;
    var CT_E1_Date = "";
    var CT_PE = 0;
    var CT_PE_Date = "";
    var CTHd = 0;
    var CTHd_Date = "";
    var CTTorso = 0;
    var CTTorso_Date = "";
    var GlomFiltRate = 0;
    var GlomFiltRate_Date = "";
    var InsSpeInd = 0;
    var InsSpeInd_Date = "";
    var KUB_Xray_AP = 0;
    var KUB_Xray_AP_Date = "";
    var LeukEstUrn = 0;
    var LeukEstUrn_Date = "";
    var LeukUrine = 0;
    var LeukUrine_Date = "";
    var MeasUriOut = 0;
    var MeasUriOut_Date = "";
    var MRI_APF = 0;
    var MRI_APF_Date = "";
    var NitrUrine = 0;
    var NitrUrine_Date = "";
    var SerLact = 0;
    var SerLact_Date = "";
    var Ultra_APF = 0;
    var Ultra_APF_Date = "";
    var X_Ray_QI = 0;
    var X_Ray_QI_Date = "";

    import spark.implicits._;
    val dmRdd = sectionUnion.map(r => {

      val name = checkElementValue_name(r)
      val code = checkElementValue_code(r)
      val flag = checkElementDateCaseUtil(r, "EffectiveDate", "arrivaldate", "enddate")


      if (name == "AbdCT") {
        AbdCT = 1;
        if (flag)
          AbdCT_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AbdCT_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "AdvBrnImging") {
        AdvBrnImging = 1;
        if (flag)
          AdvBrnImging_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          AdvBrnImging_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "BldCultre") {
        BldCultre = 1;
        if (flag)
          BldCultre_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          BldCultre_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CT_E1") {
        CT_E1 = 1;
        if (flag)
          CT_E1_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CT_E1_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CT_PE") {
        CT_PE = 1;
        if (flag)
          CT_PE_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CT_PE_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CTHd") {
        CTHd = 1;
        if (flag)
          CTHd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTHd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CTTorso") {
        CTTorso = 1;
        if (flag)
          CTTorso_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTTorso_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "GlomFiltRate") {
        GlomFiltRate = 1;
        if (flag)
          GlomFiltRate_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          GlomFiltRate_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "InsSpeInd") {
        InsSpeInd = 1;
        if (flag)
          InsSpeInd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          InsSpeInd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "LeukEstUrn") {
        LeukEstUrn = 1;
        if (flag)
          LeukEstUrn_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          LeukEstUrn_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "MeasUriOut") {
        MeasUriOut = 1;
        if (flag)
          MeasUriOut_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          MeasUriOut_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "NitrUrine") {
        NitrUrine = 1;
        if (flag)
          NitrUrine_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          NitrUrine_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "SerLact") {
        SerLact = 1;
        if (flag)
          SerLact_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          SerLact_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "X_Ray_QI") {
        X_Ray_QI = 1;
        if (flag)
          X_Ray_QI_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          X_Ray_QI_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("44115-4","30615-9").contains(code)) {
        CT_Abd_Pel = 1;
        if (flag)
          CT_Abd_Pel_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CT_Abd_Pel_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "28561-9") {
        KUB_Xray_AP = 1;
        if (flag)
          KUB_Xray_AP_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          KUB_Xray_AP_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "5799-2") {
        LeukUrine = 1;
        if (flag)
          LeukUrine_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          LeukUrine_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "24556-3") {
        MRI_APF = 1;
        if (flag)
          MRI_APF_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          MRI_APF_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (code == "24531-6") {
        Ultra_APF = 1;
        if (flag)
          Ultra_APF_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Ultra_APF_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }

      Patient_Lab_Order_M_EqualsALTER(r.getString(r.fieldIndex("visituid")), AbdCT, AbdCT_Date, AdvBrnImging, AdvBrnImging_Date, BldCultre, BldCultre_Date, CT_Abd_Pel, CT_Abd_Pel_Date, CT_E1, CT_E1_Date, CT_PE, CT_PE_Date, CTHd, CTHd_Date, CTTorso, CTTorso_Date, GlomFiltRate, GlomFiltRate_Date, InsSpeInd, InsSpeInd_Date, KUB_Xray_AP, KUB_Xray_AP_Date, LeukEstUrn, LeukEstUrn_Date, LeukUrine, LeukUrine_Date, MeasUriOut, MeasUriOut_Date, MRI_APF, MRI_APF_Date, NitrUrine, NitrUrine_Date, SerLact, SerLact_Date, Ultra_APF, Ultra_APF_Date, X_Ray_QI, X_Ray_QI_Date)
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

