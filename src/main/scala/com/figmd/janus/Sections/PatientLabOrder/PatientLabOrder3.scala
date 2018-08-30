package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object PatientLabOrder3 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Patient_Lab_Order_M_Procedure_Crosswalk] = {

    val MultiTableExtend = new PostgreUtility().getPostgresTable(spark, "MultiTableExtend")

    val mastercodesystem = spark.sql("select * from " + DataMartCreator.cdr_db_name + ".mastercodesystem")

    val lookup2 = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate, enddate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientProcedure a " +
      "inner join " + DataMartCreator.cdr_db_name + ".visit b on b.patientuid=a.patientuid " +
      "AND cast(a.effectivedate as Date) between cast(b.startdate as date) and date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1) ")

    val sectionlookupDf1 = lookupDf.join(MultiTableExtend, lookupDf("Practicecode") === MultiTableExtend("Element1"), "inner")
      .select(lookupDf("visituid"), lookupDf("orderdate") as ("EffectiveDate"), lookupDf("arrivaldate"), lookupDf("enddate"), lookupDf("Practicecode") as ("code"), MultiTableExtend("value"))

    val sectionlookupDf2 = lookup2.join(MultiTableExtend, lookup2("Practicecode") === MultiTableExtend("Element1"), "inner")
      .select(lookup2("visituid"), lookup2("EffectiveDate"), lookup2("arrivaldate"), lookup2("enddate"), lookup2("Practicecode") as ("code"), MultiTableExtend("value"))

    val sectionlookupDf3 = lookupDf.join(MultiTableExtend, lookupDf("Description") === MultiTableExtend("Element2"), "inner")
      .select(lookupDf("visituid"), lookupDf("orderdate") as ("EffectiveDate"), lookupDf("arrivaldate"), lookupDf("enddate"), lookupDf("Practicecode") as ("code"), MultiTableExtend("value"))

    val sectionlookupDf4 = lookup2.join(MultiTableExtend, lookup2("PracticeDescription") === MultiTableExtend("Element2"), "inner")
      .select(lookup2("visituid"), lookup2("EffectiveDate"), lookup2("arrivaldate"), lookup2("enddate"), lookup2("Practicecode") as ("code"), MultiTableExtend("value"))

    val sectionUnion = sectionlookupDf1.union(sectionlookupDf2)
      .union(sectionlookupDf3).union(sectionlookupDf4)


    // Elements defaut value declarations
    var ActParThrmplstnTme = 0;
    var ActParThrmplstnTme_Date = "";
    var BldCultre = 0;
    var BldCultre_Date = "";
    var Creatnine = 0;
    var Creatnine_Date = "";
    var CT_Abd_Pel = 0;
    var CT_Abd_Pel_Date = "";
    var CTParnsn = 0;
    var CTParnsn_Date = "";
    var CTTorso = 0;
    var CTTorso_Date = "";
    var EC12leorstor = 0;
    var EC12leorstor_Date = "";
    var GlomFiltRate = 0;
    var GlomFiltRate_Date = "";
    var GrAStTe = 0;
    var GrAStTe_Date = "";
    var HdCT = 0;
    var HdCT_Date = "";
    var HdCtPrfrmd = 0;
    var HdCtPrfrmd_Date = "";
    var HECTPE = 0;
    var HECTPE_Date = "";
    var INR = 0;
    var INR_Date = "";
    var KUB_Xray_AP = 0;
    var KUB_Xray_AP_Date = "";
    var LaTefoHy = 0;
    var LaTefoHy_Date = "";
    var PregUrnSrm = 0;
    var PregUrnSrm_Date = "";
    var PrtrmTme = 0;
    var PrtrmTme_Date = "";
    var RptSrmLc = 0;
    var RptSrmLc_Date = "";
    var SerLact = 0;
    var SerLact_Date = "";
    var TbcCesCon = 0;
    var TbcCesCon_Date = "";
    var TobcoUseScrn = 0;
    var TobcoUseScrn_Date = "";
    var TobcUTC = 0;
    var TobcUTC_Date = "";
    var ToUs_1 = 0;
    var ToUs_1_Date = "";
    var ToUsCeCo = 0;
    var ToUsCeCo_Date = "";
    var ToUsCePh = 0;
    var ToUsCePh_Date = "";
    var ToUsSc = 0;
    var ToUsSc_Date = "";
    var TrnsVgUS = 0;
    var TrnsVgUS_Date = "";
    var Ultra_APF = 0;
    var Ultra_APF_Date = "";
    var UltrTransAb = 0;
    var UltrTransAb_Date = "";

    import spark.implicits._;

    val dmRdd = sectionUnion.map(r => {

      val name = checkElementValue_name(r)
      val flag = checkElementDateCaseUtil(r, "EffectiveDate", "arrivaldate", "enddate")


      if (name == "ActParThrmplstnTme") {
        ActParThrmplstnTme = 1;
        if (flag)
          ActParThrmplstnTme_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ActParThrmplstnTme_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "BldCultre") {
        BldCultre = 1;
        if (flag)
          BldCultre_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          BldCultre_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "Creatnine") {
        Creatnine = 1;
        if (flag)
          Creatnine_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Creatnine_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CT_Abd_Pel") {
        CT_Abd_Pel = 1;
        if (flag)
          CT_Abd_Pel_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CT_Abd_Pel_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CTParnsn") {
        CTParnsn = 1;
        if (flag)
          CTParnsn_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTParnsn_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "CTTorso") {
        CTTorso = 1;
        if (flag)
          CTTorso_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          CTTorso_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "EC12leorstor") {
        EC12leorstor = 1;
        if (flag)
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          EC12leorstor_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "GlomFiltRate") {
        GlomFiltRate = 1;
        if (flag)
          GlomFiltRate_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          GlomFiltRate_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "GrAStTe") {
        GrAStTe = 1;
        if (flag)
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          GrAStTe_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "HdCT") {
        HdCT = 1;
        if (flag)
          HdCT_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          HdCT_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "HdCtPrfrmd") {
        HdCtPrfrmd = 1;
        if (flag)
          HdCtPrfrmd_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          HdCtPrfrmd_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "HECTPE") {
        HECTPE = 1;
        if (flag)
          HECTPE_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          HECTPE_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "INR") {
        INR = 1;
        if (flag)
          INR_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          INR_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "KUB_Xray_AP") {
        KUB_Xray_AP = 1;
        if (flag)
          KUB_Xray_AP_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          KUB_Xray_AP_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "LaTefoHy") {
        LaTefoHy = 1;
        if (flag)
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          LaTefoHy_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "PregUrnSrm") {
        PregUrnSrm = 1;
        if (flag)
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PregUrnSrm_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "PrtrmTme") {
        PrtrmTme = 1;
        if (flag)
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          PrtrmTme_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "RptSrmLc") {
        RptSrmLc = 1;
        if (flag)
          RptSrmLc_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          RptSrmLc_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "SerLact") {
        SerLact = 1;
        if (flag)
          SerLact_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          SerLact_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "TbcCesCon") {
        TbcCesCon = 1;
        if (flag)
          TbcCesCon_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TbcCesCon_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("ToUs_1", "ToUsSc", "ToUsCeCo").contains(name)) {
        TobcoUseScrn = 1;
        if (flag)
          TobcoUseScrn_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TobcoUseScrn_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("ToUs_1", "ToUsSc", "ToUsCeCo").contains(name)) {
        TobcUTC = 1;
        if (flag)
          TobcUTC_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TobcUTC_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("ToUs_1", "ToUsSc", "ToUsCeCo").contains(name)) {
        ToUs_1 = 1;
        if (flag)
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "ToUsCeCo") {
        ToUsCeCo = 1;
        if (flag)
          ToUsCeCo_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ToUsCeCo_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "ToUsCePh") {
        ToUsCePh = 1;
        if (flag)
          ToUsCePh_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ToUsCePh_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (List("ToUs_1", "ToUsSc", "ToUsCeCo").contains(name)) {
        ToUsSc = 1;
        if (flag)
          ToUsSc_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          ToUsSc_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "TrnsVgUS") {
        TrnsVgUS = 1;
        if (flag)
          TrnsVgUS_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          TrnsVgUS_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "Ultra_APF") {
        Ultra_APF = 1;
        if (flag)
          Ultra_APF_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Ultra_APF_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }
      if (name == "UltrTransAb") {
        Ultra_APF = 1;
        if (flag)
          UltrTransAb_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          UltrTransAb_Date = r.getTimestamp(r.fieldIndex("EffectiveDate")).toString
      }


      Patient_Lab_Order_M_Procedure_Crosswalk(r.getString(r.fieldIndex("visituid")), ActParThrmplstnTme, ActParThrmplstnTme_Date, BldCultre, BldCultre_Date, Creatnine, Creatnine_Date, CT_Abd_Pel, CT_Abd_Pel_Date, CTParnsn, CTParnsn_Date, CTTorso, CTTorso_Date, EC12leorstor, EC12leorstor_Date, GlomFiltRate, GlomFiltRate_Date, GrAStTe, GrAStTe_Date, HdCT, HdCT_Date, HdCtPrfrmd, HdCtPrfrmd_Date, HECTPE, HECTPE_Date, INR, INR_Date, KUB_Xray_AP, KUB_Xray_AP_Date, LaTefoHy, LaTefoHy_Date, PregUrnSrm, PregUrnSrm_Date, PrtrmTme, PrtrmTme_Date, RptSrmLc, RptSrmLc_Date, SerLact, SerLact_Date, TbcCesCon, TbcCesCon_Date, TobcoUseScrn, TobcoUseScrn_Date, TobcUTC, TobcUTC_Date, ToUs_1, ToUs_1_Date, ToUsCeCo, ToUsCeCo_Date, ToUsCePh, ToUsCePh_Date, ToUsSc, ToUsSc_Date, TrnsVgUS, TrnsVgUS_Date, Ultra_APF, Ultra_APF_Date, UltrTransAb, UltrTransAb_Date)
    }).distinct();

    return dmRdd;
  }


  def checkElementValue_name(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("value")))
      return r.getString(r.fieldIndex("value"))
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

