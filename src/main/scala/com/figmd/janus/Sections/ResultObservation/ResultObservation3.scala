package com.figmd.janus.Sections.ResultObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, PostgreUtility, Results_Observation_98494, Results_Observation_M}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ResultObservation3 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Results_Observation_M] = {

    val masterCode = spark.table(DataMartCreator.cdr_db_name + ".mastercode")
    val MasterCodeset_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeset_2018")

    val sectiondf = lookupDf.join(masterCode, lookupDf("observationcodeuid") === masterCode("codeuid"), "inner")
      .join(MasterCodeset_2018, masterCode("code") === MasterCodeset_2018("value"), "inner")

    import spark.implicits._
    val section = sectiondf
      .map(r => {

        var DngMechInj_Grp = 0;
        var DngMechInj_Grp_Date = "";
        var DngrsMechInjry = 0;
        var DngrsMechInjry_Date = "";
        var DrgAlIntoxi = 0;
        var DrgAlIntoxi_Date = "";
        var EC12leorstor = 0;
        var EC12leorstor_Date = "";
        var EvTrmHdNck = 0;
        var EvTrmHdNck_Date = "";
        var GrAStTe = 0;
        var GrAStTe_Date = "";
        var INR = 0;
        var INR_Date = "";
        var LiLiEx = 0;
        var LiLiEx_Date = "";
        var PoTrAm = 0;
        var PoTrAm_Date = "";
        var PrtrmTme = 0;
        var PrtrmTme_Date = "";
        var Rh_Negtv = 0;
        var Rh_Negtv_Date = "";
        var Seve = 0;
        var Seve_Date = "";
        var ShTeMeDe = 0;
        var ShTeMeDe_Date = "";
        var Szr_Inj_ = 0;
        var Szr_Inj__Date = "";

        val shortname = element_func(r);
        val code = element_func_code(r);
        var flag = checkElementDateCaseUtil(r, "resultdate", "arrivaldate", "enddate");


        if (shortname == "DngMechInj_Grp") {
          DngMechInj_Grp = 1;
          if (flag)
            DngMechInj_Grp_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            DngMechInj_Grp_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "DngrsMechInjry") {
          DngrsMechInjry = 1;
          if (flag)
            DngrsMechInjry_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            DngrsMechInjry_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "DrgAlIntoxi") {
          DrgAlIntoxi = 1;
          if (flag)
            DrgAlIntoxi_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            DrgAlIntoxi_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (code == "11524-6") {
          EC12leorstor = 1;
          if (flag)
            EC12leorstor_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            EC12leorstor_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "EvTrmHdNck") {
          EvTrmHdNck = 1;
          if (flag)
            EvTrmHdNck_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            EvTrmHdNck_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (code == "11268-0") {
          GrAStTe = 1;
          if (flag)
            GrAStTe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            GrAStTe_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("34714-6","38875-1","46418-0","52129-4","6301-6").contains(code)) {
          INR = 1;
          if (flag)
            INR_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            INR_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (code == "162608008") {
          LiLiEx = 1;
          if (flag)
            LiLiEx_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            LiLiEx_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "PoTrAm") {
          PoTrAm = 1;
          if (flag)
            PoTrAm_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            PoTrAm_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("14979-9","16631-4","3173-2","34571-0","43734-3","46417-2","52122-9","5902-2","5946-9","5964-2").contains(code)) {
          PrtrmTme = 1;
          if (flag)
            PrtrmTme_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            PrtrmTme_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (code == "10404-2") {
          Rh_Negtv = 1;
          if (flag)
            Rh_Negtv_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            Rh_Negtv_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "Seve") {
          Seve = 1;
          if (flag)
            Seve_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            Seve_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "ShTeMeDe") {
          ShTeMeDe = 1;
          if (flag)
            ShTeMeDe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            ShTeMeDe_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (shortname == "Szr_Inj_") {
          Szr_Inj_ = 1;
          if (flag)
            Szr_Inj__Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            Szr_Inj__Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }



        Results_Observation_M(r.getString(r.fieldIndex("visituid")),DngMechInj_Grp,DngMechInj_Grp_Date,DngrsMechInjry,DngrsMechInjry_Date,DrgAlIntoxi,DrgAlIntoxi_Date,EC12leorstor,EC12leorstor_Date,EvTrmHdNck,EvTrmHdNck_Date,GrAStTe,GrAStTe_Date,INR,INR_Date,LiLiEx,LiLiEx_Date,PoTrAm,PoTrAm_Date,PrtrmTme,PrtrmTme_Date,Rh_Negtv,Rh_Negtv_Date,Seve,Seve_Date,ShTeMeDe,ShTeMeDe_Date,Szr_Inj_,Szr_Inj__Date)
      }).distinct()


    return section;
  }

  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("name")))
      return r.getString(r.fieldIndex("name"))
    else
      return "";
  }

  def element_func_code(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("code")))
      return r.getString(r.fieldIndex("code"))
    else
      return "";
  }
}