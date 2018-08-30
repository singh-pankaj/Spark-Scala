package com.figmd.janus.Sections.SocialHistoryObservation


import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Properties

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.matching.Regex


object SocialHistoryObservation1 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Social_History_Observation_M_TobaccoScreening_New_LessThanEqual_case] = {

    val masterCodeDf = new MasterCode().getMasterCode(spark,"master");

    val lookupDf1 = lookupDf.join(masterCodeDf,lookupDf("mastersocialhistorytypeuid")===masterCodeDf("masteruid"),"inner").filter(r => EqualCondition(r, "code", "229819007"))


    // Elements defaut value declarations
    var ToUsSc = 0;
    var TobcoUseScrn = 0;
    var ToUsSc_date = "";
    var TobcoUseScrn_date = "";

    import spark.implicits._;
    val dmRdd = lookupDf1.map(r => {

      if (checkElementValue(r)) {

        ToUsSc = 1;
        TobcoUseScrn = 1;

        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          ToUsSc_date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          TobcoUseScrn_date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        } else {
          ToUsSc_date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
          TobcoUseScrn_date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }

      }
      Social_History_Observation_M_TobaccoScreening_New_LessThanEqual_case(r.getString(r.fieldIndex("visituid")), ToUsSc, ToUsSc_date, TobcoUseScrn, TobcoUseScrn_date)
    }
    ).distinct();

    return dmRdd;
  }

  def checkElementValue(r: Row): Boolean = {
    if (InCondition(r, "socialhistoryobservedvalue", "Never,Not Asked,Passive,Quit,Never Smoker,Heavy Tobacco Smoker,Light Tobacco Smoker,Current Every Day Smoker," +
      "Current Some Day Smoker,Former Smoker,Smoker,Current Status Unknown,Unknown If Ever Smoked,Passive Smoke Exposure - Never Smoker,No,Yes," +
      "not have any smokeless tobacco history,no tobacco history,Nonsmoker,Personal history of nicotine dependence")
      ||
      InCondition(r, "socialhistorytypetext", "Non smoker,Smoker,Never smoked,Prior smoking History,Unknown,Active Smoker,No,Yes,not have any smokeless tobacco history," +
        "no tobacco history,Nonsmoker,Personal history of nicotine dependence")
      ||
      LikeCondition(r, "socialhistoryobservedvalue", "Doesn%smoke")
      ||
      LikeCondition(r, "socialhistorytypetext", "Doesn%smoke")
      ||
      !CheckNull(r, "mastersocialhistorystatusuid")
      ||
      !CheckNull(r, "socialhistoryobservedvalue")
    ) {
      return true;
    } else
      return false;
  }
}

