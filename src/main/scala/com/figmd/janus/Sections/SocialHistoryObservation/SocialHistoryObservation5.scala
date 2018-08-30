package com.figmd.janus.Sections.SocialHistoryObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.{ConditionsUtil, Social_History_Observation_M2_TobaccoUser_New, Social_History_Observation_M2_TobaccoUser_New_Crosswalk}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SocialHistoryObservation5 extends ConditionsUtil with Serializable {
//Social History Observation_M2_TobaccoUser_New_Crosswalk

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Social_History_Observation_M2_TobaccoUser_New_Crosswalk] = {

    val Master_ACEP_CE_Crosswalk = new MasterCode().getMasterCode(spark,"Master_ACEP_CE_Crosswalk");
    val lookupDf1 = lookupDf.join(Master_ACEP_CE_Crosswalk,lookupDf("socialhistorytypecode")===Master_ACEP_CE_Crosswalk("code"),"inner").filter(r => EqualCondition(r, "code", "229819007"))


    // Elements defaut value declarations
    var CurntTobcNUsr = 0;
    var TobNU = 0;
    var TobcUTC = 0;
    var ToUs_1 = 0;
    var CurntTobcNUsr_Date = "";
    var TobNU_Date = "";
    var ToUs_1_Date = "";
    var TobcUTC_Date = "";

    import spark.implicits._;
    val dmRdd = lookupDf1.map(r => {

      val query_result = checkElementValue(r);

      if (query_result == 1 || query_result == 7) {
        CurntTobcNUsr = 1;
        TobNU = 1;
        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          CurntTobcNUsr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          TobNU_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          CurntTobcNUsr_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
          TobNU_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }
      }
      if (query_result == 6) {
        TobcUTC = 1;
        ToUs_1 = 1;
        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          TobcUTC_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
          TobcUTC_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }
      }


      Social_History_Observation_M2_TobaccoUser_New_Crosswalk(r.getString(r.fieldIndex("visituid")), CurntTobcNUsr, CurntTobcNUsr_Date, TobNU, TobNU_Date, TobcUTC, TobcUTC_Date, ToUs_1, ToUs_1_Date)
    }
    ).distinct();

    return dmRdd;

  }

  def checkElementValue(r: Row): Int = {
    if (InCondition(r, "code", "392521001,73425007")) {
      return 1;
    }
    else if (InCondition(r, "code", "55561003")) {
      return 6;
    }
    else if (InCondition(r, "code", "261665006")) {
      return 7;
    }
    else
      return 0;
  }


}
