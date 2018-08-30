package com.figmd.janus.Sections.SocialHistoryObservation


import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.{ConditionsUtil, Social_History_Observation_M_TobaccoUser_New_case}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SocialHistoryObservation2 extends ConditionsUtil with Serializable {
  //Social History Observation_M_TobaccoUser_New

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Social_History_Observation_M_TobaccoUser_New_case] = {

    val masterCodeDf = new MasterCode().getMasterCode(spark,"master");

    val lookupDf1 = lookupDf.join(masterCodeDf,lookupDf("mastersocialhistorytypeuid")===masterCodeDf("masteruid"),"inner").filter(r => EqualCondition(r, "code", "229819007"))


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

      Social_History_Observation_M_TobaccoUser_New_case(r.getString(r.fieldIndex("visituid")), CurntTobcNUsr, CurntTobcNUsr_Date, TobNU, TobNU_Date, TobcUTC, TobcUTC_Date, ToUs_1, ToUs_1_Date)
    }
    ).distinct();

    return dmRdd;

  }


  def checkElementValue(r: Row): Int = {
    if (InCondition(r, "socialhistoryobservedvalue", "Never smoked,Never,Quit,Passive,Former smoker,Never Smoker," +
      "Passive Smoke Exposure - Never Smoker,No,not have any smokeless tobacco history,no tobacco history,Nonsmoker,Personal history of nicotine dependence")
      ||
      InCondition(r, "socialhistorytypetext", "Prior smoking History,Non smoker,Never smoked,No," +
        "not have any smokeless tobacco history,no tobacco history,Nonsmoker,Personal history of nicotine dependence")
      ||
      LikeCondition(r, "socialhistoryobservedvalue", "Doesn%smoke")
      ||
      LikeCondition(r, "socialhistorytypetext", "Doesn%smoke")
    ) {
      return 1;
    }
    else if (InCondition(r, "socialhistoryobservedvalue", "Yes,Heavy Tobacco Smoker,Light Tobacco Smoker,Current Every Day Smoker,Current Some Day Smoker")
      ||
      InCondition(r, "socialhistorytypetext", "Smoker,Active Smoker")
    ) {
      return 6;
    }
    else if (InCondition(r, "socialhistoryobservedvalue", "Not Asked,Smoker, Current Status Unknown,Unknown If Ever Smoked")
      ||
      LikeCondition(r, "socialhistorytypetext", "%Unknown%")
    ) {
      return 7;
    }
    else
      return 0;
  }

}


