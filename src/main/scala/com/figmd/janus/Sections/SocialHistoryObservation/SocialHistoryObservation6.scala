package com.figmd.janus.Sections.SocialHistoryObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, USACS_Social_History_Observation_M_TobaccoUser_New}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SocialHistoryObservation6 extends ConditionsUtil with Serializable {
  //USACS_Social History Observation_M_TobaccoUser_New

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[USACS_Social_History_Observation_M_TobaccoUser_New] = {

    lookupDf.createOrReplaceTempView("Social_History_Observation_M2_TobaccoUser_New")
    val lookupDf1 = spark.sql("select * from Social_History_Observation_M2_TobaccoUser_New psh inner join " + DataMartCreator.cdr_db_name + ".master m on psh.mastersocialhistorystatusuid = m.masteruid ")


    // Elements defaut value declarations
    var TobNU = 0;
    var ToUs_1 = 0;
    var TobNU_Date = "";
    var ToUs_1_Date = "";

    import spark.implicits._;
    val dmRdd = lookupDf1.map(r => {

      val query_result = checkElementValue(r);

      if (query_result == 1 || query_result == 7) {
        TobNU = 1;
        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          TobNU_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          TobNU_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }
      }
      if (query_result == 6) {
        ToUs_1 = 1;
        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          ToUs_1_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }
      }


      USACS_Social_History_Observation_M_TobaccoUser_New(r.getString(r.fieldIndex("visituid")), TobNU, TobNU_Date, ToUs_1, ToUs_1_Date)
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
