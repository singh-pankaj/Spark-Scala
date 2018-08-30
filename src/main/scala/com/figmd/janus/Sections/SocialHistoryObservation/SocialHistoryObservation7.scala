package com.figmd.janus.Sections.SocialHistoryObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, Social_History_Observation_TobCessCounseling}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SocialHistoryObservation7 extends ConditionsUtil with Serializable {
  //USACS_Social History Observation_M_TobaccoUser_New

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Social_History_Observation_TobCessCounseling] = {

    val lookupDf1 = spark.sql("select * from PatientSocialHistoryObservation psh " +
      "INNER JOIN " + DataMartCreator.cdr_db_name + ".visit temp ON temp.patientuid = psh.patientuid  " +
      "inner join " + DataMartCreator.cdr_db_name + ".master m on psh.mastersocialhistorystatusuid = m.masteruid ")


    // Elements defaut value declarations
    var ToUsCeCo = 0;
    var ToUsCeCo_Date = "";

    import spark.implicits._;
    val dmRdd = lookupDf1.filter(r => CheckStringEqual(r, "code", "229819007") &&
      InCondition(r, "SocialHistoryTypeText", "Tobacco cessation information offered,Smoking Cessation Info Given"))
      .map(r => {

        ToUsCeCo = 1;
        if (checkElementDateCaseUtil(r, "documentationdate", "arrivaldate", "enddate")) {
          ToUsCeCo_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          ToUsCeCo_Date = r.getTimestamp(r.fieldIndex("documentationdate")).toString;
        }


        Social_History_Observation_TobCessCounseling(r.getString(r.fieldIndex("visituid")), ToUsCeCo, ToUsCeCo_Date)
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
