package com.figmd.janus.Sections.VitalSigns

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, PostgreUtility, VitalSigns_M1_hypertension}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object VitalSigns2 extends ConditionsUtil {
  //VitalSigns_M1_hypertension

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[VitalSigns_M1_hypertension] = {


    val master_df = new PostgreUtility().getPostgresTable(spark, "mastercodeset_2018")

    val lookup_df = lookupDf.join(master_df, lookupDf("practicecode") === master_df("value"), "inner")


    import spark.implicits._;
    val dm_rdd = lookup_df
      .filter(r => InCondition(r, "value", "8462-4,8480-6,8310-5,8328-7,8329-5,8331-1,8332-9,8333-7,8302-2,3141-9,60621009")
        &&
        CheckNumeric(r, "resultvalue")
        && (
        (CheckStringEqual(r, "practicedescription", "bp_diastolic") && CheckNumGreaterOrEqual(r, "resultvalue", 80))
          ||
          (CheckStringEqual(r, "practicedescription", "bp_systolic") && CheckNumLessOrEqual(r, "resultvalue", 120))
        )
      )
      .map(r => {

        // Elements defaut value declarations
        var FiofHy = 1;
        var LiRe = 1;
        var FiofHy_Date = "";
        var LiRe_Date = "";


        if (checkElementDateCaseUtil(r, "resultdate", "arrivaldate", "enddate")) {
          FiofHy_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          LiRe_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
        }
        else {
          FiofHy_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString;
          LiRe_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString;
        }


        VitalSigns_M1_hypertension(r.getString(r.fieldIndex("visituid")), FiofHy, FiofHy_Date, LiRe, LiRe_Date)
      }
      )
      .distinct();


    return dm_rdd;
  }
}
