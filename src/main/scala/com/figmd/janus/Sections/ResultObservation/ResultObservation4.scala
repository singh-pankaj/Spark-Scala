package com.figmd.janus.Sections.ResultObservation

import com.figmd.janus.util.{ConditionsUtil, PostgreUtility, Results_Observation_M_Equals}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ResultObservation4 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Results_Observation_M_Equals] = {

    val MasterCodeset_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeset_2018")

    val sectiondf = lookupDf.join(MasterCodeset_2018, lookupDf("practicecode") === MasterCodeset_2018("value"), "inner")

    import spark.implicits._
    val section = sectiondf
      .map(r => {


        val shortname = element_func(r);
        var flag = checkElementDateCaseUtil(r, "resultdate", "arrivaldate", "enddate")


        var SerLact = 0
        var SerLact_Date = "";

        if (shortname == "SerLact") {
          SerLact = 1;
          if (flag)
            SerLact_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          else
            SerLact_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }

        Results_Observation_M_Equals(r.getString(r.fieldIndex("visituid")), SerLact, SerLact_Date)
      }).distinct()


    return section;
  }

  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("name")))
      return r.getString(r.fieldIndex("name"))
    else
      return "";
  }

}