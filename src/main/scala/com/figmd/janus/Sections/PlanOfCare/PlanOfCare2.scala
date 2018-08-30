package com.figmd.janus.Sections.PlanOfCare

import com.figmd.janus.util.{ConditionsUtil, Plan_of_Care_M1_Crosswalk, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PlanOfCare2 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Plan_of_Care_M1_Crosswalk] = {


    val Master_ACEP_CE_Crosswalk = new PostgreUtility().getPostgresTable(spark, "Master_ACEP_CE_Crosswalk")

    val lookup_df = lookupDf.join(Master_ACEP_CE_Crosswalk, lookupDf("PracticeCode") === Master_ACEP_CE_Crosswalk("Code"), "inner")

    import spark.implicits._
    val section = lookup_df.filter(r => CheckStringEqual(r, "code", "225323000"))
      .map(r => {

        var TbcCesCon = 0;
        var TbcCesCon_Date = "";
        var ToUsCeCo = 0;
        var ToUsCeCo_Date = "";


        if (true) {
          TbcCesCon = 1;
          ToUsCeCo = 1;
          if (checkElementDateCaseUtil(r, "effectivedate", "arrivaldate", "enddate")) {
            TbcCesCon_Date = r.getTimestamp(r.fieldIndex("effectivedate")).toString
            TbcCesCon_Date = r.getTimestamp(r.fieldIndex("effectivedate")).toString
          } else {
            TbcCesCon_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
            TbcCesCon_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
          }
        }

        Plan_of_Care_M1_Crosswalk(r.getString(r.fieldIndex("visituid")), TbcCesCon, TbcCesCon_Date, ToUsCeCo, ToUsCeCo_Date)

      })


    return section;
  }
}