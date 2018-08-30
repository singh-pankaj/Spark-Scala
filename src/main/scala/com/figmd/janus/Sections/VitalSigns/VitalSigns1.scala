package com.figmd.janus.Sections.VitalSigns

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, PostgreUtility, VitalSigns_M}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object VitalSigns1 extends ConditionsUtil {
  //VitalSigns_M

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[VitalSigns_M] = {

    val master_df = new PostgreUtility().getPostgresTable(spark, "mastercode")

    val lookup_df = lookupDf.join(master_df, lookupDf("observationcodeuid") === master_df("codeuid"), "inner")

    import spark.implicits._;
    val section = lookup_df
      .filter(r => InCondition(r, "practicecode", "8462-4,8480-6,8310-5,8328-7,8329-5,8331-1,8332-9,8333-7,8302-2,3141-9,60621009")
        &&
        CheckNumeric(r, "resultvalue"))
      .map(r => {

        // Elements defaut value declarations
        var DiBlPr = 0;
        var SyBlPr = 0;
        var DiBlPr_Date = "";
        var SyBlPr_Date = "";


        val query_result = element_func(r);
        val flag = checkElementDateCaseUtil(r, "resultdate", "arrivaldate", "enddate")

        if (query_result.equals("8462-4")) {
          DiBlPr = 1;
          if (flag) {
            DiBlPr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          }
          else {
            DiBlPr_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString;
          }
        }
        if (query_result.equals("8480-6")) {
          SyBlPr = 1;
          if (flag) {
            SyBlPr_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          }
          else {
            SyBlPr_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString;
          }
        }

        VitalSigns_M(r.getString(r.fieldIndex("visituid")), DiBlPr, DiBlPr_Date, SyBlPr, SyBlPr_Date)
      }
      ).distinct();

    return section;
  }

  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("practicecode")))
      return r.getString(r.fieldIndex("practicecode"))
    else
      return "";
  }

}
