package com.figmd.janus.Sections.PatientProblem

import com.figmd.janus.util.{ConditionsUtil, Problem_ICD_Codes, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientProblem1 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Problem_ICD_Codes] = {

    val ACEPCodes_20170309 = new PostgreUtility().getPostgresTable(spark, "ACEPCodes_20170309")
    val MasterCodeSet_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeSet_2018")
    val DataDictionaryClinicalDataElement = new PostgreUtility().getPostgresTable(spark, "DataDictionaryClinicalDataElement")


    val lookup_df = lookupDf.join(ACEPCodes_20170309, lookupDf("PracticeCode") === ACEPCodes_20170309("ProblemCode"), "inner")
      .join(MasterCodeSet_2018, ACEPCodes_20170309("ICDCode_SnomedCode") === MasterCodeSet_2018("Value"), "inner")
      .join(DataDictionaryClinicalDataElement, MasterCodeSet_2018("DataDictionaryClinicalDataElementuid") === DataDictionaryClinicalDataElement("DataDictionaryClinicalDataElementuid"), "inner")

    import spark.implicits._
    val section = lookup_df
      .filter(r => CheckStringEqual(r, "DataDictionaryUid", "4DD7CD51-CE78-465A-8FDE-693549DA3653")
        && CheckCastDateEqual(r, "VisitDate", "DocumentationDate"))
      .map(r => {

        var ScndThrdDgreeBurn = 0;
        var ScndThrdDgreeBurn_Date = "";
        var Szure = 0;
        var Szure_Date = "";

        val shortname = element_func(r);

        if (shortname == "ScndThrdDgreeBurn") {
          ScndThrdDgreeBurn = 1;
          ScndThrdDgreeBurn_Date = r.getTimestamp(r.fieldIndex("DocumentationDate")).toString
        }
        if (shortname == "Szure") {
          Szure = 1;
          Szure_Date = r.getTimestamp(r.fieldIndex("DocumentationDate")).toString
        }

        Problem_ICD_Codes(r.getString(r.fieldIndex("visituid")),ScndThrdDgreeBurn,ScndThrdDgreeBurn_Date,Szure,Szure_Date)

      })


    return section;
  }


  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("name")))
      return r.getString(r.fieldIndex("name"))
    else
      return "";
  }
}