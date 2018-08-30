package com.figmd.janus.Sections.ResultObservation

import com.figmd.janus.util.{ConditionsUtil, Results_Observation_98494}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ResultObservation1 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Results_Observation_98494] = {


    import spark.implicits._
    val section = lookupDf.filter(r => CheckStringEqual(r, "practicedescription", "RG CVC Max Sterile Precautions Used"))
      .map(r => {

        var MaStBaTe = 1
        var MaStBaTe_Date = r.getTimestamp(r.fieldIndex("ValueDate")).toString;

        Results_Observation_98494(r.getString(r.fieldIndex("visituid")), MaStBaTe, MaStBaTe_Date)
      }).distinct()


    return section;
  }

  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("practicedescription")))
      return r.getString(r.fieldIndex("practicedescription"))
    else
      return "";
  }

}