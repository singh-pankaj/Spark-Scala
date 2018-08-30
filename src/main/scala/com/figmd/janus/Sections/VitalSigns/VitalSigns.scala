package com.figmd.janus.Sections.VitalSigns

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object VitalSigns extends ConditionsUtil with Serializable {

  def createDM(spark: SparkSession): DataFrame = {
    val masterCodeDf = new MasterCode().getMasterCode(spark, "master");

    val lookupDf = spark.sql("select a.resultdate,a.practicecode,a.observationcodeuid,a.resultvalue,a.resultdate," +
      "b.visituid,b.startdate as arrivaldate, b.enddate as enddate " +
      "from " + DataMartCreator.cdr_db_name + ".patientvitalsignobservation a join  " + DataMartCreator.cdr_db_name + ".visit b on a.patientuid = b.patientuid " +
      "AND ( cast(a.resultdate as Date) = cast(b.Visitdate as date)) ")
     // "AND ( cast(a.resultdate as Date) between cast(b.Visitdate as date) and coalesce(cast(b.EndDate as date),cast(b.Visitdate as date))) ")


    var subSection1 = VitalSigns1.process(lookupDf, spark)
    var subSection2 = VitalSigns2.process(lookupDf, spark)

    var finalSection = subSection1.join(subSection2, Seq("visituid"), "full_outer")

    return finalSection;

  }

}
