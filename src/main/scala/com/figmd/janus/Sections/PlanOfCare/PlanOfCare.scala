package com.figmd.janus.Sections.PlanOfCare


import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object PlanOfCare extends ConditionsUtil {

  def createDM(spark: SparkSession): DataFrame = {
    val masterCodeDf = new MasterCode().getMasterCode(spark, "master");

    val lookupDf = spark.sql("select a.*,b.visituid,b.enddate,b.startdate as arrivaldate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientPlanOfCare a join  " + DataMartCreator.cdr_db_name + ".visit b " +
      "on a.patientuid = b.patientuid " +
      "and coalesce(cast(b.EndDate as date),cast(b.VisitDate as date)) = cast (a.EffectiveDate as date)")


    var subSection1 = PlanOfCare1.process(lookupDf, spark)
    var subSection2 = PlanOfCare2.process(lookupDf, spark)

    var finalSection = subSection1.join(subSection2, Seq("visituid"), "full_outer")

    return finalSection;
  }

}