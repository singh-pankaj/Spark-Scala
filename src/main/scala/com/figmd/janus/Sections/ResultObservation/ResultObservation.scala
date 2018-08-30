package com.figmd.janus.Sections.ResultObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import org.apache.spark.sql.{DataFrame, SparkSession}

object ResultObservation extends Serializable {

  def createDM(spark: SparkSession): DataFrame = {
    val masterCodeDf = new MasterCode().getMasterCode(spark, "master");

    val lookupDf = spark.sql("select a.*,b.visituid,b.enddate,b.startdate as arrivaldate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientResultObservation a join  " + DataMartCreator.cdr_db_name + ".visit b " +
      "on a.patientuid = b.patientuid " +
      "and cast(a.resultdate AS DATE) <= cast(b.VisitDate AS DATE)")


    val lookupDf1 = spark.sql("select a.*,b.visituid,b.enddate,b.startdate as arrivaldate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientResultObservation a join  " + DataMartCreator.cdr_db_name + ".visit b " +
      "on a.patientuid = b.patientuid " +
      "and cast(a.resultdate AS DATE) <= date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1)")

    val lookupDf2 = spark.sql("select a.*,b.visituid,b.enddate,b.startdate as arrivaldate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientResultObservation a join  " + DataMartCreator.cdr_db_name + ".visit b " +
      "on a.patientuid = b.patientuid " +
      "and cast(a.resultdate AS DATE) between cast(b.startdate as date) and date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1)")

    var subSection1 = ResultObservation1.process(lookupDf, spark)
    var subSection2 = ResultObservation2.process(lookupDf, spark)
    var subSection3 = ResultObservation3.process(lookupDf1, spark)
    var subSection4 = ResultObservation4.process(lookupDf2, spark)

    var finalSection = subSection1.join(subSection2, Seq("visituid"), "full_outer")
      .join(subSection3, Seq("visituid"), "full_outer")
      .join(subSection4, Seq("visituid"), "full_outer")

    return finalSection;
  }

}
