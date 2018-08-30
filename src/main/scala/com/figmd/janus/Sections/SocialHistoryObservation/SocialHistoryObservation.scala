package com.figmd.janus.Sections.SocialHistoryObservation

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object SocialHistoryObservation extends ConditionsUtil {

  def createDM(spark: SparkSession): DataFrame = {

    val lookupDf = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate " +
      "from " + DataMartCreator.cdr_db_name + ".patientsocialhistoryobservation a join  " + DataMartCreator.cdr_db_name + ".visit b on a.patientuid = b.patientuid " +
      "and coalesce(cast(b.EndDate as date),cast(b.VisitDate as date)) = cast (a.documentationdate as date)")


    var subSection1 = SocialHistoryObservation1.process(lookupDf, spark)
    var subSection2 = SocialHistoryObservation2.process(lookupDf, spark)
    var subSection3 = SocialHistoryObservation3.process(lookupDf, spark)
    var subSection4 = SocialHistoryObservation4.process(lookupDf, spark)
    var subSection5 = SocialHistoryObservation5.process(lookupDf, spark)
    var subSection6 = SocialHistoryObservation6.process(lookupDf, spark)
    var subSection7 = SocialHistoryObservation7.process(lookupDf, spark)


    var finalSection = subSection1.join(subSection2,Seq("visituid"),"full_outer")
      .join(subSection3,Seq("visituid"),"full_outer")
      .join(subSection4,Seq("visituid"),"full_outer")
      .join(subSection5,Seq("visituid"),"full_outer")
      .join(subSection6,Seq("visituid"),"full_outer")
      .join(subSection7,Seq("visituid"),"full_outer")

    return finalSection;

  }

}
