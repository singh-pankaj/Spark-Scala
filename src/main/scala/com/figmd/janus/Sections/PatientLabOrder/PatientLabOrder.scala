package com.figmd.janus.Sections.PatientLabOrder

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object PatientLabOrder extends ConditionsUtil {

  def createDM(spark: SparkSession): DataFrame = {
    //val masterCodeDf = new MasterCode().getMasterCode(spark);

    val sectionDf = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate , enddate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientLabOrder a " +
      "inner join " + DataMartCreator.cdr_db_name + ".visit b on b.patientuid=a.patientuid " +
      "AND cast(a.OrderDate as Date) between cast(b.startdate as date) and date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1) ")


    val sectionDf1 = spark.sql("select a.*,b.visituid,b.startdate as arrivaldate , enddate " +
      "from " + DataMartCreator.cdr_db_name + ".PatientLabOrder a " +
      "inner join " + DataMartCreator.cdr_db_name + ".visit b on b.patientuid=a.patientuid " +
      "AND cast(a.OrderDate as Date) <= date_add(coalesce(cast(b.EndDate as date),cast(b.startdate as date)),1) ")


    var subSection1 = PatientLabOrder1.process(sectionDf, spark)
    var subSection2 = PatientLabOrder2.process(sectionDf1, spark)
    var subSection3 = PatientLabOrder3.process(sectionDf, spark)
    var subSection4 = PatientLabOrder4.process(sectionDf1, spark)
    var subSection5 = PatientLabOrder5.process(sectionDf, spark)

    var finalSection = subSection1.join(subSection2,Seq("visituid"),"full_outer")
      .join(subSection3,Seq("visituid"),"full_outer")
      .join(subSection4,Seq("visituid"),"full_outer")
      .join(subSection5,Seq("visituid"),"full_outer")



    return finalSection;

  }

}
