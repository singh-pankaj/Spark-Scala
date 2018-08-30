package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object PatientMedication extends ConditionsUtil {

  def createDM(spark: SparkSession): DataFrame = {
    //val masterCodeDf = new MasterCode().getMasterCode(spark);

    /*val sectionDf = spark.sql( "select a.*,b.visituid,b.startdate as arrivaldate " +
      "from "+DataMartCreator.cdr_db_name+".patientmedication a " +
      "inner join "+DataMartCreator.cdr_db_name+".individual i on i.individualuid=a.patientuid " +
      "inner join  "+DataMartCreator.cdr_db_name+".visit b on  b.Practiceuid = i.Practiceuid and i.individualuid = b.patientuid ")*/

    val sectionDf = spark.sql("select * from " + DataMartCreator.cdr_db_name + ".patientmedication_new");
    var subSection1 = PatientMedication1.process(sectionDf, spark)
    var subSection2 = PatientMedication2.process(sectionDf, spark)
    var subSection3 = PatientMedication3.process(sectionDf, spark)
    var subSection4 = PatientMedication4.process(sectionDf, spark)
    var subSection5 = PatientMedication5.process(sectionDf, spark)
    var subSection6 = PatientMedication6.process(sectionDf, spark)
    var subSection7= PatientMedication7.process(sectionDf,spark)

    var finalSection = subSection1.join(subSection2,Seq("visituid"),"full_outer")
      .join(subSection3,Seq("visituid"),"full_outer")
      .join(subSection4,Seq("visituid"),"full_outer")
      .join(subSection5,Seq("visituid"),"full_outer")
      .join(subSection6,Seq("visituid"),"full_outer")
      .join(subSection7,Seq("visituid"),"full_outer").distinct()


    //val finalSectionRefined = finalSection.groupBy("visituid").max("*_date")

  //  return subSection3;


    return finalSection;

  }

}
