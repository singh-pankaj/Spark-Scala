package com.figmd.janus.Sections.Encounter

import com.figmd.janus.DataMartCreator
import com.figmd.janus.master.MasterCode
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.SparkSession

object Encounter extends ConditionsUtil{

  def createDM(spark:SparkSession): Unit =
  {
    val masterCodeDf = new MasterCode().getMasterCode(spark,"master");

    val sectionDf = spark.sql( "select a.*,b.visituid,b.startdate as arrivaldate " +
      "from "+DataMartCreator.cdr_db_name+".patientsocialhistoryobservation a join  "+DataMartCreator.cdr_db_name+".visit b on a.patientuid = b.patientuid and coalesce(cast(b.EndDate as date),cast(b.VisitDate as date)) >= cast (a.documentationdate as date)")

    val lookupDf = sectionDf.join(masterCodeDf,sectionDf("mastersocialhistorytypeuid")===masterCodeDf("masteruid"),"inner").filter(r => EqualCondition(r, "code", "229819007"))

  /*  var socialHistoryObservation1  = new SocialHistoryObservation1()
    var socialHistoryObservation2  = new SocialHistoryObservation2()


    var subSection1= socialHistoryObservation1.process(lookupDf,spark)
    var subSection2= socialHistoryObservation2.process(lookupDf,spark)
    var finalSection = subSection1.join(subSection2,Seq("visituid"),"full_outer")*/
  }

}
