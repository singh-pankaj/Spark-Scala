package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.DataMartCreator
import com.figmd.janus.Sections.PatientMedication.PatientMedication3.{CheckDateIfTimeZero, CheckDateLessThanOrEqual}
import com.figmd.janus.util.{ConditionsUtil, Medications_M2_Antibiotics, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PatientMedication5 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Medications_M2_Antibiotics] = {


    val crosswalkndctorxnorm = new PostgreUtility().getPostgresTable(spark, "crosswalkndctorxnorm")
    val MasterCodeSet_2018 = new PostgreUtility().getPostgresTable(spark, "MasterCodeSet_2018")

    lookupDf.createOrReplaceTempView("Medications_M2_Antibiotics")

    val df1 = spark.sql("select * from Medications_M2_Antibiotics PM " +
      "inner JOIN  " + DataMartCreator.cdr_db_name + ".MasterDispensableDrug MD " +
      "ON PM.DispensableDrugUid = MD.DispensableDrugUid where  cast(PM.startdate as Date) <= date_add(coalesce(cast(PM.enddate as date),cast(PM.arrivaldate as date)),1) " +
      "AND (PM.stopdate IS NULL OR cast(PM.stopdate as date) >= coalesce(cast(PM.enddate as date),cast(PM.arrivaldate as date))  OR PM.stopdate LIKE '%1900%' )")

    val sectiondf1 = df1.join(MasterCodeSet_2018, df1("code") === MasterCodeSet_2018("value"), "inner")
      .select(df1("visituid"), df1("startdate"), df1("arrivaldate"), df1("enddate"), df1("PM.drugname"), MasterCodeSet_2018("name"))

    val sectiondf2 = df1.join(crosswalkndctorxnorm, df1("medicationcode") === crosswalkndctorxnorm("RXCUI") or df1("code") === crosswalkndctorxnorm("RXCUI"), "inner")
      .join(MasterCodeSet_2018, MasterCodeSet_2018("value") === crosswalkndctorxnorm("ATV"), "inner")
      .select(df1("visituid"), df1("startdate"), df1("arrivaldate"), df1("enddate"), df1("PM.drugname"), MasterCodeSet_2018("name"))

    val sectionUnion = sectiondf1.union(sectiondf2)

    import spark.implicits._;
    val section = sectionUnion.map(r => {
      var Antibiotics_Dis = 0;
      var Antibiotics_Dis_Date = "";

      val shortname = checkElementValue(r);
      var flag = checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate");


      if (shortname == "Antibiotics_Dis") {
        Antibiotics_Dis = 1;
        if (flag)
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        else
          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("startdate")).toString
      }

      Medications_M2_Antibiotics(r.getString(r.fieldIndex("visituid")),Antibiotics_Dis, Antibiotics_Dis_Date)
    }).distinct()


    return section;
  }


  def checkElementValue(r: Row): String = {
    return r.getString(r.fieldIndex("name"));

  }
}
