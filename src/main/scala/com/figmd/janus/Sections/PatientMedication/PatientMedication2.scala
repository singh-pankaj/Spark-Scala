package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.{ConditionsUtil, Medications_M1_Crosswalk, Medications_M_Null_Drugname, PostgreUtility}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array, lit, map, struct}

object PatientMedication2 extends ConditionsUtil with Serializable {
  //Medications_M1_Crosswalk


  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Medications_M1_Crosswalk] = {


    val master_acep_ce_crosswalk = new PostgreUtility().getPostgresTable(spark, "master_acep_ce_crosswalk")
    val crosswalkndctorxnorm = new PostgreUtility().getPostgresTable(spark, "crosswalkndctorxnorm")


    lookupDf.createOrReplaceTempView("Medications_M1_Crosswalk")

    val df1 = spark.sql("select * from  Medications_M1_Crosswalk where cast(StartDate as Date) <= date_add(coalesce(cast(EndDate as date),cast(arrivaldate as date)),1) " +
      "AND (StopDate IS NULL OR cast(StopDate as date) >= coalesce(cast(EndDate as date),cast(arrivaldate as date))  OR StopDate LIKE '%1900%' )")

    val sectiondf1 = df1.join(master_acep_ce_crosswalk, df1("medicationCode") === master_acep_ce_crosswalk("Code"), "inner")
      .select("visituid", "startdate", "arrivaldate", "enddate", "name").withColumn("TopPrepr", lit(1))

    val df2 = spark.sql("select * from Medications_M1_Crosswalk PM " +
      "inner JOIN " + DataMartCreator.cdr_db_name + ".MasterDispensableDrug MD ON PM.DispensableDrugUid = MD.DispensableDrugUid " +
      "where cast(StartDate as Date) <= date_add(coalesce(cast(EndDate as date),cast(arrivaldate as date)),1) " +
      "AND (StopDate IS NULL OR cast(StopDate as date) >= coalesce(cast(EndDate as date),cast(arrivaldate as date))  OR StopDate LIKE '%1900%' )")

    val sectiondf2 = df2.join(crosswalkndctorxnorm, df2("MedicationCode") === crosswalkndctorxnorm("ATV") or df2("code") === crosswalkndctorxnorm("ATV"), "inner")
      .join(master_acep_ce_crosswalk, master_acep_ce_crosswalk("Code") === crosswalkndctorxnorm("RXCUI"), "inner")
      .select("visituid", "startdate", "arrivaldate", "enddate", "name").withColumn("TopPrepr", lit(1))

    val sectionUnion = sectiondf1.union(sectiondf2)

    import spark.implicits._;
    val section = sectionUnion.map ( r =>{
      var TopPrepr = 0;
      var TopPrepr_date = "";

      if (checkElementValue(r)) {
        TopPrepr = 1;
        if (checkElementDateCaseUtil(r, "startdate", "arrivaldate", "enddate")) {
          TopPrepr_date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString
        } else {
          TopPrepr_date = r.getTimestamp(r.fieldIndex("startdate")).toString
        }
      }

      Medications_M1_Crosswalk(r.getString(r.fieldIndex("visituid")), TopPrepr, TopPrepr_date)

    }).distinct()


    return section;
  }

  def checkElementValue(r: Row): Boolean = {
    if (r.getString(r.fieldIndex("name")) == "TopPrepr" || r.getString(r.fieldIndex("name")).equals("TopPrepr"))
      return true;
    else
      return false;

  }
}
