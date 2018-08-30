package com.figmd.janus.Sections.PatientMedication

import com.figmd.janus.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//Medications_M_Null_Drugname

object PatientMedication1 extends ConditionsUtil with Serializable {
  def process(lookupDf: DataFrame,spark:SparkSession): Dataset[Medications_M_Null_Drugname] = {


    /*

    PENDING LOGIC

    declare @max int,@var int
    select @var=count(1) from #Medications_M2_Spec where Drugvalue='3'
    select @max=count(1)  from #Medications_M2_Spec p

    select VisitUid,case
      when @var=@max then '3'
    else '0'
    end as value,ValueDate
    from #Medications_M2_Spec p
    */


    // Elements defaut value declarations
    var Antibiotics_Dis = 0;
    var Antibiotics_Dis_Date= "";
    var AntimcThrp_Medc=0;
    var AntimcThrp_Medc_Date=""

    import spark.implicits._;
    val dmRdd = lookupDf.map(r => {

      if (checkElementValue(r)) {

        Antibiotics_Dis = 1;
        AntimcThrp_Medc = 1;

        if (checkElementDate(r)) {

          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;
          AntimcThrp_Medc_Date = r.getTimestamp(r.fieldIndex("arrivaldate")).toString;

        } else {

          Antibiotics_Dis_Date = r.getTimestamp(r.fieldIndex("startdate")).toString;
          AntimcThrp_Medc_Date = r.getTimestamp(r.fieldIndex("startdate")).toString;

        }

      }
      Medications_M_Null_Drugname(r.getString(r.fieldIndex("visituid")), Antibiotics_Dis, Antibiotics_Dis_Date, AntimcThrp_Medc, AntimcThrp_Medc_Date)
    }

    )
      .distinct();

    return dmRdd;
  }

  def checkElementDate(r: Row): Boolean = {

    //DocumentationDate > TEMP.ArrivalDate or cast(cast(DocumentationDate as datetime)as time)='00:00:00.000'

    if (CheckDateLessThanOrEqual(r,"startdate","arrivaldate") ||
      CheckDateIfTimeZero(r,"startdate"))
      return true;
    else
      return false;
  }


  def checkElementValue(r: Row): Boolean = {
      return true;

  }
}

