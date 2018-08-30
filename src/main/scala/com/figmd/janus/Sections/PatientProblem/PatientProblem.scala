package com.figmd.janus.Sections.PatientProblem

import com.figmd.janus.DataMartCreator
import com.figmd.janus.util.ConditionsUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PatientProblem extends ConditionsUtil {

  def createDM(spark: SparkSession): Dataset[com.figmd.janus.util.Problem_ICD_Codes] = {

    val sectionDf = spark.sql("select *, temp.visitdate as visitdate from FROM " + DataMartCreator.cdr_db_name + ".PatientProblem pp " +
      "INNER JOIN " + DataMartCreator.cdr_db_name + ".PatientProblemHistory pph ON pph.patientproblemuid = pp.PatientProblemUid " +
      "INNER JOIN " + DataMartCreator.cdr_db_name + ".visit TEMP  ON TEMP.patientuid = pp.patientuid AND pph.DocumentationDate = TEMP.VisitDate " +
      "AND (pph.ResolutionDate IS NULL OR pph.ResolutionDate > TEMP.VisitDate OR pph.ResolutionDate LIKE '%1900%' ) " +
      "INNER JOIN " + DataMartCreator.cdr_db_name + ".visitdiagnosis VD ON pph.PatientProblemHistoryUid = vd.PatientProblemHistoryUid AND TEMP.visituid = VD.visituid ");


    val finalSection = PatientProblem1.process(sectionDf, spark)


    return finalSection;

  }

}
