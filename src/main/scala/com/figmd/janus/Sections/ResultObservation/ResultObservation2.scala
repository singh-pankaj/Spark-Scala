package com.figmd.janus.Sections.ResultObservation

import com.figmd.janus.util.{ConditionsUtil, Results_Observation_Description}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ResultObservation2 extends ConditionsUtil with Serializable {

  def process(lookupDf: DataFrame, spark: SparkSession): Dataset[Results_Observation_Description] = {


    import spark.implicits._

    val section = lookupDf
      .map(r => {

        var DngMechInj_Grp = 0;
        var DngMechInj_Grp_Date = "";
        var DngrsMechInjry = 0;
        var DngrsMechInjry_Date = "";
        var DrgAlIntoxi = 0;
        var DrgAlIntoxi_Date = "";
        var EvTrmHdNck = 0;
        var EvTrmHdNck_Date = "";
        var FclNeuroDefct = 0;
        var FclNeuroDefct_Date = "";
        var Hdac = 0;
        var Hdac_Date = "";
        var Hdache = 0;
        var Hdache_Date = "";
        var Hed_NkTrma = 0;
        var Hed_NkTrma_Date = "";
        var Loss_Concusnes = 0;
        var Loss_Concusnes_Date = "";
        var PhylSignBsleSklFrctr = 0;
        var PhylSignBsleSklFrctr_Date = "";
        var PoTrAm = 0;
        var PoTrAm_Date = "";
        var PstTrmtcAmnsa = 0;
        var PstTrmtcAmnsa_Date = "";
        var SeizAftHdInju = 0;
        var SeizAftHdInju_Date = "";
        var Shrtrm_MemryDfict = 0;
        var Shrtrm_MemryDfict_Date = "";
        var ShrtTrmMemDef = 0;
        var ShrtTrmMemDef_Date = "";
        var ShTeMeDe = 0;
        var ShTeMeDe_Date = "";
        var Szr_Inj_ = 0;
        var Szr_Inj__Date = "";
        var SzurAftrHedInjry = 0;
        var SzurAftrHedInjry_Date = "";
        var Vomit = 0;
        var Vomit_Date = "";

        val shortname = element_func(r)

        if (List("Bedrest/immobility:", "Is this injury related to a motor vehicle accident?", "Trauma:", "Trauma Test",
          "Signs of Trauma", "Signs of trauma", "Recent trauma history", "Recent Trauma", "Recent head trauma <3mo", "Recent Head Trauma",
          "Rcnt Stroke/HdTrauma<3m", "Nipple Trauma(Rt):", "Nipple Trauma(Lt):", "Mechanism of Trauma", "*Code Trauma Called:", "Scheduled Use of Immobilizer", "Hx Head Trauma").contains(shortname)) {
          DngMechInj_Grp = 1;
          DngMechInj_Grp_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Bedrest/immobility:", "Is this injury related to a motor vehicle accident?", "Trauma:", "Trauma Test",
          "Signs of Trauma", "Signs of trauma", "Recent trauma history", "Recent Trauma", "Recent head trauma <3mo", "Recent Head Trauma",
          "Rcnt Stroke/HdTrauma<3m", "Nipple Trauma(Rt):", "Nipple Trauma(Lt):", "Mechanism of Trauma", "*Code Trauma Called:", "Scheduled Use of Immobilizer", "Hx Head Trauma").contains(shortname)) {
          DngrsMechInjry = 1;
          DngrsMechInjry_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking",
          "Rooting Behavior:", "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:",
          "Behavioral Restraints:", "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)",
          "your drinking or drug use?", "Suspected Drug/Dose:", "Prescription drugs:", "Other Drugs:", "Other Drugs/Sedation:", "Other Drugs/dose,Sedation:", "or drug use?",
          "on your drinking or drug use?", "may decr drug effect", "Illicit Drug Use:", "Illegal drugs:", "Hx of IV Drug Use:", "History of drugs in pregnancy",
          "Have you ever had a drug or alcohol problem:", "Drug:", "Drug/Dose:", "Drug Use", "DRUG NDC", "Drug IV #2:", "Drug IV #1:", "drinking or using drugs?",
          "drinking or drug use?", "Do you use any type of alcohol or drugs?", "date of last use of drug:", "ALCOHOL/DRUGS", "Adverse drug reaction:",
          "Hx Seizures/Substance Abuse/ETOH (10):", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:",
          "Level of consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:",
          "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status",
          "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Speech:", "Speech/Voice:",
          "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:", "Speech Problems:",
          "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:", "Conversational Speech:",
          "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Behavior", "Level of Consciousness", "Decreased Level of Consciousness/Alertness",
          "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Mental Health Issues", "Altered Mental Status", "Speech").contains(shortname)) {
          DrgAlIntoxi = 1;
          DrgAlIntoxi_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Right Upper Extremity Movement:",
          "Right Lower Extremity Movement:", "Reduced lingual movement", "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.",
          "Left Upper Extremity Movement:", "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:", "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:",
          "hip pain of leg movement", "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:", "Reduced Hand Strength:", "Severity of Pain", "Severity of Injury", "Trauma:", "Trauma Test",
          "Signs of Trauma", "Signs of trauma", "Recent trauma history", "Recent Trauma", "Recent head trauma <3mo", "Recent Head Trauma", "Rcnt Stroke/HdTrauma<3m", "Nipple Trauma(Rt):", "Nipple Trauma(Lt):",
          "Mechanism of Trauma", "*Code Trauma Called:", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness",
          "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:", "Consciousness:",
          "Consciousness Level:", "Conscious Sedation", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness",
          "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:",
          "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:",
          "Immediate Memory:", "Requesting Mental Health Services?", "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam",
          "Mental Status", "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Numbness:", "Recent spinal surgery",
          "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:",
          "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:",
          "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:", "Pupilary changes:", "Pupilary Changes:",
          "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):", "Pupil Equality:", "abnormal pupil",
          "Bedrest/immobility:", "Type of Cerebral Event", "CSF Culture", "Neck Pain, Stiffness, or Swelling:", "-Severe Headache or Spots in Front of Your Eyes", "Migraine Headaches:", "Headache:",
          "Headache", "Head/Neck Exam", "Head/Neck", "HEAD/NECK", "Behavior", "Problem Severity", "Level of Consciousness", "Measurably Reduced Grip Strength", "Severity", "Decreased Level of Consciousness/Alertness",
          "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Mental Health Issues", "Cervical Dilatation", "Memory", "Altered Mental Status", "Speech", "Scheduled Use of Immobilizer",
          "Cervical Laceration", "Hx Neck Pain").contains(shortname)) {
          EvTrmHdNck = 1;
          EvTrmHdNck_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:", "Behavioral Health Carrier:",
          "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Right Upper Extremity Movement:", "Right Lower Extremity Movement:",
          "Reduced lingual movement", "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.", "Left Upper Extremity Movement:",
          "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:", "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:", "hip pain of leg movement",
          "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:", "Reduced Hand Strength:", "Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:",
          "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Numbness:", "Recent spinal surgery",
          "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:",
          "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:",
          "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:", "Pupilary changes:", "Pupilary Changes:",
          "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):", "Pupil Equality:", "abnormal pupil", "Behavior",
          "Measurably Reduced Grip Strength", "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Decreased Level of Consciousness/Alertness", "Memory",
          "Altered Mental Status", "Speech", "Mental Health Issues").contains(shortname)) {
          FclNeuroDefct = 1;
          FclNeuroDefct_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Emesis:", "Emesis/NG Color:", "Emesis Type:",
          "Emesis type:", "Emesis (cc):", "Emesis", "Pt has experienced nausea/vomiting in the past 48 hrs:", "Nausea:", "NAUSEA/VOMITING:", "Nausea/Vomiting:", "Nausea Severity:", "Nausea or Vomiting +:",
          "Minimal or Absence of nausea and/or vomiting:", "Has uncontrolled symptoms (ie nausea,vomiting)", "Right Upper Extremity Movement:", "Right Lower Extremity Movement:", "Reduced lingual movement",
          "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.", "Left Upper Extremity Movement:",
          "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:", "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:", "hip pain of leg movement",
          "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:", "Reduced Hand Strength:", "Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:",
          "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?", "rate of return of physical and mental functioning.",
          "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:",
          "Hx of other mental illness:", "Hx of Depression or other mental illness", "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:",
          "Altered mental status or worsening altered mental status:", "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?",
          "Numbness:", "Recent spinal surgery", "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:",
          "Speech Sound Production Evaluation:", "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:",
          "Hearing,Vision or Speech Limitation:", "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:",
          "Pupilary changes:", "Pupilary Changes:", "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):",
          "Pupil Equality:", "abnormal pupil", "Nausea", "Output, Emesis Amount", "Nausea Comment", "Behavior", "Number of Emesis in 4 Hours", "Nausea/Vomiting", "Level of Consciousness",
          "Decreased Level of Consciousness/Alertness", "Memory", "Speech", "Measurably Reduced Grip Strength").contains(shortname)) {
          Hdac = 1;
          Hdac_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Emesis:", "Emesis/NG Color:", "Emesis Type:",
          "Emesis type:", "Emesis (cc):", "Emesis", "Pt has experienced nausea/vomiting in the past 48 hrs:", "Nausea:", "NAUSEA/VOMITING:", "Nausea/Vomiting:", "Nausea Severity:", "Nausea or Vomiting +:",
          "Minimal or Absence of nausea and/or vomiting:", "Has uncontrolled symptoms (ie nausea,vomiting)", "Right Upper Extremity Movement:", "Right Lower Extremity Movement:", "Reduced lingual movement",
          "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.", "Left Upper Extremity Movement:",
          "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:", "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:", "hip pain of leg movement",
          "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:", "Reduced Hand Strength:", "Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:",
          "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?", "rate of return of physical and mental functioning.",
          "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:",
          "Hx of other mental illness:", "Hx of Depression or other mental illness", "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:",
          "Altered mental status or worsening altered mental status:", "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?",
          "Numbness:", "Recent spinal surgery", "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:",
          "Speech Sound Production Evaluation:", "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:",
          "Hearing,Vision or Speech Limitation:", "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:",
          "Pupilary changes:", "Pupilary Changes:", "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):",
          "Pupil Equality:", "abnormal pupil", "Nausea", "Output, Emesis Amount", "Nausea Comment", "Behavior", "Number of Emesis in 4 Hours", "Nausea/Vomiting", "Level of Consciousness",
          "Decreased Level of Consciousness/Alertness", "Memory", "Speech", "Measurably Reduced Grip Strength").contains(shortname)) {
          Hdache = 1;
          Hdache_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Right Upper Extremity Movement:",
          "Right Lower Extremity Movement:", "Reduced lingual movement", "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.",
          "Left Upper Extremity Movement:", "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:", "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:",
          "hip pain of leg movement", "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:", "Reduced Hand Strength:", "Severity of Pain", "Severity of Injury", "Trauma:",
          "Trauma Test", "Signs of Trauma", "Signs of trauma", "Recent trauma history", "Recent Trauma", "Recent head trauma <3mo", "Recent Head Trauma", "Rcnt Stroke/HdTrauma<3m", "Nipple Trauma(Rt):",
          "Nipple Trauma(Lt):", "Mechanism of Trauma", "*Code Trauma Called:", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:",
          "Level of consciousness", "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:",
          "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness",
          "Level of Consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:",
          "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:",
          "Immediate Memory:", "Requesting Mental Health Services?", "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam",
          "Mental Status", "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Numbness:", "Recent spinal surgery",
          "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:",
          "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:",
          "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:", "Pupilary changes:", "Pupilary Changes:",
          "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):", "Pupil Equality:", "abnormal pupil",
          "Bedrest/immobility:", "Type of Cerebral Event", "CSF Culture", "Neck Pain, Stiffness, or Swelling:", "-Severe Headache or Spots in Front of Your Eyes", "Migraine Headaches:", "Headache:",
          "Headache", "Head/Neck Exam", "Head/Neck", "HEAD/NECK", "Behavior", "Problem Severity", "Level of Consciousness", "Measurably Reduced Grip Strength", "Severity", "Decreased Level of Consciousness/Alertness",
          "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Mental Health Issues", "Cervical Dilatation", "Memory", "Altered Mental Status", "Speech", "Scheduled Use of Immobilizer",
          "Cervical Laceration", "Hx Neck Pain").contains(shortname)) {
          Hed_NkTrma = 1;
          Hed_NkTrma_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient%s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation),Right Upper Extremity Movement:",
          "Right Lower Extremity Movement:", "Reduced lingual movement", "Pain without Movement:", "Pain with Movement:", "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.",
          "Left Upper Extremity Movement:", "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement", "Involuntary Bowel Movements +:", "hip pain of leg movement", "Fetal Movement:",
          "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement", "Air movement", "Absent air movement with/without accessory muscle use:",
          "8. Incapacitation due to abnormal movements.,Able to grip with 1 hand or staff member can assist w/ txfr:,Reduced Hand Strength:,Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation,Disoriented to:", "Disoriented to", "Disoriented,Remote/Long Term Memory:",
          "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:,Remote/Long Term Memory:", "Immediate Memory:,Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person have a serious mental illness?,Numbness:,Recent spinal surgery",
          "Rcnt Spinal Surgery,Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:",
          "Speech Problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:", "Conversational Speech:",
          "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:,Pupillary Changes:", "Pupilary changes:", "Pupilary Changes:,Pupils:", "Pupils equal/reactive to light:",
          "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):", "Pupil Equality:", "abnormal pupil", "Behavior", "Measurably Reduced Grip Strength",
          "Measurably Reduced Grip Strength", "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Mental Health Issues", "Decreased Level of Consciousness/Alertness",
          "Memory", "Altered Mental Status", "Speech").contains(shortname)) {
          Loss_Concusnes = 1;
          Loss_Concusnes_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient_s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:", "Behavioral Health Carrier:",
          "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Emesis:", "Emesis/NG Color:", "Emesis Type:", "Emesis type:", "Emesis (cc):", "Emesis",
          "Pt has experienced nausea/vomiting in the past 48 hrs:", "Nausea:", "NAUSEA/VOMITING:", "Nausea/Vomiting:", "Nausea Severity:", "Nausea or Vomiting +:", "Minimal or Absence of nausea and/or vomiting:",
          "Has uncontrolled symptoms (ie nausea,vomiting)", "Right Upper Extremity Movement:", "Right Lower Extremity Movement:", "Reduced lingual movement", "Pain without Movement:", "Pain with Movement:",
          "Pain w/movement of auricle", "Movement:", "mouth, NOT inability to sustain movement.", "Left Upper Extremity Movement:", "Left Lower Extremity Movement:", "Last Bowel Movement:", "Last bowel movement:",
          "Last bowel movement", "Last Bowel Movement", "Involuntary Bowel Movements +:", "hip pain of leg movement", "Fetal Movement:", "Digit movement:", "Decreased A-P movement of tongue:", "c/o Pain on Movement",
          "Air movement", "Absent air movement with/without accessory muscle use:", "8. Incapacitation due to abnormal movements.", "Able to grip with 1 hand or staff member can assist w/ txfr:",
          "Reduced Hand Strength:", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "Level of Consciousness",
          "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation",
          "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:",
          "Requesting Mental Health Services?", "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status",
          "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Numbness:", "Recent spinal surgery",
          "Rcnt Spinal Surgery", "Speech:", "Speech/Voice:", "Speech/Language Pathologist:", "Speech/Hearing Therapy Charge sent:", "Speech Therapy Department Notified:", "Speech Sound Production Evaluation:",
          "Speech Problems:", "Speech problems:", "Speech Language Pathologist Ext 2773:", "Speech Evaluation Charge Sent:", "Speech", "Language and Speech Evaluation:", "Hearing,Vision or Speech Limitation:",
          "Conversational Speech:", "Continue speech-language/swallowing intervention:", "Automatic Speech:", "(DN)Speech Therapy Notified:", "Pupillary Changes:", "Pupilary changes:", "Pupilary Changes:",
          "Pupils:", "Pupils equal/reactive to light:", "Pupils", "Pupil Size Right (mm):", "Pupil Size Left (mm):", "Pupil Reaction (R):", "Pupil Reaction (L):", "Pupil Equality:", "abnormal pupil",
          "Bedrest/immobility:", "Type of Cerebral Event", "CSF Culture", "Neck Pain, Stiffness, or Swelling:", "-Severe Headache or Spots in Front of Your Eyes", "Migraine Headaches:", "Headache:",
          "Headache", "Head/Neck Exam", "Head/Neck", "HEAD/NECK", "Is this injury related to a motor vehicle accident?", "Is this injury related to a motor vehicle accident?", "Trauma:", "Trauma Test",
          "Signs of Trauma", "Signs of trauma", "Recent trauma history", "Recent Trauma", "Recent head trauma <3mo", "Recent Head Trauma", "Rcnt Stroke/HdTrauma<3m", "Nipple Trauma(Rt):",
          "Nipple Trauma(Lt):", "Mechanism of Trauma", "*Code Trauma Called:", "Nausea", "Output, Emesis Amount", "Nausea Comment", "Behavior", "Number of Emesis in 4 Hours", "Nausea/Vomiting",
          "Measurably Reduced Grip Strength", "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Mental Health Issues", "Cervical Dilatation",
          "Decreased Level of Consciousness/Alertness", "Memory", "Altered Mental Status", "Speech", "Scheduled Use of Immobilizer", "Cervical Laceration", "Hx Neck Pain", "Hx Head Trauma").contains(shortname)) {
          PhylSignBsleSklFrctr = 1;
          PhylSignBsleSklFrctr_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking",
          "Rooting Behavior:", "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient%s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:",
          "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Behavior", "Measurably Reduced Grip Strength",
          "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Decreased Level of Consciousness/Alertness", "Memory", "Altered Mental Status", "Speech",
          "Mental Health Issues").contains(shortname)) {
          PoTrAm = 1;
          PoTrAm_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking",
          "Rooting Behavior:", "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient%s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:",
          "Behavioral Health Carrier:", "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Loss of Consciousness", "Level of Consciousnous:",
          "Level of Consciousness:", "Level of Consciousness r/t PCA:", "Level of consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:", "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:",
          "Recent Memory:", "Prospective Memory:", "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.", "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:",
          "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:", "Hx of other mental illness:", "Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:", "Altered mental status or worsening altered mental status:",
          "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?", "Behavior", "Measurably Reduced Grip Strength",
          "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Decreased Level of Consciousness/Alertness", "Memory", "Altered Mental Status", "Speech",
          "Mental Health Issues").contains(shortname)) {
          PstTrmtcAmnsa = 1;
          PstTrmtcAmnsa_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:", "Sudden change in behavior or attitude", "Self-destructive behavior:", "Self-destructive behavior/unnecessary risk taking", "Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:", "Patient%s Behavior:", "Non Behavioral Restraints:", "Loss fo behavioral control:", "Behavioral Restraints:", "Behavioral Health Carrier:",
          "Behavior:", "Behavior State:", "Behavior Modifications:", "Behavior Disorder:", "(suicidal behavior/ideation)", "Loss of Consciousness", "Level of Consciousnous:", "Level of Consciousness:",
          "Level of Consciousness r/t PCA:", "Level of consciousness", "IV conscious sedation:", "Informed Consent For I.V. Conscious Sedation Is Signed", "If decreased level of consciousness, pt given:",
          "Consciousness:", "Consciousness Level:", "Conscious Sedation", "Disoriented to:", "Disoriented to", "Disoriented", "Remote/Long Term Memory:", "Recent Memory:", "Prospective Memory:",
          "Orientation/Memory:", "Memory?", "Memory:", "memory loss:", "Immediate Memory:", "Requesting Mental Health Services?", "rate of return of physical and mental functioning.",
          "Poor oral transit &/or mental status", "Mental Status:", "Mental Status Exam", "Mental Status", "Mental Illness:", "Mental Health Risk Assessment Form Complete", "Mental Health Counseling:",
          "Hx of other mental illness:", "Hx of Depression or other mental illness", "evaluation by a licensed mental health professional?", "CURRENT PHYSICAL/MENTAL STATUS:", "Altered Mental Status:",
          "Altered mental status or worsening altered mental status:", "Altered Mental Status", "Alt Mental Status:", "Alt Mental Status Unable to give hx:", "23-Does the person ahve a serious mental illness?",
          "Behavior", "Measurably Reduced Grip Strength", "Acute Mental Status Fluctuation", "Hx Family Mental Health", "Level of Consciousness", "Decreased Level of Consciousness/Alertness", "Memory",
          "Altered Mental Status", "Speech", "Mental Health Issues").contains(shortname)) {
          SeizAftHdInju = 1;
          SeizAftHdInju_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:","Sudden change in behavior or attitude","Self-destructive behavior:","Self-destructive behavior/unnecessary risk taking","Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:","Patient_s Behavior:","Non Behavioral Restraints:","Loss fo behavioral control:","Behavioral Restraints:","Behavioral Health Carrier:",
          "Behavior:","Behavior State:","Behavior Modifications:","Behavior Disorder:","(suicidal behavior/ideation)","Myoclonic Jerks (3):","Type of Seizure","Seizures:","Seizures","SEIZURE:",
          "Seizure Precautions","Seizure Disorder (2):","Seizure @ Onset/PostIctal","Right Upper Extremity Movement:","Right Lower Extremity Movement:","Reduced lingual movement","Pain without Movement:",
          "Pain with Movement:","Pain w/movement of auricle","Movement:","mouth, NOT inability to sustain movement.","Left Upper Extremity Movement:","Left Lower Extremity Movement:","Last Bowel Movement:",
          "Last bowel movement:","Last bowel movement","Last Bowel Movement","Involuntary Bowel Movements +:","hip pain of leg movement","Fetal Movement:","Digit movement:","Decreased A-P movement of tongue:",
          "c/o Pain on Movement","Air movement","Absent air movement with/without accessory muscle use:","8. Incapacitation due to abnormal movements.",
          "Able to grip with 1 hand or staff member can assist w/ txfr:","Reduced Hand Strength:","Loss of Consciousness","Level of Consciousnous:","Level of Consciousness:",
          "Level of Consciousness r/t PCA:","Level of consciousness","Level of Consciousness","IV conscious sedation:","Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:","Consciousness:","Consciousness Level:","Conscious Sedation","Disoriented to:","Disoriented to","Disoriented","Remote/Long Term Memory:",
          "Recent Memory:","Prospective Memory:","Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Remote/Long Term Memory:","Recent Memory:",
          "Prospective Memory:","Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Requesting Mental Health Services?","rate of return of physical and mental functioning.",
          "Poor oral transit &/or mental status","Mental Status:","Mental Status Exam","Mental Status","Mental Illness:","Mental Health Risk Assessment Form Complete","Mental Health Counseling:",
          "Hx of other mental illness:","Hx of Depression or other mental illness","evaluation by a licensed mental health professional?","CURRENT PHYSICAL/MENTAL STATUS:","Altered Mental Status:",
          "Altered mental status or worsening altered mental status:","Altered Mental Status","Alt Mental Status:","Alt Mental Status Unable to give hx:","23-Does the person ahve a serious mental illness?",
          "Numbness:","Recent spinal surgery","Rcnt Spinal Surgery","Speech:","Speech/Voice:","Speech/Language Pathologist:","Speech/Hearing Therapy Charge sent:","Speech Therapy Department Notified:",
          "Speech Sound Production Evaluation:","Speech Problems:","Speech problems:","Speech Language Pathologist Ext 2773:","Speech Evaluation Charge Sent:","Speech","Language and Speech Evaluation:",
          "Hearing,Vision or Speech Limitation:","Conversational Speech:","Continue speech-language/swallowing intervention:","Automatic Speech:","(DN)Speech Therapy Notified:","Pupillary Changes:",
          "Pupilary changes:","Pupilary Changes:","Pupils:","Pupils equal/reactive to light:","Pupils","Pupil Size Right (mm):","Pupil Size Left (mm):","Pupil Reaction (R):","Pupil Reaction (L):",
          "Pupil Equality:","abnormal pupil","Behavior","Measurably Reduced Grip Strength","Acute Mental Status Fluctuation","Hx Family Mental Health","Level of Consciousness","Mental Health Issues",
          "Decreased Level of Consciousness/Alertness","Memory","Altered Mental Status","Speech").contains(shortname)) {
          Shrtrm_MemryDfict = 1;
          Shrtrm_MemryDfict_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if (List("Sudden change in behavior:","Sudden change in behavior or attitude","Self-destructive behavior:","Self-destructive behavior/unnecessary risk taking","Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:","Patient_s Behavior:","Non Behavioral Restraints:","Loss fo behavioral control:","Behavioral Restraints:","Behavioral Health Carrier:",
          "Behavior:","Behavior State:","Behavior Modifications:","Behavior Disorder:","(suicidal behavior/ideation)","Loss of Consciousness","Level of Consciousnous:","Level of Consciousness:",
          "Level of Consciousness r/t PCA:","Level of consciousness","Level of Consciousness","IV conscious sedation:","Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:","Consciousness:","Consciousness Level:","Conscious Sedation","Disoriented to:","Disoriented to","Disoriented","Remote/Long Term Memory:",
          "Recent Memory:","Prospective Memory:","Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.","Poor oral transit &/or mental status","Mental Status:","Mental Status Exam","Mental Status","Mental Illness:",
          "Mental Health Risk Assessment Form Complete","Mental Health Counseling:","Hx of other mental illness:","Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?","CURRENT PHYSICAL/MENTAL STATUS:","Altered Mental Status:","Altered mental status or worsening altered mental status:",
          "Altered Mental Status","Alt Mental Status:","Alt Mental Status Unable to give hx:","23-Does the person ahve a serious mental illness?","Recent spinal surgery","Rcnt Spinal Surgery",
          "Behavior","Acute Mental Status Fluctuation","Hx Family Mental Health","Level of Consciousness","Decreased Level of Consciousness/Alertness","Memory","Altered Mental Status",
          "Mental Health Issues").contains(shortname)) {
          ShrtTrmMemDef = 1;
          ShrtTrmMemDef_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if(List("Sudden change in behavior:","Sudden change in behavior or attitude","Self-destructive behavior:","Self-destructive behavior/unnecessary risk taking","Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:","Patient_s Behavior:","Non Behavioral Restraints:","Loss fo behavioral control:","Behavioral Restraints:",
          "Behavioral Health Carrier:","Behavior:","Behavior State:","Behavior Modifications:","Behavior Disorder:","(suicidal behavior/ideation)","Loss of Consciousness","Level of Consciousnous:",
          "Level of Consciousness:","Level of Consciousness r/t PCA:","Level of consciousness","Level of Consciousness","IV conscious sedation:","Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:","Consciousness:","Consciousness Level:","Conscious Sedation","Disoriented to:","Disoriented to","Disoriented","Remote/Long Term Memory:",
          "Recent Memory:","Prospective Memory:","Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Requesting Mental Health Services?",
          "rate of return of physical and mental functioning.","Poor oral transit &/or mental status","Mental Status:","Mental Status Exam","Mental Status","Mental Illness:",
          "Mental Health Risk Assessment Form Complete","Mental Health Counseling:","Hx of other mental illness:","Hx of Depression or other mental illness",
          "evaluation by a licensed mental health professional?","CURRENT PHYSICAL/MENTAL STATUS:","Altered Mental Status:","Altered mental status or worsening altered mental status:",
          "Altered Mental Status","Alt Mental Status:","Alt Mental Status Unable to give hx:","23-Does the person ahve a serious mental illness?","Recent spinal surgery","Rcnt Spinal Surgery",
          "Behavior","Acute Mental Status Fluctuation","Hx Family Mental Health","Level of Consciousness","Decreased Level of Consciousness/Alertness","Memory","Altered Mental Status",
          "Mental Health Issues").contains(shortname)){
          ShTeMeDe = 1;
          ShTeMeDe_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if(List("Sudden change in behavior:","Sudden change in behavior or attitude","Self-destructive behavior:","Self-destructive behavior/unnecessary risk taking","Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:","Patient_s Behavior:","Non Behavioral Restraints:","Loss fo behavioral control:","Behavioral Restraints:","Behavioral Health Carrier:",
          "Behavior:","Behavior State:","Behavior Modifications:","Behavior Disorder:","(suicidal behavior/ideation)","Myoclonic Jerks (3):","Type of Seizure","Seizures:","Seizures","SEIZURE:",
          "Seizure Precautions","Seizure Disorder (2):","Seizure @ Onset/PostIctal","Right Upper Extremity Movement:","Right Lower Extremity Movement:","Reduced lingual movement","Pain without Movement:",
          "Pain with Movement:","Pain w/movement of auricle","Movement:","mouth, NOT inability to sustain movement.","Left Upper Extremity Movement:","Left Lower Extremity Movement:","Last Bowel Movement:",
          "Last bowel movement:","Last bowel movement","Last Bowel Movement","Involuntary Bowel Movements +:","hip pain of leg movement","Fetal Movement:","Digit movement:","Decreased A-P movement of tongue:",
          "c/o Pain on Movement","Air movement","Absent air movement with/without accessory muscle use:","8. Incapacitation due to abnormal movements.",
          "Able to grip with 1 hand or staff member can assist w/ txfr:","Reduced Hand Strength:","Loss of Consciousness","Level of Consciousnous:","Level of Consciousness:","Level of Consciousness r/t PCA:",
          "Level of consciousness","Level of Consciousness","IV conscious sedation:","Informed Consent For I.V. Conscious Sedation Is Signed","If decreased level of consciousness, pt given:",
          "Consciousness:","Consciousness Level:","Conscious Sedation","Disoriented to:","Disoriented to","Disoriented","Remote/Long Term Memory:","Recent Memory:","Prospective Memory:",
          "Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Remote/Long Term Memory:","Recent Memory:","Prospective Memory:","Orientation/Memory:","Memory?","Memory:",
          "memory loss:","Immediate Memory:","Requesting Mental Health Services?","rate of return of physical and mental functioning.","Poor oral transit &/or mental status","Mental Status:",
          "Mental Status Exam","Mental Status","Mental Illness:","Mental Health Risk Assessment Form Complete","Mental Health Counseling:","Hx of other mental illness:",
          "Hx of Depression or other mental illness","evaluation by a licensed mental health professional?","CURRENT PHYSICAL/MENTAL STATUS:","Altered Mental Status:",
          "Altered mental status or worsening altered mental status:","Altered Mental Status","Alt Mental Status:","Alt Mental Status Unable to give hx:",
          "23-Does the person ahve a serious mental illness?","Numbness:","Recent spinal surgery","Rcnt Spinal Surgery","Speech:","Speech/Voice:","Speech/Language Pathologist:",
          "Speech/Hearing Therapy Charge sent:","Speech Therapy Department Notified:","Speech Sound Production Evaluation:","Speech Problems:","Speech problems:","Speech Language Pathologist Ext 2773:",
          "Speech Evaluation Charge Sent:","Speech","Language and Speech Evaluation:","Hearing,Vision or Speech Limitation:","Conversational Speech:","Continue speech-language/swallowing intervention:",
          "Automatic Speech:","(DN)Speech Therapy Notified:","Pupillary Changes:","Pupilary changes:","Pupilary Changes:","Pupils:","Pupils equal/reactive to light:","Pupils","Pupil Size Right (mm):",
          "Pupil Size Left (mm):","Pupil Reaction (R):","Pupil Reaction (L):","Pupil Equality:","abnormal pupil","Behavior","Measurably Reduced Grip Strength","Acute Mental Status Fluctuation",
          "Hx Family Mental Health","Level of Consciousness",   "Mental Health Issues","Decreased Level of Consciousness/Alertness","Memory",
          "Altered Mental Status","Speech").contains(shortname)){
          Szr_Inj_ = 1;
          Szr_Inj__Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if(List("Sudden change in behavior:","Sudden change in behavior or attitude","Self-destructive behavior:","Self-destructive behavior/unnecessary risk taking","Rooting Behavior:",
          "Pt Behavior Remains Unchanged, Continue Current Protocol:","Patient_s Behavior:","Non Behavioral Restraints:","Loss fo behavioral control:","Behavioral Restraints:","Behavioral Health Carrier:",
          "Behavior:","Behavior State:","Behavior Modifications:","Behavior Disorder:","(suicidal behavior/ideation)","Myoclonic Jerks (3):","Type of Seizure","Seizures:","Seizures","SEIZURE:",
          "Seizure Precautions","Seizure Disorder (2):","Seizure @ Onset/PostIctal","Right Upper Extremity Movement:","Right Lower Extremity Movement:","Reduced lingual movement","Pain without Movement:",
          "Pain with Movement:","Pain w/movement of auricle","Movement:","mouth, NOT inability to sustain movement.","Left Upper Extremity Movement:","Left Lower Extremity Movement:","Last Bowel Movement:",
          "Last bowel movement:","Last bowel movement","Last Bowel Movement","Involuntary Bowel Movements +:","hip pain of leg movement","Fetal Movement:","Digit movement:","Decreased A-P movement of tongue:",
          "c/o Pain on Movement","Air movement","Absent air movement with/without accessory muscle use:","8. Incapacitation due to abnormal movements.",
          "Able to grip with 1 hand or staff member can assist w/ txfr:","Reduced Hand Strength:","Loss of Consciousness","Level of Consciousnous:","Level of Consciousness:",
          "Level of Consciousness r/t PCA:","Level of consciousness","Level of Consciousness","IV conscious sedation:","Informed Consent For I.V. Conscious Sedation Is Signed",
          "If decreased level of consciousness, pt given:","Consciousness:","Consciousness Level:","Conscious Sedation","Disoriented to:","Disoriented to","Disoriented","Remote/Long Term Memory:",
          "Recent Memory:","Prospective Memory:","Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Remote/Long Term Memory:","Recent Memory:","Prospective Memory:",
          "Orientation/Memory:","Memory?","Memory:","memory loss:","Immediate Memory:","Requesting Mental Health Services?","rate of return of physical and mental functioning.",
          "Poor oral transit &/or mental status","Mental Status:","Mental Status Exam","Mental Status","Mental Illness:","Mental Health Risk Assessment Form Complete","Mental Health Counseling:",
          "Hx of other mental illness:","Hx of Depression or other mental illness","evaluation by a licensed mental health professional?","CURRENT PHYSICAL/MENTAL STATUS:","Altered Mental Status:",
          "Altered mental status or worsening altered mental status:","Altered Mental Status","Alt Mental Status:","Alt Mental Status Unable to give hx:","23-Does the person ahve a serious mental illness?",
          "Numbness:","Recent spinal surgery","Rcnt Spinal Surgery","Speech:","Speech/Voice:","Speech/Language Pathologist:","Speech/Hearing Therapy Charge sent:","Speech Therapy Department Notified:",
          "Speech Sound Production Evaluation:","Speech Problems:","Speech problems:","Speech Language Pathologist Ext 2773:","Speech Evaluation Charge Sent:","Speech","Language and Speech Evaluation:",
          "Hearing,Vision or Speech Limitation:","Conversational Speech:","Continue speech-language/swallowing intervention:","Automatic Speech:","(DN)Speech Therapy Notified:","Pupillary Changes:",
          "Pupilary changes:","Pupilary Changes:","Pupils:","Pupils equal/reactive to light:","Pupils","Pupil Size Right (mm):","Pupil Size Left (mm):","Pupil Reaction (R):","Pupil Reaction (L):",
          "Pupil Equality:","abnormal pupil","Behavior","Measurably Reduced Grip Strength","Acute Mental Status Fluctuation","Hx Family Mental Health","Level of Consciousness",   "Mental Health Issues",
          "Decreased Level of Consciousness/Alertness","Memory","Altered Mental Status","Speech").contains(shortname)){
          SzurAftrHedInjry = 1;
          SzurAftrHedInjry_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }
        if(List("Emesis:","Emesis/NG Color:","Emesis Type:","Emesis type:","Emesis (cc):","Emesis","Pt has experienced nausea/vomiting in the past 48 hrs:","Nausea:","NAUSEA/VOMITING:","Nausea/Vomiting:",
          "Nausea Severity:","Nausea or Vomiting +:","Minimal or Absence of nausea and/or vomiting:","Has uncontrolled symptoms (ie nausea,vomiting)","Nausea","Output, Emesis Amount","Nausea Comment",
          "Number of Emesis in 4 Hours","Nausea/Vomiting").contains(shortname)){
          Vomit = 1;
          Vomit_Date = r.getTimestamp(r.fieldIndex("resultdate")).toString
        }


        Results_Observation_Description(r.getString(r.fieldIndex("visituid")),DngMechInj_Grp,DngMechInj_Grp_Date,DngrsMechInjry,DngrsMechInjry_Date,DrgAlIntoxi,DrgAlIntoxi_Date,EvTrmHdNck,EvTrmHdNck_Date,FclNeuroDefct,FclNeuroDefct_Date,Hdac,Hdac_Date,Hdache,Hdache_Date,Hed_NkTrma,Hed_NkTrma_Date,Loss_Concusnes,Loss_Concusnes_Date,PhylSignBsleSklFrctr,PhylSignBsleSklFrctr_Date,PoTrAm,PoTrAm_Date,PstTrmtcAmnsa,PstTrmtcAmnsa_Date,SeizAftHdInju,SeizAftHdInju_Date,Shrtrm_MemryDfict,Shrtrm_MemryDfict_Date,ShrtTrmMemDef,ShrtTrmMemDef_Date,ShTeMeDe,ShTeMeDe_Date,Szr_Inj_,Szr_Inj__Date,SzurAftrHedInjry,SzurAftrHedInjry_Date,Vomit,Vomit_Date )
      }).distinct()


    return section;
  }

  def element_func(r: Row): String = {
    if (!r.isNullAt(r.fieldIndex("practicedescription")))
      return r.getString(r.fieldIndex("practicedescription"))
    else
      return "";
  }

}