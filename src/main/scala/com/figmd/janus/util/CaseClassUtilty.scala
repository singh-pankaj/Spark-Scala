package com.figmd.janus.util

import java.util.List

//broadcast
case class masterList(medicationList: Map[String, List[String]], codeList: Map[String, List[String]], code1List: Map[Double, List[String]])

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation1
case class Social_History_Observation_M_TobaccoScreening_New_LessThanEqual_case(visituid: String, ToUsSc: Int, ToUsSc_date: String, TobcoUseScrn: Int, TobcoUseScrn_date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation2
case class Social_History_Observation_M_TobaccoUser_New_case(visituid: String, CurntTobcNUsr: Int, CurntTobcNUsr_Date: String, TobNU: Int, TobNU_Date: String, TobcUTC: Int, TobcUTC_Date: String, ToUs_1: Int, ToUs_1_Date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation3
case class Social_History_Observation_M_TobaccoUser_New_Crosswalk(visituid: String, CurntTobcNUsr: Int, CurntTobcNUsr_Date: String, TobNU: Int, TobNU_Date: String, TobcUTC: Int, TobcUTC_Date: String, ToUs_1: Int, ToUs_1_Date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation4
case class Social_History_Observation_M2_TobaccoUser_New(visituid: String, CurntTobcNUsr: Int, CurntTobcNUsr_Date: String, TobNU: Int, TobNU_Date: String, TobcUTC: Int, TobcUTC_Date: String, ToUs_1: Int, ToUs_1_Date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation5
case class Social_History_Observation_M2_TobaccoUser_New_Crosswalk(visituid: String, CurntTobcNUsr: Int, CurntTobcNUsr_Date: String, TobNU: Int, TobNU_Date: String, TobcUTC: Int, TobcUTC_Date: String, ToUs_1: Int, ToUs_1_Date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation6
case class USACS_Social_History_Observation_M_TobaccoUser_New(visituid: String, TobNU: Int, TobNU_Date: String, ToUs_1: Int, ToUs_1_Date: String)

//com.figmd.janus.Sections.SocialHistoryObservation.SocialHistoryObservation6
case class Social_History_Observation_TobCessCounseling(visituid: String, ToUsCeCo: Int, ToUsCeCo_Date: String)

//com.figmd.janus.Sections.VitalSigns.VitalSigns1
case class VitalSigns_M(visituid: String, DiBlPr: Int, DiBlPr_Date: String, SyBlPr: Int, SyBlPr_Date: String)

//com.figmd.janus.Sections.VitalSigns.VitalSigns2
case class VitalSigns_M1_hypertension(visituid: String, FiofHy: Int, FiofHy_Date: String, LiRe: Int, LiRe_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication1
case class Medications_M_Null_Drugname(visituid: String, Antibiotics_Dis: Int, Antibiotics_Dis_Date: String, AntimcThrp_Medc: Int, AntimcThrp_Medc_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication2
case class Medications_M1_Crosswalk(visituid: String, TopPrepr: Int, TopPrepr_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication3
case class Medications_M1_MedCodeCrosswalk(visituid: String, AnMefoPh: Int, AnMefoPh_Date: String, Antibiotics_Dis: Int, Antibiotics_Dis_Date: String, Anticoag: Int, Anticoag_Date: String, AntimcThrp_Medc: Int, AntimcThrp_Medc_Date: String, IVtPA: Int, IVtPA_Date: String, prs_war_Med: Int, prs_war_Med_Date: String, TopPrepr: Int, TopPrepr_Date: String, ToUsCePh: Int, ToUsCePh_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication4
case class Medications_M1_MedCodeCrosswalk_Equals(visituid: String, AntiPlatThrp: Int, AntiPlatThrp_Date: String, CrystSep: Int, CrystSep_Date: String, Epnephrne: Int, Epnephrne_Date: String, IVAntibioticSep: Int, IVAntibioticSep_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication5
case class Medications_M2_Antibiotics(visituid: String, Antibiotics_Dis: Int, Antibiotics_Dis_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication6
case class Medications_M2_Specific_Equals_Replace(visituid: String, CrystSep: Int, CrystSep_Date: String, IVAntibioticSep: Int, IVAntibioticSep_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication7
case class Medications_M2_specific_LessThanEquals(visituid: String, AnMefoPh: Int, AnMefoPh_Date: String, AnPhTh: Int, AnPhTh_Date: String, Antibiotics_Dis: Int, Antibiotics_Dis_Date: String, Antibtc: Int, Antibtc_Date: String, AntimcThrp_Medc: Int, AntimcThrp_Medc_Date: String, IVtPA: Int, IVtPA_Date: String, prs_war_Med: Int, prs_war_Med_Date: String, TopPrepr: Int, TopPrepr_Date: String, ToUsCePh: Int, ToUsCePh_Date: String)

//com.figmd.janus.Sections.PatientMedication.PatientMedication8
case class Medications_M_CurrentMedication(visituid: String, Crntmed: Int, Crntmed_Date: String)

//com.figmd.janus.Sections.PlanOfCare.PlanOfCare1
case class Plan_of_Care_M_LessThanEquals(visituid: String, TbcCesCon: Int, TbcCesCon_Date: String, ToUsCeCo: Int, ToUsCeCo_Date: String)

//com.figmd.janus.Sections.PlanOfCare.PlanOfCare2
case class Plan_of_Care_M1_Crosswalk(visituid: String, TbcCesCon: Int, TbcCesCon_Date: String, ToUsCeCo: Int, ToUsCeCo_Date: String)

//com.figmd.janus.Sections.PatientProblem.PatientProblem1
case class Problem_ICD_Codes(visituid: String, ScndThrdDgreeBurn: Int, ScndThrdDgreeBurn_Date: String, Szure: Int, Szure_Date: String)

//com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder1
case class Patient_Lab_Order_M_EqualsALTER(visituid: String, AbdCT: Int, AbdCT_Date: String, AdvBrnImging: Int, AdvBrnImging_Date: String, BldCultre: Int, BldCultre_Date: String, CT_Abd_Pel: Int, CT_Abd_Pel_Date: String, CT_E1: Int, CT_E1_Date: String, CT_PE: Int, CT_PE_Date: String, CTHd: Int, CTHd_Date: String, CTTorso: Int, CTTorso_Date: String, GlomFiltRate: Int, GlomFiltRate_Date: String, InsSpeInd: Int, InsSpeInd_Date: String, KUB_Xray_AP: Int, KUB_Xray_AP_Date: String, LeukEstUrn: Int, LeukEstUrn_Date: String, LeukUrine: Int, LeukUrine_Date: String, MeasUriOut: Int, MeasUriOut_Date: String, MRI_APF: Int, MRI_APF_Date: String, NitrUrine: Int, NitrUrine_Date: String, SerLact: Int, SerLact_Date: String, Ultra_APF: Int, Ultra_APF_Date: String, X_Ray_QI: Int, X_Ray_QI_Date: String)

//com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder2
case class Patient_Lab_Order_M_LessThanEquals(visituid: String, CTScnPrnslSinsus: Int, CTScnPrnslSinsus_Date: String, D_Dimr: Int, D_Dimr_Date: String, EC12leorstor: Int, EC12leorstor_Date: String, GrAStTe: Int, GrAStTe_Date: String, INR: Int, INR_Date: String, LaTefoHy: Int, LaTefoHy_Date: String, PregUrnSrm: Int, PregUrnSrm_Date: String, PrtrmTme: Int, PrtrmTme_Date: String, Rh_immuneOdrd: Int, Rh_immuneOdrd_Date: String, Rh_immuneOrdr: Int, Rh_immuneOrdr_Date: String, TATV_performd: Int, TATV_performd_Date: String, TATV_prformd: Int, TATV_prformd_Date: String, TrnsVgUS: Int, TrnsVgUS_Date: String, UltrTransAb: Int, UltrTransAb_Date: String)

//com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder3
case class Patient_Lab_Order_M_Procedure_Crosswalk(visituid: String, ActParThrmplstnTme: Int, ActParThrmplstnTme_Date: String, BldCultre: Int, BldCultre_Date: String, Creatnine: Int, Creatnine_Date: String, CT_Abd_Pel: Int, CT_Abd_Pel_Date: String, CTParnsn: Int, CTParnsn_Date: String, CTTorso: Int, CTTorso_Date: String, EC12leorstor: Int, EC12leorstor_Date: String, GlomFiltRate: Int, GlomFiltRate_Date: String, GrAStTe: Int, GrAStTe_Date: String, HdCT: Int, HdCT_Date: String, HdCtPrfrmd: Int, HdCtPrfrmd_Date: String, HECTPE: Int, HECTPE_Date: String, INR: Int, INR_Date: String, KUB_Xray_AP: Int, KUB_Xray_AP_Date: String, LaTefoHy: Int, LaTefoHy_Date: String, PregUrnSrm: Int, PregUrnSrm_Date: String, PrtrmTme: Int, PrtrmTme_Date: String, RptSrmLc: Int, RptSrmLc_Date: String, SerLact: Int, SerLact_Date: String, TbcCesCon: Int, TbcCesCon_Date: String, TobcoUseScrn: Int, TobcoUseScrn_Date: String, TobcUTC: Int, TobcUTC_Date: String, ToUs_1: Int, ToUs_1_Date: String, ToUsCeCo: Int, ToUsCeCo_Date: String, ToUsCePh: Int, ToUsCePh_Date: String, ToUsSc: Int, ToUsSc_Date: String, TrnsVgUS: Int, TrnsVgUS_Date: String, Ultra_APF: Int, Ultra_APF_Date: String, UltrTransAb: Int, UltrTransAb_Date: String)

//com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder4
case class Patient_Lab_Order_M1_Crosswalk(visituid: String, D_Dimr: Int, D_Dimr_Date: String, EC12leorstor: Int, EC12leorstor_Date: String, GrAStTe: Int, GrAStTe_Date: String, INR: Int, INR_Date: String, LaTefoHy: Int, LaTefoHy_Date: String, PregUrnSrm: Int, PregUrnSrm_Date: String, PrtrmTme: Int, PrtrmTme_Date: String)

//com.figmd.janus.Sections.PatientLabOrder.PatientLabOrder5
case class Patient_Lab_Order_M1_Crosswalk_Equals(visituid: String, BldCultre: Int, BldCultre_Date: String, CTScnPrnslSinsus: Int, CTScnPrnslSinsus_Date: String, CTTorso: Int, CTTorso_Date: String, SerLact: Int, SerLact_Date: String)

//com.figmd.janus.Sections.ResultObservation.ResultObservation1
case class Results_Observation_98494(visituid: String, MaStBaTe: Int, MaStBaTe_Date: String)

//com.figmd.janus.Sections.ResultObservation.ResultObservation2
case class Results_Observation_Description(visituid: String, DngMechInj_Grp: Int, DngMechInj_Grp_Date: String, DngrsMechInjry: Int, DngrsMechInjry_Date: String, DrgAlIntoxi: Int, DrgAlIntoxi_Date: String, EvTrmHdNck: Int, EvTrmHdNck_Date: String, FclNeuroDefct: Int, FclNeuroDefct_Date: String, Hdac: Int, Hdac_Date: String, Hdache: Int, Hdache_Date: String, Hed_NkTrma: Int, Hed_NkTrma_Date: String, Loss_Concusnes: Int, Loss_Concusnes_Date: String, PhylSignBsleSklFrctr: Int, PhylSignBsleSklFrctr_Date: String, PoTrAm: Int, PoTrAm_Date: String, PstTrmtcAmnsa: Int, PstTrmtcAmnsa_Date: String, SeizAftHdInju: Int, SeizAftHdInju_Date: String, Shrtrm_MemryDfict: Int, Shrtrm_MemryDfict_Date: String, ShrtTrmMemDef: Int, ShrtTrmMemDef_Date: String, ShTeMeDe: Int, ShTeMeDe_Date: String, `Szr_Inj_`: Int, Szr_Inj__Date: String, SzurAftrHedInjry: Int, SzurAftrHedInjry_Date: String, Vomit: Int, Vomit_Date: String)

//com.figmd.janus.Sections.ResultObservation.ResultObservation3
case class Results_Observation_M(visituid: String, DngMechInj_Grp: Int, DngMechInj_Grp_Date: String, DngrsMechInjry: Int, DngrsMechInjry_Date: String, DrgAlIntoxi: Int, DrgAlIntoxi_Date: String, EC12leorstor: Int, EC12leorstor_Date: String, EvTrmHdNck: Int, EvTrmHdNck_Date: String, GrAStTe: Int, GrAStTe_Date: String, INR: Int, INR_Date: String, LiLiEx: Int, LiLiEx_Date: String, PoTrAm: Int, PoTrAm_Date: String, PrtrmTme: Int, PrtrmTme_Date: String, Rh_Negtv: Int, Rh_Negtv_Date: String, Seve: Int, Seve_Date: String, ShTeMeDe: Int, ShTeMeDe_Date: String, `Szr_Inj_`: Int, Szr_Inj__Date: String)

//com.figmd.janus.Sections.ResultObservation.ResultObservation4
case class Results_Observation_M_Equals(visituid: String, SerLact: Int, SerLact_Date: String)





