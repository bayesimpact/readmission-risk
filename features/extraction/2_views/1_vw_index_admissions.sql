/*
* An "index admission" is any hopital stay for which we may want to
* know or predict if the patient is readmitted.
*
* We count hospital visits that are:
*    - Inpatient stays
*    - The patient did not die before discharge
*    - The "discharge" was a true discharge, not:
*      - a transfer to another inpatient stay
*      - to a hospice
*      - something else funky that we can't be sure is a real discharge
*      - (See below for all possible types of discharge and their frequency)
*
* SELECT
* count(*), patient_status_name
* FROM hospital_account
* WHERE acct_type_name='Inpatient'
* AND patient_status_name NOT LIKE 'Expired%%'
* GROUP BY patient_status_name ORDER BY 1 desc
* 
* 167021  Discharged to Home or Self Care (Routine Discharge)
*  35097  Discharged/transferred to Home Under Care of Organized Home Health Service Org
*  31930  Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification
*   5628  Discharged/transferred to a Short-Term General Hospital for Inpatient Care
*   3530  Hospice - Home
*   2657  Left Against Medical Advice or Discontinued Care
*   2370  Discharged/transferred to an Inpatient Rehab Facility (IRF)
*   1843  Discharged/transferred to a Medicare Certified Long Term Care Hospital (LTCH)
*   1239  Hospice - Medical Facility (Certified) Providing Hospice Level of Care
*    898  Discharged/transferred to a Psychiatric Hospital or Psychiatric Hospital Unit
*    740  nan
*    313  Discharged/transferred to a Facility that Provides Custodial or Supportive Care
*    213  Disch/trans to Another Type of Health Care Inst not Defined Elsewhere in this List
*    109  Discharged/transferred to a Nursing Fac Certified under Medicaid but not Medicare
*     73  Admitted as an Inpatient to this Hospital
*     60  Still a Patient
*     51  Discharged/transferred to a Federal Health Care Facility
*     41  Discharged/transferred to a Critical Access Hospital (CAH)
*     19  Discharged/transferred to a Designated Cancer Center or Children's Hospital
*      2  Discharged/transferred to a Hospital-Based Medicare Approved Swing Bed
*      1  Discharged/transferred to Court/Law Enforcement
*/

SELECT
    acct.pat_study_id,
    acct.hsp_acct_study_id,
    acct.acct_type_name,
    v.admit_date_time,
    v.discharge_date_time,
    acct.admission_type_name,
    acct.admission_source_name,
    acct.loc_name
FROM hospital_account acct
JOIN features.bayes_vw_admission_discharge_timestamp v USING (hsp_acct_study_id)
WHERE acct.acct_type_name='Inpatient'
AND acct.patient_status_name NOT LIKE 'Expired%%'
AND acct.patient_status_name IN (
    'Discharged to Home or Self Care (Routine Discharge)',
    'Discharged/transferred to Home Under Care of Organized Home Health Service Org',
    'Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification')
AND v.admit_date_time <= v.discharge_date_time
