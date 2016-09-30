
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
AND acct.loc_name = 'MEMORIAL MEDICAL CTR MODESTO'
AND acct.patient_status_name NOT LIKE 'Expired%%'
AND acct.patient_status_name IN (
    'Discharged to Home or Self Care (Routine Discharge)',
    'Discharged/transferred to Home Under Care of Organized Home Health Service Org',
    'Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification')
AND v.admit_date_time <= v.discharge_date_time

ORDER BY acct.hsp_acct_study_id % 100
LIMIT 500
