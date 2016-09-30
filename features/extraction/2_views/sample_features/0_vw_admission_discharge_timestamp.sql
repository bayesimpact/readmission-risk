/*
* Identical to the view by the same name in the parent folder,
* but limited to 500 random (ish) patients from Modesto.
* This creates a small sample, useful for testing.
*/
SELECT
  acct.hsp_acct_study_id,
  MIN(encounters.hosp_admsn_time) as admit_date_time,
  MAX(encounters.hosp_disch_time) as discharge_date_time
FROM hospital_account acct
LEFT JOIN hospital_encounters encounters USING (hsp_acct_study_id)
WHERE acct.acct_type_name='Inpatient'
AND acct.loc_name = 'MEMORIAL MEDICAL CTR MODESTO'
GROUP BY acct.hsp_acct_study_id
