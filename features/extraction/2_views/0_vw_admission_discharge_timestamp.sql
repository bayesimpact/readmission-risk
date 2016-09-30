/*
* For a given inpatient stay, we need to know the date and time
* of admission and of discharge. Computing this
* SHOULD be straightforward, but it's not. To get accurate
* numbers, we have to look the timestamps of the *encounters*
* of each visit (hospital_encounters), not the less-correct
* columns in hospital_account.
*
* This view is meant to be joined with as the definitive source
* of admit/discharge timestamps for a given inpatient hospital stay.
*/

SELECT
  acct.hsp_acct_study_id,
  MIN(enc.hosp_admsn_time) as admit_date_time,
  MAX(enc.hosp_disch_time) as discharge_date_time
FROM hospital_account acct
LEFT JOIN hospital_encounters enc USING (hsp_acct_study_id)
WHERE acct.acct_type_name='Inpatient'
GROUP BY acct.hsp_acct_study_id
