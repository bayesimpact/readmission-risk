WITH non_hospital_encs AS
  (SELECT encs.enc_study_id,
          encs.pat_study_id,
          encs.contact_date noted_date
   FROM encounters encs
   JOIN features.bayes_vw_index_admissions USING (pat_study_id)
   LEFT JOIN hospital_encounters hsp_encs ON hsp_encs.enc_study_id=encs.enc_study_id
   WHERE hsp_encs.enc_study_id IS NULL)

SELECT DISTINCT ON (enc_study_id)
       NULL::BIGINT hsp_acct_study_id,
       non_hospital_encs.pat_study_id,
       non_hospital_encs.noted_date,
       'outpatient'::VARCHAR dx_mode,
       enc_dx.icd_9_cm_code,
       'encounter_dx'::VARCHAR AS dx_source,
       icd_9_cci_xwalk.condition_cat,
       icd_9_cci_xwalk.weight
FROM non_hospital_encs
JOIN encounter_dx enc_dx USING (enc_study_id)
JOIN icd_9_cci_xwalk USING (icd_9_cm_code)