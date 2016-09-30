WITH accounts AS (SELECT acct.hsp_acct_study_id,
                         acct.pat_study_id,
                         acct.adm_date_time AS noted_date,
                         acct.acct_type_name dx_mode
                  FROM hospital_account acct
                       JOIN features.bayes_vw_index_admissions
                       USING (pat_study_id)
                  WHERE acct.acct_type_name != 'Inpatient')

SELECT problems.*,
       icd_9_cci_xwalk.condition_cat,
       icd_9_cci_xwalk.weight
FROM (SELECT hsp.*,
             hsp_dx.icd_9_cm_code,
             'hospital_dx'::varchar AS dx_source
        FROM accounts hsp 
             JOIN hospital_dx hsp_dx USING (hsp_acct_study_id)
      UNION
      SELECT hsp.*,
             hsp_prob.icd_9_cm_code,
             'hospital_problems'::varchar AS dx_source
        FROM accounts hsp 
             JOIN hospital_problems hsp_prob USING (hsp_acct_study_id)
      UNION
      SELECT hsp.*,
             px_id_xwalk.icd_9_cm_code,
             'hospital_px'::VARCHAR AS dx_source
        FROM accounts hsp 
             JOIN hospital_px hsp_px USING (hsp_acct_study_id)
             JOIN px_id_xwalk USING (final_icd_px_id)
     ) AS problems
     JOIN icd_9_cci_xwalk 
     USING (icd_9_cm_code)