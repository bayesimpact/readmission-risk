SELECT hsp_acct_study_id,
       ccs_category_description
  FROM features.bayes_vw_index_admissions
       LEFT JOIN hospital_px USING (hsp_acct_study_id)
       LEFT JOIN px_id_xwalk USING (final_icd_px_id)
       LEFT JOIN bayes_hcup_ccs_pr USING (icd_9_cm_code)