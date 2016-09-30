SELECT acct.hsp_acct_study_id,
       med.pharm_class_name,
       med.pharm_subclass_name,
       CASE med.controlled_med_yn
           WHEN 'Y' THEN TRUE
           ELSE NULL
       END AS controlled_med,
       med.dea_class_code_name,
       enc.enc_type_name,
       ord.ordering_mode_name,
       ord.order_status_name
  FROM hospital_account acct
       JOIN features.bayes_vw_index_admissions
       USING (hsp_acct_study_id)

       LEFT JOIN encounters enc
       ON enc.pat_study_id = acct.pat_study_id
           AND enc.contact_date >= acct.adm_date_time::DATE
           AND enc.contact_date <= acct.disch_date_time::DATE

       LEFT JOIN order_medication ord
       USING (enc_study_id)

       LEFT JOIN medication_id med
       USING (medication_id)

-- This view takes 3-4 min to render on aptible.
