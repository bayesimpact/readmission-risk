SELECT adm.hsp_acct_study_id,
       pre.acct_type_name AS pre_adm_type,
       (pre.adm_date_time IS NOT NULL)::INT pre_total_adm,
       (pre.disch_date_time >= (adm.adm_date_time - INTERVAL '3 month'))::INT pre_3_month,
       (pre.disch_date_time >= (adm.adm_date_time - INTERVAL '6 month'))::INT pre_6_month,
       (pre.disch_date_time >= (adm.adm_date_time - INTERVAL '1 year'))::INT pre_12_month
FROM hospital_account adm
JOIN features.bayes_vw_index_admissions USING (hsp_acct_study_id)
LEFT JOIN hospital_account pre ON adm.pat_study_id = pre.pat_study_id
AND adm.adm_date_time > pre.disch_date_time