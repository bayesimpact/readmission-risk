SELECT adm.hsp_acct_study_id,
       CASE lower(adm.acct_fin_class_name)
           WHEN 'medicaid' THEN 'medicare'
           ELSE lower(adm.acct_fin_class_name)
       END insurance_type_cat
FROM hospital_account adm
JOIN features.bayes_vw_index_admissions USING (hsp_acct_study_id)