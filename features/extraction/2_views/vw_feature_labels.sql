SELECT index_hsp_acct_study_id AS hsp_acct_study_id,
       pat_study_id,
       full_days_after_discharge AS days_to_readmit,
       index_admit_date_time as admit_date_time,
       index_discharge_date_time as discharge_date_time
FROM features.bayes_vw_index_admissions_and_readmissions
