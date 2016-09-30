SELECT adm.hsp_acct_study_id,
       date_part('year', age(adm.adm_date_time, demo.birth_date)) age,
       (demo.sex_name='Female') AS if_female_bool,
       race.race_name,
       CASE ethnic_group_name
           WHEN 'Unknown' THEN NULL
           WHEN '' THEN NULL
           WHEN 'Prefer Not to Answer' THEN NULL
           WHEN 'Non Hispanic' THEN FALSE
           ELSE TRUE
       END if_hispanic_bool,
       CASE demo.marital_status_name
            WHEN '' THEN NULL
            ELSE demo.marital_status_name
       END marital_status_name,
       (intrptr_needed_yn=TRUE) AS if_intrptr_needed_bool
  FROM hospital_account adm
       JOIN patient_demographics demo 
       USING (pat_study_id)
       
       LEFT JOIN patient_race race 
       USING (pat_study_id)

       JOIN features.bayes_vw_index_admissions
       USING (hsp_acct_study_id)