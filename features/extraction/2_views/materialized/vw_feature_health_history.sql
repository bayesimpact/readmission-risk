WITH history_encounters AS (SELECT *
                              FROM encounters
                             WHERE enc_type_name='History'
                          ORDER BY contact_date DESC)

SELECT DISTINCT ON (acct.hsp_acct_study_id)
       acct.hsp_acct_study_id,
       CASE hx.tobacco_user_name
           WHEN '' THEN NULL
           WHEN 'Not Asked' THEN NULL
           ELSE lower(hx.tobacco_user_name)
       END tobacco_cat,
       CASE hx.alcohol_use_name
           WHEN 'Yes' THEN 'yes'
           WHEN 'No' THEN 'no'
           ELSE NULL
       END alcohol_cat,
       CASE hx.ill_drug_user_name
           WHEN 'Yes' THEN 'yes'
           WHEN 'No' THEN 'no'
           ELSE NULL
       END drugs_cat
  FROM hospital_account acct
       JOIN features.bayes_vw_index_admissions
       USING (hsp_acct_study_id)

       LEFT JOIN history_encounters enc
       ON enc.pat_study_id = acct.pat_study_id

       LEFT JOIN social_hx hx
       USING (enc_study_id)

-- This view takes ~2 min to render on aptible.
