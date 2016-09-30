      WITH accounts AS
        (SELECT hsp.hsp_acct_study_id,
                hsp.acct_type_name,
                hsp.pat_study_id,
                hsp.disch_date_time
         FROM hospital_account hsp
         JOIN features.bayes_vw_index_admissions USING (hsp_acct_study_id)),

      problems AS
        (   SELECT *,
                EXTRACT (DAY
                         FROM noted_date - lag(noted_date) OVER (PARTITION BY pat_study_id, condition_cat
                                                                 ORDER BY noted_date)) AS days_since_prior_dx
            FROM (SELECT *
                 FROM features.bayes_vw_hospital_inpatient_dx
                 UNION ALL SELECT *
                 FROM features.bayes_vw_hospital_outpatient_dx
                 UNION ALL SELECT *
                 FROM features.bayes_vw_non_hospital_dx) unions),
      problems_per_account AS
        (SELECT hsp.hsp_acct_study_id,
                problems.pat_study_id,
                problems.noted_date,
                problems.dx_mode,
                problems.icd_9_cm_code,
                problems.dx_source,
                problems.condition_cat,
                problems.weight,
                problems.days_since_prior_dx
         FROM accounts hsp
         LEFT JOIN problems ON (hsp.pat_study_id=problems.pat_study_id)
         AND (problems.noted_date<=hsp.disch_date_time) --ONLY FOR visits before CURRENT admission
         AND (problems.noted_date>=(hsp.disch_date_time - INTERVAL '12 month'))
         AND ((dx_mode='Inpatient') OR (days_since_prior_dx>=30)))

SELECT DISTINCT ON (hsp_acct_study_id, condition_cat)
      problems_per_account.*
FROM problems_per_account
ORDER BY hsp_acct_study_id,
         condition_cat,
         noted_date ASC

-- This view takes ~25 min to render on aptible.
