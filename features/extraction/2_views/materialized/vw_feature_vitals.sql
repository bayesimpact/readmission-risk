-- only consider encounters that have some kind of measurements included
--     (weight, pulse, and blood pressure are the most common, but usually appear together)
-- prioritize encounters that have vitals (e.g. blood pressure)
--     over those that only have height/weight
WITH encounters_with_measurements AS (SELECT *,
                                             (bp_systolic IS NOT NULL) has_bp
                                      FROM encounters
                                      WHERE bp_systolic IS NOT NULL
                                          OR pulse IS NOT NULL
                                          OR weight IS NOT NULL
                                      ORDER BY has_bp DESC,
                                               contact_date DESC)

SELECT DISTINCT ON (acct.hsp_acct_study_id)
       acct.hsp_acct_study_id,
       enc.contact_date,
       enc.temperature,
       enc.pulse,
       enc.respirations,
       enc.bp_systolic,
       enc.bp_diastolic,
       enc.height,
       enc.weight,
       enc.bmi
  FROM hospital_account acct
       JOIN features.bayes_vw_index_admissions
       USING (hsp_acct_study_id)

       LEFT JOIN encounters_with_measurements enc
       ON enc.pat_study_id = acct.pat_study_id
           AND enc.contact_date >= acct.adm_date_time::DATE
           AND enc.contact_date <= acct.disch_date_time::DATE

-- This view takes ~2 min to render on aptible.
