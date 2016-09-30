WITH current_hsp_problems
         AS (SELECT *,
                    format('''%s''', rpad(replace(hosp_problems.icd9, '.', ''), 5, ' ')) icd_9_cm_code
               FROM (SELECT hsp_prob.hsp_acct_study_id,
                            unnest(string_to_array(concat_ws(', ', hsp_prob.ref_bill_code, hsp_prob.current_icd9_list), ', ')) icd9
                       FROM hospital_problems hsp_prob
                            JOIN features.bayes_vw_index_admissions USING (hsp_acct_study_id)
                    ) hosp_problems
            ),
     current_hcup_conditions
         AS (SELECT current_hsp_problems.*,
                    ccs.ccs_category_description
                    FROM current_hsp_problems
                         JOIN bayes_hcup_ccs_dx ccs USING (icd_9_cm_code))

SELECT DISTINCT ON (index_admission.hsp_acct_study_id, hcup_conds.ccs_category_description)
       index_admission.hsp_acct_study_id,
       hcup_conds.ccs_category_description
FROM features.bayes_vw_index_admissions index_admission
            LEFT JOIN current_hcup_conditions hcup_conds USING (hsp_acct_study_id)