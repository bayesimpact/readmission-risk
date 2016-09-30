  SELECT DISTINCT ON (hsp_acct_study_id, component.common_name)
         adm.hsp_acct_study_id,
         result_flag_name,
         ord_num_value,
         component.*
    FROM features.bayes_vw_index_admissions adm
         LEFT JOIN order_procedures_supp proc
            ON proc.pat_study_id=adm.pat_study_id
            AND ordering_mode_name = 'Inpatient'

         LEFT JOIN hospital_account acct
         ON acct.hsp_acct_study_id = adm.hsp_acct_study_id

         LEFT JOIN order_results res
         ON res.order_proc_study_id = proc.order_proc_study_id
             AND res.result_date >= acct.adm_date_time::DATE
             AND res.result_date <= acct.disch_date_time::DATE

         LEFT JOIN component_id component
         USING (component_id)
ORDER BY hsp_acct_study_id, common_name, res.result_date DESC

-- This view takes ~10 min to render on aptible.
