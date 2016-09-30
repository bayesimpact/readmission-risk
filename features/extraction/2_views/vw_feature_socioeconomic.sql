SELECT hsp.hsp_acct_study_id,
               cen.*
        FROM hospital_account hsp
            JOIN features.bayes_vw_index_admissions index_admissions
                    USING (hsp_acct_study_id)
            LEFT  JOIN bayes_patient_location loc
                    ON (index_admissions.pat_study_id=loc.pat_study_id)
            LEFT JOIN bayes_census cen
                    USING (tract_id)