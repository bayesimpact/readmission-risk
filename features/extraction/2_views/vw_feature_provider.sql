SELECT hsp_acct_study_id,
       sp.specialty
  FROM features.bayes_vw_index_admissions
       JOIN hospital_account acc USING (hsp_acct_study_id)
       LEFT JOIN providers prov
            ON prov.prov_study_id = acc.attending_prov_study_id
       LEFT JOIN provider_specialty sp
            ON sp.prov_study_id = acc.attending_prov_study_id
