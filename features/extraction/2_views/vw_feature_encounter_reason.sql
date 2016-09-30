SELECT acc.hsp_acct_study_id,
       enc_reason_name enc_reason_name_cat
  FROM features.bayes_vw_index_admissions
       JOIN hospital_account acc USING (hsp_acct_study_id)
       LEFT JOIN encounter_rsn rsn
            ON rsn.enc_study_id = acc.prim_enc_study_id
