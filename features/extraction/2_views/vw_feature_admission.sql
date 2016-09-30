SELECT hsp_acct_study_id,
       loc_name hospital_name_cat,
       to_char(admit_date_time, 'day') admission_weekday_cat,
       CASE
           WHEN date_part('hour', admit_date_time) >= 18 THEN 'evening'
           WHEN date_part('hour', admit_date_time) >= 13 THEN 'afternoon'
           WHEN date_part('hour', admit_date_time) >= 8 THEN 'morning'
           WHEN date_part('hour', admit_date_time) < 8 THEN 'evening'
           ELSE NULL
       END admission_time_cat,
       CASE admission_source_name
           WHEN 'Transfer from a Hospital (Different Facility)' THEN 'transfer'
           WHEN 'Transfer from Another Health Care Facility' THEN 'transfer'
           WHEN 'Transfer from One Distinct Unit to another Distinct Unit in Same Hospital' THEN 'transfer'
           WHEN 'Transfer from Skilled Nursing (SNF), Intermediate Care (ICF) or Assisted Living (ALF)' THEN 'transfer'
           WHEN 'Non-Health Care Facility Point of Origin' THEN 'home'
           WHEN 'Clinic or Physician''s Office' THEN 'outpatient'
           WHEN '' THEN NULL
           WHEN 'Information Not Available' THEN NULL
           ELSE 'other'
       END admission_source_cat,
       CASE admission_type_name
           WHEN 'Trauma Center' THEN 'other'
           WHEN 'Newborn' THEN 'other'
           WHEN 'Information Not Available' THEN 'other'
           ELSE lower(admission_type_name)
       END admission_type_cat,
       CASE
           WHEN admission_type_name='Emergency' THEN 3
           ELSE 0
       END acuity_lace
FROM features.bayes_vw_index_admissions