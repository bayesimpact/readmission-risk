SELECT discharges.*,
       CASE
           WHEN length_of_stay < 4 THEN length_of_stay
           WHEN length_of_stay < 7 THEN 4
           WHEN length_of_stay < 14 THEN 5
           ELSE 7
       END length_of_stay_lace,
       CASE
           WHEN disch_hour = 0 THEN NULL
           WHEN disch_hour >= 18 THEN 'evening'
           WHEN disch_hour >= 13 THEN 'afternoon'
           WHEN disch_hour >= 8 THEN 'morning'
           WHEN disch_hour < 8 THEN 'evening'
       END disch_time_cat,
       CASE patient_status_name
           WHEN 'Discharged to Home or Self Care (Routine Discharge)' THEN 'home_no_service'
           WHEN 'Discharged/transferred to Home Under Care of Organized Home Health Service Org' THEN 'home_health'
           WHEN 'Discharged/transferred to a Facility that Provides Custodial or Supportive Care' THEN 'assisted_living'
           WHEN 'Discharged/transferred to a Medicare Certified Long Term Care Hospital (LTCH)' THEN 'assisted_living'
           WHEN 'Discharged/transferred to a Nursing Fac Certified under Medicaid but not Medicare' THEN 'snf'
           WHEN 'Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification' THEN 'snf'
           WHEN 'Hospice - Home' THEN 'hospice'
           WHEN 'Hospice - Medical Facility (Certified) Providing Hospice Level of Care' THEN 'hospice'
           WHEN 'Discharged/transferred to a Psychiatric Hospital or Psychiatric Hospital Unit' THEN 'hospital'
           WHEN 'Left Against Medical Advice or Discontinued Care' THEN 'discontinued'
           WHEN 'Discharged/transferred to an Inpatient Rehab Facility (IRF)' THEN 'rehab'
           WHEN 'Disch/trans to Another Type of Health Care Inst not Defined Elsewhere in this List' THEN 'other'
           WHEN 'Admitted as an Inpatient to this Hospital' THEN 'other'
           WHEN 'Discharged/transferred to Court/Law Enforcement' THEN 'other'
           WHEN 'Still a Patient' THEN 'other'
           WHEN '' THEN 'other'
           ELSE 'other'
       END disch_location_cat
FROM
  (SELECT hsp_acct_study_id,
          date_part('hour', discharge_date_time) disch_hour,
          to_char(admit_date_time, 'day') disch_weekday_cat,
          date_part('day', discharge_date_time)::VARCHAR(2) disch_day_of_month_cat,
          date_part('day', discharge_date_time - admit_date_time) length_of_stay
   FROM features.bayes_vw_index_admissions) discharges
  JOIN hospital_account accounts USING (hsp_acct_study_id)