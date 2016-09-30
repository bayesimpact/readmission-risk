/*
* This view creates one row per index admission (from the previous view),
* joining with the next readmission-qualifying hospital visit
* for that patient (or null columns if there is no such visit).
* Then we computing the time elapsed between the discharge of the first
* visit and the admission in the second.
*
* Visits that may be valid readmissions are:
*    - Inpatient stays
*    - NOT Elective admissions
*    - Did not arrive as a transfer from another inpatient stay **
*    - Have a previous index admission for that patient
*
** See below for all possible types of admission and their frequency.
* 
* SELECT
* count(*), admission_source_name
* FROM hospital_account
* WHERE acct_type_name='Inpatient'
* GROUP BY 2 ORDER BY 1 desc;
* 
* 232017  Non-Health Care Facility Point of Origin
*  12697  Transfer from Another Health Care Facility
*   6782  Emergency Room
*   4186  Transfer from One Distinct Unit to another Distinct Unit in Same Hospital
*   3480  Transfer from Skilled Nursing (SNF), Intermediate Care (ICF) or Assisted Living (ALF)
*    796  Transfer from a Hospital (Different Facility)
*    255  Clinic or Physician's Office
*    223  nan
*    176  Court/Law Enforcement
*    148  Information Not Available
*     21  Born Inside this Hospital
*      3  Born Outside of this Hospital
*      1  Readmission to Same Home Health Agency
*/

WITH readmissions AS (
    SELECT
        acct.hsp_acct_study_id,
        acct.pat_study_id,
        v.admit_date_time,
        v.discharge_date_time
    FROM hospital_account acct
    JOIN features.bayes_vw_admission_discharge_timestamp v USING(hsp_acct_study_id)
    WHERE acct.admission_type_name != 'Elective'
    AND acct.acct_type_name = 'Inpatient'
    AND acct.admission_source_name NOT IN (
        'Transfer from Another Health Care Facility',
        'Transfer from One Distinct Unit to another Distinct Unit in Same Hospital',
        'Transfer from a Hospital (Different Facility)')
)

SELECT
    DISTINCT ON (index_admissions.hsp_acct_study_id)
    index_admissions.pat_study_id,
    index_admissions.hsp_acct_study_id index_hsp_acct_study_id,
    index_admissions.admit_date_time index_admit_date_time,
    index_admissions.discharge_date_time index_discharge_date_time,
    index_admissions.loc_name,
    readmissions.hsp_acct_study_id re_hsp_acct_study_id,
    readmissions.admit_date_time re_admit_date_time,
    readmissions.discharge_date_time re_discharge_date_time,
    date_part('day', (readmissions.admit_date_time - index_admissions.discharge_date_time)) AS full_days_after_discharge

FROM features.bayes_vw_index_admissions index_admissions
LEFT JOIN readmissions
    ON (readmissions.hsp_acct_study_id != index_admissions.hsp_acct_study_id)
    AND (readmissions.pat_study_id = index_admissions.pat_study_id)
    AND (readmissions.admit_date_time >= index_admissions.discharge_date_time)

ORDER BY index_admissions.hsp_acct_study_id, readmissions.admit_date_time ASC NULLS LAST
