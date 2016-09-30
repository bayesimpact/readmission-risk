/*
* For convenience, creates a 1% sample of hospital visits.
*/
SELECT *
FROM features.bayes_vw_index_admissions
where pat_study_id % 100 = 0