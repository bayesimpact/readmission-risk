"""
Individual hospital account object.

Receives patient id and hospital_id.
Provides methods to get different data for that account/patient.
"""

import pandas as pd


class Account(object):
    """Hospital Account object with methods to get data from different tables."""

    def __init__(self, hsp_id, engine):
        """Initialize the object with hos_acct_study_id and db engine."""
        self.engine = engine
        self.hsp_id = hsp_id
        self.pat_id = self._get_pat_id()

    def _get_pat_id(self):
        q = """SELECT hsp_acct_study_id, pat_study_id
                  FROM hospital_account
                  WHERE hsp_acct_study_id=%d
        """ % self.hsp_id

        res = pd.read_sql(q, self.engine, index_col='hsp_acct_study_id')
        try:
            return res.loc[self.hsp_id, 'pat_study_id']
        except:
            print("No patient id could be found for hsp_study_id %d" % self.hsp_id)
            return None

    def _get_demographics(self):
        q = """
            select
                dem.*,
                race.race_name
            from patient_demographics dem
            join patient_race race on dem.pat_study_id=race.pat_study_id
            where dem.pat_study_id = {}
        """.format(self.pat_id)

        res = pd.read_sql(q, self.engine)
        return res

    def _get_hospital_account(self):
        query = """
            select
                acct.*
            from hospital_account acct
            where acct.hsp_acct_study_id = {}
        """.format(self.hsp_id)
        res = pd.read_sql(query, self.engine)

        self.adm_date = res.loc[0, 'adm_date_time']
        self.disch_date = res.loc[0, 'disch_date_time']

        msg_str = "This patient was admitted on %s and discharged on %s."
        print(msg_str % (str(self.adm_date), str(self.disch_date)))
        return res

    def _get_encounters(self):
        query = """
            select
                enc.*
            from encounters enc
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by contact_date
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)
        msg_str = """There are %d encounters in that period. All except %d has hsp_acct_study_id."""

        print(msg_str % (res.shape[0], res[res.hsp_acct_study_id != self.hsp_id].shape[0]))
        return res

    def _get_encounters_dx(self):
        query = """
            select
                enc.contact_date,
                enc.enc_study_id,
                enc.enc_type_name,
                dx.line,
                dx.dx_code,
                dx.dx_name
            from encounters enc
            left join encounter_dx dx using (enc_study_id)
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by enc.contact_date, enc.enc_study_id
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)

        self.enc_dx_list = set(res.dx_code.unique())
        return res

    def _get_encounters_rsn(self):
        query = """
            select
                enc.contact_date,
                enc.enc_study_id,
                enc.enc_type_name,
                rsn.line,
                rsn.enc_reason_name
            from encounters enc
            left join encounter_rsn rsn using (enc_study_id)
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by enc.contact_date, enc.enc_study_id
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)
        return res

    def _get_order_medications(self):
        query = """
            select
                enc.contact_date,
                enc.enc_study_id encounter_study_id,
                enc.enc_type_name,
                med.*
            from encounters enc
            left join order_medication med using (enc_study_id)
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by enc.contact_date, enc.enc_study_id
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)

        print("%d unique medicines have been ordered." % res.order_med_study_id.nunique())
        return res

    def _get_order_procedures(self):
        query = """
            select
                enc.contact_date,
                enc.enc_study_id encounter_study_id,
                enc.enc_type_name,
                proc.*
            from encounters enc
            left join order_procedures proc using (enc_study_id)
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by enc.contact_date, enc.enc_study_id
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)

        print("%d unique procedures have been ordered." % res.order_proc_study_id.nunique())
        res_grouped = res.groupby('encounter_study_id')
        proc_counts = res_grouped.order_proc_study_id.agg(lambda s: s.nunique())
        self.enc_study_id = proc_counts.idxmax()
        return res

    def _get_order_results(self):
        query = """
            select
                proc.order_proc_study_id order_procedure_study_id,
                proc.ordering_date,
                proc.order_type_name,
                proc.order_class_name,
                res.*,
                component_id.common_name,
                component_id.loinc_code
            from order_procedures proc
            join order_results res using (order_proc_study_id)
            join component_id USING (component_id)
            where proc.enc_study_id = {}
    """.format(self.enc_study_id)
        res = pd.read_sql(query, self.engine)

        print("%d unique procedures have results." % res.order_proc_study_id.nunique())
        return res

    def _get_problem_list(self):
        query = """
            select
                prob.*
            from problem_list as prob
            where prob.pat_study_id = {}
            and ((prob.noted_date <= '{}'::date)
            or (prob.noted_date is null))
        """.format(self.pat_id, self.disch_date)
        res = pd.read_sql(query, self.engine)
        self.problems = set(res.ref_bill_code.unique())
        return res

    def _get_hospital_problems(self):
        query = """
            select
                hsp_prob.*
            from hospital_problems hsp_prob
            where hsp_prob.hsp_acct_study_id = {}
        """.format(self.hsp_id)
        res = pd.read_sql(query, self.engine)
        self.hsp_prob_list = set(res.ref_bill_code.unique())
        return res

    def _get_health_history(self):
        query = """
            select
                enc.contact_date,
                enc.enc_study_id encounter_study_id,
                enc.enc_type_name,
                social.*
            from encounters enc
            left join social_hx social using (enc_study_id)
            where enc.pat_study_id = {}
            and contact_date >= '{}'::date
            and contact_date <= '{}'::date
            order by enc.contact_date, enc.enc_study_id
    """.format(self.pat_id, str(self.adm_date).split(' ')[0], str(self.disch_date).split(' ')[0])
        res = pd.read_sql(query, self.engine)
        return res

    def _get_hospital_dx(self):
        query = """
            select
                acct.*,
                dx.line dx_line,
                dx.ref_bill_code,
                dx.dx_name,
                dx.final_dx_poa_name,
                dx.final_dx_soi_name
            from hospital_account acct
            left outer join hospital_dx dx on acct.hsp_acct_study_id=dx.hsp_acct_study_id
            where acct.hsp_acct_study_id = {}
            order by acct.disch_date_time
    """.format(self.hsp_id)
        res = pd.read_sql(query, self.engine)
        self.hsp_dx_list = set(res.ref_bill_code.apply(lambda s: s.replace('V', '')).unique())
        msg_str = "There are %d unique diagnoses associated with this visit."
        print(msg_str % res.ref_bill_code.nunique())
        return res

    def _get_hospital_procedures(self):
        query = """
            select
                acct.*,
                px.line px_line,
                px.final_icd_px_id,
                px.icd_px_name,
                px.proc_date,
                px.proc_prov_study_id
            from hospital_account acct
            left join hospital_px px using (hsp_acct_study_id)
            where acct.hsp_acct_study_id = {}
    """.format(self.hsp_id)
        res = pd.read_sql(query, self.engine)

        msg_str = "There are %d unique procedures associated with this visit."
        print(msg_str % res.final_icd_px_id.nunique())
        return res

    def _get_hospital_cpt(self):
        query = """
            select
                acct.*,
                cpt.line cpt_line,
                cpt.cpt_code,
                cpt.cpt_code_desc,
                cpt.cpt_code_date,
                cpt.cpt_prov_study_id
            from hospital_account acct
            left join hospital_cpt cpt using (hsp_acct_study_id)
            where acct.hsp_acct_study_id = {}
    """.format(self.hsp_id)
        res = pd.read_sql(query, self.engine)

        msg_str = "There are %d unique cpt code associated with this visit."
        print(msg_str % res.cpt_code.nunique())
        return res
