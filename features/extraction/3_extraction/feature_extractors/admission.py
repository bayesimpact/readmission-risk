"""A feature extractor for admission."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class AdmissionExtractor(FeatureExtractor):
    """
    Generates features related to admission circumstance.

    Features:
    `admission_source_cat`: where did the patient admitted from
        'transfer':
            'Transfer from a Hospital (Different Facility)'
            'Transfer from One Distinct Unit to another Distinct Unit in Same Hospital'
            'Transfer from Skilled Nursing (SNF), Intermediate Care (ICF) or Assisted Living (ALF)'
            'Transfer from Another Health Care Facility'

        'other':
            'Born Outside of this Hospital'
            'Court/Law Enforcement'
            'Born Inside this Hospital'

        'home':
            'Non-Health Care Facility Point of Origin'

        null:
            'Information Not Available'
            ''

        outpatient:
            'Clinic or Physician's Office'

    `admission_type_cat`: what kind of admission is this?
        'emergency': Emergency'
        'urgent': 'Urgent'
        'elective': 'Elective'
        'other': e.g. 'Trauma Center', 'Newborn', 'Information Not Available'

    `admission_weekday_cat`: 'monday', 'tuesday', etc.

    `admission_time_cat`:
        'morning': 8:00 a.m. - 12:59 p.m,
        'afternoon': 1:00 - 5:59 p.m.
        'evening': 6:00 p.m. - 7:59 a.m.

    `acuity_lace`: LACE score associated with acuity (0 or 3 points)

    `hospital_name_cat`: The Sutter hospital location
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id,
                   admission_source_cat,
                   admission_type_cat,
                   admission_weekday_cat,
                   admission_time_cat,
                   acuity_lace,
                   hospital_name_cat
              FROM {}.bayes_vw_feature_admission
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col="hsp_acct_study_id")
        # Les than 5 duplicates across all hospitals. Will drop them
        res = res.groupby(res.index).first()
        return self.emit_df(res)
