"""A feature extractor for discharge features."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class DischargeExtractor(FeatureExtractor):
    """
    Generates features related to discharge.

    Features:
    `length_of_stay` - Number of days that patient has stayed in the hospital
    `length_of_stay_lace` - LACE score associated with length_of_stay (1-7)

    `disch_weekday_cat` - day of the week for discharge ('monday', 'tuesday', ...)

    `disch_day_of_month` - day of the month for discharge (int)

    `disch_time_cat`:
        morning: 8:00 a.m. - 12:59 p.m,
        afternoon: 1:00 - 5:59 p.m.
        evening: 6:00 p.m. - 7:59 a.m.

    `disch_location_cat`:
        home_no_service:
            "Discharged to Home or Self Care (Routine Discharge)"

        home_health:
            "Discharged/transferred to Home Under Care of Organized Home Health Service Org"

        assisted_living:
            "Discharged/transferred to a Facility that Provides Custodial or Supportive Care"
            "Discharged/transferred to a Medicare Certified Long Term Care Hospital (LTCH)"

        other:
            "Discharged/transferred to Court/Law Enforcement"
            "Admitted as an Inpatient to this Hospital"
            "Disch/trans to Another Type of Health Care Inst not Defined Elsewhere in this List"
            ""
            "Still a Patient"

        snf:
            "Discharged/transferred to a Nursing Fac Certified under Medicaid but not Medicare"
            "Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification"

        hospital:
            "Discharged/transferred to a Psychiatric Hospital or Psychiatric Hospital Unit"

        discontinued:
            "Left Against Medical Advice or Discontinued Care",481

        rehab:
            "Discharged/transferred to an Inpatient Rehab Facility (IRF)"

        hospice:
            "Hospice - Home"
            "Hospice - Medical Facility (Certified) Providing Hospice Level of Care"

    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id,
                   disch_weekday_cat, disch_day_of_month_cat, disch_time_cat,
                   disch_location_cat, length_of_stay, length_of_stay_lace
              FROM {}.bayes_vw_feature_discharge
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        # There are two duplicates which I'm going to ignore for now.
        res.drop_duplicates(subset='hsp_acct_study_id', inplace=True)
        res.set_index('hsp_acct_study_id', inplace=True)

        return self.emit_df(res)
