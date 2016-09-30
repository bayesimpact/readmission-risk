"""A feature extractor for patient's demographics."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_categorizers import marital_status_from_string, race_from_string
from sutter.lib.feature_extractor import FeatureExtractor


class BasicDemographicsExtractor(FeatureExtractor):
    """
    Generates features related to basic demographical information of the patient.

    Features:
    `age` - Age of the patient at the time of enrollment
    `age^2`, `age^3` - (polynomial age features)
    `tabak_age` - 0.4 points for each year over 45 (part of the Tabak mortality score)
    `if_female_bool` - If sex is female.
    `race`: black, white, other, na
    `if_hispanic_bool`:
        True:  ['Mexican', 'Other Hispanic/Latino/Spanish origin', 'Puerto Rican', 'Cuban']
        None: ['Unknown', '', 'Prefer Not to Answer']
        False: 'Non Hispanic'
    `marital_status`:
        partner: ['Significant other', 'Life Partner']
        widowed: 'Widowed'
        separated: ['Divorced', 'Legally Separated', 'Separated']
        married: 'Married'
        single: 'Single'
        other: 'Other', 'Unknown', '',
    `if_intrptr_needed_bool`: True/False
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id, age,
                    if_female_bool,
                    race_name, if_hispanic_bool,
                   marital_status_name, if_intrptr_needed_bool
              FROM {}.bayes_vw_feature_demographics
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        # Occasionally (in less than 5% of cases), we have more than 1 row per patient.
        # I randomly select one line here.
        res = res.groupby('hsp_acct_study_id').first()

        res['age^2'] = res.age.apply(lambda x: x ** 2)
        res['age^3'] = res.age.apply(lambda x: x ** 3)
        res['tabak_age'] = res.age.apply(lambda x: max(0, 0.4 * (x - 45)))
        res['race_cat'] = res.race_name.apply(race_from_string)
        res['marital_status_cat'] = res.marital_status_name.apply(marital_status_from_string)

        res.drop(['race_name', 'marital_status_name'], 1, inplace=True)

        return self.emit_df(res)
