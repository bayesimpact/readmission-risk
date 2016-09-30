"""A feature extractor for vital signs."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class VitalsExtractor(FeatureExtractor):
    """
    Generates features related to the patient's last measured standard vital signs.

    Vital signs are: temperature, pulse, respiratory rate, blood pressure.
    Also generate features for height (in inches), weight (in lb), and BMI.
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id,
                   temperature, pulse, respirations, bp_systolic, bp_diastolic,
                   height, weight, bmi
              FROM {}.bayes_m_vw_feature_vitals
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col='hsp_acct_study_id')

        res['height_in_inches'] = res.height.apply(_height_to_inches)
        res['weight_in_lb'] = res.weight / 16

        res.drop(['height', 'weight'], axis=1, inplace=True)  # remove unwanted columns
        res.fillna(res.median(), inplace=True)

        return self.emit_df(res)


def _height_to_inches(ht):
    # format: 7' 0.0"
    try:
        split = ht.replace('"', "").split("'")
        feet, inches = float(split[0]), float(split[1])
        return int(round((12 * feet) + inches))
    except:
        return None
