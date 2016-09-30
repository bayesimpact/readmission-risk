"""A feature extractor for social history."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class HealthHistoryExtractor(FeatureExtractor):
    """
    Generates features related to social history of the patient.

    Features:
    `tobacco_cat` - [Never, Quit, Yes, None, Passive]
    `alcohol_cat` - [yes, not, na]
    `drugs_cat` - [yes, not, na]
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id, tobacco_cat, alcohol_cat, drugs_cat
              FROM {}.bayes_m_vw_feature_health_history
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col="hsp_acct_study_id")
        res.fillna('na', inplace=True)

        return self.emit_df(res)
