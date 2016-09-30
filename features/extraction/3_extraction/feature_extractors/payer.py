"""A feature extractor for insurance and payer information."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class PayerExtractor(FeatureExtractor):
    """
    Generates features related to the type of insurance that the patient has.

    Features:
    `insurance_type_cat`
        self-pay: 'Self-pay'
        commercial: 'Commercial'
        medicare: 'Medicare' or 'Medicaid'
        other: 'Other'
    """

    def extract(self):
        query = """
                SELECT
                    *
                FROM {}.bayes_vw_feature_payer
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col='hsp_acct_study_id')
        # Les than 5 duplicates across all hospitals. Will drop them
        res = res.groupby(res.index).first()
        return self.emit_df(res)
