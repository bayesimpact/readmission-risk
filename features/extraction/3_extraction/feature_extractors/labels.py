"""A feature extractor for readmission."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class ReadmissionExtractor(FeatureExtractor):
    """
    Generates features related to readmission to the hospital after the discharge.

    Features:
    `full_days_after_discharge` - Number of complete days after discharge
        before the patient was readmitted again. None if this never occurred.
    `admit_date_time` - Used for filtering data before modelling.
    `discharge_date_time` - Used for filtering data before modelling.
    """

    def extract(self):
        query = """
            SELECT
                *
            FROM {}.bayes_vw_feature_labels
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col="hsp_acct_study_id")
        # Les than 5 duplicates across all hospitals. Will drop them
        res = res.groupby(res.index).first()
        return self.emit_df(res)
