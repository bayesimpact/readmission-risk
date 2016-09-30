"""A feature extractor for provider information."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor
from sutter.lib.helper import format_column_title

log = logging.getLogger('feature_extraction')


class ProviderExtractor(FeatureExtractor):
    """
    Generates features related to patient's provider.

    Features:
    - `specialty_*` - Whether the patient's provider has the given specialty (47 different ones).
    """

    def extract(self):
        query = """
            SELECT *
              FROM {}.bayes_vw_feature_provider
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        pivoted = res.dropna() \
                     .groupby('hsp_acct_study_id') \
                     .specialty \
                     .apply(_rename_columns) \
                     .str.get_dummies()

        df = pd.DataFrame(index=res.hsp_acct_study_id.unique())
        df[pivoted.columns] = pivoted.astype('bool')
        df.fillna(False, inplace=True)

        return self.emit_df(df)


def _rename_columns(specialties):
    return "|".join('specialty_' + format_column_title(s) for s in specialties)
