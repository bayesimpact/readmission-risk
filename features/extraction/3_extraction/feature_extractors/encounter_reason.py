"""A feature extractor for primary encounter reason."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor
from sutter.lib.helper import format_column_title

log = logging.getLogger('feature_extraction')


class EncounterReasonExtractor(FeatureExtractor):
    """
    Generates features related to patient's provider.

    Features:
    - `*` - Whether the patient's primary encounter had the given reason (~400 different ones).
    """

    def extract(self):
        query = """
            SELECT *
              FROM {}.bayes_vw_feature_encounter_reason
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        dummified = res.dropna() \
                       .groupby('hsp_acct_study_id') \
                       .enc_reason_name_cat \
                       .apply(lambda reasons: "|".join([format_column_title(r) for r in reasons])) \
                       .str.get_dummies()

        df = pd.DataFrame(index=res.hsp_acct_study_id.unique())
        df_columns = dummified.columns
        df[df_columns] = dummified
        df.fillna(0, inplace=True)
        df = df.astype('bool')
        return self.emit_df(df)
