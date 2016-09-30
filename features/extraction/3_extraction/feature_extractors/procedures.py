"""A feature extractor for hospital procedures."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor

log = logging.getLogger('feature_extraction')


class ProceduresExtractor(FeatureExtractor):
    """
    Generates features related to in-hospital procedures performed.

    Features:
    - `num_px` - # of different CCS procedure codes corresponding to in-hospital
                 procedures performed on the patient

    - `px_*` - whether the patient received any procedures in the given
               CCS category (~200 different ones)
    """

    def extract(self):
        query = """
            SELECT *
              FROM {}.bayes_vw_feature_procedures
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        df = pd.DataFrame()
        df['num_px'] = res.groupby('hsp_acct_study_id').ccs_category_description.count()
        df.fillna(0, inplace=True)

        categories = res.dropna() \
                        .groupby('hsp_acct_study_id') \
                        .ccs_category_description \
                        .apply(lambda px: "|".join('px_' + p for p in px)) \
                        .str.get_dummies()
        df = pd.concat([df, categories.astype('bool')], axis=1)
        df.fillna(False, inplace=True)

        return self.emit_df(df)
