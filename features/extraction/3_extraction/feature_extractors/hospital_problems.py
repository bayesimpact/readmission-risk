"""A feature extractor for patient's current problems."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor

log = logging.getLogger('feature_extraction')


class HospitalProblemsExtractor(FeatureExtractor):
    """
    Generates features related to hospital problems.

    Features:
        - one feature for each one of 286 HCUP CCS categories.
        Please refer to hospital_problems notebook for more information.

        `hospital_problems_count`: Number of hospital_problems.
    """

    def extract(self):
        query = """
            SELECT *
              FROM {}.bayes_vw_feature_hospital_problems
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        pivoted = pd.pivot_table(res,
                                 index='hsp_acct_study_id',
                                 columns='ccs_category_description',
                                 aggfunc=len)
        pivoted.columns = map(lambda c: 'hcup_category_' + c, pivoted.columns)

        df = pd.DataFrame(index=res.hsp_acct_study_id.unique())
        df[pivoted.columns] = pivoted
        df.fillna(0, inplace=True)
        df = df.astype('bool')

        df['hospital_problems_count'] = df.apply(sum, axis=1)
        return self.emit_df(df)
