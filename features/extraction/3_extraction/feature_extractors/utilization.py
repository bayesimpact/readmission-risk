"""A feature extractor for patients' utilization."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor

log = logging.getLogger('feature_extraction')


class UtilizationExtractor(FeatureExtractor):
    """
    Generates features related to the number of previous ER visits.

    Features:
    `pre_[n]_month_[adm_type]` - Number of [adm_type] (emergency, inpatient, outpatient) visits
                                 during the [n] (3, 6, 12) month before admission
    `er_visits_lace` - LACE score associated with number of ER visits:
                       the greater of number of emergency visits
                       during the 6 month before admission or 4.
    """

    def extract(self):
        query = """
          SELECT
            *
            FROM {}.bayes_vw_feature_utilization
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The pre-pivot table has %d rows.' % len(res))

        pivoted = pd.pivot_table(data=res, index='hsp_acct_study_id', columns='pre_adm_type',
                                 aggfunc=sum, dropna=True, fill_value=0,
                                 values=['pre_3_month', 'pre_6_month', 'pre_12_month'])

        df_columns = [top + "_" + bottom.lower() for top, bottom in pivoted.columns.values]
        df = pd.DataFrame(index=res.hsp_acct_study_id.unique())
        df[df_columns] = pivoted
        df.fillna(0, inplace=True)
        df['er_visits_lace'] = df['pre_6_month_emergency'].apply(lambda cnt: min(cnt, 4))

        return self.emit_df(df)
