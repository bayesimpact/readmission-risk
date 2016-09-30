"""A feature extractor for patient's comorbidities."""

from __future__ import absolute_import

import logging

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor
from sutter.lib.helper import find_cci

log = logging.getLogger('feature_extraction')


class ComorbiditiesExtractor(FeatureExtractor):
    """
    Generates features related to comorbidities.

    Features:
        one feature for each comorbid conditions:
        `PVR`
        `CPD`
        `MDM`
        `REN`
        `CHF`
        `SDM`
        `CVR`
        `MAL`
        `MST`
        `MLD`
         `MI`
         `RD`
         `PUD`
         `SLD`
         `DEM`
         `HPL`
         `AIDS`

        `charlson_index`: sum of all comorbidity weights
        `comor_index_lace`: lace score associated with charlson index
    """

    def extract(self):
        query = """
            SELECT *
              FROM {}.bayes_m_vw_feature_comorbidities
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        pivoted = pd.pivot_table(data=res,
                                 index='hsp_acct_study_id',
                                 values='weight', columns='condition_cat')
        log.info('The pivoted table has %d rows.' % len(pivoted))
        pivoted.columns = map(lambda c: 'comor_' + c.lower(), pivoted.columns)

        df = pd.DataFrame(index=res.hsp_acct_study_id.unique())
        df[pivoted.columns] = pivoted
        df['charlson_index'] = df.apply(find_cci, axis=1)
        df['charlson_index_lace'] = df.charlson_index.apply(lambda s: s if s <= 3 else 5)

        # I needed weight values to calculate cci. Now, I will replace all
        # non-null weights with 1 and all null values with 0 to have a boolean value
        # for each comorbid condition.
        df.fillna(0, inplace=True)
        for col in df.columns[:-2]:
            df[col] = df[col].astype('bool')

        return self.emit_df(df)
