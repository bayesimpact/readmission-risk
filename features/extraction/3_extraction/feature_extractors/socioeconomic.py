"""A feature extractor for socioeconomic features."""

from __future__ import absolute_import

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor


class SocioeconomicExtractor(FeatureExtractor):
    """
    Generates features related to the socioeconomic situation of the patient.

    We use tract level census data for each patient.
    Features:
    The following are feature categories:

    `house_value`: the median value of the house in that tract id
    `citizenship`: foriegn born, native or not not a citizen
    `unemployment`: pct of employed and unemployed
    `age`: pct of population under 18, 18-34, 35-64, and above 64
    `marital_status`: pct of married and not married
    `race`: pct of black, white and other
    `poverty`: pct in poverty under 18, 18-64, above 65
    `rent`: pct in 10-29, 30-49, above 50 percent of income.
    `income`: gini index and per capita
    `income_poverty_ratio`: pct .5-.74, .75-.99, etc
    `education`: pct with less than highschool, highschool, college, etc
    `household`: avg houshold income, medican household income, avg houshold size
    `population`: total and density
    """

    def extract(self):
        query = """
                SELECT
                    *
                FROM {}.bayes_vw_feature_socioeconomic
        """.format(self._schema)

        engine = postgres.get_connection()

        res = pd.read_sql(query, engine, index_col='hsp_acct_study_id')
        res.drop(['tract_id'], axis=1, inplace=True)
        # Les than 5 duplicates across all hospitals. Will drop them
        res = res.groupby(res.index).first()
        return self.emit_df(res)
