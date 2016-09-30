"""A feature extractor for patient's lab results."""

from __future__ import absolute_import

import logging

import numpy as np

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor

log = logging.getLogger('feature_extraction')


def calculate_tabak_mortality_features(tests):
    """
    Given a DataFrame of lab results for a given patient, return a DataFrame of Tabak features.

    In particular, the DataFrame includes boolean features for individual
    components (e.g. `tabak_low_albumin`, etc), and the `tabak_lab_score`,
    which is the total value of the "Laboratory Values" section of the
    Tabak score calculation.
    """
    df = pd.DataFrame()
    # Boolean features for Tabak mortality score (see http://go/tabak , p.2).
    df['tabak_very_low_albumin'] = tests['ALBUMIN'] <= 2.4
    df['tabak_low_albumin'] = (tests['ALBUMIN'] > 2.4) & (tests['ALBUMIN'] <= 2.7)
    df['tabak_high_bilirubin'] = tests['BILIRUBIN TOTAL'] > 1.4
    df['tabak_abnormal_cpk'] = (tests['CK'] <= 35) | (tests['CK'] > 300)
    df['tabak_very_low_sodium'] = tests['SODIUM'] <= 130
    df['tabak_abnormal_sodium'] = ((tests['SODIUM'] > 130) & (tests['SODIUM'] <= 135)) | \
                                  (tests['SODIUM'] > 145)
    df['tabak_high_bun'] = (tests['UREA NITROGEN'] > 35) & (tests['UREA NITROGEN'] <= 50)
    df['tabak_very_high_bun'] = (tests['UREA NITROGEN'] > 50) & (tests['UREA NITROGEN'] <= 70)
    df['tabak_very_very_high_bun'] = tests['UREA NITROGEN'] > 70
    df['tabak_abnormal_pco2'] = (tests['PCO2'] <= 35) | (tests['PCO2'] > 60)
    df['tabak_high_wbc'] = tests['WBC'] > 10.9
    df['tabak_high_troponin_or_ckmb'] = (tests['TROPONIN I'] > 1) | (tests['CK MB'] > 9)
    df['tabak_low_glucose'] = tests['GLUCOSE'] <= 70
    df['tabak_high_pt_inr'] = tests['INR'] > 1.2
    df['tabak_low_pro_bnp'] = tests['NT PRO BNP'] <= 1000
    df['tabak_high_pro_bnp'] = tests['NT PRO BNP'] > 18000
    df['tabak_low_arterial_ph'] = (tests['PH'] >= 7.26) & (tests['PH'] <= 7.33)
    df['tabak_high_arterial_ph'] = tests['PH'] > 7.49
    df['tabak_very_low_arterial_ph'] = tests['PH'] < 7.26

    # Total value of "Laboratory Values" section of Tabak mortality score
    # (linear combination of the above features).
    df['tabak_lab_score'] = (
        df['tabak_very_low_albumin'] * 0.82 +
        df['tabak_low_albumin'] * 0.58 +
        df['tabak_high_bilirubin'] * 0.55 +
        df['tabak_abnormal_cpk'] * 0.29 +
        df['tabak_very_low_sodium'] * 0.54 +
        df['tabak_abnormal_sodium'] * 0.28 +
        df['tabak_high_bun'] * 0.59 +
        df['tabak_very_high_bun'] * 0.9 +
        df['tabak_very_very_high_bun'] * 1.22 +
        df['tabak_abnormal_pco2'] * 0.45 +
        df['tabak_high_wbc'] * 0.39 +
        df['tabak_high_troponin_or_ckmb'] * 0.68 +
        df['tabak_low_glucose'] * 0.34 +
        df['tabak_high_pt_inr'] * 0.22 +
        df['tabak_low_pro_bnp'] * (-0.47) +
        df['tabak_high_pro_bnp'] * 0.34 +
        df['tabak_low_arterial_ph'] * 0.57 +
        df['tabak_high_arterial_ph'] * 0.12 +
        df['tabak_very_low_arterial_ph'] * 0.95
    ) * 10

    return df


class LabResultsExtractor(FeatureExtractor):
    """
    Generates features from the patient's most recent results for each lab test taken.

    Features:
    - `num_total_results` - Total number of lab results during the patient's hospital stay

    - `num_abnormal_results` - Number of those that were flagged "Low", "High", "Abnormal", &c.

    - `pct_abnormal_results` - Percentage of lab results that were flagged

    - `if_cocaine_bool` - Whether the patient tested positive for cocaine

    - `hosp_low_hemoglobin`, `hosp_low_sodium` - Lab test components of the HOSPITAL score

    - `tabak_very_low_albumin`, `tabak_low_albumin`,
      `tabak_high_bilirubin`, `tabak_abnormal_cpk`,
      `tabak_very_low_sodium`, `tabak_abnormal_sodium`,
      `tabak_high_bun`, `tabak_very_high_bun`,
      `tabak_very_very_high_bun`, `tabak_abnormal_pco2`,
      `tabak_high_wbc`, `tabak_high_troponin_or_ckmb`,
      `tabak_low_glucose`, `tabak_high_pt_inr`,
      `tabak_low_pro_bnp`, `tabak_high_pro_bnp`,
      `tabak_low_arterial_ph`, `tabak_high_arterial_ph`,
      `tabak_very_low_arterial_ph` - Lab test components of the Tabak mortality score

    - `tabak_lab_score` - Total value of "Laboratory Values" section of Tabak mortality score
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id, common_name, ord_num_value, result_flag_name
              FROM {}.bayes_m_vw_account_lab_results
        """.format(self._schema)
        engine = postgres.get_connection()

        res = pd.read_sql(query, engine)
        log.info('The queried table has %d rows.' % len(res))

        tests = res.pivot(index='hsp_acct_study_id', columns='common_name', values='ord_num_value')

        # Start with the Tabak features.
        scores = calculate_tabak_mortality_features(tests)

        # Boolean features for HOSPITAL score.
        scores['hosp_low_hemoglobin'] = tests['HEMOGLOBIN'] < 12
        scores['hosp_low_sodium'] = tests['SODIUM'] < 135

        # Result of most recent cocaine test (probably not as useful as "history of cocaine usage").
        scores['if_cocaine_bool'] = ~np.isnan(tests['COCAINE'])

        # Simple statistics on abnormal test results.
        scores['num_total_results'] = res.groupby('hsp_acct_study_id').common_name.count()
        scores['num_abnormal_results'] = \
            res[res.result_flag_name != ""].groupby("hsp_acct_study_id").result_flag_name.count()
        scores['pct_abnormal_results'] = \
            100.0 * scores['num_abnormal_results'] / scores['num_total_results']

        scores.fillna(0, inplace=True)

        return self.emit_df(scores)
