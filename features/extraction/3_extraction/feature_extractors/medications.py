"""A feature extractor for medications."""

from __future__ import absolute_import

import logging

import numpy as np

import pandas as pd

from sutter.lib import postgres
from sutter.lib.feature_extractor import FeatureExtractor

log = logging.getLogger('feature_extraction')


def is_valid_string(x):
    """Check if a variable is a non-empty string."""
    return (isinstance(x, basestring) and x != '')


def join_med_names(names, prefix='med_'):
    """Given a list of names (that may be NaN), format them and join them for dummification."""
    names_set = set(prefix + n.lower().replace(' ', '_') for n in names if is_valid_string(n))
    return "|".join(names_set)


def highest_dea_class(classes):
    """Given a list of DEA classifications, return the highest (numerically lowest), or NaN."""
    def class_to_int(cls):
        prefix = cls.split(" ")[0]
        return {
            "C-I": 1,
            "C-II": 2,
            "C-III": 3,
            "C-IV": 4,
            "C-V": 5
        }.get(prefix, 9999)

    classes_set = set((cls, class_to_int(cls)) for cls in classes if is_valid_string(cls))
    if len(classes_set) > 0:
        highest_class = min(classes_set, key=lambda x: x[1])[0]
        return highest_class.split(" ")[0]
    else:
        return np.nan


def is_inpatient(med):
    """Return true if the medication record belong to a valid inpatient order."""
    return med.ordering_mode_name == 'Inpatient' and \
        (med.order_status_name != 'Discontinued' and med.order_status_name != 'Canceled')


def is_outpatient(med):
    """Return true if the medication record belong to a valid outpatient order."""
    return med.ordering_mode_name == 'Outpatient' and med.order_status_name == 'Sent'


class MedicationsExtractor(FeatureExtractor):
    """
    Generates features related to medications prescribed during the hospital stay.

    Features (each category below has an `inp_` (inpatient) and `outp_` (outpatient) feature):

    `num_meds` - total number of medications prescribed or given during the hospital stay
    `num_controlled_meds` - the number among these that are controlled
    `dea_class_C-I`, ..., `dea_class_C-V` - whether or not the highest DEA designation (if any)
                                            among the medications prescribed is [C-I, C-II, ...],
                                            with C-I being most strictly controlled
    `med_alternative_medicines`, ..., `med_vitamins` - whether the patient received any medications
                                                       in a given category (about ~100 total)
    """

    def extract(self):
        query = """
            SELECT hsp_acct_study_id,
                   pharm_class_name, pharm_subclass_name, controlled_med, dea_class_code_name,
                   ordering_mode_name, order_status_name
              FROM {}.bayes_m_vw_account_medications
        """.format(self._schema)

        engine = postgres.get_connection()

        records = pd.read_sql(query, engine)
        log.info('The queried table has %d rows ...' % len(records))
        log.info('... and %d groups.' % len(records.groupby("hsp_acct_study_id")))

        # Split records into inpatient and outpatient (we will create separate features for each)
        # and group by patient. [See the medications.ipynb notebook for details on why these
        # particular order_status_names were chosen.]
        inp_groups = records[(records.ordering_mode_name == 'Inpatient') &
                             (records.order_status_name != 'Discontinued') &
                             (records.order_status_name != 'Canceled')].groupby("hsp_acct_study_id")
        outp_groups = records[(records.ordering_mode_name == 'Outpatient') &
                              (records.order_status_name == 'Sent')].groupby("hsp_acct_study_id")

        # Get dummy columns for pharm_class_name (multiple per patient).
        med_classes_inp = inp_groups.pharm_class_name \
                                    .apply(lambda m: join_med_names(m, "inp_med_")) \
                                    .str.get_dummies()
        med_classes_outp = outp_groups.pharm_class_name \
                                      .apply(lambda m: join_med_names(m, "outp_med_")) \
                                      .str.get_dummies()

        # Get dummy columns for dea_class_code_name (one per patient - just the highest found).
        dea_classes_inp = pd.get_dummies(inp_groups.dea_class_code_name.apply(highest_dea_class),
                                         prefix="inp_dea_class")
        dea_classes_outp = pd.get_dummies(outp_groups.dea_class_code_name.apply(highest_dea_class),
                                          prefix="outp_dea_class")

        # Assemble results dataframe, joining in the dummy columns created above.
        res = pd.DataFrame(index=records.hsp_acct_study_id.unique())
        res['inp_num_meds'] = inp_groups['pharm_class_name'].count()
        res['inp_num_unique_meds'] = inp_groups['pharm_class_name'].nunique()
        res['inp_num_controlled_meds'] = inp_groups['controlled_med'].sum().astype('float')
        res['outp_num_meds'] = outp_groups['pharm_class_name'].count()
        res['outp_num_unique_meds'] = outp_groups['pharm_class_name'].nunique()
        res['outp_num_controlled_meds'] = outp_groups['controlled_med'].sum().astype('float')
        res.fillna(0, inplace=True)

        res = pd.concat([res,
                         med_classes_inp.astype('bool'),
                         med_classes_outp.astype('bool'),
                         dea_classes_inp.astype('bool'),
                         dea_classes_outp.astype('bool')], axis=1)
        res.fillna(False, inplace=True)

        return self.emit_df(res)
