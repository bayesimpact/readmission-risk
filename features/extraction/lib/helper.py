"""Generic helper methods."""
import collections
import fnmatch
import os
import random
from functools import wraps
from itertools import cycle

import numpy as np

import pandas as pd

import sklearn.metrics as sk_m

import sutter


def get_metrics(predictions, actual, intervention_threshold=None):
    """Return a dictionary of metrics scoring these predictions."""
    predictions = np.array(predictions)
    actual = np.array(actual)

    fpr, tpr, _ = sk_m.roc_curve(actual, predictions)
    auc = sk_m.auc(fpr, tpr)

    metrics = {
        'fpr': fpr,
        'tpr': tpr,
        'auc': auc,
        'num_predictions': len(predictions),
        'intervention_threshold': intervention_threshold,
        'num_positive': actual.sum(),
        'num_negative': len(actual) - actual.sum(),
    }

    if intervention_threshold is not None:
        intervened = np.array(predictions > intervention_threshold).astype(bool)
        actual = np.array(actual).astype(bool)

        tp = float((intervened & actual).sum())
        tn = float((~intervened & ~actual).sum())
        fp = float((intervened & ~actual).sum())
        fn = float((~intervened & actual).sum())
        p = float(intervened.sum())
        n = float((~intervened).sum())

        metrics['precision'] = tp / p if p else np.nan
        metrics['recall'] = tp / actual.sum() if actual.sum() else np.nan
        metrics['sensitivity'] = metrics['recall']
        metrics['specificity'] = tn / (~actual).sum() if (~actual).sum() else np.nan
        metrics['num_intervened'] = p
        metrics['num_intervened_correct'] = tp
        metrics['fraction_intervened'] = float(p) / (p + n) if (p + n) else np.nan
        metrics['ppv'] = metrics['precision']
        metrics['npv'] = tn / n if n else np.nan
        metrics['tp'] = tp
        metrics['tn'] = tn
        metrics['fp'] = fp
        metrics['fn'] = fn
    return metrics


def combine_fold_metrics(folds):
    """Combine a list of dictionaries into one by averaging their numeric values."""
    combined = {}
    for k in folds[0].keys():
        values = [f[k] for f in folds]
        if hasattr(values[0], '__iter__'):
            combined[k] = None
        else:
            values = [v for v in values if pd.notnull(v)]
            if len(values):
                combined[k] = np.mean(values)
            else:
                combined[k] = None
    return combined


def plot_auc_curves(model_metrics, ax):
    """
    Plot AUC curves for the given models on the given axis.

    `model_metrics` is a dict of {model name: [per-fold metrics]}.

    If there are multiple folds, a curve will be plotted for each fold of each model.
    """
    color_cycle = ax._get_lines.color_cycle
    style_cycle = cycle(['-', '--', '-.', ':'])

    for name, metrics in sorted(model_metrics.items()):
        combined_metrics = combine_fold_metrics(metrics)
        cpu_time = combined_metrics['total_cpu_time']
        auc = combined_metrics['auc']

        if len(metrics) > 1:
            # If k-fold, print out each fold's AUC, because why not?
            print "AUCs for {}: {}".format(name, [round(m['auc'], 3) for m in metrics])

        color, style = color_cycle.next(), style_cycle.next()
        for i, m in enumerate(metrics):
            label = "%s [took %0.0f sec avg], AUC=%0.3f" % (name, cpu_time, auc)
            ax.plot(m['fpr'], m['tpr'], style, c=color, label=(label if i == 0 else None))

    ax.plot([0, 1], [0, 1], '--', c='gray', alpha=0.3, label='Random chance, AUC=0.500')
    ax.legend(loc=2)


def relevant_hospitals():
    """
    Return a list of names of short-term acute-care facilities.

    Based on Sylvia's message in Freedcamp.
    """
    return [
        "ALTA BATES SUMMIT - ALTA BATES",
        "ALTA BATES SUMMIT - MERRITT",
        # "CPMC CALIFORNIA CAMPUS - WEST",  # Women and children's center, so not "typical"
        "CPMC DAVIES CAMPUS",
        "CPMC PACIFIC CAMPUS",
        "CPMC ST LUKE'S HOSPITAL",
        "DELTA MEDICAL CENTER",
        "EDEN MEDICAL CENTER",
        "MEMORIAL HOSPITAL LOS BANOS",
        "MEMORIAL MEDICAL CTR MODESTO",
        "NOVATO COMMUNITY HOSPITAL",
        "PENINSULA MEDICAL CENTER",
        "SUTTER AMADOR HOSPITAL",
        "SUTTER AUBURN FAITH HOSPITAL",
        "SUTTER DAVIS HOSPITAL",
        "SUTTER LAKESIDE HOSPITAL",
        # "SUTTER MATERNITY AND SURGERY CENTER OF SANTA CRUZ",  # Likely atypical (~0 readmissions)
        "SUTTER MEDICAL CENTER SACRAMENTO",
        "SUTTER ROSEVILLE MEDICAL CENTER",
        "SUTTER SANTA ROSA REGIONAL HOSPITAL",
        "SUTTER SOLANO MEDICAL CENTER",
        "SUTTER TRACY COMM HOSPITAL",
    ]


def age_groups():
    """Return a list of 2-tuples, defining age buckets for cohort analysis."""
    return [(0, 18), (19, 44), (45, 64), (65, 84), (85, 120)]


def cms_conditions():
    """Return a list of conditions that CMS scores hospitals on."""
    return ['acute_mi', 'copd', 'pneumonia', 'chf_nonhp']


def find_cci(comor_list):
    """
    Calculate the Charlson Comorbidity Index (CCI) from list of patient comorbidities.

    CCI is a weighted sum of conditions, excluding those that their elevated version present.
    """
    elevated_condition_dict = {"MLD": "SLD", "MAL": "MST", "MDM": "SDM"}
    comor_list = comor_list.dropna()
    for lower_cond, higher_cond in elevated_condition_dict.iteritems():
        if higher_cond in comor_list.index:
            try:
                comor_list.pop(lower_cond)
            except:
                pass
    return comor_list.sum()


def format_icd9_code(code):
    """Nicely format an ICD9 code into the form used in the bayes_hcup_ccs table."""
    return "'{:5}'".format(code.replace('.', ''))


def format_column_title(c_name):
    """Clean up a column title by removing/replacing special characters."""
    for from_ch_g, to_ch in {"'": "", " -./:;": "_"}.iteritems():
        for ch in from_ch_g:
            c_name = c_name.replace(ch, to_ch).strip()
    return c_name.lower()


def recursive_update(orig, new):
    """Recursively updates the dicts in `orig` with data from `new`."""
    for k, v in new.iteritems():
        if isinstance(v, collections.Mapping):
            r = recursive_update(orig.get(k, {}), v)
            orig[k] = r
        else:
            orig[k] = new[k]
    return orig


def get_path(route=None):
    """Return the root directory of the project."""
    module_path = os.path.dirname(os.path.abspath(sutter.__file__))
    project_path = os.path.join(module_path, '..')
    abs_path = project_path if route is None else os.path.join(project_path, route)
    return abs_path


def get_view_query(feature_name):
    """Get a feature name and returns the query from its corresponding view."""
    views_path = get_path('views')
    fname = '*vw_*{}.sql'.format(feature_name)
    for file in os.listdir(views_path):
        if fnmatch.fnmatch(file, fname):
            print("Loading script from %s." % file)
            with open(os.path.join(views_path, file), 'r') as sql:
                query = sql.read()
                return query
    print("Could not find a view file for %s!" % feature_name)
    return False


def counter(func):
    """
    Count the number of times a function has been called.

    # of times a function has been called can be access by <funcname>.count
    """
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        wrapped_func.count += 1
        return func(*args, **kwargs)
    wrapped_func.count = 0
    return wrapped_func


def create_sample_csv(infile, outfile, n):
    """Create a sampled version of a csv file."""
    print "Creating a %d-sample version of %s called %s" % (n, infile, outfile)
    rows = open(infile).read().splitlines()
    with open(outfile, 'w') as f:
        print >>f, rows[0]  # Always save header row
        rows = rows[1:]
        for r in random.sample(rows, n):
            print >>f, r


def _cleanup_features(features_df):
    """Column-specific cleanup tasks go here and are run immediately after CSV import."""
    # Fix format of DischargeExtractor__disch_day_of_month_cat column
    day_of_month_col = 'DischargeExtractor__disch_day_of_month_cat'
    features_df[day_of_month_col] = features_df[day_of_month_col].astype('str')

    # Min-normalize LabResultsExtractor__tabak_lab_score to avoid negative values
    tabak_col = 'LabResultsExtractor__tabak_lab_score'
    t_min = features_df[tabak_col].min()
    features_df.loc[:, tabak_col] = features_df[tabak_col].apply(lambda x: x - t_min)

    return features_df


def load_sutter_csv(path):
    """Load features from a CSV file and categorize + process columns."""
    # Load CSV and perform basic cleanup.
    features_df = pd.read_csv(get_path(path), index_col=0)
    features_df = _cleanup_features(features_df)
    print "Loaded {} rows.".format(len(features_df))

    # Drop data from non-acute-care hospitals
    hospitals = set(relevant_hospitals())
    features_df = features_df[features_df['AdmissionExtractor__hospital_name_cat'].apply(
        lambda h: h in hospitals)]

    # Split into feature and label columns.
    label_cols = [col for col in features_df.columns if 'Readmission' in col]
    feature_cols = [col for col in features_df.columns if col not in label_cols]

    # Separate out feature types.
    boolean_cols = [c for c in feature_cols if c.endswith('_bool') or
                    features_df[c].dtype == 'bool']
    numeric_cols = [c for c in feature_cols if c not in boolean_cols and
                    features_df[c].dtype in ['int', 'float']]
    categorical_cols = [c for c in feature_cols if c.endswith('_cat')]
    uncategorized_cols = [c for c in feature_cols if c not in
                          boolean_cols + numeric_cols + categorical_cols]

    # Convert boolean and numerical variables to float.
    features_df[boolean_cols] = features_df[boolean_cols].astype('float')
    features_df[numeric_cols] = features_df[numeric_cols].astype('float')

    # Convert datetime columns
    for col in label_cols:
        if col.endswith('_date_time'):
            features_df[col] = pd.to_datetime(features_df[col])

    # Separate out features_df and labels_df.
    labels_df = features_df[label_cols]
    features_df = features_df[boolean_cols + numeric_cols + categorical_cols]

    # Impute missing feature values.
    for col in (boolean_cols + numeric_cols):
        features_df[col] = features_df[col].fillna(features_df[col].mean())

    # Dummify categorical variables.
    n_non_categorical = features_df.shape[1] - len(categorical_cols)
    features_df = pd.get_dummies(features_df, columns=categorical_cols)
    n_dummies = features_df.shape[1] - n_non_categorical

    # Sanity check: are all columns categorized into [boolean, numeric, categorical]?
    print "Found %d columns:" % features_df.shape[1]
    print " - %d boolean" % len(boolean_cols)
    print " - %d numeric" % len(numeric_cols)
    print " - %d categorical (%d when dummified)" % (len(categorical_cols), n_dummies)
    print " - %d uncategorized" % len(uncategorized_cols)
    print " * %d labels" % len(label_cols)

    if len(uncategorized_cols) > 0:
        print "WARNING: Found uncategorized columns! {}".format(list(uncategorized_cols))

    return features_df, labels_df
