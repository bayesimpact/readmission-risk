"""Splitter and its subclasses are used by Pipeline to split data into train/test sets."""

import numpy as np

import sklearn.cross_validation as sk_cv


class Splitter(object):
    """Abstract base class for train/test splitters."""

    def split(self, features_df, labels_df, label_period_days=30, seed=None):
        """
        Split a feature DataFrame and a label DataFrame into train and test sets.

        Returns an array of folds (KFold returns multiple folds; all other splitters return 1 fold).
        Each fold is a dict with { X_train, X_test, y_train, y_test } keys.
        """
        raise NotImplementedError()

    def _prepare_labels_vector(self, labels_df, label_period_days):
        """Convert a labels DataFrame into a vector of binary labels."""
        return (labels_df['ReadmissionExtractor__days_to_readmit'] < label_period_days).astype(int)


class TrainTest(Splitter):
    """Simple train/test splitter."""

    def __init__(self, test_size=0.3):
        """Initialize with test_size."""
        self.test_size = test_size

    def split(self, features_df, labels_df, label_period_days=30, seed=None):
        """Return a train set of size `(1-test_size)` and a test set of size `test_size`."""
        test_pct = int(self.test_size * 100)
        print "Performing a {}/{} split ...".format(100 - test_pct, test_pct)

        labels = self._prepare_labels_vector(labels_df, label_period_days)
        X_train, X_test, y_train, y_test = \
            sk_cv.train_test_split(features_df, labels, test_size=self.test_size, random_state=seed)

        return [{
            "X_train": X_train,
            "X_test": X_test,
            "y_train": np.ravel(y_train),
            "y_test": np.ravel(y_test)
        }]


class KFold(Splitter):
    """K-fold validation train/test splitter."""

    def __init__(self, n_folds=5):
        """Initialize with n_folds."""
        self.n_folds = n_folds

    def split(self, features_df, labels_df, label_period_days=30, seed=None):
        """Return `n_folds` folds, each containing a test set of size `1/n_folds`."""
        print "Splitting into {} folds ...".format(self.n_folds)

        labels = self._prepare_labels_vector(labels_df, label_period_days)
        fold_indices = sk_cv.KFold(n=features_df.shape[0], n_folds=self.n_folds, random_state=seed)

        folds = []
        for train_idx, test_idx in fold_indices:
            train_idx, test_idx = features_df.index[train_idx], features_df.index[test_idx]
            folds.append({
                "X_train": features_df.ix[train_idx],
                "X_test": features_df.ix[test_idx],
                "y_train": labels[train_idx],
                "y_test": labels[test_idx]
            })
        return folds


class Temporal(Splitter):
    """Temporal validation train/test splitter."""

    def __init__(self, split_at):
        """Initialize with a split_at datetime."""
        self.split_at = split_at

    def split(self, features_df, labels_df, label_period_days=30, seed=None):
        """Return a train set consisting of all admission times < `split_at`, and vice versa."""
        print "Performing a temporal split around {}".format(self.split_at)

        train_rows = labels_df['ReadmissionExtractor__admit_date_time'] < self.split_at
        test_rows = labels_df['ReadmissionExtractor__admit_date_time'] >= self.split_at
        labels = self._prepare_labels_vector(labels_df, label_period_days)

        return [{
            "X_train": features_df[train_rows],
            "X_test": features_df[test_rows],
            "y_train": labels[train_rows],
            "y_test": labels[test_rows]
        }]
