"""FeatureSelector and its subclasses are used by Pipeline as feature selection strategies."""

import pandas as pd

import scipy as sp

import sklearn.decomposition as sk_d


class FeatureSelector(object):
    """Abstract base class for feature selectors - objects that select features from a set."""

    def fit(self, features_df, label_df):
        """
        Fit the selector to training features (and optionally label).

        The fitted selector will then be used to select the training and test features.
        """
        raise NotImplementedError()

    def select(self, features_df):
        """Return some subset (or transformation - e.g. PCA) of features_df."""
        raise NotImplementedError()


class LACESelector(FeatureSelector):
    """Not really a feature selection strategy, but useful for implementing LACE."""

    def fit(self, features_df, label_df):
        """No fitting necessary."""
        pass

    def select(self, features_df):
        """Select only LACE features."""
        lace_columns = [col for col in features_df.columns if col.endswith("_lace")]
        return features_df[lace_columns]


class PCASelector(FeatureSelector):
    """Implements PCA as a feature transformation strategy."""

    def __init__(self, num_features):
        """Initialize a PCA instance with num_features."""
        self.pca = sk_d.PCA(n_components=num_features)

    def fit(self, features_df, label_df):
        """Fit PCA with the train feature set and find the top N components."""
        self.pca.fit(features_df)

    def select(self, features_df):
        """Transform features with the fitted PCA."""
        self.pca.transform(features_df)


class TopCorrelationSelector(FeatureSelector):
    """Implements the top-linear-correlation feature selection strategy."""

    def __init__(self, num_features):
        """Initialize with num_features."""
        self.num_features = num_features

    def fit(self, features_df, label_df):
        """Select the top N features, by linear correlation with label set."""
        correlations = {}
        for col in features_df.columns:
            corr = sp.stats.pearsonr(features_df[col], label_df)[0]
            correlations[col] = corr

        corr_df = pd.DataFrame.from_dict(correlations, 'index').dropna()
        corr_df.columns = ["correlation"]
        corr_df["abs_correlation"] = abs(corr_df["correlation"])

        self.top_n_features = corr_df.sort_values(by="abs_correlation", ascending=False) \
                                     .index[:self.num_features]

    def select(self, features_df):
        """Return the selected features."""
        return features_df[self.top_n_features]


class FeatureGroupSelector(FeatureSelector):
    """Implements a feature selection based on the feature group name."""

    def __init__(self, feature_groups_name):
        """Initialize with a list of feature groups."""
        self.feature_groups_name = feature_groups_name

    def fit(self, features_df, label_df):
        """No fitting necessary."""
        pass

    def select(self, features_df):
        """Select only features in the list."""
        group_names = self.feature_groups_name
        feature_columns = [col for col in features_df.columns if col.split('__')[0] in group_names]
        return features_df[feature_columns]
