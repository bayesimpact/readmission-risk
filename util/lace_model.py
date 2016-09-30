"""Baseline LACE model."""

import numpy as np

from sklearn.base import BaseEstimator


class LACEModel(BaseEstimator):
    """Estimator class implementing the LACE scoring model."""

    def fit(self, X, y):
        """Fit the model (do nothing)."""
        pass

    def predict_proba(self, X):
        """
        Predict probabilities.

        Returns the LACE score (0-19) divided by 20, as a sort of
        "fake" probabilistic interpretation of the LACE risk score.
        """
        cols = {
            'L': 'DischargeExtractor__length_of_stay_lace',
            'A': 'AdmissionExtractor__acuity_lace',
            'C': 'ComorbiditiesExtractor__charlson_index_lace',
            'E': 'UtilizationExtractor__er_visits_lace'
        }
        lace_score = X[cols['L']] + X[cols['A']] + X[cols['C']] + X[cols['E']]
        proba = lace_score / 20
        return np.vstack([(1 - proba).values, proba.values]).T
