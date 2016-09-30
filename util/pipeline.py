"""Entry point for model building and evaluation."""

import time
from collections import defaultdict

import keras
from keras.models import model_from_json

import numpy as np

import pandas as pd

from sklearn.base import clone
import sklearn.grid_search as sk_gs
import sklearn.metrics as sk_m

from sutter.lib import helper
from sutter.lib.model.splitters import TrainTest


class Pipeline(object):
    """
    Use this class to load features + train and evaluate models.

    Usage:
        pipeline = Pipeline("features.csv")
        pipeline.build_train_and_test_sets(label_period_days=30)

        pipeline.train_model("LACE", lace_model)
        lace_results = pipeline.evaluate_model("LACE")
        pipeline.train_model("RF", rf_model)
        rf_results = pipeline.evaluate_model("RF")

        helper.plot_auc({"LACE": lace_results, "RF": rf_results}, ax)
    """

    def __init__(self, csv_path=None):
        """Initialize instance variables."""
        self.features_df = pd.DataFrame()
        self.labels_df = pd.DataFrame()
        self.folds = []
        self.clear_models()

        if csv_path:
            self.set_features_labels(*helper.load_sutter_csv(csv_path))

    def set_features_labels(self, f, l):
        """Manually set the features_df and labels_df for this pipeline."""
        self.features_df, self.labels_df = f, l

    def clear_models(self):
        """Clear the `models` dict, so the modelling can be run afresh."""
        self.models = defaultdict(list)

    def build_train_and_test_sets(self, label_period_days=30, splitter=TrainTest(0.3),
                                  seed=None, health_conditions=None):
        """Split features and labels into train and test sets.

        Also configures the label to predict (readmission period) and filters the
        dataset (by hospital or health conditions).

        @param label_period_days - Number of days in the future to predict (default=30).
        @param splitter - An instance of Splitter to split train/test sets (default=TrainTest(0.3)).
        @param seed - (optional) Seed value to use for the train/test split.
        @param health_conditions - (optional) A list of health conditions to limit the data to.
        """
        # Our dataset naturally cuts off at a certain date, which has to be handled carefully.
        # We don't want to consider a patient as "not readmitted" just because
        # we don't have data on the days following their discharge. We should drop all
        # such patients from consideration.
        last_good_date = self.labels_df['ReadmissionExtractor__admit_date_time'].max()
        date_cutoff = last_good_date - pd.DateOffset(days=label_period_days)
        row_filter = self.labels_df['ReadmissionExtractor__discharge_date_time'] < date_cutoff
        print "Removing %d visits where discharge was within %d days of the end of our data, %s" % (
            len(self.labels_df) - row_filter.sum(),
            label_period_days,
            last_good_date.strftime('%Y-%m-%d'))

        # Additional filtering for specific health conditions if (given).
        if health_conditions:
            print "Removing visits with health conditions not in the health_conditions list"
            cond = False
            for condition in health_conditions:
                condition_col_name = 'HospitalProblemsExtractor__hcup_category_%s' % condition
                if condition_col_name in self.features_df.columns:
                    cond = cond | (self.features_df[condition_col_name] == 1)
                else:
                    print("%s is not in the database." % condition)
            row_filter &= cond

        # Filter rows.
        features_df = self.features_df[row_filter]
        labels_df = self.labels_df[row_filter]

        # Assemble train and test sets using the given splitter.
        self.folds = splitter.split(features_df, labels_df, label_period_days, seed)
        self.clear_models()  # Reset results dict, just in case.

    def train_model(self, name, model, feature_selector=None, normalizer=None, sampler=None,
                    **kwargs):
        """
        Train a new model.

        An array of {model, predictions, train_cpu_time} for each fold
        (1 if k-fold is not used) is stored in self.models[name].

        @param name - Name of the model to use in the models dict.
        @param model - An instance of sklearn.Estimator.
        @param feature_selector - (optional) An instance of FeatureSelector.
        @param normalizer - (optional) An instance of sklearn.TransformerMixin.
        @param sampler - (optional) An instance of imblearn.SamplerMixin.
        @kwargs - Kwargs are passed to model.fit().
        """
        print "Training {} model on {} fold(s) ...".format(name, len(self.folds))

        for f in self.folds:
            X_train, X_test, y_train, y_test = f['X_train'], f['X_test'], f['y_train'], f['y_test']
            start_clock = time.clock()
            if len(self.folds) > 1:
                print "*",

            # Build the training and test feature matrices by feature selection strategy (if given).
            if feature_selector:
                feature_selector.fit(X_train, y_train)
                X_train, X_test = feature_selector.select(X_train), feature_selector.select(X_test)

            # Normalize the feature matrices if a normalizer is given.
            if normalizer:
                normalizer.fit(X_train)
                X_train = self._transform_df(X_train, normalizer)
                X_test = self._transform_df(X_test, normalizer)

            # Over/under-sample if a sampler is given.
            if sampler:
                sampled_X, y_train = sampler.fit_sample(X_train, y_train)
                X_train = pd.DataFrame(sampled_X, columns=X_train.columns)

            # Fit model and predict probabilities.
            if isinstance(model, keras.models.Model):
                # Reset the model between folds.
                # Annoyingly, Keras models can't be cloned, so we have to export+import+recompile.
                trained_model = model_from_json(model.to_json())
                trained_model.compile(optimizer=model.optimizer, loss=model.loss,
                                      metrics=['accuracy'])

                # Keras neural net models operate on numpy arrays rather than DataFrames,
                # so we must convert X_train and X_test to numpy arrays.
                # predict_proba() returns an array of one-element arrays.
                trained_model.fit(X_train.values, y_train,
                                  validation_data=(X_test.values, y_test),  # (For per-epoch stats.)
                                  **kwargs)
                y_predicted_test = trained_model.predict_proba(X_test.values)[:, 0]
                y_predicted_train = trained_model.predict_proba(X_train.values)[:, 0]
                print ""  # (Keras debug output doesn't end in a linebreak, so we add our own.)
            else:
                # Clone the model so that a different model is trained for each fold.
                trained_model = clone(model, safe=True)

                # Sklearn-style models are smart enough to work with DataFrames directly.
                # predict_proba() returns an array of 2-element arrays (we want the second column).
                trained_model.fit(X_train, y_train, **kwargs)
                y_predicted_test = trained_model.predict_proba(X_test)[:, 1]
                y_predicted_train = trained_model.predict_proba(X_train)[:, 1]

            end_clock = time.clock()

            self.models[name].append({
                'model': trained_model,
                'predictions_test': y_predicted_test,
                'predictions_train': y_predicted_train,
                'train_cpu_time': end_clock - start_clock
            })
        print ""

    def evaluate_model(self, name, filter_col=None, filter_condition=None, intervention_pct=25):
        """
        Evaluate the previously trained (with train_model()) model with the given name.

        Returns an array of metrics for each fold (1 if k-fold was not used to train the model).

        @param name - The name the model was given when trained.
        @param filter_col - (optional) The column to use for computing the filtered rows.
        @param filter_condition - (optional) The condition to filter a subset of rows by, if any.
        @param intervene_pct - After a model has been fit, we simulate an intervention
            on the test set and record metrics. We first calculate the
            threshold on the training set that would result in intervene_pct of samples
            being flagged for intervention. We then use that threshold to compute
            precision / recall / etc on this hypothetical test intervention.
        """
        metrics = []
        for fold, model_for_fold in zip(self.folds, self.models[name]):
            start_clock = time.clock()
            X_test, y_test = fold['X_test'], fold['y_test']
            y_predicted = model_for_fold['predictions_test']

            intervention_threshold = np.percentile(model_for_fold['predictions_train'],
                                                   100 - intervention_pct)

            if filter_col and filter_condition:
                if filter_col not in X_test:
                    raise Exception("No such column: {}!".format(filter_col))
                mask = X_test[filter_col].apply(filter_condition).values.astype('bool')
                if mask.sum() == 0:
                    raise Exception("Condition returned zero rows!")
                y_test = y_test[mask]
                y_predicted = y_predicted[mask]

            metrics_for_fold = helper.get_metrics(y_predicted, y_test, intervention_threshold)

            end_clock = time.clock()
            train_cpu_time = model_for_fold['train_cpu_time']
            test_cpu_time = end_clock - start_clock
            metrics_for_fold['test_cpu_time'] = test_cpu_time
            metrics_for_fold['total_cpu_time'] = train_cpu_time + test_cpu_time

            metrics.append(metrics_for_fold)
        return metrics

    def simulate_interventions(self, pcts_interventions):
        """
        Simulate scenarios in which a hospital performs interventions for a patient subset.

        Assume that a hospital has N patients and only has the resources to perform intensive
        discharge procedures for X of them (X << N). For each model, we select X patients
        by taking the top X predicted labels, and check the actual labels of those patients.

        Returns a dict of {model: stats_df}.

        @param pcts_interventions - List of %s of patients that interventions will be performed on.
        """
        num_total_patients = self.folds[0]['y_test'].size
        num_total_readmitted = self.folds[0]['y_test'].sum()
        print ("Running intervention simulation on {} patients, {} of whom were " +
               "readmitted ...").format(num_total_patients, num_total_readmitted)

        results = {}
        for name, model in sorted(self.models.items()):
            stats = []
            for pct in pcts_interventions:
                per_fold_metrics = self.evaluate_model(name, intervention_pct=pct)
                combined_metrics = helper.combine_fold_metrics(per_fold_metrics)
                combined_metrics["%"] = pct
                stats.append(combined_metrics)

            cols = ["%", "num_intervened", "num_intervened_correct",
                    "precision", "recall", "specificity"]
            stats_df = pd.DataFrame.from_records(stats, columns=cols)
            results[name] = stats_df.round(3)
        return results

    def grid_search(self, model_class, params, n_iter=10, feature_selector=None, normalizer=None):
        """
        Perform randomized grid search and return results as a DataFrame.

        AUC is used as the scoring metric.

        @param model_class - An instance of sklearn.Estimator.
        @param params - A dict of parameter distributions.
        @param n_iter - Number of iterations (default=10).
        @param feature_selector - (optional) An instance of FeatureSelector.
        @param normalizer - (optional) An instance of sklearn.TransformerMixin.
        """
        def auc_scorer(estimator, x, y):
            """The scoring metric for grid search (AUC)."""
            y_predicted = estimator.predict_proba(x)
            fpr, tpr, _ = sk_m.roc_curve(y, y_predicted[:, 1])
            return sk_m.auc(fpr, tpr)

        # Build the training feature matrix by feature selection strategy (if defined).
        X_train = self.X_train
        if feature_selector:
            feature_selector.fit(X_train, self.y_train)
            X_train = feature_selector.select(X_train)

        # Normalize the feature matrix if a normalizer is given.
        if normalizer:
            normalizer.fit(X_train)
            X_train = self._transform_df(X_train, normalizer)

        # Initialize and run grid search.
        grid_search = sk_gs.RandomizedSearchCV(estimator=model_class,
                                               param_distributions=params,
                                               scoring=auc_scorer, n_iter=n_iter)
        grid_search.fit(X_train, self.y_train)

        # Assemble the DataFrame of grid search results.
        grid_points = []
        for gp in grid_search.grid_scores_:
            grid_point = {"auc": gp.mean_validation_score}
            for param, value in gp.parameters.items():
                grid_point[param] = value
            grid_points.append(grid_point)
        return pd.DataFrame.from_records(grid_points)

    def num_features(self):
        """Return number of features in features_df, if defined."""
        return self.features_df.shape[1]

    def _transform_df(self, df, transformer):
        """
        Helper method to modify a DataFrame using a sklearn Transformer.

        Preserves columns and indices of the original DataFrame, changing only the cells.
        """
        return pd.DataFrame(transformer.transform(df), columns=df.columns, index=df.index)
