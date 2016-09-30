"""Generic feature extraction framework."""

from __future__ import absolute_import

import inspect
import logging
import os
from collections import defaultdict

try:
    import cPickle as pickle
except:
    import pickle

import pandas as pd

from sutter.lib.helper import recursive_update

log = logging.getLogger('sutter.lib.databuilder')


class MetaInconsistentException(Exception):
    """
    This exception is thrown when someone tries to set the meta variables for a row inconsistently.

    For example, setting the `missing` value for one row to `None` and for another one to `0`
    """

    pass


class FeatureExtractor(object):
    """Abstract class defining a feature extraction engine."""

    def __init__(self):
        """Instantiate all of the feature extractor's properties."""
        # The variable prefix can be overwritten to prefix all columns of
        # a certain feature extractor
        filename = os.path.basename(inspect.getfile(self.__class__))
        self.prefix = filename.split('.')[0]
        self.name = self.__class__.__name__
        self._data_store = defaultdict(dict)
        self._debug_store = defaultdict(dict)
        self._meta_store = defaultdict(dict)
        self._test_column_subset = False  # enable in tests
        source = inspect.getsource(self.__class__)
        self.hash = hash(source)

    def extract(self):
        """Override this function."""
        raise NotImplementedError

    def emit_df(self, df):
        """
        Emit all cells in a DataFrame of extracted features.

        If in testing mode, only emit a subset of columns.
        """
        columns = df.columns
        if self._test_column_subset:
            columns = df.columns[:(self._test_column_subset)]

        for feature in columns:
            for hsp_acct_study_id, value in df[feature].iteritems():
                self.emit(hsp_acct_study_id, feature, value)

    def emit(self, row_id, feature_id, value, missing=None, debug=None):
        """
        Emit an extracted feature value.

        NOTE: You probably want to call emit_df() instead!

        :param row_id: string uniquely identifying the row.
        :param feature_id: string uniquely identifying the column.
        :param value: float
        :param debug: optional debugging information.
        """
        row_id = str(row_id)
        feature_id = self.prefix + '__' + str(feature_id)

        self._data_store[row_id][feature_id] = value
        if debug is not None:
            self._debug_store[row_id][feature_id] = str(debug)

        meta = self._meta_store[feature_id]
        if meta and meta['missing'] != missing:
            msg = "All rows must have the same missing value"
            raise MetaInconsistentException(msg)
        else:
            meta['missing'] = missing


class DatabuilderFramework(object):
    """Represents a set of feature extractors that can be run and cached."""

    def __init__(self, load_state=True):
        """
        Instantiate a DatabuilderFramework.

        Set load_state=False in tests to save a few minutes of unpickling time.
        """
        self.feature_extractors_ = []
        self.cache_path = 'databuilder-cache.pckl'
        if load_state and os.path.exists(self.cache_path):
            log.info('loading state from %s ...' % self.cache_path)
            self._cache = pickle.load(open(self.cache_path))
        else:
            self._cache = defaultdict(dict)

    def add_feature_extractor(self, feature_extractor):
        """Add a feature extractor to be run."""
        self.feature_extractors_.append(feature_extractor)

    def run(self, dataset_path, debug_path=None):
        """Run the feature extractor framework, saving results to the given path."""
        features, debug = self.generate_features(self.feature_extractors_)

        features.to_csv(dataset_path)
        if debug_path is not None:
            debug.to_csv(debug_path)

    def generate_features(self, feature_extractors):
        """
        Run all feature extractors, dump results, and return as a DataFrame.

        :param feature_extractors: iterable of :class:`FeatureExtractor`
            objects.
        """
        results, debug, meta = {}, {}, {}
        n_ext = len(feature_extractors)

        for i, extractor in enumerate(feature_extractors):
            info_str = "'{}' ({}/{})".format(extractor.name, i + 1, n_ext)
            cached_extractor = self._cache[extractor.name]
            if cached_extractor and extractor.hash == cached_extractor.hash:
                log.info('from cache: ' + info_str)
                recursive_update(results, cached_extractor._data_store)
                recursive_update(meta, cached_extractor._meta_store)
            else:
                log.info('running: ' + info_str)
                extractor.extract()
                recursive_update(results, extractor._data_store)
                recursive_update(meta, extractor._meta_store)
                self._cache[extractor.name] = extractor

        log.info('extraction complete, assembling dataframe ...')
        features = pd.DataFrame.from_dict(results, orient='index')
        debug = pd.DataFrame.from_dict(debug, orient='index')
        features.index.name = 'hsp_acct_study_id'
        debug.index.name = 'hsp_acct_study_id'

        fill_vals = {k: v["missing"] for (k, v) in meta.items()
                     if v["missing"] is not None}
        features.fillna(fill_vals, inplace=True)

        log.info('writing state to %s ...' % self.cache_path)
        with open(self.cache_path, 'wb') as f:
            pickle.dump(self._cache, f)

        return features, debug
