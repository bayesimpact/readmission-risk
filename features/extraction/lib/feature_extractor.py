"""Our subclass of Fex."""

import logging
import re

import fex

import numpy as np


log = logging.getLogger('feature_extraction')


class FeatureExtractor(fex.FeatureExtractor):
    """
    Our subclass of fex.FeatureExtractor.

    Offers some additional functionality:
        - _validate_df() does some sanity checks for testing FeatureExtractor output.
        - "df" output mode to output the DataFrame rather than saving to CSV.
    """

    def __init__(self, output_mode='csv', schema='features'):
        """
        Sutter-specific initialization, delegating to superclass constructor.

        Output mode can be "csv" or "df".
        """
        fex.FeatureExtractor.__init__(self)
        self._schema = schema  # set to "sample_features" in tests to use a smaller sample
        self._output_mode = output_mode  # toggle between output to csv or df

    def emit_df(self, df):
        """Run verification, then emit a DataFrame of extracted features."""
        log.info('The final table has %d rows.' % len(df))
        self._validate_df(df)

        if self._output_mode == 'df':
            return df

        else:
            self.emit(df)

    def _validate_df(self, df):
        """Perform several checks on the dataframe, raising an Exception if one fails.

        - For boolean (`*_bool`) columns, check to make sure that all values are bool or NaN.
        - For all columns, check that the column name is valid.
        """
        for colname in df:
            # print "  * " + colname  # Uncomment to display all column names while testing
            column = df[colname]

            if re.match("[A-Z] ", colname):
                # Column names shouldn't contain spaces or uppercase characters.
                raise Exception("Bad column name: %s!" % colname)
            elif colname.endswith("_bool"):
                # Check that the column contains only boolean values and None/NaN.
                values = set(column.values)
                if not values <= {True, False, None, np.NaN}:
                    raise Exception("Column %s contains non-boolean values (%s)!"
                                    % (colname, values))
