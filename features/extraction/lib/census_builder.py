"""Assembles a census dataset from raw CSV."""

# ../../data/census/ACS2010_5yr_R11112814_data.csv
# '../../sutter/json/census_dictionary.json'
import json
import os
import sys

import pandas as pd

from sutter.lib.helper import get_path


def build_census_dataset(inputfile, outputfile, field_dict, export=True):
    """
    Given a CSV input file and a field-mapping JSON dict, build a census dataset.

    Map the column names using the field mapping, and do other processing tasks.
    """
    with open(inputfile, 'r') as f:
        df = pd.read_csv(f, index_col='Geo_FIPS')
    census = pd.DataFrame(index=df.index)

    with open(field_dict, 'r') as f:
        col_dict = json.load(f)

    for col_type, cat_cols in col_dict.iteritems():
        for col_bin, col_title in cat_cols.iteritems():
            final_col_name = '__'.join([col_type, col_bin])
            if type(col_title) == list:
                census[final_col_name] = df[col_title].sum(axis=1)
            else:
                census[final_col_name] = df[col_title]

    if export:
        output_path = os.path.join(get_path(), 'data', 'census', outputfile)
        with open(output_path, 'w') as outfile:
            census.to_csv(outfile)

    return census


if __name__ == '__main__':
    try:
        inputfile = sys.argv[1]
        outfile = sys.argv[2]
        field_dict = sys.argv[3]
    except:
        inputfile = 'ACS2010_5yr_R11112814_data.csv'
        outfile = 'census_processed.csv'
        field_dict = 'sutter/json/census_dictionary.json'

    input_path = os.path.join(get_path(), 'data', 'census', inputfile)
    output_path = os.path.join(get_path(), 'data', 'census', outfile)
    dict_path = os.path.join(get_path(), field_dict)
    build_census_dataset(input_path, output_path, dict_path)
