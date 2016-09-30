"""upload hcup ccs data

Revision ID: cf981f6e83
Revises: 925a969806b
Create Date: 2016-03-09 14:21:46.907803

"""

# revision identifiers, used by Alembic.
revision = 'cf981f6e83'
down_revision = '925a969806b'
branch_labels = None
depends_on = None

import logging

import os

from alembic import op

import pandas as pd

from sutter.lib.helper import format_column_title, get_path

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)

tables = {'bayes_hcup_ccs_dx': '$dxref 2015.csv', 'bayes_hcup_ccs_pr': '$prref 2015.csv'}


def upgrade():
    engine = op.get_bind().engine

    for table_name, filename in tables.iteritems():
        data_path = os.path.join(get_path(), 'data/hcup/Single_Level_CCS_2015/%s' % filename)
        with open(data_path, 'r') as f:
            hcup_ccs = pd.read_csv(f, skiprows=1)

        hcup_ccs.columns = map(format_column_title, hcup_ccs.columns.values)
        # I am going to pivot this df on css_category_description. So I have to format it now.
        col_name = 'ccs_category_description'
        hcup_ccs[col_name] = hcup_ccs[col_name].apply(format_column_title)

        log.info('Creating %s' % table_name)
        hcup_ccs.to_sql(table_name, engine, index=False)


def downgrade():

    for table_name, filename in tables.iteritems():
        log.info('Dropping %s' % table_name)
        query = "DROP TABLE %s CASCADE" % table_name
        op.execute(query)
