"""fix nan on problem_list and order_medication tables

Revision ID: 111895a5d7e4
Revises: 1b047914a0cf
Create Date: 2016-03-07 11:42:50.691667

"""

# revision identifiers, used by Alembic.
revision = '111895a5d7e4'
down_revision = '1b047914a0cf'
branch_labels = None
depends_on = None

from alembic import op
import logging

fix_dict = {"order_medication": ['start_date', 'end_date',
                                 'ord_prov_study_id'],
            "problem_list": ['noted_date', 'resolved_date']}


def upgrade():
    for table_name, columns in fix_dict.iteritems():
        for col in columns:
            log.info(
                "column {} in table {} is already fixed.".format(col, table_name))
            query = "UPDATE {0} SET {1} = REPLACE({1}, 'nan', NULL)".format(
                table_name, col)
        # op.execute(query) commented to avoid running this again.


def downgrade():
    for table_name, columns in fix_dict.iteritems():
        for col in columns:
            query = "UPDATE {0} SET {1} = REPLACE({1}, NULL, 'nan')".format(
                table_name, col)
            op.execute(query)
