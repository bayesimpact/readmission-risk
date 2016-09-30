"""format zip code in demographics table

Revision ID: 1b047914a0cf
Revises: 55011d30a182
Create Date: 2016-03-07 09:02:24.205016

"""

# revision identifiers, used by Alembic.
revision = '1b047914a0cf'
down_revision = '55011d30a182'
branch_labels = None
depends_on = None

from alembic import op
import logging

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)

table_name = "patient_demographics"

def upgrade():
    # 5 digit "zip_code" needs to be extracted from the "zip" column first
    # First I create a column called zip_code
    query = "ALTER TABLE {} ADD COLUMN zip_code VARCHAR".format(table_name)
    op.execute(query)

    # Next, I copy the first 5 digits of zip into zip_code
    regex = '(^[0-9]{1,5})'
    query = "UPDATE {} SET zip_code=substring(zip, '{}')".format(table_name, regex)
    op.execute(query)

def downgrade():
    op.drop_column(table_name, 'zip_code')
