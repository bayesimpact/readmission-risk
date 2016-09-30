"""create features schema

Revision ID: 925a969806b
Revises: 14aabf22490a
Create Date: 2016-03-07 14:27:03.668025

"""

# revision identifiers, used by Alembic.
revision = '925a969806b'
down_revision = '14aabf22490a'
branch_labels = None
depends_on = None

from alembic import op

schema_name = 'features'


def upgrade():
    q = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
    op.execute(q)


def downgrade():
    q = "DROP SCHEMA IF EXISTS {} CASCADE".format(schema_name)
    op.execute(q)
