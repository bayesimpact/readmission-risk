"""fix pax_id_xwalk ids format

Revision ID: 14aabf22490a
Revises: 2eb95b9c76e2
Create Date: 2016-03-07 13:27:55.414963

"""

# revision identifiers, used by Alembic.
revision = '14aabf22490a'
down_revision = '2eb95b9c76e2'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa

table_name = 'px_id_xwalk'
col_name = 'final_icd_px_id'
def upgrade():
    q = """ALTER TABLE {0} 
                ALTER COLUMN {1} TYPE varchar 
                USING ({1}::varchar)""".format(table_name, col_name)
    op.execute(q)

def downgrade():
    q = """ALTER TABLE {0} 
                ALTER COLUMN {1} TYPE integer 
                USING ({1}::integer)""".format(table_name, col_name)
    op.execute(q)
