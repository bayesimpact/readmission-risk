"""create patient_location table

Revision ID: 131709af5cc
Revises: cf981f6e83
Create Date: 2016-03-21 09:44:47.499939

"""

# revision identifiers, used by Alembic.
revision = '131709af5cc'
down_revision = 'cf981f6e83'
branch_labels = None
depends_on = None


from alembic import op
import pandas as pd
from sutter.lib.path import get_path
import sqlalchemy as sa
from numpy import isnan

table_name = "bayes_patient_location"


def upgrade():
    engine = op.get_bind()
    file_address = get_path('data/census/CA_pat_lat_lng_tract.csv')
    column_types = {'pat_study_id': long,
                    'latitude': float,
                    'longitude': float,
                    'tract_id': str}
    with open(file_address) as f:

        df = pd.read_csv(f, dtype=column_types)

    op.create_table(
        table_name,
        sa.Column('pat_study_id', sa.BigInteger),
        sa.Column('latitude', sa.Float),
        sa.Column('longitude', sa.Float),
        sa.Column('tract_id', sa.VARCHAR(11)))
    op.create_index(table_name + "pat_study_id", table_name, ["pat_study_id", "tract_id"])
    df.to_sql(table_name, engine, index=None, if_exists='append')


def downgrade():
    op.drop_table(table_name)
    # pass
