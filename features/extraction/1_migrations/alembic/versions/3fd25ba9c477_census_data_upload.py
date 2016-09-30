"""census data upload

Revision ID: 3fd25ba9c477
Revises: 131709af5cc
Create Date: 2016-03-24 14:20:25.488054

"""

# revision identifiers, used by Alembic.
revision = '3fd25ba9c477'
down_revision = '131709af5cc'
branch_labels = None
depends_on = None

from alembic import op
import pandas as pd
from sutter.lib.path import get_path
import sqlalchemy as sa
from numpy import isnan

table_name = "bayes_census"


def upgrade():
    engine = op.get_bind()
    file_address = get_path('data/census/census_processed.csv')

    with open(file_address) as f:
        df = pd.read_csv(f, dtype={'Geo_FIPS': long})
        df.rename(columns={'Geo_FIPS': 'tract_id'}, inplace=True)
        df['tract_id'] = df.tract_id.apply(lambda f: "{:011.0f}".format(f) if ~isnan(f) else None)
    print df.shape

    df.to_sql(table_name, engine, index=None, if_exists='append')
    op.create_index(table_name + "tract_id", table_name, ["tract_id"])


def downgrade():
    op.drop_table(table_name)
    # pass
