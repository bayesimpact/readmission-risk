"""fix icd-9 code format

Revision ID: 2eb95b9c76e2
Revises: 4f2f665f301a
Create Date: 2016-03-07 12:33:12.946886

"""

# revision identifiers, used by Alembic.
revision = '2eb95b9c76e2'
down_revision = '4f2f665f301a'
branch_labels = None
depends_on = None

from alembic import op
from sqlalchemy import Column, VARCHAR
import logging

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)

icd9_columns = {'icd_9_cci_xwalk': 'icd_9_cm_code',
                            'problem_list': 'ref_bill_code',
                            'hospital_dx': 'ref_bill_code',
                            'encounter_dx': 'dx_code',
                            'hospital_problems': 'current_icd9_list',
                            'px_id_xwalk': 'ref_bill_code'
                            }

def upgrade():
    for tb_name, col_name in icd9_columns.iteritems():
        base_q = """DO $$ 
                                BEGIN
                                    BEGIN
                                        ALTER TABLE {0} ADD COLUMN {1} VARCHAR;
                                    EXCEPTION
                                        WHEN duplicate_column THEN RAISE NOTICE
                                            'column {1} already exists in {0}.';
                                    END;
                                END;
                            $$"""
        q = base_q.format(tb_name, col_name)
        op.execute(q)
        log.info("formatting icd-9-cm codes on {}.".format(tb_name))
        query = """
            UPDATE {} SET
                icd_9_cm_code = format('''%s''', rpad(replace({}, '.', ''), 5, ' '))
        """.format(tb_name, col_name)
        op.execute(query)

def downgrade():
    for tb_name in icd9_columns.keys():
        log.info("dropping 'icd_9_cm_code' column on {}.".format(tb_name))
        op.drop_column(tb_name, 'icd_9_cm_code')
