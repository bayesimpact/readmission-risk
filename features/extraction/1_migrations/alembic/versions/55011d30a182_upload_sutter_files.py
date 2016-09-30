"""upload sutter files

Revision ID: 55011d30a182
Revises:
Create Date: 2016-03-04 10:28:57.785567

"""

# revision identifiers, used by Alembic.
revision = '55011d30a182'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import logging
import os
import pandas as pd
from sutter.lib.helper import get_path
from sutter.lib.upload import CsvToSql, get_table_name_dict
import json

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)

table_name_dict = get_table_name_dict()
log.info(len(table_name_dict))
data_path = os.path.dirname(
    os.path.realpath("__file__")) + "/data/sutter/decrypted"


def upgrade():
    engine = op.get_bind().engine
    for tablename, _ in table_name_dict.iteritems():
        log.info('Uploading %s.' % tablename)
        upload_obj = CsvToSql(tablename, engine)
        log.info('%d records from %s could not be uploaded.' %
                 (upload_obj.get_errors().shape[0], tablename))
        error_file_name = '_ERROR_' + tablename
        log.info('I have copied the error part in %s.' % error_file_name)
        with open(data_path + '/errors/' + error_file_name, 'w') as errorfile:
            upload_obj.get_errors().to_csv(errorfile)
    return


def downgrade():
    for tablename, _ in table_name_dict.iteritems():
        log.info(
            'NOTICE: Dropping is inactive by default.')
        log.info(
            'Uncomment the execute command to drop table %s, .' % tablename)
        q = "DROP TABLE IF EXISTS {}".format(tablename)
        # op.execute(q)
        return
