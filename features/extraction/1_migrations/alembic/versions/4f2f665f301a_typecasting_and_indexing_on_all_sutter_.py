"""typecasting and indexing on all sutter tables

Revision ID: 4f2f665f301a
Revises: 111895a5d7e4
Create Date: 2016-03-07 11:43:13.452634

"""

# revision identifiers, used by Alembic.
revision = '4f2f665f301a'
down_revision = '111895a5d7e4'
branch_labels = None
depends_on = None


import logging

from alembic import op

from sutter.lib.upload import change_column_type, change_table_indices, get_table_name_dict

table_name_dict = get_table_name_dict()

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)


def upgrade():

    for table_name, table_details in table_name_dict.iteritems():
        if not table_details['converted']:
            log.info("\nConverting data types in table {}.".format(table_name))
            change_column_type(table_name,
                               table_details['column_types'],
                               op.get_bind().engine,
                               kind='up',
                               log=log)

            log.info("Adding indices to table {}.".format(table_name))
            change_table_indices(table_name,
                                 table_details['index_columns'],
                                 op.get_bind().engine,
                                 kind='create')
        else:
            log.info("Table {} has already typecasted.".format(table_name))


def downgrade():
    for table_name, table_details in table_name_dict.iteritems():
        log.info("Droping indices to table {}.".format(table_name))
        # change_table_indices(table_name, table_details['index_columns'], kind='drop')

        log.info("Converting back data types in table {}.".format(table_name))
        # change_column_type(table_name, table_details['column_types'], kind='down')
