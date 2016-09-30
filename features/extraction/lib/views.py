"""
(Re-)create database views.

This is a helper to (re-) create all the database views found in the views
folder. To update the views on the production database, just run the deploy
fab command.

We chose this method over migrations in order to make it easier to look at
diffs of views.
"""

import glob
import logging
import os
import re
import sys
import time

from sutter.lib import config, postgres

log = logging.getLogger('sutter.lib.views')
logging.basicConfig(format='%(levelname)s:%(name)s:%(asctime)s=> %(message)s',
                    datefmt='%m/%d %H:%M:%S',
                    level=logging.INFO)

DEFAULT_SCHEMA = "features"

CREATE_OR_REPLACE_STR = "CREATE OR REPLACE VIEW {0}.{1} AS {2};"
DROP_IF_EXISTS_STR = "DROP VIEW IF EXISTS {0}.{1} CASCADE;"
CREATE_VIEW_STR = "CREATE VIEW {0}.{1} AS {2};"
CREATE_MATERIALIZED_VIEW_STR = "CREATE MATERIALIZED VIEW {0}.{1} AS {2};"


def _filename_to_viewname(f_name, prefix="bayes_"):
    """
    Compute the view name from its filename.

    * some of them have leading numbers to change the order of execution
    * we prefix all views with bayes_
    """
    base_name = os.path.basename(f_name)
    without_numbers = re.search('vw_.*', base_name).group()
    without_ext = os.path.splitext(without_numbers)[0]
    return prefix + without_ext


def _read_view_file(filepath, schema):
    """
    Read a SQL view file from its path.

    * If a schema-specific override exists (under `views/<schema>/*.sql`), read that instead.
    * Replace all instances of 'features.<view>' with '<schema>.<view>'.
    """
    base_name = os.path.basename(filepath)
    schema_specific_path = os.path.join('views', schema, base_name)
    if os.path.exists(schema_specific_path):
        filepath = schema_specific_path

    with open(filepath) as f:
        content = f.read().encode('ascii').replace(DEFAULT_SCHEMA + ".", schema + ".")
    return content


def update_views(schema):
    """
    Update the views in the database from all views in the `views` folder.

    We simply iterate over all the files in the folder, drop views that might
    already exist and re-create them again with from new code.
    """
    engine = postgres.get_connection()
    files = glob.glob(os.path.join('views', '*.sql'))
    for filename in files:
        view_name = _filename_to_viewname(filename)
        content = _read_view_file(filename, schema)
        # Queries that use the % character somehow get misinterpreted as formatting characters
        # when passed to engine.execute. So we have to "escape" them here.
        content = content.replace('%', '%%')

        try:
            # Best-case scenario: if the view is "similar" enough to what is currently
            # in the db, we can REPLACE} it in-place to avoid having to DROP CASCADE.
            log.info("Attempting to replace {}.{} ...".format(schema, view_name))
            engine.execute(CREATE_OR_REPLACE_STR.format(schema, view_name, content))
            log.info("... success!")
        except:
            # Worst-case scenario: if the view has changed significantly from what is
            # currently in the db, we have to DROP CASCADE. The sad part is that this
            # will drop any materialized views that depend on this view!
            log.info("couldn't replace it in-place. dropping and recreating it instead ...")
            engine.execute(DROP_IF_EXISTS_STR.format(schema, view_name))
            engine.execute(CREATE_VIEW_STR.format(schema, view_name, content))


def create_materialized_views(schema):
    """
    Create materialized views in the database from all views in the `views/materialized` folder.

    If a materialized view already exists with a given name, we simply skip creating it.
    But note that the existing view could be different from what's currently in the file
    (in this case, you should delete the view in the database before running this method).

    Also, this method doesn't handle refreshing existing materialized views in the database.
    If you want to refresh a materialized view, do it manually.
    """
    engine = postgres.get_connection()
    files = glob.glob(os.path.join('views/materialized', '*.sql'))
    for filename in files:
        view_name = _filename_to_viewname(filename, prefix="bayes_m_")
        content = _read_view_file(filename, schema)

        if engine.has_table(view_name, schema):
            log.info("Materialized view {}.{} already exists".format(schema, view_name))
            log.info("(If you want to update it, you must do so manually!)")
        else:
            log.info("Creating materialized view {}.{} ...".format(schema, view_name))
            start_time = time.time()  # Let's time it, because materialized views can take a while!
            engine.execute(CREATE_MATERIALIZED_VIEW_STR.format(schema, view_name, content))
            end_time = time.time()
            log.info("... success! (took %.2f sec to render)" % (end_time - start_time))


def main():
    """Update views in the current database, on the given schema."""
    if len(sys.argv) > 1:
        schema_name = sys.argv[1]
    else:
        schema_name = DEFAULT_SCHEMA

    log.info("Using database {}".format(config.get("default-db")))
    update_views(schema_name)
    create_materialized_views(schema_name)

if __name__ == '__main__':
    main()
