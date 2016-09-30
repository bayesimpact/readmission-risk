"""
This file contains a class for uploading csv data files to a database.

It supports chunksize uploading and returns the
error lines as a dataframe by .get_errors method for further
cleaning and re-uploading.
"""

import json
import logging
import os
import string
import subprocess
from StringIO import StringIO

import pandas as pd

import psycopg2

from sutter.lib.helper import get_path

log = logging.getLogger('sutter.lib.upload')
log.setLevel(logging.INFO)


def change_column_type(table_name, convert_dict, engine, kind='up', log=None):
    """Alter column types (used inside migrations)."""
    for col_name, col_type in convert_dict.iteritems():

        up_query = """
            ALTER TABLE {0}
            ALTER COLUMN {1} TYPE {2}
            USING (NULLIF(regexp_replace({1}, '\.0$', ''),'')::{2})
        """.format(table_name, col_name, col_type)
        down_query = """
            ALTER TABLE {0}
            ALTER COLUMN {1} TYPE varchar
            USING ({1}::varchar)
        """.format(table_name, col_name)

        query = up_query if kind == 'up' else down_query
        if log is not None:
            log.info("Converting column {}.".format(col_name))
        conn = psycopg2.connect(str(engine.url))
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except Exception, e:
            log.info(e)
        cursor.close()
        conn.close()


def change_table_indices(table_name, indices_list, engine, kind):
    """Create or drop indices (used inside migrations)."""
    for ind in indices_list:
        ind_name = table_name + '_' + ind
        create_query = "CREATE INDEX {} on {} ({})".format(
            ind_name, table_name, ind)
        drop_query = "DROP INDEX IF EXISTS {}".format(ind_name)
        query = create_query if kind == 'create' else drop_query
        if log is not None:
            log.info("Creating index on column {}.".format(ind))
        conn = psycopg2.connect(str(engine.url))
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except Exception, e:
            log.info(e)
        cursor.close()
        conn.close()


def get_table_name_dict():
    """Load a dictionary with table name mappings."""
    with open(get_path() + '/sutter/json/file_tablename_dict.json') as f:
        return json.load(f)['sutter']


class CsvToSql(object):
    """A class that converts CSV files to tables in an SQL database."""

    def __init__(self, tablename, engine, **args):
        """Instantiate a CsvToSql object with a database connection."""
        self.table_name = tablename
        self.engine = engine
        self.params = {}
        self.params['chunksize'] = 1000000
        self.params['encoding'] = None
        self.params['convert'] = False
        self.params['encoding'] = None
        self.params['csv_engine'] = 'c'
        self.params['sep'] = '\t'
        self.params['encode_fix_cols'] = []
        self.params['auto_load'] = True

        path = os.path.join(get_path(), 'data/sutter/decrypted')
        if args is not None:
            for key, value in args.iteritems():
                self.params[key] = value

        self.error_chunks, self.error_lines = [], []
        fnames = get_table_name_dict()[tablename]['file_name']
        self.fpaths = [os.path.join(path, fname) for fname in fnames]

        # filename = csv_file_path.split('/')[-1]

        self.get_file_size()
        self.find_columns()
        self.error_db = pd.DataFrame(columns=self.columns)

        if self.params['auto_load']:
            if (self.load_csv()):
                self.upload_errors()
                self.conn.close()

    def get_errors(self):
        """Return a DataFrame of any entries that encountered errors during upload."""
        return self.error_db

    def get_file_size(self):
        """Log the total number of lines in all of the CSV files passed in."""
        self.nlines, self.total_lines = {}, 0
        for f in self.fpaths:
            nlines = subprocess.check_output('wc -l %s' % f, shell=True)
            self.nlines[f] = int(nlines.split()[0]) - 1
            self.total_lines += int(nlines.split()[0]) - 1

        log.info("There are %d rows in all the file." % self.total_lines)

    def find_columns(self):
        """Populate self.columns based on the columns of the first CSV file passed in."""
        def string_filter(s):
            filter(lambda x: x in string.printable, str(s).lower())

        with open(self.fpaths[0]) as f:
            columns = pd.read_csv(f,
                                  sep=self.params['sep'],
                                  engine=self.params['csv_engine'],
                                  encoding=self.params['encoding'],
                                  nrows=2).columns
            self.columns = map(string_filter,
                               columns)

    def split_file(self, fpath):
        """Return row indices for appropriately splitting a file into chunks, given a file path."""
        if (self.nlines[fpath] > self.params['chunksize']):
            self.step_size = self.nlines[fpath]
        else:
            self.step_size = self.params['chunksize']

        return range(0, self.nlines[fpath], self.step_size)

    def open_file_lower_columns(self, path, nrows, skiprows, sep='\t'):
        """Helper method used by load_csv()."""
        def fix_unicode(s):
            filter(lambda x: x in string.printable, str(s).replace('\\', '_'))

        if (self.params['convert']):
            fix_unicode = {self.params['encode_fix_cols']: fix_unicode}
        else:
            fix_unicode = None

        with open(path) as f:
            try:
                csv_file = pd.read_csv(f,
                                       header=0,
                                       sep=sep,
                                       quotechar="'",
                                       engine=self.params['csv_engine'],
                                       encoding=self.params['encoding'],
                                       nrows=nrows,
                                       skiprows=skiprows,
                                       names=self.columns,
                                       error_bad_lines=False,
                                       converters=fix_unicode)
                return csv_file
            except Exception, e:
                print(str(e))
                return False

    def load_csv(self):
        """The main method of the class. Load the CSV file and attempt to upload its rows."""
        try:
            temp = pd.read_sql("SELECT COUNT(*) FROM {}".format(self.table_name),
                               self.engine)
            current_nrows = temp['count'].iloc[0]
        except:
            current_nrows = 0

        if current_nrows >= (self.total_lines):
            msg = "if you want to replace table {}, drop it manually."
            log.info(msg.format(self.table_name))
            return False

        for f in self.fpaths:
            log.info("file path is {}".format(f))
            for i in self.split_file(f):
                msg = "if you want to replace table {}, drop it manually."
                log.info(msg.format(i, i + self.step_size))
                self.skiprows = i
                csv_file = self.open_file_lower_columns(f, self.step_size, self.skiprows)
                if csv_file is False:
                    self.error_chunks.append(i)
                    continue
                self.conn = psycopg2.connect(str(self.engine.url))
                bite_size = self.step_size // 10
                self.start_index_list = range(0, len(csv_file), bite_size) + [len(csv_file)]
                for start_index in self.start_index_list:
                    self.write_csv_to_db(csv_file, start_index, start_index + bite_size)
        return True

    def create_table(self, cursor):
        """Create the appropriate DB table name."""
        q = "create table if not exists %s (" % self.table_name
        q += "\n{0} {1}".format(self.columns[0], "BIGINT")
        for col_name in self.columns[1:]:
            q += ",\n{0} {1}".format(col_name, "VARCHAR")
        q += ");"
        cursor.execute(q)

        return True

    def write_csv_to_db(self, csv, from_line, to_line):
        """Write a range of rows from CSV file into the DB, aggressively pinpointing errors."""
        self.str_io = StringIO()
        mini_csv = csv.iloc[from_line: to_line]
        mini_csv.to_csv(self.str_io, sep='\t', index=False,
                        header=False, encoding=self.params['encoding'])
        self.str_io.seek(0)
        if self.conn.closed:
            self.conn = psycopg2.connect(str(self.engine.url))
        cursor = self.conn.cursor()
        self.create_table(cursor)
        try:
            cursor.copy_from(self.str_io, self.table_name, sep='\t', columns=self.columns)
            self.conn.commit()
            # log.info("\rRows {} to {} were successfully recorded.".format(from_line, to_line))
        except Exception, e:
            print str(e)
            self.conn.close()

            if to_line > from_line + 1:
                # log.warning("An error occured between line {} and {}".format(from_line, to_line))
                mid_point = from_line + (to_line - from_line) // 2
                # Writing the first half of df
                self.write_csv_to_db(csv, from_line, mid_point)
                # Writing the second half of df,
                self.write_csv_to_db(csv, mid_point, to_line)
            else:
                # log.warning("An error occured at line {}.".format(from_line))
                self.error_db = pd.concat(
                    [self.error_db, csv.iloc[from_line:from_line + 1]], axis=0)
                pass

    def upload_errors(self):
        """Attempt to re-upload rows that encountered errors."""
        def clean_str(s):
            filter(lambda x: x in string.printable, str(s).replace('\\', '_'))

        for col in self.error_db.columns[1:]:
            self.error_db[col] = self.error_db[col].apply(clean_str)

        try:
            log.info('Trying to upload rows with errors.')
            self.error_db.to_sql(
                self.table_name, self.engine, if_exists='append', index=False)
            self.error_db = pd.DataFrame()
        except Exception, e:
            log.info('Error on writing the error table: %s' % e)
            log.info("There are still rows with errors. I'll return them")
            return self.error_db
