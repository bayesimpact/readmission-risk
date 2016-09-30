"""Database-related methods."""

import sqlalchemy as sa

from sutter.lib import config


def get_connection():
    """
    Get a connection to the database.

    This method assumes that you have a tunnel to the database open. See
    project readme for an explanation of how to open the tunnel.

    Information for the connection read from our config system. This allows
    to use the connection e.g. with pd.read_sql(query, connection)
    """
    config.reload()
    db_name = config.get("default-db")
    db_config = config.get("databases.{}".format(db_name))

    config_string = "postgresql://{}:{}@{}:{}/{}".format(db_config['user'],
                                                         db_config['password'],
                                                         db_config['host'],
                                                         db_config['port'],
                                                         db_config['database'])
    return sa.create_engine(config_string)
