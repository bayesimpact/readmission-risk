"""Methods for dealing with configuration files."""

import os

try:
    import ujson as json
except ImportError:
    import json
import sutter

config_ = None


class Config(object):
    """
    An internal representation of a configuration file.

    Handles multiple possible config sources (path or env var) and nested-key lookups.
    """

    def __init__(self, config):
        """Instantiate a Config object with config file contents."""
        self.config_ = config

    @classmethod
    def from_path(cls, path):
        """Load configuration from a given path."""
        with open(path) as file:
            return cls(json.loads(file.read()))

    @classmethod
    def from_enviroment(cls):
        """Load configuration based on information provided by enviromental variables."""
        path = os.environ.get('CONFIGPATH')
        if path is None:
            raise ValueError('CONFIGPATH not set!')

        return cls.from_path(path)

    def get(self, key, default=None):
        """
        Fetch a configuration variable, returning `default` if the key does not exist.

        :param key: Variable key.
        :param default: Default value to return if `key` is not found.
        :returns: The value, or `default` if the key does not exist.
        """
        try:
            value = self[key]
        except KeyError:
            value = default
        return value

    def __getitem__(self, key):
        """
        Fetch a configuration variable, returning `default` if the key does not exist.

        :param key: Variable key.
        :returns: The value.
        :raises: TypeError if key is not found.
        """
        # Handle nested parameters
        return_object = self.config_
        for key in key.split('.'):
            return_object = return_object[key]

        return return_object


def get(key, default=None):
    """
    Fetch a configuration variable, returning `default` if the key does not exist.

    :param key: Variable key, possibly nested via `.`s.
    :param default: Default value to return.
    :returns: The value, or `default` if the key does not exist.
    """
    global config_
    if config_ is None:
        reload()
    return config_.get(key, default)


def reload(path=None):
    """
    Reload configuration.

    This method looks in three places, in order, to find the config file:
        1. an explicit path, if one is passed as an argument
        2. the CONFIGPATH env variable, if set
        3. the default path at ../config.json
    """
    module_base = os.path.dirname(os.path.abspath(sutter.__file__))
    fixed_path = os.path.join(module_base, '..', 'config.json')

    global config_
    if path is not None:
        config_ = Config.from_path(path)
    elif os.environ.get('CONFIGPATH'):
        config_ = Config.from_enviroment()
    elif os.path.exists(fixed_path):
        config_ = Config.from_path(fixed_path)
    else:
        print("no configuration path or file given (see README)")
