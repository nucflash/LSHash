# lshash/storage.py
# Copyright 2012 Kay Zhu (a.k.a He Zhu) and contributors (see CONTRIBUTORS.txt)
#
# This module is part of lshash and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import ujson
import cPickle as pickle
from scipy.sparse import csr_matrix 
from numpy import array

import logging
logging.basicConfig(format='[%(levelname)s] (%(threadName)-10s) %(asctime)s %(message)s',level=logging.DEBUG)

try:
    import redis
except ImportError:
    redis = None

try:
    import cql
except ImportError:
    cql = None

__all__ = ['storage']


def storage(storage_config, index):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        storage_config['redis']['db'] = index
        return RedisStorage(storage_config['redis'])
    elif 'cassandra' in storage_config:
        storage_config['cassandra']['db'] = index
        return CassandraStorage(storage_config['cassandra'])
    else:
        raise ValueError("Only in-memory dictionary, Redis and Cassandra are supported.")


class BaseStorage(object):
    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def set_val(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def get_val(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def append_val(self, key, val):
        """ Append `val` to the list stored at `key`.

        If the key is not yet present in storage, create a list with `val` at
        `key`.
        """
        raise NotImplementedError

    def get_list(self, key):
        """ Returns a list stored in storage at `key`.

        This method should return a list of values stored at `key`. `[]` should
        be returned if the list is empty or if `key` is not present in storage.
        """
        raise NotImplementedError


class InMemoryStorage(BaseStorage):
    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()

    def keys(self):
        return self.storage.keys()

    def set_val(self, key, val):
        self.storage[key] = val

    def get_val(self, key):
        return self.storage[key]

    def append_val(self, key, val):
        self.storage.setdefault(key, []).append(val)

    def get_list(self, key):
        return self.storage.get(key, [])


class RedisStorage(BaseStorage):
    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = redis.StrictRedis(**config)

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def set_val(self, key, val):
        self.storage.set(key, val)

    def get_val(self, key):
        return self.storage.get(key)

    def append_val(self, key, val):
        self.storage.rpush(key, json.dumps(val))

    def get_list(self, key):
        return self.storage.lrange(key, 0, -1)

class CassandraStorage(BaseStorage):
    def __init__(self, config):
        if not cql:
            raise ImportError("cql is required to use Cassandra as storage.")
        self.name = 'cassandra'
        self.storage = cql.connect(config["host"], config["port"], cql_version='3.0.0')
        cursor = self.storage.cursor()
        cursor.execute("""USE %s""" % config["keyspace"])

    def keys(self, pattern="*"):
        cursor = self.storage.cursor()
        cursor.execute("""SELECT key FROM lsh""")
        return cursor.fetchall()

    def set_val(self, key, val):
        cursor = self.storage.cursor()
        cursor.execute("""INSERT INTO lsh (key, val) VALUES (:key, :val)""", dict(key=key, val=val))

    def get_val(self, key):
        cursor = self.storage.cursor()
        cursor.execute("""SELECT val FROM lsh WHERE key=:key LIMIT 1""", dict(key=key))
        return cursor.fetchone()

    def append_val(self, key, val):
        cursor = self.storage.cursor()
        logging.debug("Dumping JSON...")
        # s = ujson.dumps(val)
        logging.debug("done")
        s = pickle.dumps(csr_matrix(array(val)))
        
        logging.debug(s)
        cursor.execute("""INSERT INTO lsh (key, val) VALUES (:key, :val)""", dict(key=key, val=s))

    def get_list(self, key):
        cursor = self.storage.cursor()
        cursor.execute("""SELECT val FROM lsh WHERE key=:key""", dict(key=key))
        out = []
        for row in cursor:
            out.append(row[0])
        
        return out
