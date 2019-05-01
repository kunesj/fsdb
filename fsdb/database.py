#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import shutil
import logging

from .exceptions import FsdbError, FsdbObjectDeleted, FsdbDatabaseClosed, FsdbObjectNotFound
from .tools import sanitize_filename
from .table import Table
from .cache import Cache

_logger = logging.getLogger(__name__)


class Database(object):

    data_fname = sanitize_filename('data.json')
    _closed = False
    _deleted = False

    def __init__(self, name, root_path):
        self.name = sanitize_filename(name)
        self.root_path = root_path

        self.db_path = os.path.join(self.root_path, self.name)
        self.data_path = os.path.join(self.db_path, self.data_fname)

        self.tables = {}
        self.cache = Cache()

        if os.path.exists(self.data_path):
            self.load_data()
            self.load_tables()

    def __getattribute__(self, name):
        # check if database is deleted
        if object.__getattribute__(self, '_deleted'):
            raise FsdbObjectDeleted('Can\'t access deleted database objects!')
        # check if database is closed
        if object.__getattribute__(self, '_closed'):
            raise FsdbDatabaseClosed
        # return attribute
        return object.__getattribute__(self, name)

    def save_data(self):
        cache_size, cache_size_limit = self.cache.get_cache_size()

        # format data dict
        data = copy.deepcopy({
            'name': self.name,
            'cache_size': cache_size,
            'cache_size_limit': cache_size_limit,
        })

        # write to file
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(data, sort_keys=True, indent=2))

    def load_data(self):
        # load from file
        with open(self.data_path, 'r') as f:
            data = json.loads(f.read())

        # parse data dict
        cache_size = data.get('cache_size')
        cache_size_limit = data.get('cache_size_limit')
        if cache_size:
            self.cache.set_cache_size(cache_size, cache_size_limit)

    def load_tables(self):
        self.tables = {}
        for name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, name)
            if not os.path.isdir(table_path):
                continue
            self.tables[name] = Table(name, self)
        return self.tables

    # create/delete/open/close

    @classmethod
    def create(cls, root_path, name):
        _logger.info('CREATE DATABASE "{}"'.format(name))

        # get valid db name
        db_name = sanitize_filename(name)
        if db_name != name:
            raise FsdbError('Name "{}" is not valid database name!'.format(name))

        # detect if db already exists
        if os.path.exists(os.path.join(root_path, db_name)):
            raise FsdbError('Database "{}" already exists!'.format(db_name))

        # create db object
        obj = cls(db_name, root_path)

        # init db directory and data
        os.makedirs(obj.db_path)
        obj.save_data()

        # load database runtime data
        obj.load_data()
        obj.load_tables()

        return obj

    def delete(self):
        _logger.info('DELETE DATABASE "{}"'.format(self.name))
        # delete cached records
        self.cache.clear()
        # delete data
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
        # mark object as deleted
        self._deleted = True

    @classmethod
    def open(cls, root_path, name):
        _logger.info('OPEN DATABASE "{}"'.format(name))

        # test if DB exists
        if not os.path.exists(os.path.join(root_path, name)):
            raise FsdbObjectNotFound('Database "{}" does not exist!'.format(name))

        # open db
        obj = cls(name, root_path)
        obj._closed = False

        return obj

    def close(self):
        _logger.info('CLOSE DATABASE "{}"'.format(self.name))
        self._closed = True
