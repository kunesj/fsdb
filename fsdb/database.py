#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import logging

from .tools import sanitize_filename
from .table import Table
from .cache import Cache

_logger = logging.getLogger(__name__)


class Database(object):

    data_fname = sanitize_filename('data.json')

    def __init__(self, name, root_path):
        self.name = sanitize_filename(name)
        self.root_path = root_path

        self.db_path = os.path.join(self.root_path, self.name)
        self.data_path = os.path.join(self.db_path, self.data_fname)

        self.tables = {}
        self.cache = Cache()

        # init db directory
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

        # init db data (config)
        if not os.path.exists(self.data_path):
            self.save_data()

        self.load_data()
        self.load_tables()

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

    # TODO: create/update/delete - move some code from manager
