#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename
from .table import Table
from .cache import Cache

_logger = logging.getLogger(__name__)


class Database(object):

    def __init__(self, name, root_path):
        self.name = name
        self.root_path = root_path

        self.db_path = None
        self.data_fname = 'data.json'
        self.data_path = None

        self.tables = {}
        self.cache = Cache()

        self.init()
        self.load_data()
        self.load_tables()

    def init(self):
        assert self.name and self.root_path and self.data_fname

        # make name valid + build db_path
        self.name = sanitize_filename(self.name)
        self.db_path = os.path.join(self.root_path, self.name)

        # make data filename valid + build data_path
        self.data_fname = sanitize_filename(self.data_fname)
        self.data_path = os.path.join(self.db_path, self.data_fname)

        # init db directory
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

        # init db data (config)
        if not os.path.exists(self.data_path):
            self.save_data()

    def save_data(self):
        # format data dict
        data = copy.deepcopy({
            'name': self.name,
        })

        # write to file
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def load_data(self):
        # load from file
        with open(self.data_path, 'r') as f:
            data = json.loads(f.read())

        # parse data dict
        pass

    def load_tables(self):
        self.tables = {}
        for name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, name)
            if not os.path.isdir(table_path):
                continue
            self.tables[name] = Table(name, self)

    # query

    # def record_
