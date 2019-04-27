#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename
from .table import Table
from .record import Record
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

    # query (records)

    def query(self, action, table_name, values, where):
        """
        :param action: INSERT/SELECT/UPDATE/DELETE [str]
        :param table_name: table name [str]
        :param values: new record values (used by action=INSERT/UPDATE) [dict]
        :param where: basic search domain (used by action=SELECT/UPDATE/DELETE) [domain]
            - only search by index field allowed (right now)
        :return: list of records/results
        """
        if table_name not in self.tables:
            FsdbError('Table "{}" not found!'.format(table_name))
        table = self.tables[table_name]

        if action.upper() == 'INSERT':
            return [Record.create(table, values), ]

        elif action.upper() == 'SELECT':
            where = where if where else []
            record_ids = copy.deepcopy(table.record_ids)

            for dom in where:
                dom_field, dom_eq, dom_value = tuple(dom)
                if dom_field != table.index:
                    FsdbError('Only index field can be used in domain!')

                for rid in record_ids:
                    if dom_eq == '=':
                        if dom_value != rid:
                            record_ids.remove(rid)
                    elif dom_eq == '!=':
                        if dom_value == rid:
                            record_ids.remove(rid)

            records = []
            for rid in record_ids:
                records.append(Record(rid, table))

            return records

        elif action.upper() == 'UPDATE':
            records = self.query('SELECT', table_name, False, where)
            for rec in records:
                rec.write(values)
            return records

        elif action.upper() == 'DELETE':
            records = self.query('SELECT', table_name, False, where)
            return [rec.delete() for rec in records]

    def query_insert(self, table_name, values):
        return self.query('INSERT', table_name, values, False)

    def query_select(self, table_name, where):
        return self.query('SELECT', table_name, False, where)

    def query_update(self, table_name, values, where):
        return self.query('UPDATE', table_name, values, where)

    def query_delete(self, table_name, where):
        return self.query('DELETE', table_name, False, where)
