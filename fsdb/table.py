#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import datetime
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename

_logger = logging.getLogger(__name__)


class Table(object):

    # all field types that should be saved in json file in data_path of record
    FIELD_TYPES_IN_DATA = ['bool', 'str', 'int', 'float', 'list', 'tuple', 'dict', 'datetime']
    # all valid field types
    FIELD_TYPES = FIELD_TYPES_IN_DATA + []

    def __init__(self, name, database):
        self.name = name
        self.database = database
        self.cache = self.database.cache
        self.db_path = self.database.db_path

        self.table_path = None
        self.data_fname = 'data.json'
        self.data_path = None

        self.fields = {
            'id': {
                'type': 'number',
            }
        }
        self.index = 'id'  # used to name record folders
        self.record_ids = []

        self.init()
        self.load_data()
        self.get_record_ids()

    def init(self):
        assert self.name and self.db_path and self.data_fname

        # make name valid + build table_path
        self.name = sanitize_filename(self.name)
        self.table_path = os.path.join(self.db_path, self.name)

        # make data filename valid + build data_path
        self.data_fname = sanitize_filename(self.data_fname)
        self.data_path = os.path.join(self.table_path, self.data_fname)

        # init table directory
        if not os.path.exists(self.table_path):
            os.makedirs(self.table_path)

        # init table data (config)
        if not os.path.exists(self.data_path):
            self.save_data()

    def save_data(self):
        # validate
        self.validate()

        # format data dict
        data = copy.deepcopy({
            'name': self.name,  # just for info
            'fields': self.fields,
            'index': self.index,
        })

        # write to file
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def load_data(self):
        # load from file
        with open(self.data_path, 'r') as f:
            data = json.loads(f.read())

        # parse data dict
        self.fields = data['fields']
        self.index = data['index']

        # validate
        self.validate()

    def validate(self):
        # field types
        for name in self.fields:
            field_type = self.fields[name]['type'].lower()
            if field_type not in self.FIELD_TYPES:
                _logger.warning('Invalid field type "{}" for field "{}" in table "{}".'.format(field_type, name, self.name))

        # index
        if self.index not in self.fields:
            raise FsdbError('Index "{}" of table "{}" is not in fields!'.format(self.index, self.name))
        if self.fields[self.index]['type'].lower() not in ['int', 'float', 'datetime']:
            raise FsdbError('Index "{}" of table "{}" has invalid index type!'.format(self.index, self.name))

    def get_record_ids(self):
        record_ids = []
        for index in sorted(os.listdir(self.table_path)):
            record_path = os.path.join(self.table_path, index)
            if not os.path.isdir(record_path):
                continue
            record_ids.append(index)
        self.record_ids = record_ids
        return record_ids

    def get_next_index(self):
        field_type = self.fields[self.index]['type'].lower()

        if field_type == 'int':
            record_ids = [int(record_id) for record_id in self.record_ids]
            return max(record_ids) + 1

        elif field_type == 'float':
            record_ids = [float(record_id) for record_id in self.record_ids]
            return max(record_ids) + 1

        elif field_type == 'datetime':
            return datetime.datetime.now()

        else:
            raise FsdbError('Unexpected type "{}" for index "{}" in table "{}"!'.format(field_type, self.index, self.name))
