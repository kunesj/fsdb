#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
from operator import itemgetter
import natsort
import logging

from .exceptions import FsdbError

_logger = logging.getLogger(__name__)


class Field(object):

    # all field types that should be saved in json file in data_path of record
    FIELD_TYPES_IN_DATA = ['bool', 'str', 'int', 'float', 'list', 'tuple', 'dict', 'datetime']
    # all valid field types
    FIELD_TYPES = FIELD_TYPES_IN_DATA + []

    # indexable field types
    INDEXABLE_FIELD_TYPES = ['int', 'float', 'datetime']

    DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'

    def __init__(self, name, type, table, index=False, unique_index=False, main_index=False):
        # field data
        self.name = name.strip().lower()
        self.type = type.strip().lower()
        self.index = index or unique_index or main_index  # True if field should be indexed
        self.unique_index = unique_index or main_index  # If no duplicate values allowed
        self.main_index = main_index  # True if field is main index (can be only one)

        # run variables
        self.table = table
        self.database = self.table.database
        self.in_data = self.type in self.FIELD_TYPES_IN_DATA
        self.indexed_values = []  # [(value, record_id), ...] natsorted by value

        # validate
        self.validate()

    def to_dict(self):
        data = {
            'name': self.name,
            'type': self.type,
        }
        if self.index:
            data['index'] = self.index
        if self.unique_index:
            data['unique_index'] = self.unique_index
        if self.main_index:
            data['main_index'] = self.main_index
        return data

    @classmethod
    def from_dict(cls, table, data):
        obj = cls(data['name'], data['type'], table)
        obj.index = data.get('index') or data.get('unique_index') or data.get('main_index')
        obj.unique_index = data.get('unique_index') or data.get('main_index')
        obj.main_index = data.get('main_index')
        return obj

    def validate(self):
        if self.type not in self.FIELD_TYPES:
            raise FsdbError('Field "{}" of table "{}" has invalid type "{}"!'.format(self.name, self.table.name, self.type))

        if self.index and self.type not in self.INDEXABLE_FIELD_TYPES:
            raise FsdbError('Index "{}" of table "{}" has type "{}" that is not indexable!'.format(self.name, self.table.name, self.type))

    def build_index(self, records=None):
        _logger.info('Building index of field "{}" in table "{}"'.format(self.name, self.table.name))
        if records is None:
            records = self.table.browse_records(self.table.record_ids)

        # create list of (value, id)
        self.indexed_values = []
        for rec in records:
            self.indexed_values.append((rec.read([self.name])[self.name], rec.index))

        # natsort list of (value, id) by value
        self.indexed_values = natsort.natsorted(self.indexed_values, key=itemgetter(0))

        return self.indexed_values

    def add_to_index(self, value, rid):
        if not self.index:
            _logger.warning('Attempted to add value to index of field that is not index! Ignoring.')
            return

        # remove if already exists
        for sublist in self.indexed_values:
            if sublist[1] == rid:
                self.indexed_values.remove(sublist)
                break

        # add new value and sort
        self.indexed_values.append((value, rid))
        self.indexed_values = natsort.natsorted(self.indexed_values, key=itemgetter(0))

    def remove_from_index(self, rid):
        if not self.index:
            _logger.warning('Attempted to remove value from index of field that is not index! Ignoring.')
            return

        for sublist in self.indexed_values:
            if sublist[1] == rid:
                self.indexed_values.remove(sublist)
                break

    def get_from_index(self, rid):
        if not self.index:
            raise FsdbError('Attempted to get value from index of field that is not index!')

        for sublist in self.indexed_values:
            if sublist[1] == rid:
                return sublist[0]

        raise FsdbError('Record ID not found in index!')

    def get_new_index_value(self):
        if not self.index:
            raise FsdbError('Field "{}" of table "{}" is not index!'.format(self.name, self.table.name))

        if self.type in ['int', 'float']:
            if len(self.indexed_values) > 0:
                last_value = float(self.indexed_values[-1][0])
            else:
                last_value = 0
            next_value = last_value + 1.0
            return int(next_value) if self.type == 'int' else float(next_value)

        elif self.type == 'datetime':
            return datetime.datetime.now()

        else:
            raise FsdbError('Unsupported index type "{}" of field "{}" in table "{}"!'.format(self.type, self.name, self.table.name))