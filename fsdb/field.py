#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import logging

from .exceptions import FsdbError

_logger = logging.getLogger(__name__)


class Field(object):

    # all field types that should be saved in json file in data_path of record
    FIELD_TYPES_IN_DATA = ['bool', 'str', 'int', 'float', 'list', 'tuple', 'dict', 'datetime']
    # all valid field types
    FIELD_TYPES = FIELD_TYPES_IN_DATA + []

    # format used to save and load datetime fields from/to json
    DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S.%f'  # MUST BE filename compatible!!!!! (Could be used as folder name)

    def __init__(self, name, type, table, index=False, unique_index=False, main_index=False):
        # field data
        self.name = name.strip().lower()
        self.type = type.strip().lower()
        self.index = index or unique_index or main_index  # True if field should be indexed
        self.unique_index = unique_index or main_index  # If no duplicate values allowed  # TODO
        self.main_index = main_index  # True if field is main index (can be only one)

        # run variables
        self.table = table
        self.database = self.table.database
        self.index_build = False
        self.indexed_values = []  # [(value, record_id), ...] NOT sorted

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

    # read/write

    def read(self, record, data_values):
        """
        :param record: Record object to read from
        :param data_values: "pointer" to dict with values read from data.json
        :return: value
        """
        # read value from index
        if self.index and self.index_build:
            return self.get_value_from_index(record.index)

        # read values saved in self.data_path
        if self.type in Field.FIELD_TYPES_IN_DATA:
            value = data_values.get(self.name)
            if value is None:
                return value

            if self.type == 'datetime':
                return self.str2val(value)
            elif self.type == 'tuple':
                return tuple(value)
            else:
                return value

        # read files
        # TODO

    def write(self, record, value, data_values):
        """
        :param record: Record object to write to
        :param value: new value
        :param data_values: "pointer" to dict with values that will be written to data.json
        :return:
        """
        # write values saved in self.data_path
        if self.type in Field.FIELD_TYPES_IN_DATA:
            if value is None:
                data_values[self.name] = value

            if self.type == 'datetime':
                data_values[self.name] = self.val2str(value)
            elif self.type == 'tuple':
                data_values[self.name] = list(value)
            else:
                data_values[self.name] = value

        # write files
        pass  # TODO

    # to string / from string

    def val2str(self, val):
        if self.type == 'str':
            val_str = str(val)
        elif self.type == 'int':
            val_str = str(val)
        elif self.type == 'float':
            val_str = float(val)
        elif self.type == 'datetime':
            val_str = datetime.datetime.strftime(val, self.DATETIME_FORMAT)
        else:
            raise FsdbError('Unsupported val2str type "{}"!'.format(self.type))
        return val_str

    def str2val(self, val_str):
        if self.type == 'str':
            val = str(val_str)
        elif self.type == 'int':
            val = int(val_str)
        elif self.type == 'float':
            val = float(val_str)
        elif self.type == 'datetime':
            val = datetime.datetime.strptime(val_str, self.DATETIME_FORMAT)
        else:
            raise FsdbError('Unsupported str2val type "{}"!'.format(self.type))
        return val

    # index

    def build_index(self, records=None):
        _logger.debug('Building index of field "{}" in table "{}"'.format(self.name, self.table.name))
        if records is None:
            records = self.table.browse_records(self.table.record_ids)

        # create list of (value, id)
        self.indexed_values = []
        for rec in records:
            self.indexed_values.append((rec.read([self.name])[self.name], rec.index))
        self.index_build = True

        return self.indexed_values

    def add_to_index(self, value, rid):
        if not self.index:
            raise FsdbError('Attempted to add value to index of field that is not index! Ignoring.')
        if not self.index_build:
            raise FsdbError('Attempted access index that\'s not build yet!')

        # remove if already exists
        for sublist in self.indexed_values:
            if sublist[1] == rid:
                self.indexed_values.remove(sublist)
                break

        # add new value
        self.indexed_values.append((value, rid))

    def remove_from_index(self, rid):
        if not self.index:
            raise FsdbError('Attempted to remove value from index of field that is not index!')
        if not self.index_build:
            raise FsdbError('Attempted access index that\'s not build yet!')

        for sublist in self.indexed_values:
            if sublist[1] == rid:
                self.indexed_values.remove(sublist)
                break

    def get_value_from_index(self, rid):
        if not self.index:
            raise FsdbError('Attempted to get value from index of field that is not index!')
        if not self.index_build:
            raise FsdbError('Attempted access index that\'s not build yet!')

        for sublist in self.indexed_values:
            if sublist[1] == rid:
                return sublist[0]

        raise FsdbError('Record ID not found in index!')

    def search_index(self, eq, value):  # TODO: not used anywhere right now
        """
        :param eq: =, !=, in, not in, >, >=, <, <=, ...
        :param value: value or list of values
        :return: list of record ids
        """
        if eq == '=':
            items = filter(lambda x: x[0] == value, self.indexed_values)
        elif eq == '!=':
            items = filter(lambda x: x[0] != value, self.indexed_values)
        elif eq == 'in':
            items = filter(lambda x: x[0] in value, self.indexed_values)
        elif eq == 'not in':
            items = filter(lambda x: x[0] not in value, self.indexed_values)
        elif eq == '>':
            items = filter(lambda x: x[0] > value, self.indexed_values)
        elif eq == '>=':
            items = filter(lambda x: x[0] >= value, self.indexed_values)
        elif eq == '<':
            items = filter(lambda x: x[0] < value, self.indexed_values)
        elif eq == '<=':
            items = filter(lambda x: x[0] <= value, self.indexed_values)
        else:
            raise NotImplementedError

        return map(lambda x: x[1], items)

    # misc

    def get_new_sequence_value(self):
        if not self.index:
            raise FsdbError('Field "{}" of table "{}" is not index!'.format(self.name, self.table.name))

        if self.type in ['int', 'float']:
            if len(self.indexed_values) > 0:
                last_value = self.indexed_values[-1][0]
            else:
                last_value = 0
            next_value = last_value + 1.0
            return int(next_value) if self.type == 'int' else float(next_value)

        elif self.type == 'datetime':
            return datetime.datetime.now()

        else:
            raise FsdbError('Unsupported index type "{}" of field "{}" in table "{}"!'.format(self.type, self.name, self.table.name))
