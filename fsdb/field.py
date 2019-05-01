#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbError, FsdbDatabaseClosed, FsdbObjectNotFound
from .tools import sanitize_filename

import os
import shutil
import datetime
import logging

_logger = logging.getLogger(__name__)


class Field(object):

    database = None

    # all valid field types
    FIELD_TYPES = [
        # simple - field types that are saved in data.json file of record
        'bool', 'str', 'int', 'float', 'list', 'tuple', 'dict', 'datetime',
        # more complex
        'file', 'file_list',
    ]

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

    def __getattribute__(self, name):
        # check if database is closed
        database = object.__getattribute__(self, 'database')
        if database and object.__getattribute__(database, '_closed'):
            raise FsdbDatabaseClosed
        # return attribute
        return object.__getattribute__(self, name)

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
            return self.get_value_from_index(record.id)

        # read
        if self.type == 'file':
            filename = data_values.get(self.name)
            if filename is None:
                data_values[self.name] = None
                return None

            # get file path
            file_path = os.path.join(record.record_path, data_values[self.name])
            if not os.path.exists(file_path):
                data_values[self.name] = None
                return None

            return {'name': filename, 'data': None, 'path': file_path, }

        elif self.type == 'file_list':
            # get file dir path
            file_dir_path = os.path.join(record.record_path, self.name)
            if not os.path.exists(file_dir_path):
                return []

            # read files
            file_list = []
            for filename in os.listdir(file_dir_path):
                # get file path
                file_path = os.path.join(file_dir_path, filename)
                if not os.path.isfile(file_path):
                    continue
                # save file data
                file_list.append({'name': filename, 'data': None, 'path': file_path, })

            return file_list

        elif self.type == 'datetime':
            value = data_values.get(self.name)
            return self.str2val(value) if value is not None else None

        elif self.type == 'tuple':
            value = data_values.get(self.name)
            return tuple(value) if value is not None else None

        else:
            return data_values.get(self.name)

    def write(self, record, value, data_values):
        """
        :param record: Record object to write to
        :param value: new value
        :param data_values: "pointer" to dict with values that will be written to data.json
        :return:
        """
        if self.type == 'file':
            # remove old file
            if data_values.get(self.name) is not None:
                old_file_path = os.path.join(record.record_path, data_values[self.name])
                if os.path.exists(old_file_path):
                    os.remove(old_file_path)

            # return if new value is None
            if value is None:
                data_values[self.name] = value
                return

            # validate new value
            if not isinstance(value, dict) or not value.get('name') or not value.get('data'):
                raise FsdbError('Invalid file field value!')
            if not isinstance(value['data'], bytes):
                raise FsdbError('File data must be bytes type!')
            if value['name'] in ['data.json', ] + list(record.fields.keys()):
                raise FsdbError('Filename is reserved name!')
            if value['name'] != sanitize_filename(value['name']):
                raise FsdbError('Filename is not equal to sanitized filename!')
            for name in record.fields:
                if self.name == name:
                    continue
                if record.fields[name].type == 'file' and data_values.get(name) == value['name']:
                    raise FsdbError('Filename is in conflict with value of field "{}"!'.format(name))

            # save new file
            file_path = os.path.join(record.record_path, value['name'])
            with open(file_path, 'wb') as f:
                f.write(value['data'])
            data_values[self.name] = value['name']

        elif self.type == 'file_list':
            file_list = value if isinstance(value, list) else []

            # get file dir path
            file_dir_path = os.path.join(record.record_path, self.name)

            # remove old values
            if os.path.exists(file_dir_path):
                shutil.rmtree(file_dir_path)
            os.makedirs(file_dir_path)

            # validate new value
            processed_filenames = []
            for file in file_list:
                if not isinstance(file, dict) or not file.get('name') or not file.get('data'):
                    raise FsdbError('Invalid file field value!')
                if not isinstance(file['data'], bytes):
                    raise FsdbError('File data must be bytes type!')
                if file['name'] != sanitize_filename(file['name']):
                    raise FsdbError('Filename is not equal to sanitized filename!')
                if file['name'] in processed_filenames:
                    raise FsdbError('Conflicting filename values in file_list field!')
                processed_filenames.append(file['name'])

            # write files
            for file in file_list:
                file_path = os.path.join(file_dir_path, file['name'])
                with open(file_path, 'wb') as f:
                    f.write(file['data'])

        elif self.type == 'datetime':
            data_values[self.name] = self.val2str(value) if value is not None else None

        elif self.type == 'tuple':
            data_values[self.name] = list(value) if value is not None else None

        else:
            data_values[self.name] = value

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

        # fake build main index
        if self.name == self.table.main_index:
            self.indexed_values = []
            self.index_build = True

        # build other indexes
        else:
            # get list of records if not given
            if records is None:
                records = self.table.browse_records(self.table.record_ids)

            # create list of (value, id)
            self.indexed_values = []
            for rec in records:
                self.indexed_values.append((rec.read([self.name])[self.name], rec.id))
            self.index_build = True

    def add_to_index(self, value, rid):
        if not self.index:
            raise FsdbError('Attempted to add value to index of field that is not index! Ignoring.')
        if not self.index_build:
            raise FsdbError('Attempted access index that\'s not build yet!')

        # main index - using self.table.record_ids as index
        if self.name == self.table.main_index:
            return  # should already be added

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

        # main index - using self.table.record_ids as index
        if self.name == self.table.main_index:
            return  # should already be removed

        # other indexes
        for sublist in self.indexed_values:
            if sublist[1] == rid:
                self.indexed_values.remove(sublist)
                break

    def get_value_from_index(self, rid):
        if not self.index:
            raise FsdbError('Attempted to get value from index of field that is not index!')
        if not self.index_build:
            raise FsdbError('Attempted access index that\'s not build yet!')

        # main index - using self.table.record_ids as index
        if self.name == self.table.main_index:
            if rid in self.table.record_ids:
                return rid

        # other indexes
        for sublist in self.indexed_values:
            if sublist[1] == rid:
                return sublist[0]

        raise FsdbObjectNotFound('Record ID not found in index!')

    def search_index(self, eq, value):  # TODO: not used anywhere right now
        """
        :param eq: =, !=, in, not in, >, >=, <, <=, ...
        :param value: value or list of values
        :return: list of record ids
        """
        if self.name == self.table.main_index:
            indexed_values = list(zip(self.table.record_ids, self.table.record_ids))
        else:
            indexed_values = self.indexed_values

        if eq == '=':
            items = filter(lambda x: x[0] == value, indexed_values)
        elif eq == '!=':
            items = filter(lambda x: x[0] != value, indexed_values)
        elif eq == 'in':
            items = filter(lambda x: x[0] in value, indexed_values)
        elif eq == 'not in':
            items = filter(lambda x: x[0] not in value, indexed_values)
        elif eq == '>':
            items = filter(lambda x: x[0] > value, indexed_values)
        elif eq == '>=':
            items = filter(lambda x: x[0] >= value, indexed_values)
        elif eq == '<':
            items = filter(lambda x: x[0] < value, indexed_values)
        elif eq == '<=':
            items = filter(lambda x: x[0] <= value, indexed_values)
        else:
            raise NotImplementedError

        return map(lambda x: x[1], items)
