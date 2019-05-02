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
    DATETIME_FORMAT = '%Y-%m-%dT%H-%M-%S.%f'  # MUST BE filename compatible!!!!! (Could be used as folder name)

    def __init__(self, name, type, table, default=None, required=False, unique=False):
        # field data
        self.name = name.strip().lower()
        self.type = type.strip().lower()
        self.default = default
        self.required = required  # TODO: implement
        self.unique = unique  # TODO: implement

        # run variables
        self.table = table
        self.database = self.table.database

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
        if self.default:
            data['default'] = self.default
        if self.required:
            data['required'] = self.required
        if self.unique:
            data['unique'] = self.unique
        return data

    @classmethod
    def from_dict(cls, table, data):
        obj = cls(data['name'], data['type'], table)
        obj.default = data.get('default')
        obj.required = data.get('required', False)
        obj.unique = data.get('unique', False)
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
            if not isinstance(value, dict) or not value.get('name') or value.get('data') is None:
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
            data_values[self.name] = None

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
