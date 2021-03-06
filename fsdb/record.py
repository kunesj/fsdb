#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbError, FsdbObjectDeleted, FsdbDatabaseClosed
from .tools import sanitize_filename

import os
import shutil
import json
import copy
import datetime
import logging

_logger = logging.getLogger(__name__)


class Record(object):

    data_fname = sanitize_filename('data.json')
    database = None
    _deleted = False

    def __init__(self, id, table):
        self.id = id
        self.table = table
        self.database = self.table.database
        self.cache = self.database.cache
        self.fields = self.table.fields
        self.table_path = self.table.table_path

        self.id_str = self.fields['id'].val2str(self.id)
        self.record_path = os.path.join(self.table_path, self.id_str)
        self.data_path = os.path.join(self.record_path, self.data_fname)

        self.cache_key = self.generate_cache_key()

    def __getattribute__(self, name):
        # check if record is deleted
        if object.__getattribute__(self, '_deleted'):
            raise FsdbObjectDeleted('Can\'t access deleted record objects!')
        # check if database is closed
        database = object.__getattribute__(self, 'database')
        if database and object.__getattribute__(database, '_closed'):
            raise FsdbDatabaseClosed
        # return attribute
        return object.__getattribute__(self, name)

    def generate_cache_key(self):
        return "{}-{}".format(self.table.name, self.id_str)

    # create/write/read/delete

    @classmethod
    def create(cls, table, values):
        _logger.info('CREATE RECORD IN TABLE "{}" SET values={}'.format(table.name, values))

        # get/generate record id
        values['id'] = values['id'] if values.get('id') else table.get_new_id()

        # init values of system fields
        values['create_datetime'] = datetime.datetime.utcnow()
        values['modify_datetime'] = values['create_datetime']

        # init default values, remove bad field names
        for name in table.fields:
            if name not in values:
                values[name] = copy.deepcopy(table.fields[name].default)
        for name in list(values.keys()):
            if name not in table.fields.keys():
                _logger.warning('Write to invalid field name "{}" in table "{}"'.format(name, table.name))
                del(values[name])

        # convert id to string (will be used as folder name) - check if record folder already exists
        id_str = table.fields['id'].val2str(values['id'])
        if os.path.exists(os.path.join(table.table_path, id_str)):
            raise FsdbError('ID must be unique!')

        # create record object
        obj = cls(values['id'], table)

        # init record directory
        os.makedirs(obj.record_path)

        # save all values
        data_values = {}
        for name in values:
            table.fields[name].write(obj, values[name], data_values)
        with open(obj.data_path, 'w') as f:
            data_values = {k: data_values.get(k) for k in table.fields}
            f.write(json.dumps(copy.deepcopy(data_values), sort_keys=True, indent=2))

        # add record to table record ids
        if obj.id not in table.record_ids:
            table.record_ids.append(obj.id)

        return obj

    def write(self, values):
        _logger.info('UPDATE RECORD "{}" IN TABLE "{}" SET values={}'.format(self.id_str, self.table.name, values))
        # changing Index value is forbidden
        if 'id' in values:
            _logger.warning('Attempted to change record ID. ignoring.')
            del(values['id'])
        if 'id_str' in values:
            del(values['id_str'])

        # detect invalid field names
        for name in values:
            if name not in self.fields.keys():
                _logger.warning('Write to invalid field name "{}" in table "{}"'.format(name, self.table.name))
                del(values[name])

        # delete cached value
        self.cache.del_cache(self.cache_key)

        # change modify_datetime value
        values['modify_datetime'] = datetime.datetime.utcnow()

        # load old values and update them with defaults
        with open(self.data_path, 'r') as f:
            data_values = json.loads(f.read())
        for name in self.table.fields:
            if name not in data_values:
                data_values[name] = copy.deepcopy(self.table.fields[name].default)
        for name in data_values:
            if name not in self.fields.keys():
                _logger.info('Removing old field "{}" from record data."'.format(name))
                del(data_values[name])

        # save all values
        for name in values:
            self.fields[name].write(self, values[name], data_values)
        with open(self.data_path, 'w') as f:
            data_values = {k: data_values.get(k) for k in self.fields}
            f.write(json.dumps(copy.deepcopy(data_values), sort_keys=True, indent=2))

    def read(self, field_names=None):
        _logger.info('READ RECORD "{}" IN TABLE "{}" GET {}'.format(self.id_str, self.table.name, field_names or 'ALL'))
        if field_names is None:
            field_names = list(self.fields.keys())

        # detect invalid field names
        for name in field_names:
            if name not in self.fields:
                _logger.warning('Read from invalid field name "{}" in table "{}"'.format(name, self.table.name))
                field_names.remove(name)

        # get cached data
        values = self.cache.from_cache(self.cache_key) or {}

        # add id
        if 'id' in field_names:
            values['id'] = self.id
            field_names.append('id_str')
            values['id_str'] = self.id_str

        # get list of fields that need to be read
        read_field_names = [name for name in field_names if name not in values]
        if len(read_field_names) == 0:
            # return what was requested
            return {k: values[k] for k in field_names}

        # read values
        with open(self.data_path, 'r') as f:
            data_values = json.loads(f.read())
        for name in self.table.fields:
            if name not in data_values:
                data_values[name] = copy.deepcopy(self.table.fields[name].default)
        for name in read_field_names:
            values[name] = self.fields[name].read(self, data_values)

        # cache data
        self.cache.to_cache(self.cache_key, values)

        # return what was requested
        return {k: values[k] for k in field_names}

    def delete(self):
        _logger.info('DELETE RECORD "{}" IN TABLE "{}"'.format(self.id_str, self.table.name))
        # delete cached version
        self.cache.del_cache(self.cache_key)
        # remove from table list of ids
        if self.id in self.table.record_ids:
            self.table.record_ids.remove(self.id)
        # delete data
        if os.path.exists(self.record_path):
            shutil.rmtree(self.record_path)
        # mark object as deleted
        self._deleted = True
