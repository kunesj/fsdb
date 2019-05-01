#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbError, FsdbObjectDeleted, FsdbDatabaseClosed
from .tools import sanitize_filename

import os
import shutil
import json
import logging

_logger = logging.getLogger(__name__)


class Record(object):

    data_fname = sanitize_filename('data.json')
    database = None
    _deleted = False

    def __init__(self, index, table):
        self.index = index
        self.table = table
        self.database = self.table.database
        self.cache = self.database.cache
        self.fields = self.table.fields
        self.table_path = self.table.table_path

        self.index_str = self.fields[self.table.main_index].val2str(self.index)
        self.record_path = os.path.join(self.table_path, self.index_str)
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
        return "{}-{}".format(self.table.name, self.index_str)

    # create/write/read/delete

    @classmethod
    def create(cls, table, values):
        _logger.info('CREATE RECORD IN TABLE "{}" SET values={}'.format(table.name, values))

        # get/generate record index/id
        if table.main_index in values:
            index = values[table.main_index]
        else:
            index = table.fields[table.main_index].get_new_sequence_value()
        values[table.main_index] = index

        # convert index to string (will be used as folder name)
        index_str = table.fields[table.main_index].val2str(index)
        if os.path.exists(os.path.join(table.table_path, index_str)):
            raise FsdbError('Index must be unique!')

        # create record object
        obj = cls(index, table)

        # init record directory
        os.makedirs(obj.record_path)

        # save all values
        data_values = {}
        for name in values:
            table.fields[name].write(obj, values[name], data_values)
        with open(obj.data_path, 'w') as f:
            data_values = {k: data_values.get(k) for k in table.fields}
            f.write(json.dumps(data_values, sort_keys=True, indent=2))

        # update main index
        table.fields[table.main_index].add_to_index(index, index)

        # add record to table record ids
        if obj.index not in table.record_ids:
            table.record_ids.append(obj.index)

        return obj

    def write(self, values):
        _logger.info('UPDATE RECORD "{}" IN TABLE "{}" SET values={}'.format(self.index_str, self.table.name, values))
        # changing Index value is forbidden
        if self.table.main_index in values:
            raise FsdbError('Changing main index value is not allowed!')

        # detect invalid field names
        for name in values:
            if name not in self.fields:
                _logger.warning('Write to invalid field name "{}" in table "{}"'.format(name, self.table.name))
                del(values[name])

        # delete cached value
        self.cache.del_cache(self.cache_key)

        # save all values
        with open(self.data_path, 'r') as f:
            data_values = json.loads(f.read())

        for name in values:
            self.fields[name].write(self, values[name], data_values)

        with open(self.data_path, 'w') as f:
            data_values = {k: data_values.get(k) for k in self.fields}
            f.write(json.dumps(data_values, sort_keys=True, indent=2))

        # update changed indexes
        for name in values:
            if self.fields[name].index:
                self.table.fields[name].add_to_index(values[name], self.index)

    def read(self, field_names=None):
        _logger.info('READ RECORD "{}" IN TABLE "{}" GET {}'.format(self.index_str, self.table.name, field_names or 'ALL'))
        if field_names is None:
            field_names = list(self.fields.keys())

        # detect invalid field names
        for name in field_names:
            if name not in self.fields:
                _logger.warning('Read from invalid field name "{}" in table "{}"'.format(name, self.table.name))
                field_names.remove(name)

        # get cached data
        values = self.cache.from_cache(self.cache_key) or {}

        # get list of fields that need to be read
        read_field_names = [name for name in field_names if name not in values]
        if len(read_field_names) == 0:
            # return what was requested
            return {k: values[k] for k in field_names}

        # read values
        with open(self.data_path, 'r') as f:
            data_values = json.loads(f.read())
        for name in read_field_names:
            values[name] = self.fields[name].read(self, data_values)

        # cache data
        self.cache.to_cache(self.cache_key, values)

        # return what was requested
        return {k: values[k] for k in field_names}

    def delete(self):
        _logger.info('DELETE RECORD "{}" IN TABLE "{}"'.format(self.index_str, self.table.name))
        # delete cached version
        self.cache.del_cache(self.cache_key)
        # remove from index
        for name in self.fields:
            if self.fields[name].index:
                self.table.fields[name].remove_from_index(self.index)
        # remove from table list of ids
        if self.index in self.table.record_ids:
            self.table.record_ids.remove(self.index)
        # delete data
        if os.path.exists(self.record_path):
            shutil.rmtree(self.record_path)
        # mark object as deleted
        self._deleted = True
