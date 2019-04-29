#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import shutil
import json
import copy
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename
from .field import Field

_logger = logging.getLogger(__name__)


class Record(object):

    data_fname = sanitize_filename('data.json')
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
        table = object.__getattribute__(self, 'table')
        table_deleted = object.__getattribute__(table, '_deleted') if table else True
        record_deleted = object.__getattribute__(self, '_deleted')
        if table_deleted or record_deleted:
            FsdbError('Can\'t access deleted records!')
        else:
            return object.__getattribute__(self, name)

    def save_values(self, values):
        # init default values
        default_values = {k: None for k in self.fields}
        values = dict(default_values, **copy.deepcopy(values))

        # convert values to json compatible format
        for name in values:
            if name not in self.fields or values[name] is None:
                continue
            field_type = self.fields[name].type

            if field_type == 'datetime':
                values[name] = self.fields[name].val2str(values[name])
            elif field_type == 'tuple':
                values[name] = list(values[name])

        # write to file
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(values, sort_keys=True, indent=2))

    def load_values(self):
        # read values
        with open(self.data_path, 'r') as f:
            values = json.loads(f.read())

        # init default values
        default_values = {k: None for k in self.fields}
        values = dict(default_values, **values)

        # parse values that were converted to json compatible format
        for name in values:
            if name not in self.fields or values[name] is None:
                continue
            field_type = self.fields[name].type

            if field_type == 'datetime':
                values[name] = self.fields[name].str2val(values[name])
            elif field_type == 'tuple':
                values[name] = tuple(values[name])

        # return
        return values

    def generate_cache_key(self):
        return "{}-{}".format(self.table.name, self.index_str)

    @classmethod
    def create(cls, table, values):
        if table.main_index in values:
            index = values[table.main_index]
            del(values[table.main_index])
        else:
            index = table.fields[table.main_index].get_new_sequence_value()

        index_str = table.fields[table.main_index].val2str(index)
        if os.path.exists(os.path.join(table.table_path, index_str)):
            raise FsdbError('Index must be unique!')

        obj = cls(index, table)

        # init record directory
        if not os.path.exists(obj.record_path):
            os.makedirs(obj.record_path)

        # init values
        if not os.path.exists(obj.data_path):
            obj.save_values({obj.table.main_index: index})

        # update main index
        table.fields[table.main_index].add_to_index(index, index)

        # update new record with values
        obj.write(values)

        return obj

    def write(self, values):
        # changing Index value is forbidden
        if self.table.main_index in values:
            raise FsdbError('Changing main index value is not allowed!')

        # detect invalid field names
        for name in values:
            if name not in self.fields:
                _logger.warning('Write to invalid field name "{}" in table "{}"'.format(name, self.table.name))
                del(values[name])

        # write values saved in self.data_path
        data_values = {}
        for name in values:
            if self.fields[name].type in Field.FIELD_TYPES_IN_DATA:
                data_values[name] = values[name]
        if len(data_values) > 0:
            old_data_values = self.load_values()
            self.save_values(dict(old_data_values, **data_values))

        # write files
        pass  # TODO

        # update cached version
        old_values = self.cache.from_cache(self.cache_key) or {}
        self.cache.to_cache(self.cache_key, dict(old_values, **values))

        # update changed indexes
        for name in values:
            if self.fields[name].index:
                self.table.fields[name].add_to_index(values[name], self.index)

    def read(self, field_names=None):
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

        # read values from index (if one exists)
        for name in list(read_field_names):
            if not self.fields[name].index or not self.fields[name].index_build:
                continue
            values[name] = self.fields[name].get_from_index(self.index)
            read_field_names.remove(name)

        # read values saved in self.data_path
        data_values = None
        for name in list(read_field_names):
            if self.fields[name].type not in Field.FIELD_TYPES_IN_DATA:
                continue
            if data_values is None:
                data_values = self.load_values()
            values[name] = data_values[name]
            read_field_names.remove(name)

        # read files
        pass  # TODO

        # cache data
        self.cache.to_cache(self.cache_key, values)

        # return what was requested
        return {k: values[k] for k in field_names}

    def delete(self):
        # delete cached version
        self.cache.del_cache(self.cache_key)
        # remove from index
        for name in self.fields:
            if self.fields[name].index:
                self.table.fields[name].remove_from_index(self.index)
        # delete data
        shutil.rmtree(self.record_path)
        # mark object as deleted
        self._deleted = True
