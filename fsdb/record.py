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
        self.data_fname = sanitize_filename('data.json')
        self.data_path = os.path.join(self.record_path, self.data_fname)

    def __getattribute__(self, name):
        table = object.__getattribute__(self, 'table')
        table_deleted = object.__getattribute__(table, '_deleted') if table else True
        record_deleted = object.__getattribute__(self, '_deleted')
        if table_deleted or record_deleted:
            FsdbError('Can\'t access deleted records!')
        else:
            return object.__getattribute__(self, name)

    def save_values(self, values):
        default_values = {k: None for k in self.fields}
        values = dict(default_values, **copy.deepcopy(values))
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(values, sort_keys=True, indent=2))

    def load_values(self):
        default_values = {k: None for k in self.fields}
        with open(self.data_path, 'r') as f:
            values = json.loads(f.read())
        return dict(default_values, **values)

    def get_cache_key(self):
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
            default_values = {k: None for k in obj.fields}
            default_values[obj.table.main_index] = index
            obj.save_values(default_values)

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
        data_values = None
        for name in values:
            if self.fields[name].type in Field.FIELD_TYPES_IN_DATA:
                data_values = self.load_values()
                break
        for name in list(values.keys()):
            if self.fields[name].type in Field.FIELD_TYPES_IN_DATA:
                data_values[name] = values[name]
                if self.fields[name].type == 'datetime' and data_values[name] is not None:
                    data_values[name] = self.fields[name].val2str(data_values[name])
        if data_values is not None:
            self.save_values(data_values)

        # write files
        pass  # TODO

        # update cached version
        cache_key = self.get_cache_key()
        old_values = self.cache.from_cache(cache_key) or {}
        self.cache.to_cache(cache_key, dict(old_values, **values))

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
        cache_key = self.get_cache_key()
        values = self.cache.from_cache(cache_key) or {}

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
            field_type = self.fields[name].type
            if field_type not in Field.FIELD_TYPES_IN_DATA:
                continue
            if data_values is None:
                data_values = self.load_values()

            if field_type == 'int' and data_values[name] is not None:
                values[name] = int(data_values[name])
            elif field_type == 'float' and data_values[name] is not None:
                values[name] = float(data_values[name])
            elif field_type == 'tuple' and data_values[name] is not None:
                values[name] = tuple(data_values[name])
            elif field_type == 'datetime' and data_values[name] is not None:
                values[name] = self.fields[name].str2val(data_values[name])
            else:
                values[name] = data_values[name]

            read_field_names.remove(name)

        # read files
        pass  # TODO

        # cache data
        self.cache.to_cache(cache_key, values)

        # return what was requested
        return {k: values[k] for k in field_names}

    def delete(self):
        # delete cached version
        cache_key = self.get_cache_key()
        self.cache.del_cache(cache_key)
        # remove from index
        for name in self.fields:
            if self.fields[name].index:
                self.table.fields[name].remove_from_index(self.index)
        # delete data
        shutil.rmtree(self.record_path)
        # mark object as deleted
        self._deleted = True
