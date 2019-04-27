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


class Record(object):

    def __init__(self, index, table):
        self.index = index
        self.table = table
        self.database = self.table.database
        self.cache = self.database.cache
        self.fields = self.table.fields
        self.table_path = self.table.table_path

        self.record_path = None
        self.data_fname = 'data.json'
        self.data_path = None

        self.init()

    def init(self):
        assert self.index and self.table_path and self.data_fname

        # make index valid + build record_path
        self.index = sanitize_filename(str(self.index))
        self.record_path = os.path.join(self.table_path, self.index)

        # make data filename valid + build data_path
        self.data_fname = sanitize_filename(self.data_fname)
        self.data_path = os.path.join(self.record_path, self.data_fname)

        # init db directory
        if not os.path.exists(self.record_path):
            os.makedirs(self.record_path)

        # init values
        if not os.path.exists(self.data_path):
            values = {k: None for k in self.fields}
            values[self.table.index] = self.index
            self.save_values(values)

    def save_values(self, values):
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(copy.deepcopy(values), sort_keys=True, indent=4))

    def load_values(self):
        with open(self.data_path, 'r') as f:
            values = json.loads(f.read())
        return values

    def get_cache_key(self):
        return "{}-{}".format(self.table.name, self.index)

    def read(self, field_names):
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

        # read values saved in self.data_path
        data_values = None
        for name in read_field_names:
            field_type = self.fields[name]['type']
            if field_type not in self.table.FIELD_TYPES_IN_DATA:
                continue
            if data_values is None:
                data_values = self.load_values()
            values[name] = data_values[name]

            if field_type == 'int' and values[name] is not None:
                values[name] = int(values[name])
            elif field_type == 'float' and values[name] is not None:
                values[name] = float(values[name])
            elif field_type == 'tuple' and values[name] is not None:
                values[name] = tuple(values[name])
            elif field_type == 'datetime' and values[name] is not None:
                values[name] = datetime.datetime.fromisoformat(values[name])

            read_field_names.remove(name)

        # read files
        pass  # TODO

        # cache data
        self.cache.to_cache(cache_key, values)

        # return what was requested
        return {k: values[k] for k in field_names}

    def write(self, values):
        # changing Index value is forbidden
        if self.table.index in values:
            raise FsdbError('Changing index values is not allowed!')

        # detect invalid field names
        for name in values:
            if name not in self.fields:
                _logger.warning('Write to invalid field name "{}" in table "{}"'.format(name, self.table.name))
                del(values[name])

        # delete cached version
        cache_key = self.get_cache_key()
        self.cache.del_cache(cache_key)

        # write values saved in self.data_path
        data_values = None
        for name in values:
            if self.fields[name]['type'] in self.table.FIELD_TYPES_IN_DATA:
                data_values = self.load_values()
                break
        for name in values:
            if self.fields[name]['type'] in self.table.FIELD_TYPES_IN_DATA:
                data_values[name] = values[name]
                if self.fields[name]['type'] == 'datetime' and data_values[name] is not None:
                    data_values[name] = datetime.datetime.isoformat(data_values[name])
                del(values[name])
        if data_values is not None:
            self.save_values(data_values)

        # write files
        pass  # TODO

    @classmethod
    def create(cls, table, values):
        if table.index in values:
            index = values[table.index]
            del(values[table.index])
        else:
            index = table.get_next_index()

        index = sanitize_filename(str(index))
        if os.path.exists(os.path.join(table.table_path, index)):
            raise FsdbError('Index must be unique!')

        obj = cls(index, table)
        obj.write(values)
        table.record_ids.append(obj.index)
