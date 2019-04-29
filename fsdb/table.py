#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import shutil
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename
from .field import Field
from .record import Record

_logger = logging.getLogger(__name__)


class Table(object):

    _deleted = False

    def __init__(self, name, database):
        self.name = sanitize_filename(name)
        self.database = database
        self.cache = self.database.cache
        self.db_path = self.database.db_path

        self.table_path = os.path.join(self.db_path, self.name)
        self.data_fname = sanitize_filename('data.json')
        self.data_path = os.path.join(self.table_path, self.data_fname)

        self.fields = {
            'id': Field('id', 'int', self, index=True, main_index=True)
        }
        self.main_index = 'id'  # used to name record folders (parsed from self.fields at load_data())
        self.record_ids = []  # NOT sorted

        if os.path.exists(self.data_path):
            self.load_data()
            self.load_record_ids()
            self.build_indexes()

    def __getattribute__(self, name):
        if object.__getattribute__(self, '_deleted'):
            FsdbError('Can\'t access deleted tables!')
        else:
            return object.__getattribute__(self, name)

    def save_data(self):
        # validate
        self.validate()

        # format data dict
        data = copy.deepcopy({
            'name': self.name,  # just for info
            'fields': [self.fields[name].to_dict() for name in sorted(self.fields.keys())],
        })

        # write to file
        with open(self.data_path, 'w') as f:
            f.write(json.dumps(data, sort_keys=True, indent=2))

    def load_data(self):
        # load from file
        with open(self.data_path, 'r') as f:
            data = json.loads(f.read())

        # parse data dict
        self.fields = {}
        for field_data in data['fields']:
            self.fields[field_data['name']] = Field.from_dict(self, field_data)

        # parse main index
        for name in self.fields:
            if self.fields[name].main_index:
                self.main_index = self.fields[name].name
                break

        # validate
        self.validate()

    def validate(self):
        # one and only one main index
        main_index_found = False
        for name in self.fields:
            if self.fields[name].main_index and main_index_found:
                FsdbError('Only one main index allowed for table "{}"!'.format(self.name))
            elif self.fields[name].main_index:
                main_index_found = True
        if not main_index_found:
            FsdbError('No main index defined for table "{}"!'.format(self.name))

    def load_record_ids(self):
        self.record_ids = []
        for index_str in os.listdir(self.table_path):
            record_path = os.path.join(self.table_path, index_str)
            if not os.path.isdir(record_path):
                continue
            index = self.fields[self.main_index].str2val(index_str)
            self.record_ids.append(index)
        return self.record_ids

    def browse_records(self, ids):
        if isinstance(ids, list):
            return [Record(rid, self) for rid in ids if rid in self.record_ids]
        else:
            return Record(ids, self) if ids in self.record_ids else None

    def build_indexes(self):
        _logger.info('Building indexes for table "{}"'.format(self.name))

        # get list of fields that are indexes
        index_field_names = []
        for name in self.fields:
            if self.fields[name].index:
                index_field_names.append(name)

        # prefetch data to cache
        records = self.browse_records(self.record_ids)
        for rec in records[:10000]:
            rec.read(index_field_names)

        # build indexes
        for name in index_field_names:
            self.fields[name].build_index(records)

    # create/update/delete

    @classmethod
    def create(cls, database, values):
        # get table name
        if 'name' not in values:
            raise FsdbError('Missing table name value!')
        table_name = sanitize_filename(values['name'])
        del(values['name'])

        # detect if table already exists
        if os.path.exists(os.path.join(database.db_path, table_name)):
            raise FsdbError('Table "{}" name already exists!'.format(table_name))

        # init empty table
        obj = cls(table_name, database)
        os.makedirs(obj.table_path)
        obj.save_data()

        # write table values
        obj.update(values)

        return obj

    def update(self, values):
        if 'name' in values:
            _logger.warning('Attempted to update table name of table "{}". Ignoring.'.format(self.name))
        if 'fields' in values:
            self.fields = {}
            for field_data in values['fields']:
                self.fields[field_data['name']] = Field.from_dict(self, field_data)
        self.validate()
        self.save_data()
        self.load_record_ids()
        self.build_indexes()

    def delete(self):
        # delete cached records
        self.cache.clear_cache()
        # delete data
        shutil.rmtree(self.table_path)
        # mark object as deleted
        self._deleted = True
