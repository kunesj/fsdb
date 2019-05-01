#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbError, FsdbObjectDeleted, FsdbDatabaseClosed
from .tools import sanitize_filename
from .field import Field
from .record import Record

import os
import json
import copy
import shutil
import datetime
import logging

_logger = logging.getLogger(__name__)


class Table(object):

    data_fname = sanitize_filename('data.json')
    database = None
    _deleted = False

    def __init__(self, name, database):
        self.name = sanitize_filename(name)
        self.database = database
        self.cache = self.database.cache
        self.db_path = self.database.db_path

        self.table_path = os.path.join(self.db_path, self.name)
        self.data_path = os.path.join(self.table_path, self.data_fname)

        self.fields = {
            'id': Field('id', 'int', self, index=True, primary_index=True)
        }
        self.primary_index = 'id'  # used to name record folders (parsed from self.fields at load_data())
        self.record_ids = []  # NOT sorted

        if os.path.exists(self.data_path):
            self.load_data()
            self.load_record_ids()
            self.build_indexes()

    def __getattribute__(self, name):
        # check if table is deleted
        if object.__getattribute__(self, '_deleted'):
            raise FsdbObjectDeleted('Can\'t access deleted table objects!')
        # check if database is closed
        database = object.__getattribute__(self, 'database')
        if database and object.__getattribute__(database, '_closed'):
            raise FsdbDatabaseClosed
        # return attribute
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

        # parse primary index
        for name in self.fields:
            if self.fields[name].primary_index:
                self.primary_index = self.fields[name].name
                break

        # validate
        self.validate()

    def validate(self):
        # one and only one primary index
        primary_index_found = False
        for name in self.fields:
            if self.fields[name].primary_index and primary_index_found:
                FsdbError('Only one primary index allowed for table "{}"!'.format(self.name))
            elif self.fields[name].primary_index:
                primary_index_found = True
        if not primary_index_found:
            FsdbError('No primary index defined for table "{}"!'.format(self.name))

    def load_record_ids(self):
        self.record_ids = []
        for id_str in os.listdir(self.table_path):
            record_path = os.path.join(self.table_path, id_str)
            if not os.path.isdir(record_path):
                continue
            id = self.fields[self.primary_index].str2val(id_str)
            self.record_ids.append(id)
        return self.record_ids

    def build_indexes(self):
        _logger.debug('Building indexes for table "{}"'.format(self.name))

        # build id index
        self.fields[self.primary_index].build_index()

        # get list of fields that are indexes
        index_field_names = []
        for name in self.fields:
            if name == self.primary_index:
                continue
            if self.fields[name].index:
                index_field_names.append(name)

        # build indexes
        if len(index_field_names) > 0:
            # prefetch data to cache
            records = self.browse_records(self.record_ids)
            for rec in records[:1000]:
                rec.read(index_field_names)

            # build indexes
            for name in index_field_names:
                self.fields[name].build_index(records)

    def get_new_id(self):
        primary_index_field = self.fields[self.primary_index]

        if primary_index_field.type in ['int', 'float']:
            last_value = max(self.record_ids) if len(self.record_ids) > 0 else 0
            next_value = last_value + 1.0
            return int(next_value) if primary_index_field.type == 'int' else float(next_value)

        elif primary_index_field.type == 'datetime':
            return datetime.datetime.now()

        else:
            raise FsdbError('Unable to generate new ID for table "{}"!'.format(self.name))

    # records - browse/search

    def browse_records(self, ids):
        if isinstance(ids, list):
            return [Record(rid, self) for rid in ids if rid in self.record_ids]
        else:
            return Record(ids, self) if ids in self.record_ids else None

    def search_records(self, domain=None, limit=None):
        domain = domain if domain else []
        limit = limit if (limit and limit >= 0) else None

        # validate domain  # TODO: make better
        for dom in domain:
            if isinstance(dom, str):
                if dom not in ['&', '|']:
                    FsdbError('Invalid domain logic operator "{}"! Only "&" and "|" are allowed.'.format(dom))

            else:
                dom_field, dom_eq, dom_value = tuple(dom)
                if dom_field not in self.fields:
                    FsdbError('Invalid field name "{}" for table "{}"!'.format(dom_field, self.name))
                if dom_field != self.primary_index:
                    _logger.warning('Searching by field "{}" will be very slow and resource intensive, '
                                    'because it\'s not index!'.format(dom_field))
                if dom_eq in ['in', 'not in'] and not isinstance(dom_value, list):
                    FsdbError('Domain with \'in\' or \'not in\' must have value of type list!')

        # if emjpty domain return all records
        if len(domain) == 0:
            return [Record(rid, self) for rid in self.record_ids[:limit]]

        # filter record ids with domain
        records = []
        for rid in self.record_ids:
            record = Record(rid, self)

            domain_processed = []
            for dom in domain:
                # & or |
                if isinstance(dom, str):
                    domain_processed.append(dom)
                    continue

                # get sub-domain
                dom_field, dom_eq, dom_value = tuple(dom)

                # get field value
                field_value = record.read([dom_field, ])[dom_field]

                # filter record by sub-domain
                if dom_eq == '=':
                    domain_processed.append(field_value == dom_value)
                elif dom_eq == '!=':
                    domain_processed.append(field_value != dom_value)
                elif dom_eq == 'in':
                    domain_processed.append(field_value in dom_value)
                elif dom_eq == 'not in':
                    domain_processed.append(field_value not in dom_value)
                elif dom_eq == '>':
                    domain_processed.append(field_value > dom_value)
                elif dom_eq == '>=':
                    domain_processed.append(field_value >= dom_value)
                elif dom_eq == '<':
                    domain_processed.append(field_value < dom_value)
                elif dom_eq == '<=':
                    domain_processed.append(field_value <= dom_value)
                else:
                    raise FsdbError('Invalid domain! {}'.format(domain))

            # evaluate processed domain  # TODO: test this
            while len(domain_processed) >= 2:
                domain_changed = False
                for i in range(len(domain_processed)-1):
                    if isinstance(domain_processed[i], bool) and isinstance(domain_processed[i+1], bool):
                        op = domain_processed[i-1] if (i >= 1 and isinstance(domain_processed[i-1], str)) else '&'
                        if op == '&':
                            out = domain_processed[i] and domain_processed[i+1]
                        elif op == '|':
                            out = domain_processed[i] or domain_processed[i+1]
                        else:
                            raise FsdbError('Invalid processed domain! {}'.format(domain_processed))

                        if i == 0:
                            domain_processed.pop(i+1)
                            domain_processed[i] = out
                        else:
                            domain_processed.pop(i+1)
                            domain_processed[i-1] = out
                            domain_processed.pop(i)

                        domain_changed = True
                        break

                if not domain_changed:
                    break

            if len(domain_processed) > 1:
                raise FsdbError('Invalid processed domain! {}'.format(domain_processed))
            result = bool(domain_processed[0])

            # evaluate result
            if result:
                records.append(record)
                if limit is not None and len(records) >= limit:
                    break

        # return records
        return records

    # create/delete

    @classmethod
    def create(cls, database, name, fields):
        _logger.info('CREATE TABLE "{}" SET fields={}'.format(name, fields))

        # get valid table name
        table_name = sanitize_filename(name)
        if table_name != name:
            raise FsdbError('Name "{}" is not valid table name!'.format(name))

        # detect if table already exists
        if os.path.exists(os.path.join(database.db_path, table_name)):
            raise FsdbError('Table "{}" already exists!'.format(table_name))

        # create table object
        obj = cls(table_name, database)
        obj.fields = {}
        for field_data in fields:
            obj.fields[field_data['name']] = Field.from_dict(obj, field_data)
        obj.validate()

        # create table folder and save data
        os.makedirs(obj.table_path)
        obj.save_data()

        # load record ids and build indexes (should be instant, since it's a new table)
        obj.load_record_ids()
        obj.build_indexes()

        # add to database list of tables
        database.tables[obj.name] = obj

        return obj

    def delete(self):
        _logger.info('DELETE TABLE "{}"'.format(self.name))
        # delete cached records
        self.cache.clear()
        # delete data
        if os.path.exists(self.table_path):
            shutil.rmtree(self.table_path)
        # mark object as deleted
        self._deleted = True
