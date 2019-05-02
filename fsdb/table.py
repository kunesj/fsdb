#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbError, FsdbObjectDeleted, FsdbDatabaseClosed, FsdbOrderError, FsdbDomainError
from .tools import sanitize_filename, validate_order, validate_domain, evaluate_domain
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

    RESERVED_FIELD_NAMES = [data_fname, 'id', 'id_str', 'create_datetime', 'modify_datetime']

    def __init__(self, name, database):
        self.name = sanitize_filename(name)
        self.database = database
        self.cache = self.database.cache
        self.db_path = self.database.db_path

        self.table_path = os.path.join(self.db_path, self.name)
        self.data_path = os.path.join(self.table_path, self.data_fname)

        self.fields = {}
        self.record_ids = []  # sorted unless record with custom id vas created

        if os.path.exists(self.data_path):
            self.load_data()
            self.load_record_ids()

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

        # validate
        self.validate()

    def validate(self):
        if 'id' not in self.fields:
            raise FsdbError('Table "{}" is missing "id" field!')
        if 'id_str' in self.fields:
            raise FsdbError('Table "{}" has "id_str" field!')
        if 'create_datetime' not in self.fields:
            raise FsdbError('Table "{}" is missing "create_datetime" field!')
        if 'modify_datetime' not in self.fields:
            raise FsdbError('Table "{}" is missing "modify_datetime" field!')

    def load_record_ids(self):
        self.record_ids = []
        for id_str in os.listdir(self.table_path):
            # check if folder is valid record, if not delete it
            record_path = os.path.join(self.table_path, id_str)
            data_path = os.path.join(self.table_path, id_str, Record.data_fname)
            if not os.path.isdir(record_path):
                continue
            if not os.path.isfile(data_path) or not os.path.exists(data_path):
                shutil.rmtree(record_path)
                continue
            # parse id and add to list of ids
            id = self.fields['id'].str2val(id_str)
            self.record_ids.append(id)
        self.record_ids.sort()

        return self.record_ids

    def get_new_id(self):

        if self.fields['id'].type == 'int':
            last_value = max(self.record_ids) if len(self.record_ids) > 0 else 0
            next_value = last_value + 1
            return int(next_value)

        elif self.fields['id'].type == 'datetime':
            return datetime.datetime.utcnow()

        else:
            raise FsdbError('Unable to generate new ID for table "{}"!'.format(self.name))

    # IDs conversion

    def ids2str(self, ids):
        id_field = self.fields['id']
        if isinstance(ids, list):
            return [id_field.val2str(rid) for rid in ids]
        else:
            return id_field.val2str(ids)

    def str2ids(self, ids_str):
        id_field = self.fields['id']
        if isinstance(ids_str, list):
            return [id_field.str2val(rid_str) for rid_str in ids_str]
        else:
            return id_field.str2val(ids_str)

    # records - browse/search

    def browse_records(self, ids):
        if isinstance(ids, list):
            return [Record(rid, self) for rid in ids if rid in self.record_ids]
        else:
            return Record(ids, self) if ids in self.record_ids else None

    def search_records(self, domain=None, order=None, limit=None):
        domain = domain if domain else []
        limit = limit if (limit and limit >= 0) else None

        # if empty domain return all records
        if len(domain) == 0:
            records = [Record(rid, self) for rid in self.record_ids[:limit]]

        # filter record ids with domain
        else:
            validate_domain(domain, self.fields.keys())
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
                    if dom_field == 'id':
                        field_value = rid
                    else:
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
                        raise FsdbDomainError(domain)

                # evaluate processed domain
                result = evaluate_domain(domain_processed)

                # evaluate result
                if result:
                    records.append(record)
                    if limit is not None and len(records) >= limit:
                        break

        # sort records
        if order:
            validate_order(order)
            for o in reversed(order.split(',')):
                name = o.strip().split(' ')[0]
                reverse = o.strip().split(' ')[-1].lower() == 'desc'
                if name not in self.fields:
                    raise FsdbOrderError('Invalid field name "{}"!'.format(name))
                records.sort(key=lambda x: x.read([name])[name], reverse=reverse)

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

        # parse ID field type
        id_type = 'int'
        for field_data in list(fields):
            if field_data['name'] == 'id':
                if field_data['type'] not in ['int', 'datetime']:
                    raise FsdbError('ID field can\'t be of type "{}"!'.format(field_data['type']))
                id_type = field_data['type']
                fields.remove(field_data)

        # detect if any fields have reserved names
        for field_data in fields:
            if field_data['name'] in cls.RESERVED_FIELD_NAMES:
                raise FsdbError('Field name "{}" is reserved name!'.format(field_data['name']))

        # add default fields
        fields.append({'name': 'id', 'type': id_type, 'required': True, 'unique': True})
        fields.append({'name': 'create_datetime', 'type': 'datetime'})
        fields.append({'name': 'modify_datetime', 'type': 'datetime'})

        # create table object
        obj = cls(table_name, database)
        obj.fields = {}
        for field_data in fields:
            obj.fields[field_data['name']] = Field.from_dict(obj, field_data)
        obj.validate()

        # create table folder and save data
        os.makedirs(obj.table_path)
        obj.save_data()

        # load data and record ids (should be instant, since it's a new table)
        obj.load_data()
        obj.load_record_ids()

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
