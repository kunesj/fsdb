#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import shutil
import logging

from .exceptions import FsdbError
from .database import Database
from .table import Table
from .record import Record

_logger = logging.getLogger(__name__)


def dec_check_database_opened(f):
    def wrapper(self, *args, **kwargs):
        if not self.database:
            raise FsdbError('Database must be opened first!')
        return f(self, *args, **kwargs)
    return wrapper


def dec_check_table_exists(f):
    def wrapper(self, *args, **kwargs):
        name = args[0] if len(args) > 0 else (kwargs.get('table_name') or kwargs.get('name'))
        if not self.is_table(name):
            raise FsdbError('Table with name "{}" does not exist!'.format(name))
        return f(self, *args, **kwargs)
    return wrapper


class Manager(object):  # TODO: wait for one transaction to finish before doing next one

    def __init__(self, root_path):
        self.root_path = root_path
        self.database = None

    # Database

    def is_database(self, name):
        db_path = os.path.join(self.root_path, name)
        if os.path.isdir(db_path) and os.path.exists(db_path):
            return True
        return False

    def create_database(self, name):
        _logger.info('CREATE DATABASE "{}"'.format(name))
        if self.is_database(name):
            raise FsdbError('Database with name "{}" already exists!'.format(name))
        Database(name, self.root_path)

    def open_database(self, name):
        _logger.info('OPEN DATABASE "{}"'.format(name))
        if not self.is_database(name):
            raise FsdbError('Database with name "{}" does not exist!'.format(name))
        self.database = Database(name, self.root_path)

    def close_database(self):
        _logger.info('CLOSE DATABASE "{}"'.format(self.database.name if self.database else None))
        self.database = None

    def delete_database(self, name):
        _logger.info('DELETE DATABASE "{}"'.format(name))
        if not self.is_database(name):
            raise FsdbError('Database with name "{}" does not exist!'.format(name))
        shutil.rmtree(os.path.join(self.root_path, name))

    # Table

    @dec_check_database_opened
    def is_table(self, name):
        return True if (name in self.database.tables) else False

    @dec_check_database_opened
    def create_table(self, name, fields):
        if self.is_table(name):
            raise FsdbError('Table with name "{}" already exists!'.format(name))
        return Table.create(self.database, name, fields)

    @dec_check_database_opened
    @dec_check_table_exists
    def update_table(self, name, fields):
        self.database.tables[name].update(fields)

    @dec_check_database_opened
    @dec_check_table_exists
    def delete_table(self, name):
        self.database.tables[name].delete()
        del(self.database.tables[name])

    # Record

    @dec_check_database_opened
    @dec_check_table_exists
    def create_records(self, table_name, values):
        table = self.database.tables[table_name]
        return Record.create(table, values)

    @dec_check_database_opened
    @dec_check_table_exists
    def update_records(self, table_name, values, domain=None):
        records = self.search_records(table_name, domain)
        map(lambda rec: rec.update(values), records)
        return records

    @dec_check_database_opened
    @dec_check_table_exists
    def browse_records(self, table_name, ids):
        """
        :param table_name:
        :param ids: list of ids / one id
        :return: list of records / one record
        """
        table = self.database.tables[table_name]
        return table.browse_records(ids)

    @dec_check_database_opened
    @dec_check_table_exists
    def search_records(self, table_name, domain=None, limit=None):
        table = self.database.tables[table_name]
        domain = domain if domain else []
        limit = limit if (limit and limit >= 0) else None

        # validate domain  # TODO: make better
        for dom in domain:
            if isinstance(dom, str):
                if dom not in ['&', '|']:
                    FsdbError('Invalid domain logic operator "{}"! Only "&" and "|" are allowed.'.format(dom))

            else:
                dom_field, dom_eq, dom_value = tuple(dom)
                if dom_field not in table.fields:
                    FsdbError('Invalid field name "{}" for table "{}"!'.format(dom_field, table.name))
                if dom_field != table.main_index:
                    _logger.warning('Searching by field "{}" will be very slow and resource intensive, '
                                    'because it\'s not index!'.format(dom_field))
                if dom_eq in ['in', 'not in'] and not isinstance(dom_value, list):
                    FsdbError('Domain with \'in\' or \'not in\' must have value of type list!')

        # if emjpty domain return all records
        if len(domain) == 0:
            return [Record(rid, table) for rid in table.record_ids[:limit]]

        # filter record ids with domain
        records = []
        for rid in table.record_ids:
            record = Record(rid, table)

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
                        op = domain_processed[i-1] if i != 0 else '&'
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

    @dec_check_database_opened
    @dec_check_table_exists
    def delete_records(self, table_name, domain=None):
        table = self.database.tables[table_name]
        records = self.search_records(table_name, domain)
        for rec in records:
            if rec.index in table.record_ids:
                table.record_ids.remove(rec.index)
            rec.delete()
