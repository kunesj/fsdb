#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbDatabaseClosed, FsdbObjectNotFound
from .database import Database
from .table import Table
from .record import Record

import os
import logging

_logger = logging.getLogger(__name__)


def dec_check_database_opened(f):
    def wrapper(self, *args, **kwargs):
        if not self.database:
            raise FsdbDatabaseClosed('Database must be opened first!')
        return f(self, *args, **kwargs)
    return wrapper


def dec_check_table_exists(f):
    def wrapper(self, *args, **kwargs):
        name = args[0] if len(args) > 0 else (kwargs.get('table_name') or kwargs.get('name'))
        if not self.is_table(name):
            raise FsdbObjectNotFound('Table with name "{}" does not exist!'.format(name))
        return f(self, *args, **kwargs)
    return wrapper


class Manager(object):  # TODO: wait for one transaction to finish before doing next one

    def __init__(self, root_path):
        self.root_path = root_path
        self.database = None

        # Used to detect
        self.transaction_ongoing = False

    # Database

    def is_database(self, name):
        db_path = os.path.join(self.root_path, name)
        if os.path.isdir(db_path) and os.path.exists(db_path):
            return True
        return False

    def create_database(self, name):
        Database.create(self.root_path, name)

    def open_database(self, name):
        if self.database:
            self.database.close()
        self.database = Database.open(self.root_path, name)

    def close_database(self):
        if self.database:
            self.database.close()
        self.database = None

    def delete_database(self, name):
        if not self.is_database(name):
            raise FsdbObjectNotFound('Database with name "{}" does not exist!'.format(name))

        if self.database and self.database.name == name:
            _logger.warning('Deleting database opened in manager!')
            self.close_database()
        Database(name, self.root_path).delete()

    # Table

    @dec_check_database_opened
    def is_table(self, name):
        return True if (name in self.database.tables) else False

    @dec_check_database_opened
    def create_table(self, name, fields):
        return Table.create(self.database, name, fields)

    @dec_check_database_opened
    @dec_check_table_exists
    def delete_table(self, name):
        self.database.tables[name].delete()
        del(self.database.tables[name])

    # Record

    @dec_check_database_opened
    @dec_check_table_exists
    def create_record(self, table_name, values):
        table = self.database.tables[table_name]
        return Record.create(table, values)

    @dec_check_database_opened
    @dec_check_table_exists
    def write_records(self, table_name, values, domain=None):
        records = self.search_records(table_name, domain)
        map(lambda rec: rec.write(values), records)
        return records

    @dec_check_database_opened
    @dec_check_table_exists
    def browse_records(self, table_name, ids):
        table = self.database.tables[table_name]
        return table.browse_records(ids)

    @dec_check_database_opened
    @dec_check_table_exists
    def search_records(self, table_name, domain=None, limit=None):
        table = self.database.tables[table_name]
        return table.search_records(domain=domain, limit=limit)

    @dec_check_database_opened
    @dec_check_table_exists
    def delete_records(self, table_name, domain=None):
        records = self.search_records(table_name, domain)
        map(lambda x: x.delete(), records)
