#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import copy
import logging

from .exceptions import FsdbError
from .tools import sanitize_filename
from .table import Table
from .record import Record
from .cache import Cache

_logger = logging.getLogger(__name__)


class Database(object):
    # TODO: wait for one transaction to finish before doing next one

    # data_fname = sanitize_filename('data.json')

    def __init__(self, name, root_path):
        self.name = sanitize_filename(name)
        self.root_path = root_path

        self.db_path = os.path.join(self.root_path, self.name)
        # self.data_path = os.path.join(self.db_path, self.data_fname)

        self.tables = {}
        self.cache = Cache()

        # init db directory
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

        # # init db data (config)
        # if not os.path.exists(self.data_path):
        #     self.save_data()

        # self.load_data()
        self.load_tables()

    # def save_data(self):
    #     # format data dict
    #     data = copy.deepcopy({
    #         'name': self.name,
    #     })
    #
    #     # write to file
    #     with open(self.data_path, 'w') as f:
    #         f.write(json.dumps(data, sort_keys=True, indent=2))

    # def load_data(self):  # TODO: use for cache size
    #     # load from file
    #     with open(self.data_path, 'r') as f:
    #         data = json.loads(f.read())
    #
    #     # parse data dict
    #     pass

    def load_tables(self):
        self.tables = {}
        for name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, name)
            if not os.path.isdir(table_path):
                continue
            self.tables[name] = Table(name, self)
        return self.tables

    # table

    def create_table(self, values):
        table = Table.create(self, values)
        self.tables[table.name] = table
        return table

    def update_table(self, name, values):
        table = self.get_table(name)
        table.update(values)

    def get_table(self, name, raise_exception=True):
        if name not in self.tables:
            if raise_exception:
                raise FsdbError('Table "{}" not found!'.format(name))
            return None
        return self.tables[name]

    def delete_table(self, name):
        table = self.get_table(name)
        table.delete()
        del(self.tables[name])

    # record

    def create(self, table_name, values):
        table = self.get_table(table_name)
        rec = Record.create(table, values)
        if rec.index not in table.record_ids:
            table.record_ids.append(rec.index)
        return rec

    def write(self, table_name, values, domain=None):
        records = self.search(table_name, domain)
        for rec in records:
            rec.write(values)
        return records

    def browse(self, table_name, ids):
        """
        :param table_name:
        :param ids: list of ids / one id
        :return: list of records / one record
        """
        table = self.get_table(table_name)
        return table.browse_records(ids)

    def search(self, table_name, domain=None, limit=None):
        table = self.get_table(table_name)
        domain = domain if domain else []
        limit = limit if (limit and limit >= 0) else None

        # validate domain
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

        # filter record ids with domain
        if len(domain) == 0:
            return [Record(rid, table) for rid in table.record_ids[:limit]]

        else:
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

    def delete(self, table_name, domain=None):
        table = self.get_table(table_name)
        records = self.search(table_name, domain)
        for rec in records:
            if rec.index in table.record_ids:
                table.record_ids.remove(rec.index)
            rec.delete()
