#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import sys
import os
import tempfile
import shutil
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import fsdb
from fsdb.exceptions import FsdbDatabaseClosed, FsdbObjectDeleted


class TestFSDB(unittest.TestCase):

    auto_delete = True

    def setUp(self):
        self.root_path = tempfile.mkdtemp(prefix='fsdb_test_')
        self.fsdb = fsdb.Manager(self.root_path)
        self.fsdb.create_database('test_db')
        self.fsdb.open_database('test_db')

    def tearDown(self):
        self.fsdb = None
        if self.root_path and self.auto_delete:
            shutil.rmtree(self.root_path)

    def test_basic(self):
        # create test data
        self.fsdb.create_table(
            name='test_table',
            fields=[
                {'name': 'id', 'type': 'int', 'main_index': True, },
                {'name': 'val1', 'type': 'str', },
                {'name': 'val2', 'type': 'datetime', },
                {'name': 'val3', 'type': 'list', },
            ]
        )
        rec1_id = self.fsdb.create_records('test_table', {
            'val1': 'test_val1-1',
            'val2': datetime.datetime(2000, 1, 1),
        }).index
        rec2_id = self.fsdb.create_records('test_table', {
            'val1': 'test_val1-2',
            'val2': datetime.datetime(2000, 1, 2),
        }).index

        self.fsdb.create_table(
            name='test_table_datetime',
            fields=[
                {'name': 'id', 'type': 'datetime', 'main_index': True, },
            ]
        )
        self.fsdb.create_records('test_table_datetime', {})
        rec_dt2 = self.fsdb.create_records('test_table_datetime', {}).read()['id']
        self.fsdb.create_records('test_table_datetime', {})

        # reopen database
        db_name = self.fsdb.database.name
        self.fsdb.close_database()
        self.fsdb.open_database(db_name)

        # test if data was correctly created
        records = self.fsdb.search_records('test_table', [])
        self.assertEqual(len(records), 2)

        rec1 = self.fsdb.browse_records('test_table', rec1_id)
        self.assertIsNotNone(rec1)
        rec2 = self.fsdb.browse_records('test_table', rec2_id)
        self.assertIsNotNone(rec2)
        rec1_data = rec1.read()
        rec2_data = rec2.read()

        self.assertEqual(rec1_data['val1'], 'test_val1-1')
        self.assertEqual(rec1_data['val2'], datetime.datetime(2000, 1, 1))
        self.assertEqual(rec1_data['val3'], None)
        self.assertEqual(rec2_data['val1'], 'test_val1-2')
        self.assertEqual(rec2_data['val2'], datetime.datetime(2000, 1, 2))
        self.assertEqual(rec2_data['val3'], None)

        # test search
        records = self.fsdb.search_records('test_table_datetime', [])
        self.assertEqual(len(records), 3)

        records = self.fsdb.search_records('test_table_datetime', [('id', '>=', rec_dt2)])
        self.assertEqual(len(records), 2)

        records = self.fsdb.search_records('test_table_datetime', [('id', '>', rec_dt2)])
        self.assertEqual(len(records), 1)

        records = self.fsdb.search_records('test_table_datetime', [
            '&',
            ('id', '!=', rec_dt2),
            '|',
            ('id', '<', rec_dt2),
            ('id', '>', rec_dt2),
        ])
        self.assertEqual(len(records), 2)

        # test write
        rec1.update({'val1': 'edited_1', 'val2': datetime.datetime(2000, 10, 1)})
        self.fsdb.close_database()
        self.fsdb.open_database(db_name)
        rec1 = self.fsdb.browse_records('test_table', rec1_id)
        rec1_data = rec1.read()
        self.assertEqual(rec1_data['val1'], 'edited_1')
        self.assertEqual(rec1_data['val2'], datetime.datetime(2000, 10, 1))

    def test_closed_access(self):
        # init data
        db = self.fsdb.database
        tbl = self.fsdb.create_table(
            name='test_table',
            fields=[{'name': 'id', 'type': 'int', 'main_index': True, }, ]
        )
        rec = self.fsdb.create_records('test_table', {})

        # close db
        self.fsdb.close_database()

        # try to access objects
        self.assertRaises(FsdbDatabaseClosed, lambda: db.name)
        self.assertRaises(FsdbDatabaseClosed, lambda: tbl.name)
        self.assertRaises(FsdbDatabaseClosed, lambda: rec.name)

    def test_deleted_access(self):
        # init data
        db = self.fsdb.database
        tbl = self.fsdb.create_table(
            name='test_table',
            fields=[{'name': 'id', 'type': 'int', 'main_index': True, }, ]
        )
        rec = self.fsdb.create_records('test_table', {})

        # test record
        rec.delete()
        self.assertRaises(FsdbObjectDeleted, lambda: rec.name)

        # test table
        tbl.delete()
        self.assertRaises(FsdbObjectDeleted, lambda: tbl.name)

        # test database
        db.delete()
        self.assertRaises(FsdbObjectDeleted, lambda: db.name)


if __name__ == '__main__':
    unittest.main()
