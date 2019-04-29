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


class TestFSDB(unittest.TestCase):

    root_path = None
    auto_delete = True

    @classmethod
    def setUpClass(cls):
        cls.root_path = tempfile.mkdtemp(prefix='fsdb_test_')

    def setUp(self):
        self.db = fsdb.Database('test_db', self.root_path)
        self.db_path = self.db.db_path

    def tearDown(self):
        self.db = None
        if self.db_path and self.auto_delete:
            shutil.rmtree(self.db_path)

    @classmethod
    def tearDownClass(cls):
        if cls.root_path and cls.auto_delete:
            shutil.rmtree(cls.root_path)

    def test_create(self):
        # create test data
        self.db.create_table({
            'name': 'test_table',
            'fields': [
                {'name': 'id', 'type': 'int', 'main_index': True, },
                {'name': 'val1', 'type': 'str', },
                {'name': 'val2', 'type': 'datetime', },
                {'name': 'val3', 'type': 'list', },
            ],
        })
        rec1_id = self.db.create('test_table', {
            'val1': 'test_val1-1',
            'val2': datetime.datetime(2000, 1, 1),
        }).index
        rec2_id = self.db.create('test_table', {
            'val1': 'test_val1-2',
            'val2': datetime.datetime(2000, 1, 2),
        }).index

        self.db.create_table({
            'name': 'test_table_datetime',
            'fields': [
                {'name': 'id', 'type': 'datetime', 'main_index': True, },
            ],
        })
        self.db.create('test_table_datetime', {})
        self.db.create('test_table_datetime', {})
        self.db.create('test_table_datetime', {})

        # reopen database
        self.db = fsdb.Database(self.db.name, self.root_path)

        # test if data was correctly created
        records = self.db.search('test_table', [])
        self.assertEqual(len(records), 2)

        rec1 = self.db.browse('test_table', rec1_id)
        self.assertIsNotNone(rec1)
        rec2 = self.db.browse('test_table', rec2_id)
        self.assertIsNotNone(rec2)
        rec1_data = rec1.read()
        rec2_data = rec2.read()

        self.assertEqual(rec1_data['val1'], 'test_val1-1')
        self.assertEqual(rec1_data['val2'], datetime.datetime(2000, 1, 1))
        self.assertEqual(rec1_data['val3'], None)
        self.assertEqual(rec2_data['val1'], 'test_val1-2')
        self.assertEqual(rec2_data['val2'], datetime.datetime(2000, 1, 2))
        self.assertEqual(rec2_data['val3'], None)

        records = self.db.search('test_table_datetime', [])
        self.assertEqual(len(records), 3)


if __name__ == '__main__':
    unittest.main()
