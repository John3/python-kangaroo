# -*- coding: utf-8 -*-
import os
import unittest
import logging

from kangaroo.bucket import Bucket

class KangarooTest(unittest.TestCase):
    test_path = os.path.dirname(__file__)

    def tearDown(self):
        filesToDelete = ["test.kg"]
        for f in filesToDelete:
            p = os.path.join(self.test_path, f)
            if os.path.exists(p):
                os.remove(p)

    def test_create_bucket(self):
        bucket = Bucket()
        self.assertTrue(isinstance(bucket, Bucket))

    def test_add_table(self):
        bucket = Bucket()
        name = bucket.new_table.tbl_name 
        self.assertEqual(name, "new_table")
        self.assertEqual(len(bucket.tables), 1)
    
    def test_get_tables(self):
        bucket = Bucket()
        name = bucket.new_table.tbl_name 
        name2 = bucket.new_table2.tbl_name 
        
        tables = bucket.tables 
        self.assertEqual(len(tables), 2)
        self.assertTrue(tables[0].tbl_name in [name, name2])
        self.assertTrue(tables[1].tbl_name, [name, name2])
        self.assertNotEqual(tables[0].tbl_name, tables[1].tbl_name)
        
    def test_delete_table(self):
        bucket = Bucket()
        name = bucket.new_table.tbl_name 
        bucket.delete_table(name)
        self.assertEqual(len(bucket.tables), 0)

    def test_add_row(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))

        self.assertEqual(len(bucket.zoo.find_all()), 2)

    def test_table_find(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))

        self.assertEqual(bucket.zoo.find(animal="kangaroo")["animal"], 
            "kangaroo")

    def test_filter_row_gt(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        f = dict(number__gt=2)
        self.assertEqual(len(bucket.zoo.find_all(**f)), 1)

    def test_filter_row_gte(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        f = dict(number__gte=2)
        self.assertEqual(len(bucket.zoo.find_all(**f)), 2)

    def test_filter_row_eq(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        f = dict(number=2)
        self.assertEqual(len(bucket.zoo.find_all(**f)), 1)

    def test_flush_no_storage(self):
        bucket = Bucket(storage_format=None)
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        bucket.flush()

    def test_storage_pickle(self):
        p = os.path.join(self.test_path, "test.kg")
        bucket = Bucket(storage_format="pickle", storage_path=p)
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        bucket.flush()
        self.assertTrue(os.path.exists(p))

        bucket = Bucket(storage_format="pickle", storage_path=p)
        self.assertEqual(len(bucket.zoo.find_all()), 2)        
        f = dict(number=2)
        self.assertEqual(bucket.zoo.find(**f).number, 2)
    
    def test_storage_json(self):
        p = os.path.join(self.test_path, "test.kg")
        bucket = Bucket(storage_format="json", storage_path=p)
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        bucket.flush()
        self.assertTrue(os.path.exists(p))

        bucket = Bucket(storage_format="json", storage_path=p)
        self.assertEqual(len(bucket.zoo.find_all()), 2)        

        f = dict(number=2)
        self.assertEqual(bucket.zoo.find(**f).number, 2)

    def test_storage_csv(self):
        p = os.path.join(self.test_path, "test.kg")
        bucket = Bucket(storage_format="csv", storage_path=p,
            storage_options=dict(use_first_row_as_column_name=True))
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        bucket.flush()
        self.assertTrue(os.path.exists(p))

        bucket = Bucket(storage_format="csv", storage_path=p, 
            storage_options=dict(
                    use_first_row_as_column_name=True,
                    table_name="zoo"))
        
        self.assertEqual(len(bucket.zoo.find_all()), 2)        

        f = dict(number="2")
        self.assertEqual(bucket.zoo.find(**f).number, "2")

    def test_storage_csv_load(self):
        p = os.path.join(self.test_path, "tb_cidades.csv")
        bucket = Bucket(storage_format="csv", storage_path=p, 
            storage_options=dict(
                    use_first_row_as_column_name=False,
                    table_name="cidades"))
        self.assertEqual(len(bucket.cidades.find_all()), 9714)        

        f = dict(row0="0051")
        self.assertEqual(bucket.cidades.find(**f).row2, '"AL"')

    def test_storage_csv_conversion(self):
        p = os.path.join(self.test_path, "tb_cidades.csv")
        bucket = Bucket(storage_format="csv", storage_path=p, 
            storage_options=dict(
                    use_first_row_as_column_name=False,
                    table_name="cidades",
                    conversion_methods=[int, int, str, str]
                    ))
        f = dict(row0=51)
        self.assertEqual(bucket.cidades.find(**f).row2, '"AL"')

    def test_index(self):
        bucket = Bucket()
        bucket.zoo.add_index("number")
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        f = dict(number=2)
        self.assertEqual(len(bucket.zoo.find_all()), 2)        
        self.assertEqual(len(bucket.zoo.find_all(**f)), 1)        

    def test_delete_row(self):
        bucket = Bucket()
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        self.assertEqual(len(bucket.zoo.find_all()), 2)
        bucket.zoo.delete_row(bucket.zoo.find_all()[0])
        self.assertEqual(len(bucket.zoo.find_all()), 1)

    def test_delete_row_with_index(self):
        bucket = Bucket()
        bucket.zoo.add_index("number")
        bucket.zoo.insert(dict(animal="lion", number=2))
        bucket.zoo.insert(dict(animal="kangaroo", number=100))
        self.assertEqual(len(bucket.zoo.find_all()), 2)
        bucket.zoo.delete_row(bucket.zoo.find_all()[0])
        self.assertEqual(len(bucket.zoo.find_all()), 1)

    def test_update_row_with_index(self):
        bucket = Bucket()
        bucket.zoo.add_index("number")
        bucket.zoo.insert(dict(animal="lion", number=2))
        row = bucket.zoo.insert(dict(animal="lion", number=2))
        self.assertEqual(len(bucket.zoo.find_all(number=2)), 2)
        
        row["number"] = 9
        self.assertEqual(len(bucket.zoo.find_all(number=2)), 1)

    def test_update_row(self):
        bucket = Bucket()
        bucket.zoo.add_index("number")
        bucket.zoo.insert(dict(animal="lion", number=2))
        row = bucket.zoo.find()
        row["new_value"] = 1
        row.new_value = 2

        self.assertEqual(bucket.zoo.find().new_value, 2)
        self.assertEqual(bucket.zoo.find()["new_value"], 2)
    
    def test_weird_name_override(self):
        bucket = Bucket()
        names = ["__setitem__", "__setattr__", "__init__"]
        d ={}
        for n in names:
            d[n] = n
        row = bucket.zoo.insert(d)
        for n in names:
            self.assertNotEqual(type(getattr(row, n)), type(""))

        for n in names:
            self.assertEqual(type(row[n]), type(""))

if __name__ == '__main__':
    unittest.main()
