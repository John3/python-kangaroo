import os 

from kangaroo.storage import StorageCPickle, StorageJson
from kangaroo.table import Table

class Bucket(object):
    def __init__(self, storage_format="pickle", storage_path=None):
        """Creates a Bucket where you will save information
        :param storage_apth: a valid path where we want to save the information.
        If it's None, the information will be keeped in memory
        :param storage_format: a valid format with you want to use to save 
        information. It's ignored if storage_path it's None. Valid values are
        ["pickle", "json", "xml"]
        """
        self.__tables = {}

        if storage_format == "pickle":
            self.__storage = StorageCPickle(storage_path, self)
        elif storage_format == "json":
            self.__storage = StorageJson(storage_path, self)
        else:
            raise Exception("Invalid storage format")

        if storage_path is not None and os.path.exists(storage_path):
            self.__storage.load()

    def __getattr__(self, name):
        if name in self.__tables:
            return self.__tables[name]

        table = self.add_table(Table(tbl_name=name))
        return table

    def add_table(self, table):
        if table.tbl_name in self.__tables:
            raise Exception("The table already exists")
        self.__tables[table.tbl_name] = table
        return table
    
    def delete_table(self, tbl_name):
        if tbl_name in self.__tables:
            del self.__tables[tbl_name]
        else:
            raise Exception("The table {0} Does't no exists".format(name))

    @property
    def tables(self):
        return self.__tables.values()

    def flush(self):
        self.__storage.dump()

