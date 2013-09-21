import os 

from kangaroo.storage import StorageCPickle, StorageJson, StorageCsv
from kangaroo.table import Table

class Bucket(object):
    def __init__(self, storage_format=None, storage_path=None, 
        storage_options={}):
        """Creates a new Bucket instance.

        :param storage_path: a valid path where we want to save the information.
        :param storage_format: a valid format with you want to use to save 
            information. Valid values are [None, "pickle", "json"].
            If it's None, the information will be keeped in memory and 
            storage_path will be ignored
        :param storage_options: a dictionary with specific options for every
            storage. 
        """
        self.__tables = {}
        self.__storage = None

        if storage_format == "pickle":
            self.__storage = StorageCPickle(storage_path, self, storage_options)
        elif storage_format == "json":
            self.__storage = StorageJson(storage_path, self, storage_options)
        elif storage_format == "csv":
            self.__storage = StorageCsv(storage_path, self, storage_options)
        elif storage_format is None:
            self.__storage = None
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
        """Adds a new table to the bucket
        
        :param table: an instance of kangaroo.Table
        :returns: the same instance added in table

        """
        if table.tbl_name in self.__tables:
            raise Exception("The table already exists")
        self.__tables[table.tbl_name] = table
        return table
    
    def delete_table(self, tbl_name):
        """Deletes a table from the bucket.

        This method with raise an exception if there is no table to delete.
        
        :param tbl_table: the name of the table that we want to delete.
        :raises: Exception
        """
        if tbl_name in self.__tables:
            del self.__tables[tbl_name]
        else:
            raise Exception("The table {0} Does't no exists".format(name))

    @property
    def tables(self):
        """Returns the list of available tables
        """
        return self.__tables.values()

    def flush(self):
        """Saves the information from memory to disk
        """
        if self.__storage is not None:
            self.__storage.dump()

