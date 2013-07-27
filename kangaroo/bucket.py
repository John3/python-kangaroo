import os 

from kangaroo.filters import get_operator
from kangaroo.storage import StorageCPickle
from kangaroo.unique import generate_aleatory_string

class Row(dict):

    def __init__(self, **kwargs):
        super(Row, self).__init__(**kwargs)
        self.__id = generate_aleatory_string()

    def __getattr__(self, name):
        if name in self.keys():
            return self[name]
        raise AttributeError(" error")
        
    
class Table(object):
    def __init__(self, tbl_name, tbl_index=[], **kwargs):
        self.__tbl_name = tbl_name
        self.__rows = []
        self.__tbl_index = tbl_index
        self.__index = []

    def __unicode__(self):
        return u"Kangaroo.Table<{0}>".format(unicode(self.tbl_name))

    @property
    def tbl_name(self):
        return self.__tbl_name

    def insert(self, data):
        row = Row(**data)
        self.__rows.append(row)
        for k in row.keys():
            if k in self.__tbl_index:
                if k not in self.__index:
                    self.__index[k] = {}
                if getattr(row, k) not in self.__index[k]:
                    self.__index[k][getattr(row, k)] = []    
                self.__index[k][getattr(row, k)].append(row)

    def find(self, **kwargs):
        result_set = self.find_all(**kwargs)
        if len(result_set) > 0:
            return result_set[0]
        return None

    def find_all(self, **kwargs):
        filters = []
        for k in kwargs.keys():
            args = k.split("__")
            key = args[0]
            value = kwargs[k]
            operator_name = args[1] if len(args) == 2 else "eq"
            op_class = get_operator(operator_name)
            operator = op_class(key, value)
            filters.append(operator)

        result_set = self.__rows
        for f in filters:
            result_set = filter(operator.compare, result_set)

        return result_set


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

