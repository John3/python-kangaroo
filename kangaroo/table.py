from kangaroo.filters import get_operator
from kangaroo.unique import generate_aleatory_string

class Row(dict):

    def __init__(self, callback_update=None, **kwargs):
        super(Row, self).__init__(**kwargs)
        self.__id = generate_aleatory_string()

    def __getattr__(self, name):
        try:
            return super(Row, self).__getattr__(name)
        except:
            if name in self.keys():
                return self[name]
            raise
    
    def __setitem__(self, key, value):
        super(Row, self).__setitem__(key, value)

    def __setattr__(self, name, value):
        if name in self:
            self[name] = value
        else:
            super(Row, self).__setattr__(name, value)
        
    @property
    def idd(self):
        return self.__id


class Table(object):
    def __init__(self, tbl_name, tbl_index=[], **kwargs):
        self.__tbl_name = tbl_name
        self.__rows = []
        self.__index = {}
        self.__index_map = {}
        for i in tbl_index:
            self.__index[i] = {}

    def __unicode__(self):
        return u"Kangaroo.Table<{0}>".format(unicode(self.tbl_name))

    @property
    def tbl_name(self):
        return self.__tbl_name

    def add_index(self, index_name):
        if index_name not in self.__index:
            self.__index[index_name] = {}
            self.__build_index(index_name)
    
    def delete_index(self, index_name):
        if index_name in self.__index:
            del self.__index[index_name]

    def __delete_row_from_index(self, row):
        if row.idd in self.__index_map:
            for index_name, v in self.__index_map[row.idd]:
                self.__index[index_name][v].remove(row)
                
    def __build_index(self, index_name, rows=None):
        if rows is None:
            rows = self.__rows

        for row in rows:
            if index_name in row:
                v = row[index_name]
                if v not in self.__index[index_name]:
                    self.__index[index_name][v] = []    
                self.__index[index_name][v].append(row)
                
                if row.idd not in self.__index_map:
                    self.__index_map[row.idd] = []
                self.__index_map[row.idd].append((index_name, v))

    def delete_row(self, row):
        self.__delete_row_from_index(row)
        self.__rows.remove(row)

    def insert(self, data):
        row = Row(**data)
        self.__rows.append(row)
        for k in self.__index.keys():
            self.__build_index(k, [row])

    def find(self, **kwargs):
        result_set = self.find_all(**kwargs)
        if len(result_set) > 0:
            return result_set[0]
        return None

    def __reduce_row_by_index(self, filters):
        row_groups = []
        active_indexs = 0
        for filter_name, filter_value in filters.items():
            if filter_name in self.__index.keys():
                active_indexs += 1
                row_groups.append(
                    self.__index[filter_name].get(filter_value, []))
        
        if active_indexs == 0:
            return self.__rows

        rows = []
        row_groups.sort(key=lambda x: len(x))
        for row in row_groups[0]:
            add = True
            for ll in row_groups[1:]:
                add = a in ll
                if not add:
                    break
            if add:
                rows.append(row)
        return rows
        
    
    def __parse_filters(self, params):
        filters = []
        fields = {}
        for k in params.keys():
            args = k.split("__")
            key = args[0]
            value = params[k]
            operator_name = args[1] if len(args) == 2 else "eq"
            fields[key] = value
            op_class = get_operator(operator_name)
            operator = op_class(key, value)
            filters.append(operator)

        return fields, filters

    def find_all(self, **kwargs):
        fields, filters = self.__parse_filters(kwargs)
        result_set = self.__reduce_row_by_index(fields)

        for f in filters:
            result_set = filter(f.compare, result_set)

        return result_set