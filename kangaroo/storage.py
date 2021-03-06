import csv
import json
import pickle
import time 
from kangaroo.table import Table

class Storage(object):

    def __init__(self, path, bucket, options):
        """Base class of Storage
        

        :param path: A valid path where you will save the database
        :param bucket: An instance of Bucket that we want to save
        :param options: A dictionary of options for the storage
        """
        self.path = path
        self.bucket = bucket
        self.options = options

    def dump(self):
        """Dumps the bucket from memory to Disk
        """
        raise NotImplementedError()

    def load(self):
        """Load the bucket from disk to memory
        """
        raise NotImplementedError()

    def get_data_to_save(self):
        data = {
            "tables": self.bucket.tables,
            "time": time.time()
        }
        return data

    def load_into_memory(self, data):
        for t in data["tables"]:
            self.bucket.add_table(t)

class StorageCPickle(Storage):
    
    def load(self):
        with open(self.path, 'rb') as f:
            data = pickle.load(f)
        self.load_into_memory(data)

    def dump(self):
        data = self.get_data_to_save()
        with open(self.path, 'wb') as f:
            pickle.dump(data, f)

class StorageJson(Storage):
    
    def load(self):
        with open(self.path, 'r') as f:
            database = json.loads(f.read())
        data = dict(tables=[], time=database["time"])
        for t in database["tables"]:
            table = Table(tbl_name=t["tbl_name"],
                tbl_index=t["tbl_index"])
            for row in t["rows"]:
                table.insert(row)
            data["tables"].append(table)
        self.load_into_memory(data)

    def dump(self):
        data = self.get_data_to_save()
        tables = []
        for table in self.bucket.tables:
            d = {
                "tbl_name": table.tbl_name,
                "tbl_index": table.tbl_index,
                "rows": table.find_all()
            }
            tables.append(d)
        
        database = dict(time=time.time(), tables=tables)

        with open(self.path, 'w') as f:
            f.write(json.dumps(database))


class StorageCsv(Storage):
    
    def load(self):
        data = dict(tables=[])
        table = Table(tbl_name=self.options.get("table_name", "table1"))

        with open(self.path, 'r') as f:
            database = csv.reader(f, 
                delimiter=self.options.get("delimiter", ","), 
                quotechar=self.options.get("quotechar", "|"))
            
            names = None
            if self.options.get("use_first_row_as_column_name", True):
                names = next(database)

            for row in database:
                if names is None:
                    names = ["row{0}".format(i) for i in range(len(row))]                    
                drow = {}
                if self.options.get("conversion_methods") is not None:
                    row = map(lambda x, y: x(y), 
                            self.options.get("conversion_methods"), row)
                drow = dict(zip(names, row))
                table.insert(drow)
        data["tables"].append(table)
        self.load_into_memory(data)


    def dump(self):
        data = self.get_data_to_save()
        i = -1
        for table in self.bucket.tables:
            i += 1
            p = self.path if i == 0 else "{1}_{0}".format(self.path, i)
            with open(p, 'w') as f:
                writter = csv.writer(f, 
                        delimiter=self.options.get("delimiter", ","), 
                        quotechar=self.options.get("quotechar", "|"),
                        quoting=self.options.get("quoting", csv.QUOTE_MINIMAL))

                if self.options.get("use_first_row_as_column_name", True):
                    title = table.find()
                    if title is not None:
                        writter.writerow(list(title.keys()))

                for drow in table.find_all():
                    writter.writerow(list(drow.values()))


