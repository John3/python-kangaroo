import pickle
import time 

class Storage(object):

    def __init__(self, path, bucket):
        self.path = path
        self.bucket = bucket

    def dump(self, bucket):
        raise NotImplementedError()

    def load(self, bucket):
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
