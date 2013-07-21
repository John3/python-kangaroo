
def get_operator(operator_name):
    if operator_name == "gt":
        return Gt
    elif operator_name == "gte":
        return Gte
    elif operator_name == "eq":
        return Eq
    elif operator_name == "in":
        return In
    elif operator_name == "range":
        return Range
    elif operator_name == "contains":
        return Contains
    elif operator_name == "startswith":
        return StartsWith
    elif operator_name == "endswith":
        return EndsWith

    raise Exception("Invalid operator name {0}".format(operator_name))

class Filter(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value 

    def compare(self, item):
        raise NotImplementedError()

class Gt(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key] > self.value

class Gte(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key] >= self.value

class Eq(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key] == self.value

class In(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key] in self.value

class Range(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key] <= self.value[1] and \
                item[self.key] >= self.value[0]

class Contains(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return self.value in item[self.key]

class StartsWith(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key].startswith(self.value)

class EndsWith(Filter):
    def compare(self, item):
        if self.key not in item:
            return False
        return item[self.key].endswith(self.value)

