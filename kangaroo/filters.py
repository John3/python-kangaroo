
def get_operator(operator_name):
    """Returns an Filter Class that represents the operator_name
    
    If there is not class that match with operator_name it will raise an 
    exception. 

    :param operator_name: An string that represents the operator
    :returns: An instance of Filter Class
    """
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
        """Base class for for filters
    
        :param key: A name that will be used to inspect the compared row.
        :param value: A value that will be used for the comparison.
        :returns: An instance of Filter Class
        """
        self.key = key
        self.value = value 

    def compare(self, item):
        """Compares the given item with value given
        
        :param item: An instance of Row
        :returns: A boolean representing the result of the comparison
        """
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

