from errors import *

class HashMap:
    def __init__(self, unique_keys=True):
        self.map = {}
        self.unique_keys = unique_keys
        self.length = 0

    def insert(self, key, value):
        self.length += 1
        if key not in self.map:
            self.map[key] = []
        elif self.unique_keys:
            raise NonUniqueKeyError(key)

        self.map[key].append(value)

    def bulk_insert(self, items):
        for key, value in items:
            self.insert(key, value)

    def get(self, key):
        return self.map.get(key) if key in self.map else []
    
    def get_range(self, low_key, high_key):
        values = []
        for key, value in self.map.items():
            if key >= low_key and key <= high_key:
                values.extend(value)

        return values
        
    def minimum(self):
        minimum = None
        for key, value in self.map.items():
            if minimum is None:
                minimum = key

            minimum = min(minimum, key)

        return self.map[minimum] if minimum is not None else None
    
    def maximum(self):
        maximum = None
        for key, value in self.map.items():
            if maximum is None:
                maximum = key
            
            maximum = max(maximum, key)

        return self.map[maximum] if maximum is not None else None
    
    def __contains__(self, key):
        return self.map.get(key) is not None
    
    def __len__(self):
        return self.length
    
    def keys(self):
        return iter(self.map.keys())
    
    def values(self):
        return iter(self.map.values())
    
    def items(self):
        # return iter(self.map.items())
        items = self.map.items()
        # Step 1: Flatten each tuple in first_list
        items = [(key, value) for key, values in items for value in values]
        return iter(items)

    
    def remove(self, key, value=None):
        if key not in self.map:
            raise KeyError(key)
        
        if not self.unique_keys:
            assert(value is not None)
            values = self.map[key]
            values.remove(value)
            if len(values) == 0:
                del self.map[key]
        else:
            del self.map[key]

        self.length -= 1

        
    def update(self, old_key, new_key, value=None):
        if self.unique_keys == False:
            assert(value is not None)

        values = self.get(old_key)

        if len(values) == 0:
            raise KeyError(old_key)
        
        if self.unique_keys:
            value = values[0]
        else:
            found = False
            for v in values:
                if v == value:
                    found = True
                    break

            if not found:
                raise KeyError(old_key, value)

        self.remove(old_key, value)

        try:
            self.insert(new_key, value)
        except NonUniqueKeyError:
            self.insert(old_key, value)
            raise NonUniqueKeyError(new_key)