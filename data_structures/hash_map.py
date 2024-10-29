class HashMap:
    def __init__(self):
        self.map = {}
        self.length = 0

    def insert(self, key, value):
        self.length += 1
        self.map[key] = value

    def get(self, key):
        return [self.map.get(key)] if key in self.map else []
    
    def get_range(self, low_key, high_key):
        values = []
        for key, value in self.map.items():
            if key >= low_key and key <= high_key:
                values.append((value, key))

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
        return self.map.keys()
    
    def values(self):
        return self.map.values()
    
    def remove(self, key):
        if key in self.map:
            del self.map[key]
            self.length -= 1
        else:
            raise KeyError
        
    def update(self, old_key, new_key):
        value = self.get(old_key)
        self.remove(old_key)
        self.insert(new_key, value)