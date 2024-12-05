import threading
from data_structures.linked_list import LinkedList

class Queue:

    def __init__(self, elements = None):
        self.queue = LinkedList()
        self.lock = threading.Lock()
        self.size = 0
        
        if elements:
            self.size = len(elements)
            for element in elements:
                self.queue.push(element)

    def push(self, data):
        with self.lock:
            self.queue.push(data)
            self.size += 1

    def pop(self):
        with self.lock:
            self.size -= 1
            return self.queue.pop_head()

    def isEmpty(self):
        if self.size == 0:
            return True
        return False
    
    def __len__(self):
        return self.size