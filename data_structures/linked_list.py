class Node():
    
    def __init__(self, data):
        self.data = data
        self.next = None


class LinkedList():

    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0

    def push(self, data):
        newNode = Node(data)
        self.size += 1

        if not self.head:
            self.head = newNode
            self.tail = newNode
        else:
            self.tail.next = newNode
            self.tail = newNode

    def pop_head(self):
        if not self.head:
            return self.head
        
        self.size -= 1
        head = self.head
        self.head = head.next

        if self.size == 0:
            self.tail = None

        return head.data
        
    def __len__(self):
        return self.size


            

