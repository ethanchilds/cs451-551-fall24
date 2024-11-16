"""TABLE ERRORS"""
class TableNotUniqueError(Exception):
    def __init__(self, message="A Table must have a unique name"):
        self.message = message
        super().__init__(self.message)

class TableDoesNotExistError(Exception):
    def __init__(self, message="The table does not exist"):
        self.message = message
        super().__init__(self.message)

class PrimaryKeyOutOfBoundsError(Exception):
    def __init__(self, primary_key, num_columns):
        self.message = f"The primary key `{primary_key}` is out of range `{num_columns}"
        super().__init__(self.message)

class TotalColumnsInvalidError(Exception):
    def __init__(self, num_columns):
        self.message = f"The total number of columns `{num_columns}` is not greater than zero."
        super().__init__(self.message)

"""PAGE ERRORS"""
class PageNoCapacityError(Exception):
    def __init__(self, message="Page does not have enough capacity to write to"):
        self.message = message
        super().__init__(self.message)

class PageValueTooLargeError(Exception):
    def __init__(self, cell_size, value_thats_too_large):
        self.message = f"The value `{value_thats_too_large}` is too large for a {cell_size} byte page cell"
        super().__init__(self.message)

class PageKeyError(Exception):
    def __init__(self, key):
        self.message = "key `{key}` points to an uninitialized or invalid page location"
        super().__init__(self.message)

class ColumnDoesNotExist(Exception):
    def __init__(self,column_index, num_columns):
        self.column_index = column_index
        self.num_columns = num_columns
        super().__init__(f'Column index {self.column_index} does not exist, table has {self.num_columns} columns')

"""B+TREE ERRORS"""
# bptree_exceptions.py

class BPlusTreeError(Exception):
    """Base class for exceptions in B+ Tree."""
    pass

class MinimumDegreeError(BPlusTreeError):
    def __init__(self, minimum_degree, node):
        self.minimum_degree = minimum_degree
        self.node = node
        super().__init__(f"Node is NOT maintained: minimum_degree is {self.minimum_degree} for node {self.node}")

class LeafNodeValueCountError(BPlusTreeError):
    def __init__(self, values_count, keys_count, node):
        self.values_count = values_count
        self.keys_count = keys_count
        self.node = node
        super().__init__(f"Node is NOT maintained: Leaf node has {self.values_count} values and {self.keys_count} keys for node {self.node}")

class InternalNodeValueCountError(BPlusTreeError):
    def __init__(self, values_count, keys_count, node):
        self.values_count = values_count
        self.keys_count = keys_count
        self.node = node
        super().__init__(f"Node is NOT maintained: Internal node has {self.values_count} values and {self.keys_count} keys for node {self.node}")

class InternalNodeTypeError(BPlusTreeError):
    def __init__(self, node):
        self.node = node
        super().__init__(f"Node is NOT maintained: Internal node value is not a Node for node {self.node}")

class NonRootNodeKeyCountError(BPlusTreeError):
    def __init__(self, keys_count, minimum_required, node):
        self.keys_count = keys_count
        self.minimum_required = minimum_required
        self.node = node
        super().__init__(f"Node is NOT maintained: Non-root node has {self.keys_count} keys, less than minimum required ({self.minimum_required}) for node {self.node}")

class MaxKeysExceededError(BPlusTreeError):
    def __init__(self, keys_count, maximum_keys, node):
        self.keys_count = keys_count
        self.maximum_keys = maximum_keys
        self.node = node
        super().__init__(f"Node is NOT maintained: Node has {self.keys_count} keys, exceeding the limit of {maximum_keys} keys for node {self.node}")

class NonDecreasingOrderError(BPlusTreeError):
    def __init__(self, node):
        self.node = node
        super().__init__(f"Node is NOT maintained: Keys are not in non-decreasing order for node {self.node}")

class OrphanNodeError(BPlusTreeError):
    def __init__(self, node):
        self.node = node
        super().__init__(f"Node is NOT maintained: Missing parent for non root node {self.node}")

class RootParentError(BPlusTreeError):
    def __init__(self, node):
        self.node = node
        super().__init__(f"Node is NOT maintained: Root node cannot have parent {self.node}")


"""
The keys of an internal node aren't arbitrary,
they are a list of the first key in the child nodes...

except for the first child node.
example:
internal node(keys=[5, 10])
~
leaf node(keys=[0, 1])
leaf node(keys=[5, 6])
leaf node(keys=[10, 11])
"""
class LikeFatherLikeSonError(BPlusTreeError):
    def __init__(self, parent_node, child_node):
        super().__init__(f"Node is NOT maintained: {parent_node}'s keys should contain the first key of child {child_node}*** \n ***read error.py for a better description because there is some nuance here")

class InvalidParentError(BPlusTreeError):
    def __init__(self, real_parent, false_parent, node):
        self.real_parent = real_parent
        self.false_parent = false_parent
        self.node = node
        super().__init__(f"{self.false_parent}, you are ... NOT the father of {self.node}!!! {self.real_parent} is.")

class UnbalancedTreeError(BPlusTreeError):
    def __init__(self, node):
        self.node = node
        super().__init__(f"B+ Tree is NOT maintained: The tree is unbalanced. All leaves are not at the same height")

class BrokenLinkError(BPlusTreeError):
    def __init__(self, link_len, tree_len):
        super().__init__(f"B+ Tree is NOT maintained: the leaf link hits {link_len} items, but there are {tree_len} items in the tree")

class NonUniqueKeyError(BPlusTreeError):
    def __init__(self, key):
        super().__init__(f"Attempted to insert the existing key {key} in B+ Tree Unique Key Mode")

class KeyError(BPlusTreeError):
    def __init__(self, key, value=None):
        if value is None:
            message = f"Key {key} is not in the B+ Tree"
        else:
            message = f"Key value pair ({key}, {value}) is not in the B+ Tee"

        super().__init__(message)

"""PriorityQueue ERRORS"""
class PriorityQueueCapacityOutOfBoundsError(Exception):
    def __init__(self, capacity):
        self.message = f"The given capacity {0} is out of bounds.".format(capacity)
        super().__init__(self.message)

class PriorityQueueInvalidPolicyError(Exception):
    def __init__(self):
        self.message = "The given policy is not a valid CachePolicy."
        super().__init__(self.message)

