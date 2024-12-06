from utilities.algorithms import binary_search, linear_search
from utilities.latch import Latch
from config import Config
from errors import *
from random import random
from heapq import merge

class Node:
    def __init__(self, minimum_degree=16, is_leaf: bool=False, parent=None):
        assert(minimum_degree >= 2)
        self.minimum_degree = minimum_degree
        self.is_leaf: bool = is_leaf
        self.link = None    # Should be none unless a leaf.
        self.rev_link = None
        self.parent = parent  # Should be none only if a root.
        self.latch = Latch()
        
        self.keys = []
        self.values = []

    def is_maximum_size(self) -> bool:
        return len(self.keys) == (2 * self.minimum_degree) - 1
    
    def is_minimum_size(self) -> bool:
        return len(self.keys) == self.minimum_degree - 1
        

    """
    A maintained B+ Tree Node has multiple properties.
    This function checks if a node is maintained.
    Use for debuging.
    """
    # TODO: rename to check_maintenance because this doesn't return bool's, it just raises errors if things are wrong.
    def is_maintained(self, is_root) -> bool:
        if self.minimum_degree < 2:
            raise MinimumDegreeError(self.minimum_degree, self)
        
        # if not (is_root or self.parent):
        #     raise OrphanNodeError(self)
        
        if not is_root:
            if len(self.keys) < self.minimum_degree - 1:
                raise NonRootNodeKeyCountError(len(self.keys), self.minimum_degree - 1, self)

        if len(self.keys) > 2 * self.minimum_degree - 1:
            raise MaxKeysExceededError(len(self.keys), self.minimum_degree * 2 - 1, self)

        if self.is_leaf:
            if len(self.values) != len(self.keys):
                raise LeafNodeValueCountError(len(self.values), len(self.keys), self)
        else:
            if len(self.values) != len(self.keys) + 1:
                raise InternalNodeValueCountError(len(self.values), len(self.keys), self)
            
            for value in self.values:
                if not isinstance(value, Node):
                    raise InternalNodeTypeError(self)

        for i in range(1, len(self.keys)):
            if self.keys[i - 1] > self.keys[i]:
                raise NonDecreasingOrderError(self)
            
        if not (is_root or self.parent):
            raise OrphanNodeError(self)
        
        if is_root and self.parent:
            raise RootParentError(self)
        
        # I don't like the tripple nesting here
        if not self.is_leaf:
            for value in self.values:
                if value.parent != self:
                    raise InvalidParentError(self, value.parent, value)

        # TODO: The range of a nodes keys must be in between (inclusive I think) two keys of the parent

        # I now know the true rule to follow. The old rule is a special case of the true rule (which made for some horrific bugs)
        # Each internal key is the smallest key in the associated keys subbranch
        # I thought it was each internal key equals the smallest key in associated keys child node
        if not self.is_leaf:
            for index in range(len(self.keys)):
                if self.values[index + 1].keys[0] < self.keys[index]:
                    print(f"{self.values[index + 1].keys[0]}, {self.keys[index]}")
                    # Can you tell i'm have fun with these error names yet?
                    raise LikeFatherLikeSonError(self, self.values[index + 1])



        return True
    
    def __repr__(self):
        if self.is_leaf:
            node_type = "Leaf"
        elif self.parent:
            node_type = "Internal"
        else:
            node_type = "Root"
        
        max_items = 4
        keys_to_show = self.keys[:max_items]
        values_to_show = self.values[:max_items] if self.is_leaf else []
        
        value_string = f", values={values_to_show}" if self.is_leaf else f", *{len(self.values)} values*"
        if len(self.keys) > max_items:
            keys_to_show.append("...")  # formats with quotes, but it isn't worth my time to make it look pretty.
        
        if self.is_leaf and len(self.values) > max_items:
            value_string += "..."

        return f"{node_type} Node(keys={keys_to_show}{value_string})"
        


class BPlusTree:
    """
    B+ Tree

    is_maintained() -> bool
    insert(key, value)
    get(key) -> value
    contains_key(key) -> bool
    minimum() -> value
    maximum() -> value
    remove(key)
    len() -> b+ tree length

    """
    def __init__(
            self, 
            minimum_degree: int=Config.b_plus_tree_minimum_degree, 
            unique_keys: bool=True,
            return_keys: bool=False,
            debug_mode: bool=False, 
            search_algorithm_threshold=Config.b_plus_tree_search_algorithm_threshold,
            bulk_insert_threshold=Config.b_plus_tree_bulk_insert_ratio_threshold,
        ):
        self.height = 0
        self.length = 0
        self.minimum_degree = minimum_degree
        self.unique_keys = unique_keys
        self.return_keys = return_keys  # return [(key, value)] OR return [(value)]
        self.root = Node(minimum_degree, is_leaf=True)

        self.search_algorithm_threshold = search_algorithm_threshold # When do we binary search keys and when do we linear scan keys?
        self.bulk_insert_threshold = bulk_insert_threshold # When do we insert items one by one vs create new tree and bulk insert

        self.debug_mode = debug_mode
        if self.debug_mode:
            print("WARNING: b+ tree is in debug mode. This significantly slows down every operation.")

    def is_maintained(self):
        # Raises descriptive error is root is not maintained.
        self.root.is_maintained(is_root=True)
        
        # Check if all leaves are at the same height
        leaf_height = self._check_leaves_height(self.root, 0)
        if leaf_height is None:
            raise UnbalancedTreeError
        
        link_size = len(list(self.items()))
        tree_size = len(self)
        if link_size != tree_size:
            raise BrokenLinkError(link_size, tree_size)

        return True

    def _check_leaves_height(self, node: Node, current_height: int):
        node.is_maintained(is_root=(current_height==0))
        
        if node.is_leaf:
            # If it's a leaf node, return its height
            return current_height
    
        
        # If it's not a leaf, go through its children
        heights = []
        for child in node.values:
            if isinstance(child, Node):
                child_height = self._check_leaves_height(child, current_height + 1)
                if child_height is not None:
                    heights.append(child_height)

        # Check if all heights are the same
        if heights and all(h == heights[0] for h in heights):
            return heights[0]  # Return the common height
        else:
            return None  # Heights are inconsistent

    def insert(self, key, value):
        if self.length == 0:
            self.root = Node(self.minimum_degree, is_leaf=True)
            self.root.keys.append(key)
            self.root.values.append([value])
            self.length += 1
            return
        
        node = self.root

        while not node.is_leaf:
            # Finds index where node.keys == key OR key could be inserted to maintain a non-decreasing node.keys
            index = self._find_key_index(node.keys, key)

            # I worked through every case logically to get this
            if index == len(node.keys) or key < node.keys[index]:
                node = node.values[index]
            else:
                node = node.values[index + 1]

        # At this point, we should be at the correct leaf node to insert a key
        index = self._find_key_index(node.keys, key)

        # If we are in unique_keys mode, and the key already exists, raise error
        if self.unique_keys and index < len(node.keys) and node.keys[index] == key:
            raise NonUniqueKeyError(key)
        
        if index >= len(node.keys) or node.keys[index] != key:
            node.keys.insert(index, key)
            node.values.insert(index, [])

        node.values[index].append(value)
        self.length += 1

        if len(node.keys) == 2 * self.minimum_degree:
            self._split_leaf_node(node)

        if self.debug_mode:
            self.is_maintained()

    def bulk_insert(self, items):
        """
        Bulk Insert Method
        Inserts many items at a time by building tree layer by layer
        Up to 10 times faster than inserting items one at a time
        items: a list of key-value tuples
        """
        items.sort(key=lambda item: item[0])

        # We choose an insertion strategy depending on the size of the tree
        item_ratio = len(items) / len(self) if len(self) else len(items)
        if len(self) < Config.b_plus_tree_bulk_insert_start_threshold or item_ratio < Config.b_plus_tree_bulk_insert_ratio_threshold:
            # print("BULK INSERT: new items list too small. Inserting one by one")
            for key, value in items:
                self.insert(key, value)
                
            return
                
            # print("BULK INSERT: new items list large enough. Bulk insert as planned!")

        # if len(self):
        #     items = list(merge(items, list(self.items())))
        #     self.reset()

        if len(self):
            items += list(self.items())
            self.reset()

        items.sort(key=lambda item: item[0])
    
        
        self.length = len(items)

        leaf_nodes = [] # containes elements (node, min_key_in_subtree)

        group_size = self.minimum_degree
        i = 0
        # Linear scan through items, combining values and making leaf nodes.
        while i < len(items):
            node = Node(self.minimum_degree, is_leaf=True)
            while i < len(items) and len(node.keys) < group_size:
                key = items[i][0]
                value = []
                while i < len(items) and items[i][0] == key:
                    value.append(items[i][1])
                    i += 1
                node.keys.append(key)
                node.values.append(value)
            leaf_nodes.append(node)

        # Merge last two nodes if the last one is too small
        if len(leaf_nodes) >= 2 and len(leaf_nodes[-1].keys) < group_size:
            leaf_node_2 = leaf_nodes.pop()
            leaf_node_1 = leaf_nodes.pop()
            leaf_node_1.keys.extend(leaf_node_2.keys)
            leaf_node_1.values.extend(leaf_node_2.values)
            leaf_nodes.append(leaf_node_1)

        for i in range(1, len(leaf_nodes)):
            leaf_nodes[i - 1].link = leaf_nodes[i]
            leaf_nodes[i].rev_link = leaf_nodes[i - 1]
            
        min_keys_of_leaf_nodes = [node.keys[0] for node in leaf_nodes]

        self._build_layer(leaf_nodes, min_keys_of_leaf_nodes)
        
        if self.debug_mode:
            self.is_maintained()


    def _build_layer(self, nodes_at_level, min_keys_of_node_subtrees):
        """
        Recursively takes a layer of nodes and constructs the smaller layer above it.
        nodes_at_level contains a list of elements (Node, min_key_in_subtree)
        """
        if len(nodes_at_level) == 1:
            self.root = nodes_at_level[0]
            return
        
        assert len(nodes_at_level) == len(min_keys_of_node_subtrees)
        
        limit = len(nodes_at_level) - (self.minimum_degree * 2 + 1)
        nodes_by_parent = []
        parents_keys = []

        group_size = self.minimum_degree + 1
        stopping_point = 0
        for i in range(0, limit + 1, group_size):
            nodes_by_parent.append(nodes_at_level[i:i + group_size])
            parents_keys.append(min_keys_of_node_subtrees[i:i + group_size])
            stopping_point = i + group_size

        items_are_remaining = limit < len(nodes_at_level)
        if items_are_remaining:
            nodes_by_parent.append(nodes_at_level[stopping_point:])
            parents_keys.append(min_keys_of_node_subtrees[stopping_point:])

        parent_nodes = []
        for i, children in enumerate(nodes_by_parent):
            parent = Node(self.minimum_degree)
            parent.keys = parents_keys[i][1:]
            parent.values = children

            for child in children:
                child.parent = parent

            parent_nodes.append(parent)

        min_keys_of_parent_subtrees = []
        for parent_keys in parents_keys:
            min_keys_of_parent_subtrees.append(parent_keys[0])

        self._build_layer(parent_nodes, min_keys_of_parent_subtrees)
        

    # Not using for now becuase self[key] = value always inserts a value, never updates a value.
    # def __setitem__(self, key, value):
    #    self.insert(key, value)

    def _split_leaf_node(self, leaf_node):
        new_leaf = Node(self.minimum_degree, is_leaf=True)
        
        # Move half the keys and values to the new leaf node
        mid_index = self.minimum_degree
        new_leaf.keys = leaf_node.keys[mid_index:]
        new_leaf.values = leaf_node.values[mid_index:]
        new_leaf.link = leaf_node.link
        new_leaf.rev_link = leaf_node

        if new_leaf.link is not None:
            new_leaf.link.rev_link = new_leaf

        # Update the original leaf node
        leaf_node.keys = leaf_node.keys[:mid_index]
        leaf_node.values = leaf_node.values[:mid_index]
        leaf_node.link = new_leaf


        # If the leaf is the root, create a new root
        if leaf_node == self.root:
            # Parents are correct
            new_root = Node(self.minimum_degree, is_leaf=False)
            new_root.keys.append(new_leaf.keys[0])  # Add the first key of the new leaf
            new_root.values.append(leaf_node)  # Left child
            new_root.values.append(new_leaf)  # Right child
            self.root = new_root
            self.height += 1

            leaf_node.parent = new_root
            new_leaf.parent = new_root
        else:
            # Pretty sure parents are legit

            # Insert the new key into the parent node
            parent_node = leaf_node.parent
            index = self._find_key_index(parent_node.keys, new_leaf.keys[0])
            parent_node.keys.insert(index, new_leaf.keys[0])
            parent_node.values.insert(index + 1, new_leaf)


            new_leaf.parent = parent_node

            # Split the parent node if too many keys
            if len(parent_node.keys) >= 2 * self.minimum_degree:
                self._split_internal_node(parent_node)

    def _split_internal_node(self, internal_node):
        new_internal = Node(self.minimum_degree, is_leaf=False)
        
        mid_index = self.minimum_degree
        mid_key = internal_node.keys[mid_index]
        new_internal.keys = internal_node.keys[mid_index + 1:]
        new_internal.values = internal_node.values[mid_index + 1:]

        # Divorce operation
        for value in new_internal.values:
            value.parent = new_internal

        # Update the original internal node
        internal_node.keys = internal_node.keys[:mid_index]
        internal_node.values = internal_node.values[:mid_index + 1]

        # If the internal node is the root, create a new root
        if internal_node == self.root:
            new_root = Node(self.minimum_degree, is_leaf=False)
            new_root.keys.append(mid_key)  # Promote the middle key
            new_root.values.append(internal_node)  # Left child
            new_root.values.append(new_internal)  # Right child
            self.root = new_root
            self.height += 1

            internal_node.parent = new_root
            new_internal.parent = new_root
        else:
            # Insert the promoted key into the parent node
            parent_node = internal_node.parent   # Should be correct and fastest, but isn't correct.
            index = self._find_key_index(parent_node.keys, internal_node.keys[-1])
            # parent_node.keys.insert(index, internal_node.keys[-1])
            parent_node.keys.insert(index, mid_key)
            parent_node.values.insert(index + 1, new_internal)

            new_internal.parent = internal_node.parent

            # Split the parent if necessary (recursive)
            if len(parent_node.keys) >= 2 * self.minimum_degree:
                self._split_internal_node(parent_node)
    
    def _find_key_index(self, keys, key):
        if len(keys) < self.search_algorithm_threshold:
            return linear_search(keys, key)
        return binary_search(keys, key)
    
    # TODO: pretty sure that this should throw KeyError if self.get(key) doesn't return anything. However, a value is allowed to be None, so I don't know what to do!
    def __getitem__(self, key):
        return self.get(key)
    
    def get(self, key):
        """
        Always returns a list (even if garanteed to return only one item)
        It's confusing when we sometimes return a list and sometimes return a value.
        Therefore all index data structures return lists
        """
        # if not self.unique_keys:
        #     if self.return_keys:
        #         return [value for key, value in self.get_range(key, key)]
        #     else:
        #         return [value for value in self.get_range(key, key)]

        node = self._get_leaf(key)
        if node is None:
            return node

        index = self._find_key_index(node.keys, key)

        if index < len(node.keys) and key == node.keys[index]:
            return [(node.keys[index], value) for value in node.values[index]] if self.return_keys else node.values[index]
        
        return []
    
    """
    Get Range
    Get all items with a key between low_key and high_key inclusive.
    If low_key = None, there is no lower bound.
    If high_key = None, there is no upper bound.
    If both low_key and high_key are None, this function returns all of the items.

    This function is arguably the b+trees greatest strength.
    O(keys within range) as opposed to O(total keys)
    """
    def get_range(self, low_key=None, high_key=None):
        result = []

        if low_key is not None:
            leaf = self._get_leaf(low_key)
            if leaf is None:
                return result
            index = self._find_key_index(leaf.keys, low_key)
        else:
            # If no low_key, start at the begining
            leaf = self._minimum_leaf()
            if leaf is None:
                return result
            index = 0
            
        if index >= len(leaf.keys):
            leaf = leaf.link
            index = 0

        while leaf:
            while index < len(leaf.keys):
                if high_key is not None and leaf.keys[index] > high_key:
                    break

                # result.append((leaf.keys[index], leaf.values[index]))
                # result.append((leaf.keys[index], leaf.values[index]) if self.return_keys else leaf.values[index])
                
                for value in leaf.values[index]:
                    result.append((leaf.keys[index], value) if self.return_keys else value)

                index += 1
            leaf = leaf.link
            index = 0

        return result      


    def _get_leaf(self, key) -> Node:
        node = self.root

        if len(node.keys) == 0:
            return None

        while not node.is_leaf:
            index = self._find_key_index(node.keys, key)

            if index == len(node.keys) or key < node.keys[index]:
                node = node.values[index]
            else:
                node = node.values[index + 1]

        # if self.unique_keys == False:
        #     while node.keys[0] == key and node.rev_link is not None and node.rev_link.keys[-1] == key:
        #         node = node.rev_link


        return node
    


    # Cannot do return self.get(key) is not None because the value itself could be None.
    def __contains__(self, key) -> bool:
        # TODO: make this compatable with non unique keys
        node = self._get_leaf(key)
        if node is None:
            return False

        index = self._find_key_index(node.keys, key)

        if index < len(node.keys) and key == node.keys[index]:
            return True
        
        return False
    
    
    def minimum(self):
        minimum_node = self._minimum_leaf()
        if minimum_node is None:
            return None
        
        return [(minimum_node.keys[0], value) for value in minimum_node.values[0]] if self.return_keys else minimum_node.values[0]
    
    def _minimum_leaf(self) -> Node:
        node = self.root
        if len(node.keys) == 0:
            return None
        
        while not node.is_leaf:
            node = node.values[0]

        return node
    
    def maximum(self):
        maximum_node = self._maximum_leaf()
        if maximum_node is None:
            return None
        
        return [(maximum_node.keys[-1], value) for value in maximum_node.values[-1]] if self.return_keys else maximum_node.values[-1]
    
    def _maximum_leaf(self) -> Node:
        node = self.root
        if len(node.keys) == 0:
            return None

        while not node.is_leaf:
            node = node.values[-1]

        return node
    
    def remove(self, key, value=None):  
        if self.unique_keys == False:
            assert(value is not None)

        leaf_node = self._get_leaf(key)
        if leaf_node is None:
            raise KeyError(key)
        
        index = self._find_key_index(leaf_node.keys, key)
        if index >= len(leaf_node.keys) or leaf_node.keys[index] != key:
            raise KeyError(key)
        
        # if self.unique_keys == False:
            # leaf_node, index = self._get_item_from_link(leaf_node, index, value)

        if self.unique_keys == False:
            try:
                leaf_node.values[index].remove(value)
            except ValueError:
                # Raise our own error
                print(f"{leaf_node.values[index]=}")
                print(f"{value=}")
                raise KeyError(key, value)
        else:
            leaf_node.values[index].pop()

        if len(leaf_node.values[index]) <= 0:
            leaf_node.keys.pop(index)
            leaf_node.values.pop(index)
        
        self.length -= 1

        node = leaf_node

        while (len(node.keys) < self.minimum_degree - 1) and (node.parent is not None):
            node_index_in_parent = node.parent.values.index(node)

            left_sibling = node.parent.values[node_index_in_parent - 1] if node_index_in_parent > 0 else None
            right_sibling = node.parent.values[node_index_in_parent + 1] if node_index_in_parent < len(node.parent.values) - 1 else None

            spare_item_in_left_sibling = left_sibling and len(left_sibling.keys) > self.minimum_degree - 1
            spare_item_in_right_sibling = right_sibling and len(right_sibling.keys) > self.minimum_degree - 1
            
            if spare_item_in_left_sibling:
                self._borrow_left(node, left_sibling, node_index_in_parent)
            elif spare_item_in_right_sibling:
                self._borrow_right(node, right_sibling, node_index_in_parent)
            elif left_sibling:
                self._merge_siblings(left_sibling, node, node_index_in_parent)
            elif right_sibling:
                self._merge_siblings(node, right_sibling, node_index_in_parent + 1)
            else:
                raise Exception
            
            node = node.parent

        node_is_root = node.parent is None
        if node_is_root and not node.is_leaf and len(node.values) == 1:
            self.root = node.values[0]
            self.root.parent = None

        if self.debug_mode:
            self.is_maintained()

    def _borrow_left(self, node, left_sibling, node_index_in_parent):
        borrowed_key = left_sibling.keys.pop()
        borrowed_value = left_sibling.values.pop()
        
        if node.is_leaf:
            node.keys.insert(0, borrowed_key)
            node.values.insert(0, borrowed_value)

            node.parent.keys[node_index_in_parent - 1] = borrowed_key

        else:
            parent_key = node.parent.keys.pop(node_index_in_parent - 1)
            borrowed_value.parent = node

            node.parent.keys.insert(node_index_in_parent - 1, borrowed_key)
            node.keys.insert(0, parent_key)
            node.values.insert(0, borrowed_value)

        if self.debug_mode:
            print(f"Borrowed from right sibling to make {node}")

    def _borrow_right(self, node, right_sibling, node_index_in_parent):
        borrowed_key = right_sibling.keys.pop(0)
        borrowed_value = right_sibling.values.pop(0)
        
        if node.is_leaf:
            node.keys.append(borrowed_key)
            node.values.append(borrowed_value)

            node.parent.keys[node_index_in_parent] = right_sibling.keys[0]

        else:
            parent_key = node.parent.keys.pop(node_index_in_parent)
            borrowed_value.parent = node

            node.parent.keys.insert(node_index_in_parent, borrowed_key)
            node.keys.append(parent_key)
            node.values.append(borrowed_value)

        if self.debug_mode:
            print(f"Borrowed from right sibling to make {node}")
    
    def _merge_siblings(self, left_sibling, right_sibling, right_sibling_index_in_parent):
        parent = left_sibling.parent

        right_sibling_is_internal = not right_sibling.is_leaf
        if right_sibling_is_internal:
            # Give right sibling an even amount of keys and values
            # right_sibling.keys.insert(0, right_sibling.values[0].keys[0])
            # right_sibling.keys.insert(0, parent.keys[0])
            demoted_key = parent.keys[right_sibling_index_in_parent - 1]
            right_sibling.keys.insert(0, demoted_key)

            # Update right sibling's item's parent
            for child in right_sibling.values:
                child.parent = left_sibling

        # Take all right sibling vlaues
        left_sibling.keys.extend(right_sibling.keys)
        left_sibling.values.extend(right_sibling.values)

        # Update link
        left_sibling.link = right_sibling.link
        if right_sibling.link is not None:
            assert(right_sibling.link.rev_link == right_sibling)
            right_sibling.link.rev_link = left_sibling

        # Remove reference to right sibling
        parent.keys.pop(right_sibling_index_in_parent - 1)
        parent.values.pop(right_sibling_index_in_parent)

        if self.debug_mode:
            print(f"Merged siblings into {left_sibling}")

    def update(self, old_key, new_key, value=None):
        if self.unique_keys == False:
            assert(value is not None)

        values = self.get(old_key)

        if len(values) == 0:
            raise KeyError(old_key)

        if self.unique_keys:
            if self.return_keys:
                value = values[0][1]
            else:
                value = values[0]

        else:
            found = False
            if self.return_keys:
                for v in values:
                    if v[1] == value:
                        found = True
                        break
            else:
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


    def __len__(self):
        return self.length

    """Iterator over leaf keys"""
    def keys(self):
        leaf = self._minimum_leaf()
        if leaf is None:
            return
        
        while leaf is not None:
            for key in leaf.keys:
                yield key
            leaf = leaf.link

    """Iterator over leaf values"""
    def values(self):
        leaf = self._minimum_leaf()
        if leaf is None:
            return
        
        while leaf is not None:
            for value in leaf.values:
                yield value
            leaf = leaf.link

    """Iterator over leaf key value pairs"""
    def items(self):
        leaf = self._minimum_leaf()
        if leaf is None:
            return
        
        while leaf is not None:
            for i in range(len(leaf.keys)):
                for value in leaf.values[i]:
                    yield (leaf.keys[i], value)
            leaf = leaf.link

    def items_rev(self):
        leaf = self._maximum_leaf()
        if leaf is None:
            return
        
        while leaf is not None:
            for key, values in zip(reversed(leaf.keys), reversed(leaf.values)):
                for value in values:
                    yield (key, value)

            leaf = leaf.rev_link

    def __eq__(self, other_tree):
        return self._compare_nodes(self.root, other_tree.root)

    def _compare_nodes(self, node1, node2):
        if node1.minimum_degree != node2.minimum_degree:
            return False

        if node1.is_leaf != node2.is_leaf:
            return False

        if node1.keys != node2.keys:
            return False

        # If both nodes are internal, compare their children
        if node1.is_leaf and node1.values != node2.values:
            return False

        if not node1.is_leaf:
            if len(node1.values) != len(node2.values):
                return False
            for child1, child2 in zip(node1.values, node2.values):
                if not self._compare_nodes(child1, child2):
                    return False
        


        return True
    
    def __repr__(self):
        if not self.root:
            return "Empty B+ Tree"
        
        result = ["### Depth 0 ###", "~~~"]
        queue = [(self.root, 0)]
        current_depth = 0

        while queue:
            current_node, depth = queue.pop(0)

            if depth > current_depth:
                result.append(f"~~~")
                result.append(f"### Depth {depth} ###")
                current_depth = depth

            result.append(str(current_node))


            if type(current_node) is Node and not current_node.is_leaf:
                queue.append((f"~~~", depth + 1))
                queue.extend((child, depth + 1) for child in current_node.values)

        return '\n'.join(result) 
    
    def __str__(self):
        return f"{list(self.items())}"

    def reset(self):
        self.root = Node(minimum_degree=Config.b_plus_tree_minimum_degree, is_leaf=True)
        self.length = 0
        self.height = 0  