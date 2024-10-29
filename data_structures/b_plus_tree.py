from utilities.algorithms import binary_search, linear_search
from config import Config
from errors import *
from random import random
import unittest

class Node:
    def __init__(self, minimum_degree=16, is_leaf: bool=False, parent=None):
        assert(minimum_degree >= 2)
        self.minimum_degree = minimum_degree
        self.is_leaf: bool = is_leaf
        self.link = None    # Should be none unless a leaf.
        self.parent = parent  # Should be none only if a root.
        
        self.keys = []
        self.values = []

    """
    A maintained B+ Tree Node has multiple properties.
    This function checks if a node is maintained.
    Use for debuging.
    """
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

        if not self.is_leaf:
            for index in range(len(self.keys)):
                if self.values[index + 1].keys[0] < self.keys[index]:
                    print(f"{self.values[index + 1].keys[0]}, {self.keys[index]}")
                    # Can you tell i'm have fun with these error names yet?
                    raise LikeFatherLikeSonError(self, self.values[index + 1])



        return True
    
    def __str__(self):
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


        

class TestNode(unittest.TestCase):

    def test_initialization(self):
        node = Node()
        self.assertEqual(node.minimum_degree, 16)
        self.assertFalse(node.is_leaf)
        self.assertEqual(node.keys, [])
        self.assertEqual(node.values, [])

        node = Node(minimum_degree=4, is_leaf=True)
        self.assertEqual(node.minimum_degree, 4)
        self.assertTrue(node.is_leaf)

    def test_is_maintained_leaf_node(self):
        node = Node(minimum_degree=2, is_leaf=True)
        node.keys = [1, 2, 3]
        node.values = [10, 20, 30]
        node.parent = Node()

        self.assertTrue(node.is_maintained(is_root=False))

    def test_is_maintained_internal_node(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1]
        node.parent = Node()

        child_node1 = Node(minimum_degree=2, is_leaf=True, parent=node)
        child_node2 = Node(minimum_degree=2, is_leaf=True, parent=node)

        child_node1.keys = [0]
        child_node2.keys = [1]

        node.values = [child_node1, child_node2]

        self.assertTrue(node.is_maintained(is_root=False))

    def test_is_maintained_invalid_leaf_node(self):
        node = Node(minimum_degree=2, is_leaf=True)
        node.keys = [1, 2]
        node.values = [10]  # len(values) != len(keys)

        with self.assertRaises(LeafNodeValueCountError):
            node.is_maintained(is_root=False)

    def test_is_maintained_invalid_internal_node(self):
        child_node = Node(minimum_degree=2, is_leaf=True)
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = []
        node.values = [child_node]

        with self.assertRaises(NonRootNodeKeyCountError):
            node.is_maintained(is_root=False)

    def test_keys_order(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1, 3, 2]  # This should be an invalid case
        node.values = [Node()] * 4

        with self.assertRaises(NonDecreasingOrderError):
            node.is_maintained(is_root=False)

    def test_keys_within_limits(self):
        node = Node(minimum_degree=2, is_leaf=False)
        node.keys = [1, 2, 3]
        node.values = [Node(parent=node) for _ in range(4)]
        node.parent = Node()
        for i in range(4):
            node.values[i].keys = [i]

        self.assertTrue(node.is_maintained(is_root=False))

        node.keys = list(range(1, 10))  # 9 keys, invalid if minimum_degree is 5
        with self.assertRaises(MaxKeysExceededError):
            node.is_maintained(is_root=False)

        






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
            debug_mode: bool=False, 
            search_algorithm_threshold=Config.b_plus_tree_search_algorithm_threshold
        ):
        self.height = 0
        self.length = 0
        self.minimum_degree = minimum_degree
        self.unique_keys = unique_keys
        self.root = Node(minimum_degree, is_leaf=True)

        self.search_algorithm_threshold = search_algorithm_threshold # When do we binary search keys and when do we linear scan keys?
        
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
            self.root.values.append(value)
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
        
        node.keys.insert(index, key)
        node.values.insert(index, value)
        self.length += 1

        if len(node.keys) == 2 * self.minimum_degree:
            self._split_leaf_node(node)

        if self.debug_mode:
            self.is_maintained()


    """
    Bulk Insert Method
    If you already many items, use this instead of insert. It's way faster.
    items: a list of key-value tuples
    """
    def bulk_insert(self, items):
        # Doesn't work for most sized inputs... yet.
        # 5 fold efficiency gains for whoever finishes this.
        raise NotImplementedError  

        # TODO: make this its own actual error
        if len(self.root.keys):
            print("build insert needs to be on an empty tree")
            raise KeyError("uhh tree too big")

        # Sort items by key
        items_sorted = sorted(items, key=lambda item: item[0])
        leaf_nodes = []
        current_leaf = []

        for item in items_sorted:
            current_leaf.append(item)
            if len(current_leaf) == self.minimum_degree * 2 - 1:
                leaf_nodes.append(current_leaf)
                current_leaf = []

        if current_leaf:
            leaf_nodes.append(current_leaf)

        self._build_tree(leaf_nodes)
        # print(self.is_maintained())

    def _build_tree(self, leaf_nodes):
        if not leaf_nodes:
            return None
        
        leaf_parent = Node(self.minimum_degree, is_leaf=False)
        for i, items in enumerate(leaf_nodes):
            new_leaf = Node(self.minimum_degree, is_leaf=True)
            new_leaf.parent = leaf_parent
            new_leaf.keys = [item[0] for item in items]
            new_leaf.values = [item[1] for item in items]
            leaf_parent.values.append(new_leaf)
            if i != 0:
                leaf_parent.keys.append(new_leaf.keys[0])



        for i in range(len(leaf_parent.values) - 1):
            leaf_parent.values[i].link  = leaf_parent.values[i + 1]

        self.root = leaf_parent

        if len(leaf_parent.keys) >= self.minimum_degree:
            self._split_internal_node(leaf_parent)


        

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
        If self.unique_keys, returns a single value.
        If not self.unique_keys, returns a list of values. Values in no particular order.
        """
        if not self.unique_keys:
            return [value for key, value in self.get_range(key, key)]

        node = self._get_leaf(key)
        if node is None:
            return node

        index = self._find_key_index(node.keys, key)

        if index < len(node.keys) and key == node.keys[index]:
            return [node.values[index]]
        
        return None
    
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

        if low_key:
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

                result.append((leaf.keys[index], leaf.values[index]))
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

        return node
    


    # Cannot do return self.get(key) is not None because the value itself could be None.
    def __contains__(self, key) -> bool:
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
        
        return (minimum_node.keys[0], minimum_node.values[0])
    
    def _minimum_leaf(self) -> Node:
        node = self.root
        if len(node.keys) == 0:
            return None
        
        while not node.is_leaf:
            node = node.values[0]

        return node
    
    def maximum(self):
        minimum_node = self._maximum_leaf()
        if minimum_node is None:
            return None
        
        return (minimum_node.keys[-1], minimum_node.values[-1])

    def _maximum_leaf(self) -> Node:
        node = self.root
        if len(node.keys) == 0:
            return None

        while not node.is_leaf:
            node = node.values[-1]

        return node

    def remove(self, key):
        if not self.unique_keys:
            # remove(key, value). Key is not unique, but value is in our use case.
            # key helps us find where value is and value helps us confirm we are at the right spot.
            raise NotImplementedError

        # Find the leaf node that contains the key
        leaf_node = self._get_leaf(key)
        if leaf_node is None:
            raise KeyError(key)

        #  Find the index of the key in the leaf node
        index = self._find_key_index(leaf_node.keys, key)
        if index >= len(leaf_node.keys) or leaf_node.keys[index] != key:
            raise KeyError(key)

        # Remove the key and corresponding value
        leaf_node.keys.pop(index)
        leaf_node.values.pop(index)
        self.length -= 1

        # Handle underflow
        if len(leaf_node.keys) < self.minimum_degree - 1:
            self._handle_underflow(leaf_node)

        if self.debug_mode:
            assert self.is_maintained()

    def _handle_underflow(self, node):
        """
        pseudo code:
        1  IF the node we removed from is the root DO 
        2      return
        3  END IF
        4  find the path from the node parent to the node
        5  IF there is a left libling with an item to spare DO
        6      borrow from the left sibling, return
        7  END IF
        8  IF there is a right sibling with an item to spare DO
        9      borrow from the right sibling, return
        10 END IF
        11 IF there is a left sibling DO
        12     merge with the left sibling
        13     IF parent underflows DO
        14         resursively call this function on the parent
        15     END IF
        16 ELSE DO     # Garanteed to be a right sibling
        17     merge with the right sibling
        18     IF parent underflows DO
        19         resursively call this function on the parent
        20 END IF-ELSE

        TLDR
        Attempt to borrow a sibling
        Otherwise merge with a sibling and recursion

        Prioritize the left sibling
        """
        parent = node.parent

        node_is_root = parent is None
        if node_is_root:
            root_has_no_keys = len(node.keys) == 0
            if root_has_no_keys:
                root_has_a_value = len(node.values) != 1
                if root_has_a_value: 
                    print(f"LEN NODE VALUES : {len(node.values)}")
                    self.root = node.values[0]
                    self.root.parent = None
                else:
                    self.root = Node(self.minimum_degree, is_leaf=True)
                print("Height is being lowered...")
                self.height -= 1
            print("handling root")
            return

        # Find the index of the node in the parent's values
        # Raises error if node isn't in index. Won't happen in a properly maintained tree.
        index = parent.values.index(node)

        left_sibling = parent.values[index - 1] if index > 0 else None
        right_sibling = parent.values[index + 1] if index < len(parent.values) - 1 else None
        assert (left_sibling or right_sibling)

        if left_sibling:
            left_sibling_has_spare_item = len(left_sibling.keys) > self.minimum_degree - 1
            if left_sibling_has_spare_item:
                # left_sibling(k11, k12, k13) node(k21)
                # left_sibling(k11, k21) node(k13, k21)

                if self.debug_mode:
                    print(f"{node} is borrowing from its left sibling {left_sibling}")
    
                # Borrow last item in left sibling
                borrowed_key = left_sibling.keys.pop()
                borrowed_value = left_sibling.values.pop()

                # Borrowed item becomes first in node
                node.keys.insert(0, borrowed_key)
                node.values.insert(0, borrowed_value)

                # Update borrowed items parent
                if not node.is_leaf:
                    borrowed_value.parent = node
                
                # Update parents keys. [index - 1] cuz that's how keys and values are related to eachother
                parent.keys[index - 1] = borrowed_key

                return

        if right_sibling:
            right_sibling_has_spare_item = len(right_sibling.keys) > self.minimum_degree - 1
            if right_sibling_has_spare_item:
                # node(k11) right_sibling(k21, k22, k23)
                # node(k11, k21) right_sibling(k22, k23)

                if self.debug_mode:
                    print(f"{node} is borrowing from its right sibling {right_sibling}")

                # Borrow first item in right sibling
                borrowed_key = right_sibling.keys.pop(0)
                borrowed_value = right_sibling.values.pop(0)

                # Borrowed item becomes last in node
                node.keys.append(borrowed_key)
                node.values.append(borrowed_value)

                # Update borrowed items parent
                if not node.is_leaf:
                    borrowed_value.parent = node

                # Update parents keys. It's the key associated with the right sibling
                parent.keys[index] = right_sibling.keys[0]
                return

        # If we can't borrow, merge
        if left_sibling:
            right_sibling = node
            index -= 1
        else:
            left_sibling = node


        if self.debug_mode:
            print(f"left {left_sibling} is merging with right {right_sibling}")

        right_sibling_is_internal = not right_sibling.is_leaf
        if right_sibling_is_internal:
            # Give right sibling an even amount of keys and values
            right_sibling.keys.insert(0, right_sibling.values[0].keys[0])

            # Update right sibling's item's parent
            for child in right_sibling.values:
                child.parent = left_sibling

        # Take all right sibling vlaues
        left_sibling.keys.extend(right_sibling.keys)
        left_sibling.values.extend(right_sibling.values)

        # Update link
        left_sibling.link = right_sibling.link

        # Remove reference to right sibling
        parent.keys.pop(index)
        parent.values.pop(index + 1)

        if self.debug_mode:
            print(f"Merged node {node}")

        parent_is_underflowed = len(parent.keys) < self.minimum_degree - 1
        if parent_is_underflowed:
            self._handle_underflow(parent)

        


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
                yield (leaf.keys[i], leaf.values[i])
            leaf = leaf.link

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
        if not node1.is_leaf:
            if len(node1.values) != len(node2.values):
                return False
            for child1, child2 in zip(node1.values, node2.values):
                if not self._compare_nodes(child1, child2):
                    return False

        return True
    
    def __str__(self):
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

class TestBPlusTree(unittest.TestCase):
    def setUp(self):
        self.tree = BPlusTree(minimum_degree=2, unique_keys=False)

    # I used https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html to help me design this. -Kai
    # Its what a tree should look like after inserting 1 through 10 in order.
    def make_generic_tree(self):
        # Root Node
        tree = BPlusTree(2)

        tree.root.keys = [7]
        tree.root.is_leaf = False  # On by default. Manually turn off.
        
        # Internal Nodes
        internal1 = Node(2, False)
        internal2 = Node(2, False)
        internal1.parent = tree.root
        internal2.parent = tree.root
        tree.root.values = [internal1, internal2]
        
        # Leaf Nodes
        leaf11 = Node(2, True)
        leaf12 = Node(2, True)
        leaf13 = Node(2, True)

        leaf21 = Node(2, True)
        leaf22 = Node(2, True)

        internal1.keys = [3, 5]
        internal1.values = [leaf11, leaf12, leaf13]
        leaf11.parent = internal1
        leaf12.parent = internal1
        leaf13.parent = internal1

        internal2.keys = [9]
        internal2.values = [leaf21, leaf22]
        leaf21.parent = internal2
        leaf22.parent = internal2
        

        # Leaf Node values
        leaf11.keys = [1, 2]
        leaf11.values = [1, 2]
        leaf11.link = leaf12

        leaf12.keys = [3, 4]
        leaf12.values = [3, 4]
        leaf12.link = leaf13

        leaf13.keys = [5, 6]
        leaf13.values = [5, 6]
        leaf13.link = leaf21

        leaf21.keys = [7, 8]
        leaf21.values = [7, 8]
        leaf21.link = leaf22

        leaf22.keys = [9, 10]
        leaf22.values = [9, 10]
        leaf22.link = None

        tree.length = 10
        tree.height = 2

        return tree
    
    def test_generic_tree_is_maintained(self):
        tree = self.make_generic_tree()
        self.assertTrue(tree.is_maintained())

    def test_eq(self):
        tree1 = self.make_generic_tree()
        tree2 = self.make_generic_tree()
        self.assertEqual(tree1, tree2)

    def test_not_eq(self):
        tree1 = self.make_generic_tree()
        tree2 = self.make_generic_tree()
        tree2._minimum_leaf().keys[0] = -10
        self.assertTrue(tree2.is_maintained())
        self.assertNotEqual(tree1, tree2)

    def test_generated_tree_is_maintained_and_equals_generic_tree(self):
        tree = self.tree
        for i in range(1, 11):
            tree.insert(i, i)

        for i in range(1, 11):
            self.assertEqual(tree.get(i), [i])

        tree2 = self.make_generic_tree()

        self.assertTrue(tree.is_maintained())
        self.assertEqual(tree, tree2)

    def test_insert_no_splits(self):
        tree = self.tree
        for i in range(1, 4):
            tree.insert(i, i)

        self.assertTrue(tree.is_maintained())

        # The root node can hold all three so it shouldn't split
        self.assertEqual(tree.root.keys, [1, 2, 3])

    def test_insert_one_leaf_split(self):
        tree = self.tree
        for i in range(1, 5):
            tree.insert(i, i)

        tree.is_maintained()
        self.assertEqual(tree.root.keys, [3])
        self.assertEqual(tree.root.values[0].keys, [1, 2])
        self.assertEqual(tree.root.values[1].keys, [3, 4])

    def test_insert_two_leaf_split(self):
        tree = self.tree
        for i in range(6):
            tree.insert(i, i)

        tree.is_maintained()
        self.assertEqual(tree.root.keys, [2, 4])
        self.assertEqual(tree.root.values[0].keys, [0, 1])
        self.assertEqual(tree.root.values[1].keys, [2, 3])
        self.assertEqual(tree.root.values[2].keys, [4, 5]) 

    def test_insert_duplicate_keys_1(self):
        tree = BPlusTree(minimum_degree=2, unique_keys=False)
        for i in range(10):
            tree.insert(i, i)
        
        for i in range(10):
            tree.insert(i, i)

    def test_insert_duplicate_keys_2(self):
        tree = BPlusTree(minimum_degree=2, unique_keys=True)
        for i in range(10):
            tree.insert(i, i*i)

        with self.assertRaises(NonUniqueKeyError):
            tree.insert(0, 0)


    def test_valid_leaf_link(self):
        tree = self.tree
        for i in range(101):
            message = ""
            if i % 3 == 0:
                message += "fizz"
            if i % 5 == 0:
                message += "buzz"
            tree.insert(i, message)

        leaf = tree._minimum_leaf() 

        while leaf.link is not None:
            leaf = leaf.link

        # The last value in the link should be the maximum value.
        self.assertEqual((leaf.keys[-1], leaf.values[-1]), tree.maximum())

        # The items iterator should contain as many items as are in the tree.
        self.assertEqual(len(list(tree.items())), len(tree))

    def test_min_and_max_item(self):
        from random import shuffle
        keys = [i for i in range(1_000)]
        shuffle(keys)

        for key in keys:
            self.tree.insert(key, key)

        self.assertEqual(self.tree.minimum(), (0, 0))
        self.assertEqual(self.tree.maximum(), (999, 999))

        
    def test_get_range_1(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(10, 15.5), [(10, None), (11, None), (12, None), (13, None), (14, None), (15, None)])

    def test_get_range_2(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(None, 4), [(0, None), (1, None), (2, None), (3, None), (4, None)])

    def test_get_range_3(self):
        tree = self.tree
        for i in range(100):
            tree.insert(i, None)

        self.assertEqual(tree.get_range(97, None), [(97, None), (98, None), (99, None)])

    def test_get_duplicates(self):
        from random import random
        tree = self.tree
        tree.unique_keys = False
        for i in range(50):
            if i % 10 == 0:
                tree.insert(0, i)
            else:
                tree.insert(random(), i)
        self.assertEqual(sorted(tree.get(0)), [0, 10, 20, 30, 40])

    def test_in_operator(self):
        tree = self.tree
        for i in range(1000):
            if i == 42:
                continue

            tree.insert(i, i ^ 10)
        
        self.assertFalse(42 in tree)
        self.assertTrue(10 in tree)

    def test_many_trees(self):
        for size in range(100):
            self._test_many_trees(size * 3)

    def _test_many_trees(self, size):
        tree = BPlusTree(minimum_degree=2)
        for i in range(size):
            tree.insert(random(), i)
        tree.is_maintained()

    def test_parent_pointer(self):
        tree = self.tree
        for i in range(50):
            tree.insert(i, i*i)

        node = tree.root
        while not node.is_leaf:
            self.assertTrue(node.values[-1].parent == node)
            node = node.values[-1]

    def test_remove_once(self):
        tree = self.tree
        tree.unique_keys = True
        for i in range(50):
            tree.insert(i, i*i)

        tree.remove(42)
        
        self.assertTrue(tree.is_maintained())
        self.assertFalse(tree.get(42))
        self.assertTrue(len(tree) == 49)

    def test_fill_and_empty_from_left(self):
        tree = self.tree
        tree.unique_keys = True
        for i in range(100):
            tree.insert(i, i*i)

        for i in range(100):
            tree.remove(i)

        self.assertEqual(len(tree), 0)

    def test_fill_and_empty_from_right(self):
        tree = self.tree
        tree.unique_keys = True
        tree.debug_mode = True
        try:
            for i in range(28):
                tree.insert(i, i*i)

            print(tree)

            for i in range(27, -1, -1):
                if i == 14:
                    print(tree)
                tree.remove(i)
        except Exception as e:
            print("TEST FILL AND EMPTY FROM RIGHT FAILED. PRINTING TREE STATE")
            print(tree)
            raise e

        self.assertEqual(len(tree), 0)


    def test_random_insert_and_remove(self):
        tree = self.tree
        tree.unique_keys = True
        items = [[random(), random()] for _ in range(64)]

        for i in range(32):
            tree.insert(items[i][0], items[i][1])

        for i in range(16):
            tree.remove(items[i][0])

        for i in range(32, 64):
            tree.insert(items[i][0], items[i][1])

        for i in range(32, 48):
            tree.remove(items[i][0])

        self.assertTrue(tree.is_maintained())

    def test_erratic_insert_and_remove(self):
        from random import shuffle
        tree = self.tree
        tree.unique_keys = True
        tree.debug_mode = True

        # Make a list of insert and remove operations in a random order, with insert before remove 
        items = []
        for i in range(100):
            key = i
            value = None
            items.append([key, value])
            items.append([key, value])

        shuffle(items)

        item_set = set()
        for i, item in enumerate(items):
            item[1] = i
            if item[0] not in item_set:
                item.append("Insert")
                item_set.add(item[0])
            else:
                item.append("Remove")
        try:
            for item in items:
                # print(item)
                if item[2] == "Insert":
                    tree.insert(item[0], item[1])
                else:
                    tree.remove(item[0])
        except Exception as e:
            print("TEST FAILED. PRINTING TREE STATE")
            # print(tree)
            raise e

        self.assertTrue(tree.is_maintained())


    def test_large_tree(self):
        tree = BPlusTree(minimum_degree=Config.b_plus_tree_minimum_degree)
        for i in range(50_000):
            tree.insert(random(), i)

        self.assertTrue(tree.is_maintained())