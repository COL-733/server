import unittest
import os
import threading
import random
from storage import VectorClock, VersionedValue, Storage
from operation import Operation
from message import Message, MessageType
from config import config

class TestVectorClock(unittest.TestCase):
    def test_initialization(self):
        # Test empty initialization
        vc1 = VectorClock()
        self.assertEqual(vc1.to_dict(), {})

        # Test initialization with a clock
        initial_clock = {"server1": 1, "server2": 2}
        vc2 = VectorClock(initial_clock)
        self.assertEqual(vc2.to_dict(), initial_clock)

    def test_add(self):
        # Test adding new server entries to the vector clock
        vc = VectorClock()
        vc.add("server1", 1)
        self.assertEqual(vc.to_dict(), {"server1": 1})

        # Test updating an existing server entry
        vc.add("server1", 3)
        self.assertEqual(vc.to_dict(), {"server1": 3})

    def test_lt(self):
        # Test merging vector clocks
        vc1 = VectorClock({"server1": 2, "server2": 3})
        vc2 = VectorClock({"server2": 4, "server3": 1})

        vc3 = VectorClock({"server1": 2, "server2": 3})
        vc4 = VectorClock({"server1": 4, "server2": 9})

        self.assertEqual(vc1 < vc2, False)
        self.assertEqual(vc2 < vc1, False)

        self.assertEqual(vc3 < vc4, True)

        # Test merging with an empty clock
        vc_empty = VectorClock()
        self.assertEqual(vc1 < vc_empty, False)


    def test_merge(self):
        # Test merging vector clocks
        vc1 = VectorClock({"server1": 2, "server2": 3})
        vc2 = VectorClock({"server2": 4, "server3": 1})

        # Merge vc2 into vc1
        vc1.merge(vc2)
        vc2.merge(vc1)
        expected_result = {"server1": 2, "server2": 4, "server3": 1}
        self.assertEqual(vc1.to_dict(), expected_result)
        self.assertEqual(vc2.to_dict(), expected_result)

        # Test merging with an empty clock
        vc_empty = VectorClock()
        vc1.merge(vc_empty)
        self.assertEqual(vc1.to_dict(), expected_result)

    def test_delete(self):
        # Test deleting a server entry from the vector clock
        vc = VectorClock({"server1": 1, "server2": 2})
        vc.delete("server1")
        vc.delete("server1")
        self.assertEqual(vc.to_dict(), {"server2": 2})

        # Test deleting a non-existent server (should have no effect)
        vc.delete("server3")
        self.assertEqual(vc.to_dict(), {"server2": 2})

    def test_to_dict_and_from_dict(self):
        # Test to_dict and from_dict methods
        initial_data = {"server1": 1, "server2": 2}
        vc = VectorClock()
        vc.from_dict(initial_data)
        self.assertEqual(vc.to_dict(), initial_data)

    def test_string_representation(self):
        # Test the string representation of the vector clock
        vc = VectorClock({"server1": 1, "server2": 2})
        self.assertEqual(str(vc), "{'server1': 1, 'server2': 2}")

class TestStorage(unittest.TestCase):
    def setUp(self):
        # self.db = Storage("test_version_tree.db")
        self.db = Storage("test_version_tree.db")

    @staticmethod
    def print_tree(tree: dict[str |VersionedValue, set[VersionedValue]], str1: str):
        print("\n")
        print(str1)
        for key, versions in tree.items():
            temp = key if isinstance(key,str) else key.value
            if key == "root": temp+="  "
            print(temp, end=" -> ")
            for version in versions:
                print(version.value, end=" ")
            print()

    def tearDown(self):
        self.db.close()
        self.db.close()
        if os.path.exists("test_version_tree.db"):
            os.remove("test_version_tree.db")

    def test_add_version(self):
        context1 = VectorClock({"A": 2})
        context2 = VectorClock({"A": 2, "B": 1})
        context3 = VectorClock({"A": 2, "C": 1 })

        # Reconciled context
        context4 = VectorClock({"A": 2, "B": 2, "C": 1})

        # Restructuring context
        context5 = VectorClock({"A": 2, "B": 2})
        context6 = VectorClock({"B": 1})
        context7 = VectorClock({"C": 1})
        context8 = VectorClock({"A": 1})
        
        version_value1 = VersionedValue("value1", context1)
        version_value2 = VersionedValue("value2", context2)
        version_value3 = VersionedValue("value3", context3)

        # Reconciled version
        version_value4 = VersionedValue("value4", context4)

        # Testing restructuring of tree
        version_value5 = VersionedValue("value5", context5)
        version_value6 = VersionedValue("value6", context6)
        version_value7 = VersionedValue("value7", context7)
        version_value8 = VersionedValue("value8", context8)
        
        # Adding versions to the version tree for key=1
        # Checking duplication
        self.db.add_version(1, "value1", context1)
        self.db.add_version(1, "value1", context1)
        self.db.add_version(1, "value2", context2)
        self.db.add_version(1, "value3", context3)
        self.db.add_version(1, "value3", context3)

        # Retrieve the full version tree and check structure
        tree = self.db.get_version_tree(1)
        self.assertIn(version_value1, tree["root"])
        self.assertIn(version_value2, tree[version_value1])
        self.assertIn(version_value3, tree[version_value1])

        self.assertIn(version_value2, tree["leaves"])
        self.assertIn(version_value3, tree["leaves"])
        print("Basic Test Passed:          ",tree["leaves"])

        # Added reconciled version
        self.db.add_version(1, "value4", context4)
        tree = self.db.get_version_tree(1)

        self.assertNotIn(version_value2, tree["leaves"])
        self.assertNotIn(version_value3, tree["leaves"])
        self.assertIn(version_value4, tree["leaves"])
        print("Reconcilation Test Passed:  ",tree["leaves"])

        # Testing restructuring-1
        self.db.add_version(1, "value5", context5)
        tree = self.db.get_version_tree(1)
        
        self.assertIn(version_value5, tree[version_value2])
        self.assertIn(version_value4, tree[version_value5])
        self.assertIn(version_value4, tree[version_value3])
        # self.assertNotIn(version_value4, tree[version_value2])
        self.assertIn(version_value4, tree["leaves"])
        print("Restructering Test-1 Passed:", tree["leaves"])

        # Testing restructuring-2
        self.db.add_version(1, "value6", context6)
        self.db.add_version(1, "value7", context7)
        tree = self.db.get_version_tree(1)
        
        self.assertIn(version_value6, tree["root"])
        self.assertIn(version_value7, tree["root"])

        self.assertIn(version_value2, tree[version_value6])
        self.assertIn(version_value2, tree[version_value1])
        self.assertIn(version_value3, tree[version_value7])
        self.assertIn(version_value3, tree[version_value1])

        self.assertIn(version_value4, tree["leaves"])
        print("Restructering Test-2 Passed:", tree["leaves"])

        # Testing restructuring-3
        self.db.add_version(1, "value8", context8)
        tree = self.db.get_version_tree(1)
        
        self.assertIn(version_value8, tree["root"])
        self.assertIn(version_value1, tree[version_value8])
        # self.assertNotIn(version_value1, tree["root"])
        self.assertIn(version_value4, tree["leaves"])
        print("Restructering Test-3 Passed:", tree["leaves"])
        self.print_tree(tree,"real")
        
    @staticmethod
    def compare_tree(t1: dict[str |VersionedValue, set[VersionedValue]],\
                      t2: dict[str |VersionedValue, set[VersionedValue]]):
        ans = True
        for key, value in t2.items():
            if key in t1:
                if not value==t1[key]:
                    ans = False
                    print(f"Value not same for {key.value}: ", value, " != ", t1[key])
            else:
                ans = False
                print("Key not found: ", key)

        return ans

    def test_restructuring(self):

        context1 = VectorClock({"A": 2})
        context2 = VectorClock({"A": 2, "B": 1})
        context3 = VectorClock({"A": 2, "C": 1 })
        context4 = VectorClock({"A": 2, "B": 2, "C": 1})
        context5 = VectorClock({"A": 2, "B": 2})
        context6 = VectorClock({"B": 1})
        context7 = VectorClock({"C": 1})
        context8 = VectorClock({"A": 1})
        
        version_value1 = VersionedValue("value1", context1)
        version_value2 = VersionedValue("value2", context2)
        version_value3 = VersionedValue("value3", context3)
        version_value4 = VersionedValue("value4", context4)
        version_value5 = VersionedValue("value5", context5)
        version_value6 = VersionedValue("value6", context6)
        version_value7 = VersionedValue("value7", context7)
        version_value8 = VersionedValue("value8", context8)

        answer = {
            'root': {version_value8, version_value6, version_value7}, 
            'leaves': {version_value4}, 
            version_value1: {version_value3, version_value2}, 
            version_value2: {version_value5}, 
            version_value3: {version_value4}, 
            version_value5: {version_value4}, 
            version_value6: {version_value2}, 
            version_value7: {version_value3}, 
            version_value8: {version_value1}
        }

        version_entries = [
            (1, context1, "value1"),
            (1, context2, "value2"),
            (1, context3, "value3"),
            (1, context4, "value4"),
            (1, context5, "value5"),
            (1, context6, "value6"),
            (1, context7, "value7"),
            (1, context8, "value8"),
        ]

        # Shuffle the version entries list to randomize the order
        random.shuffle(version_entries)
        
        # Add each version entry to the database in the randomized order
        print("order")
        for key, context, value in version_entries:
            print(value)
            self.db.add_version(key, value, context)

        tree1 = self.db.get_version_tree(1)
        self.print_tree(tree1,"result")

        self.assertDictEqual(tree1,answer)
        if self.compare_tree(tree1,answer):
            print("MATHCED")
        else:
            print("UNMATHCED")

    def test_get_leaf_nodes(self):
        context4 = VectorClock({"A": 4})
        version_value4 = VersionedValue("value4", context4)
        
        # Adding a new leaf version to the tree
        self.db.add_version(1, "value4", context4)

        # Retrieve leaf nodes and verify
        leaves = self.db.get_leaf_nodes(1)
        self.assertIn(version_value4, leaves)

    def test_compare_versioned_value(self):
        # Test comparison of vector clocks
        vc1 = VectorClock({"A": 1})
        vc2 = VectorClock({"A": 2})
        vv1 = VersionedValue("value1", vc1)
        vv2 = VersionedValue("value2", vc2)
        
        self.assertEqual(Storage.compare_versioned_value(vv1, vv2), "child")
        self.assertEqual(Storage.compare_versioned_value(vv2, vv1), "sibling")
        self.assertEqual(Storage.compare_versioned_value(vv1, vv1), "equal")

    def test_get_version_tree_empty_key(self):
        # Test retrieving a tree for a non-existent key
        tree = self.db.get_version_tree(99)
        self.assertEqual(tree, {"root": set(), "leaves": {"root"}})

if __name__ == "__main__":
    unittest.main()