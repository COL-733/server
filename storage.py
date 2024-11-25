import pickle
import threading
from typing import Any, Optional
import bsddb3
import os

class VectorClock:
    def __init__(self, clock: dict[str, int] = None):
        """
        Initialize the vector clock.
        Args: clock (dict[str, int])
        """
        self.clock = clock if clock else {}

    def add(self, server_name: str, version: int) -> None:
        """Add the server to clock."""
        self.clock[server_name] = version

    def __lt__(self, other: 'VectorClock') -> bool:
        """Retrun True if self.clock is less than other.clock."""

        for server_name, version in self.clock.items():
            if not (server_name in other.clock and version <= other.clock[server_name]):
                return False
            
        return True

    def __eq__(self, other: 'VectorClock') -> bool:
        """Retrun True if self.clock is equal other.clock."""

        for server_name, version in self.clock.items():
            if not (server_name in other.clock and version == other.clock[server_name]):
                return False
            
        return len(self.clock) == len(other.clock)
    
    def merge(self, other: 'VectorClock') -> None:
        """Merge this vector clock with another vector clock, taking the maximum version for each server."""
        for server_name, version in other.clock.items():
            self.clock[server_name] = max(self.clock.get(server_name, 0), version)

    def delete(self, server_name: str) -> None:
        """Delete the server to clock."""
        if server_name in self.clock:
            del self.clock[server_name]

    def to_dict(self) -> dict[str, int]:
        """Convert the vector clock to a dictionary format."""
        return self.clock

    def from_dict(self, data: dict[str, int]) -> None:
        """Load the vector clock from a dictionary."""
        self.clock = data

    def __str__(self) -> str:
        return str(self.clock)

class VersionedValue:
    def __init__(self, value: str, vector_clock: VectorClock):
        self.value: str = value
        self.vector_clock: VectorClock = vector_clock

    # Made VersionedValue hashable by implementing __hash__ and __eq__ methods
    def __hash__(self):
        return hash((self.value, tuple(self.vector_clock.clock.items())))

    def __eq__(self, other):
        return isinstance(other, VersionedValue) and self.value == other.value and self.vector_clock == other.vector_clock
    
    def serialize(self) -> list:
        return [self.value, self.vector_clock.to_dict()]
    
    def __repr__(self):
        return f"VersionedValue(value={self.value}, vector_clock={self.vector_clock})"

class KeyVersion:
    def __init__(self, db_path: str) -> None:

        if os.path.exists(db_path):
            # Load the existing database
            print(f"Loading existing database at {db_path}.")
            self.db = bsddb3.hashopen(db_path, 'w')  # Open in read/write mode
        else:
            # Create a new database
            print(f"Database not found at {db_path}. Creating a new one.")
            self.db = bsddb3.hashopen(db_path, 'c')  # Create if it doesn't exist

    def encode(self, key: str) -> bytes:
        """Convert an integer key to a bytes representation."""
        return bytes(key, 'utf-8') if isinstance(key, str) else key
    
    def add_key(self, key: str):
        """Add key to the key version storage if the key is new."""
        key_bytes = self.encode(key)

        if not self.db.has_key(key_bytes):
            self.db[key_bytes] = pickle.dumps(0)

    def get_version(self, key: str) -> int:
        """Return version of the key"""
        key_bytes = self.encode(key)

        return int(pickle.loads(self.db[key_bytes]))
    
    def inc_version(self, key: str):
        """Increse version of the key."""
        key_bytes = self.encode(key)

        ver = int(pickle.loads(self.db[key_bytes]))
        ver+=1

        self.db[key_bytes] = pickle.dumps(ver)
    
    def exists(self, key: str) -> bool:
        key_bytes = self.encode(key)

        return self.db.has_key(key_bytes)

class Storage:
    def __init__(self, db_path: str):

        if os.path.exists(db_path):
            # Load the existing database
            print(f"Loading existing database at {db_path}.")
            self.db = bsddb3.hashopen(db_path, 'w')  # Open in read/write mode
        else:
            # Create a new database
            print(f"Database not found at {db_path}. Creating a new one.")
            self.db = bsddb3.hashopen(db_path, 'c')  # Create if it doesn't exist
        self.lock = threading.Lock()

    def encode(self, key: str) -> bytes:
        """Convert an integer key to a bytes representation."""
        return bytes(key, 'utf-8') if isinstance(key, str) else key

    @staticmethod
    def compare_versioned_value(v1: str|VersionedValue, v2: VersionedValue) -> str:
        """
        "child" if v2 is child of v1
        "equal" if v2==v1 return "equal"
        "sibling" otherwise (although v1 can be child of v2)
        """
        if v1 == "root":
            return 'child'
        
        if v1.vector_clock < v2.vector_clock:
            if v1.vector_clock == v2.vector_clock:
                return "equal"
            else:
                return 'child'
        else:
            return "sibling"
    
    def _load_tree(self, key: bytes) -> dict[str| VersionedValue , set[VersionedValue]]:
        """Load the adjacency list for a given key from Berkeley DB"""
        if key in self.db:
            return pickle.loads(self.db[key])
        else:
            # Initialize an empty adjacency list with a root node
            return {"root": set(), "leaves": {"root"}}

    def add_version(self, key: str, value: Any ,context: VectorClock) -> None:
        """Add a new versioned value to the version tree for the given key."""
        key_bytes = self.encode(key)
        version_value = VersionedValue(value, context)

        with self.lock:
            tree = self._load_tree(key_bytes)

            # Determine the parents for the new version
            last_parent: str | VersionedValue  = "root"

            parents:  set[str | VersionedValue] = set()
            children: set[str | VersionedValue] = set()

            # Start from root's children
            visited: set[str | VersionedValue] = set()
            stack: list[str | VersionedValue] = list(tree.get("root", set()))

            while stack:
                node = stack.pop()

                if node in visited:
                    continue  # Skip nodes that have been fully processed
                visited.add(node)

                # Add the last parent when you leave the subtree of the last parent
                if not self.compare_versioned_value(last_parent, node)=='child':
                    parents.add(last_parent)

                # Finding the deepest parent or 
                if self.compare_versioned_value(node, version_value) == "child":
                    last_parent = node
                elif self.compare_versioned_value(version_value, node) == "child":
                    children.add(node)

                # Only push children of the current node if it hasn't already been explored
                stack.extend(tree.get(node, set()) - visited)

            # Adding last_parent in case of edge case
            parents.add(last_parent)
            
            # Cleaning of parents and children
            cleaned = set()
            for child in children:
                # child shouldn't be child of any other child
                if all(self.compare_versioned_value(other, child) != "child" for other in children - {child}):
                    cleaned.add(child)
            children = cleaned

            cleaned = set()
            for parent in parents:
                # parent shouldn't be parent of any other parent
                if all(self.compare_versioned_value(parent, other) != "child" for other in parents - {parent}):
                    cleaned.add(parent)
            parents = cleaned

            # Add the new version and adjust parent-child links
            for parent in parents:
                tree.setdefault(parent, set()).add(version_value)
                # Remove existing child links from parents to children
                for child in children:
                    tree[parent].discard(child)

            # Update leaves
            for parent in parents:
                tree["leaves"].discard(parent)  # Remove any non-leaves
            if not children:
                tree["leaves"].add(version_value)  # Only add as a leaf if it has no children
            else:
                tree[version_value] = children # Add the children to new node

            # Save the updated tree
            self.db[key_bytes] = pickle.dumps(tree)

    def get_leaf_nodes(self, key: str) -> Optional[set[VersionedValue]]:
        """Retrieve the leaf nodes for the given key."""
        key_bytes = self.encode(key)

        if self.db.has_key(key_bytes):
            tree = self._load_tree(key_bytes)
            return tree["leaves"]
        else:
            return None

    def get_version_tree(self, key: str) -> dict[str |VersionedValue, set[VersionedValue]]:
        """Retrieve the full version tree for the given key."""
        key_bytes = self.encode(key)
        return self._load_tree(key_bytes)

    def exists(self, key: str) -> bool:
        """Checks wether we've key or not"""
        key_bytes = self.encode(key)
        return self.db.has_key(key_bytes)

    def close(self):
        """Close the database"""
        self.db.close()