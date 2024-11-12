import pickle
import threading
from typing import Any, Optional
import bsddb3 as bdb

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

    def lt(self, other: 'VectorClock') -> bool:
        """Retrun True if self.clock is less than other.clock."""

        for server_name, version in self.clock.items():
            if not (server_name in other.clock and version <= other.clock[server_name]):
                return False
            
        return True

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
    def __init__(self, value: Any, vector_clock: dict):
        self.value = value
        self.vector_clock = vector_clock
    
    # Make a merge of two two VersionedValue

    def __repr__(self):
        return f"VersionedValue(value={self.value}, vector_clock={self.vector_clock})"

class Storage:
    def __init__(self, db_path: str):
        self.db = bdb.hashopen(db_path, 'c')
        self.lock = threading.Lock()

    def encode(self, key: int) -> bytes:
        """Convert an integer key to a bytes representation."""
        return key.to_bytes(4, byteorder="big") if isinstance(key, int) else key

    def decode(self, key_bytes: bytes) -> int:
        """Convert bytes back to an integer"""
        return int.from_bytes(key_bytes, byteorder="big")

    def put(self, key: int, value: Any, vector_clock: VectorClock) -> None:
        """Stores a VersionedValue in DB, appending new versions."""

        b_key = self.encode(key)

        # Load existing versions 
        versions_dict = self.get_versions_dict(b_key) 

        with self.lock:
            # Update or add the new version to the dictionary
            versions_dict[tuple(vector_clock.to_dict().items())] = VersionedValue(value, vector_clock)

            # Store the updated versions dict
            self.db[b_key] = pickle.dumps(versions_dict)
            
    def get_version(self, key: int, vector_clock: VectorClock) -> Optional[VersionedValue]:
        """Retrieve specific version for a given key."""

        b_key = self.encode(key)
        tup = tuple(vector_clock.to_dict().items())

        with self.lock:
            # Retrieve all versions for the specified key
            if self.db.has_key(b_key):
                versions_dict = pickle.loads(self.db.get(b_key))
                if tup in versions_dict:
                    return versions_dict[tuple(vector_clock.to_dict().items())]
                else:
                    print(f"VersionError: {vector_clock.to_dict()} for key={key} not found in the database.")
            else:
                print(f"KeyError: {key} not found in storage.")

            return None

    def get(self, key: int) -> Optional[dict]:
        """Retrieve versions for a given key."""

        b_key = self.encode(key)

        with self.lock:
            # Retrieve all versions for the specified key
            if self.db.has_key(b_key):
                versions_dict = pickle.loads(self.db.get(b_key))
                return versions_dict
            else:
                print(f"KeyError: {key} not found in storage.")
                return None

    def get_versions_dict(self, key: int) -> dict:
        """Load existing versions dict or create new one."""

        b_key = self.encode(key)

        with self.lock:
            if self.db.has_key(b_key):
                return pickle.loads(self.db.get(b_key))
            else:
                return {}

    def exists(self, key: int) -> bool:
        """Check wether we have key or not."""

        b_key = self.encode(key)

        with self.lock:
            if self.db.has_key(b_key):
                return True
            else:
                return False

    def delete(self, key: int, vector_clock: VectorClock = None) -> None:
        """Delete whole key if no context given"""

        b_key = self.encode(key)
        tup = tuple(vector_clock.to_dict().items()) if vector_clock else None

        with self.lock:
            if b_key in self.db:
                if vector_clock is None:
                    del self.db[b_key]
                else:
                    versions_dict = pickle.loads(self.db.get(b_key))
                    if tup in versions_dict:
                        del versions_dict[tup]
                        self.db[b_key] = pickle.dumps(versions_dict)
                    else:
                        print(f"VersionError: {vector_clock.to_dict()} for key={key} not found in the database.")
            else:
                print(f"Key={key} not found in the database.")

    def close(self):
        """Close the database."""
        self.db.close()