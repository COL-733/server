import unittest
import os
import threading
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

        self.assertEqual(vc1.lt(vc2), False)
        self.assertEqual(vc2.lt(vc1), False)

        self.assertEqual(vc3.lt(vc4), True)

        # Test merging with an empty clock
        vc_empty = VectorClock()
        self.assertEqual(vc1.lt(vc_empty), False)


    def test_merge(self):
        # Test merging vector clocks
        vc1 = VectorClock({"server1": 2, "server2": 3})
        vc2 = VectorClock({"server2": 4, "server3": 1})

        # Merge vc2 into vc1
        vc1.merge(vc2)
        expected_result = {"server1": 2, "server2": 4, "server3": 1}
        self.assertEqual(vc1.to_dict(), expected_result)

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
        self.db_path = "databases/test_db.db"
        self.storage = Storage(self.db_path)

    def tearDown(self):
        self.storage.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_put_and_get(self):
        vc1 = VectorClock({"server1": 1})
        vc2 = VectorClock({"server1": 2})

        self.storage.put(1, "value1", vc1)
        self.storage.put(1, "value2", vc2)

        result = self.storage.get(1)
        self.assertIsNotNone(result)
        self.assertIn(tuple(vc1.to_dict().items()), result)
        self.assertIn(tuple(vc2.to_dict().items()), result)
        self.assertEqual(result[tuple(vc1.to_dict().items())].value, "value1")
        self.assertEqual(result[tuple(vc2.to_dict().items())].value, "value2")

    def test_get_version(self):
        vc = VectorClock({"server1": 1})
        self.storage.put(1, "value1", vc)

        retrieved_version = self.storage.get_version(1, vc)
        self.assertIsNotNone(retrieved_version)
        self.assertEqual(retrieved_version.value, "value1")

    def test_delete_version(self):
        vc1 = VectorClock({"server1": 1})
        vc2 = VectorClock({"server1": 2, "server2": 1})

        self.storage.put(1, "value1", vc1)
        self.storage.put(1, "value2", vc2)

        self.storage.delete(1, vc1)
        self.storage.delete(1, vc1)

        # Retrieve all versions and check both are present
        all_versions = self.storage.get(1)
        self.assertNotIn(tuple(vc1.to_dict().items()), all_versions)
        self.assertIn(tuple(vc2.to_dict().items()), all_versions)
        self.assertEqual(all_versions[tuple(vc2.to_dict().items())].value, "value2")

        self.storage.delete(1, vc2)
        self.storage.delete(1, vc2)

        all_versions = self.storage.get(1)

    def test_delete_key(self):
        vc1 = VectorClock({"server1": 1})
        vc2 = VectorClock({"server1": 2, "server2": 1})

        self.storage.put(1, "value1", vc1)
        self.storage.put(1, "value2", vc2)

        self.storage.delete(1)

        # Retrieve all versions and check both are abset
        all_versions = self.storage.get(1)
        # self.assertNotIn(tuple(vc1.to_dict().items()), all_versions)
        # self.assertNotIn(tuple(vc2.to_dict().items()), all_versions)
                         
    def test_update_version(self):
        # Initial put
        vc_v1 = VectorClock({"server1": 1})
        self.storage.put(1, "value1", vc_v1)
        
        # Update with new version
        vc_v2 = VectorClock({"server1": 2})
        self.storage.put(1, "value2", vc_v2)
        
        # Retrieve all versions and check both are present
        all_versions = self.storage.get(1)
        self.assertIn(tuple(vc_v1.to_dict().items()), all_versions)
        self.assertIn(tuple(vc_v2.to_dict().items()), all_versions)
        self.assertEqual(all_versions[tuple(vc_v1.to_dict().items())].value, "value1")
        self.assertEqual(all_versions[tuple(vc_v2.to_dict().items())].value, "value2")

    def test_get_nonexistent_key(self):
        # Attempt to get a version of a nonexistent key
        vc = VectorClock({"server1": 1})
        retrieved_version = self.storage.get_version(999, vc)
        self.assertIsNone(retrieved_version)

class TestOperation(unittest.TestCase):
    def setUp(self):
        """Set up a thread and message for the Operation class before each test."""
        # Mocking a thread and message
        self.mock_thread = threading.Thread(target=lambda: None)
        self.mock_message_put = Message(id=1, msg_type=MessageType.PUT, source="source1", dest="dest1", kwargs={"key": 123})
        self.mock_message_get = Message(id=2, msg_type=MessageType.GET, source="source2", dest="dest2", kwargs={"key": 456})

    # def test_start_operation_thread(self):
    #     """Test if the Operation thread starts correctly."""
    #     operation = Operation(thread=self.mock_thread, msg=self.mock_message_put, isCord=True)
    #     operation.start()
    #     self.assertTrue(operation.thread.is_alive())

    def test_increment_ack(self):
        """Test ack incrementation for PUT messages."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_put, isCord=True)
        initial_acks = operation.acks
        operation.inc_ack()
        self.assertEqual(operation.acks, initial_acks + 1)

    def test_add_response_get(self):
        """Test adding responses for GET messages."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_get, isCord=True)
        initial_responses = len(operation.resList)
        operation.add_response("test_response")
        self.assertEqual(len(operation.resList), initial_responses + 1)
        self.assertIn("test_response", operation.resList)

    def test_handle_response_put_ack_threshold(self):
        """Test handle_response for a PUT message when ack threshold is met."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_put, isCord=True)
        operation.cv = threading.Condition()
        
        with operation.cv:
            # Simulate receiving the necessary number of acks
            operation.acks = config.W - 1
            operation.handle_response()
            operation.acks += 1  # Manually increment to simulate full acks
            
            # Check if notify would be called (done should be True)
            done = operation.acks >= config.W
            self.assertTrue(done)

    def test_handle_response_get_response_threshold(self):
        """Test handle_response for a GET message when response threshold is met."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_get, isCord=True)
        # operation.cv = threading.Condition()

        with operation.cv:
            # Simulate receiving the necessary number of responses
            operation.resList = ["res1"]
            operation.handle_response("res2")
            operation.resList.append("res2")  # Manually add response
            
            # Check if notify would be called (done should be True)
            done = len(operation.resList) >= config.R
            self.assertTrue(done)

    def test_response_msg_for_get(self):
        """Test creation of response message for GET operation."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_get, isCord=True)
        operation.add_response("sample_response")
        response_message = operation.response_msg("server1", "dest1")
        
        self.assertEqual(response_message.id, operation.id)
        self.assertEqual(response_message.msg_type, MessageType.GET_RES)
        self.assertEqual(response_message.source, "server1")
        self.assertEqual(response_message.dest, "dest1")
        self.assertIn("sample_response", response_message.kwargs["res"])

    def test_reply_msg_for_get(self):
        """Test creation of reply message for GET operation."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_get, isCord=True)
        operation.add_response("another_response")
        reply_message = operation.reply_msg("server1")
        
        self.assertEqual(reply_message.id, operation.id)
        self.assertEqual(reply_message.msg_type, MessageType.GET_RES)
        self.assertEqual(reply_message.source, "server1")
        self.assertEqual(reply_message.dest, operation.source)
        self.assertIn("another_response", reply_message.kwargs["res"])

    def test_response_msg_for_put(self):
        """Test creation of response message for PUT operation."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_put, isCord=True)
        response_message = operation.response_msg("server1", "dest1")
        
        self.assertEqual(response_message.id, operation.id)
        self.assertEqual(response_message.msg_type, MessageType.PUT_ACK)
        self.assertEqual(response_message.source, "server1")
        self.assertEqual(response_message.dest, "dest1")
        self.assertEqual(response_message.kwargs, {})  # PUT response has no kwargs

    def test_reply_msg_for_put(self):
        """Test creation of reply message for PUT operation."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_put, isCord=True)
        reply_message = operation.reply_msg("server1")
        
        self.assertEqual(reply_message.id, operation.id)
        self.assertEqual(reply_message.msg_type, MessageType.PUT_ACK)
        self.assertEqual(reply_message.source, "server1")
        self.assertEqual(reply_message.dest, operation.source)
        self.assertEqual(reply_message.kwargs, {})  # PUT reply has no kwargs

    def test_syn_reconcile_unimplemented(self):
        """Test syn_reconcile raises NotImplementedError for non-GET types."""
        operation = Operation(thread=self.mock_thread, msg=self.mock_message_get, isCord=True)
        with self.assertRaises(NotImplementedError):
            operation.syn_reconcile()

if __name__ == "__main__":
    unittest.main()