import sys
import socket
import time
from multiprocessing import Process
from threading import Thread, Condition
import logging
import queue
from dataclasses import dataclass
from message import Message, MessageType
from storage import Storage, VersionedValue, VectorClock
import log
import json

class Client:
    def __init__(self, server_host: str, server_port: int, client_name : str):
        self.server_host = server_host
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cv = Condition()
        self.list_res: list[VersionedValue] = []
        self.to_reconcile_context = []
        self.current_msg_id = None
        self.current_source = None
        self.current_dest = None
        self.current_key = None
        self.current_request_type = None
        self.final_read_ans = None
        self.client_name = client_name 
        self.connect()
        self.run()

    def deserialize_res(self, serialized: list[list]) -> list[VersionedValue]:
            """Deserialize a list of lists into a set of VersionedValue objects."""
            deserialized = []

            if (not len(serialized[0]) == len(serialized[1])) and len(serialized[0])==1:

                new_vc = VectorClock()
                for vc in serialized[1]:
                    new_vc = new_vc.merge(vc)
                
                deserialized.append(VersionedValue(serialized[0],new_vc))
            
            else:

                for i in range(0,len(serialized[0])):
                    deserialized.append(VersionedValue(serialized[0][i], VectorClock(serialized[1][i])))

            return deserialized
    def connect(self):
        try:
            self.socket.connect((self.server_host, self.server_port))
            logging.info(f"Connected to load balancer at {self.server_host}:{self.server_port}")
        except ConnectionRefusedError:
            logging.error("Failed to connect to load balancer")
            raise
            
            
    def close(self):
        self.socket.close()
    def run(self):
        receiver_thread = Thread(target=self.receive_from_switch)
        receiver_thread.start()

    def string_add(self, json_str1, json_str2):
        dict1 = json.loads(json_str1)
        dict2 = json.loads(json_str2)

        combined_dict = {}
        for key, value in {**dict1, **dict2}.items():
            combined_dict['val'] = combined_dict.get('val', []) + [(key, value)]
        return json.dumps(combined_dict)

    def compute_vector_clock(self, versioned_values: set[VersionedValue]) -> VectorClock:
        """Compute the maximum vector clock from a set of versioned values"""
        if not versioned_values:
            return VectorClock()
            
        merged_clock = {}
        server_ids = set()
        for vv in versioned_values:
            server_ids.update(vv.vector_clock.clock.keys())
            
        for server_id in server_ids:
            max_value = max(
                vv.vector_clock.clock.get(server_id, 0) 
                for vv in versioned_values
            )
            merged_clock[server_id] = max_value
            
        return VectorClock(merged_clock)

    def reconcile_values(self, versioned_values: set[VersionedValue]) -> str:
        """Reconcile multiple versioned values into a single value"""
        combined_value = ""
        for vv in versioned_values:
            if combined_value:
                combined_value = self.string_add(combined_value, vv.value)
            else:
                combined_value = vv.value
        return combined_value

    def reconcile_and_send(self,i):
        """Reconcile versioned values and send appropriate request"""
        with self.cv:
            if len(self.list_res) == 0:
                return True
            elif len(self.list_res) == 1 and self.current_request_type == MessageType.GET_RES:
                return True
            else:
                all_values = set()
                for value_set in self.list_res:
                    all_values.update(value_set)

                merged_clock = self.compute_vector_clock(all_values)
                reconciled_value = self.reconcile_values(all_values)

                if i==0 :
                    self.final_read_ans = reconciled_value
                    message = Message(
                        self.current_msg_id,
                        MessageType.PUT,
                        self.current_source,
                        self.current_dest,
                        key=self.current_key,
                        context=merged_clock.clock,
                        value=reconciled_value
                    )
                    
                else:  # PUT
                    message = Message(
                        self.current_msg_id,
                        MessageType.PUT,
                        self.current_source,
                        self.current_dest,
                        key=self.current_key,
                        value=reconciled_value,
                        context=merged_clock.clock
                    )

                try:
                    self.socket.send(message.serialize())
                    logging.info(f"Sent reconciled {self.current_request_type} message: {message}")
                    return True
                except Exception as e:
                    logging.error(f"Failed to send reconciled message: {e}")
                    return False

    def handle_switch(self, msg: Message):
        with self.cv:
            if msg.msg_type in {MessageType.GET_RES, MessageType.PUT_ACK}:
                res = self.deserialize_res(msg.kwargs["res"])
                self.list_res = res
                self.cv.notify()
                return True

    def receive_from_switch(self) -> None:
        """Thread to receive messages from Switch."""
        while True:
            try:
                response, _ = self.socket.recvfrom(1024)
                msg: Message = Message.deserialize(response)
                logging.info(f"Received Message: {msg}")
                print(f"Received Message {msg}")
                ret = self.handle_switch(msg)
                print(ret)
            except Exception as e:
                logging.error(f"Error in receive_from_switch: {e}")
                break

    def send_get_request(self, msg_id: str, source: str, destination: str, key: str):
        with self.cv:
            self.current_msg_id = msg_id
            self.current_source = source
            self.current_dest = destination
            self.current_key = key
            self.current_request_type = MessageType.GET
            self.list_res = []
            
            message = Message(msg_id, MessageType.GET, source, destination,{"key":key})
            try:
                self.socket.send(message.serialize())
                logging.info(f"Sent GET message: {message}")
                self.cv.wait()
                if self.final_read_ans :
                    logging.info(f"Final GET answer {self.final_read_ans}")
                # if there is some listres
                if self.list_res:
                    x =self.reconcile_and_send(0)
                    return self.list_res
                return True
            except Exception as e:
                logging.error(f"Failed to send GET message: {e}")
                raise

    def send_put_request(self, msg_id: str, source: str, destination: str, key: int, value: str):
        with self.cv:
            self.current_msg_id = msg_id
            self.current_source = source
            self.current_dest = destination
            self.current_key = key
            self.current_request_type = MessageType.GET
            self.list_res = []

            message = Message(msg_id, MessageType.GET, source, destination, key=key, value=value)
            try:
                self.socket.send(message.serialize())
                logging.info(f"Sent PUT message: {message}")
                self.cv.wait()
                # if the there is some list_res
                if self.list_res:
                    return self.reconcile_and_send(1)
                return True
            except Exception as e:
                logging.error(f"Failed to send PUT message: {e}")
                raise

def test_request():
    client = Client('localhost', 4000)
    
    try:
        
        get_test_cases = [
            ("msg1", "client1", "server1", "key1"),
            ("msg2", "client1", "server2", "key2"),
        ]
        
        put_test_cases = [
            ("msg3", "client1", "server1", "key1", "value1"),
            ("msg4", "client1", "server2", "key2", "value2"),
        ]
        
        for msg_id, source, dest, key in get_test_cases:
            client.send_get_request(msg_id, source, dest, key)
            time.sleep(1)
            
        for msg_id, source, dest, key, value in put_test_cases:
            client.send_put_request(msg_id, source, dest, key, value)
            time.sleep(1)
            
    except Exception as e:
        logging.error(f"Test failed: {e}")
    finally:
        client.close()

def handle_interactive_mode():
    client = Client('localhost', 4000,7777)
    try:
        
        while True:
            try:
                request_type = input("Enter request type (GET/PUT): ").upper()
                msg_id = input("Enter message ID: ")
                source = input("Enter source: ")
                dest = input("Enter destination: ")
                key = input("Enter key: ")
                
                if request_type == "GET":
                    client.send_get_request(msg_id, source, dest, key)
                elif request_type == "PUT":
                    value = input("Enter value: ")
                    client.send_put_request(msg_id, source, dest, key, value)
                else:
                    print("Invalid request type. Please enter GET or PUT.")
                    continue
                    
                print(f"{request_type} request sent successfully!")
                
            except KeyboardInterrupt:
                break
            except ValueError:
                print("Invalid input format")
                    
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_request()
    else:
        handle_interactive_mode()