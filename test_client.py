import socket
import logging
import sys
import time
from message import Message, MessageType
from server import Server
from load_balancer import LoadBalancer
from storage import VectorClock, VersionedValue
from message import Message, MessageType   

class Client:
    def __init__(self, server_host: str, server_port: int):
        self.server_host = server_host
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    def connect(self):
        try:
            self.socket.connect((self.server_host, self.server_port))
            logging.info(f"Connected to load balancer at {self.server_host}:{self.server_port}")
        except ConnectionRefusedError:
            logging.error("Failed to connect to load balancer")
            raise
            
    def send_request(self, msg_id: str, source: str, destination: str):
        message = Message(msg_id, MessageType.GET, source, destination)
        try:
            self.socket.send(message.serialize())
            logging.info(f"Sent message: {message}")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise
            
    def close(self):
        self.socket.close()

def test_request():
    client = Client('localhost', 4000)
    
    try:
        client.connect()
        
        test_cases = [
            ("msg1", "client1", "server1"),
            ("msg2", "client1", "server2"),
            ("msg3", "client1", "server3"),
        ]
        
        for msg_id, source, dest in test_cases:
            client.send_request(msg_id, source, dest)
            time.sleep(1)
            
    except Exception as e:
        logging.error(f"Test failed: {e}")
    finally:
        client.close()

def main():

    context1 = VectorClock({"A": 2})
    context2 = VectorClock({"A": 2, "B": 1})
    context3 = VectorClock({"A": 2, "C": 1 })
    context4 = VectorClock({"A": 2, "B": 2, "C": 1})
    context5 = VectorClock({"A": 2, "B": 2})
    context6 = VectorClock({"B": 1})
    context7 = VectorClock({"C": 1})
    context8 = VectorClock({"A": 1})

    temp = {
        "context1" : VectorClock({"A": 2}),
        "context2" : VectorClock({"A": 2, "B": 1}),
        "context3" : VectorClock({"A": 2, "C": 1 }),
        "context4" : VectorClock({"A": 2, "B": 2, "C": 1}),
        "context5" : VectorClock({"A": 2, "B": 2}),
        "context6" : VectorClock({"B": 1}),
        "context7" : VectorClock({"C": 1}),
        "context8" : VectorClock({"A": 1})
    }
    
    version_value1 = VersionedValue("value1", context1)
    version_value2 = VersionedValue("value2", context2)
    version_value3 = VersionedValue("value3", context3)
    version_value4 = VersionedValue("value4", context4)
    version_value5 = VersionedValue("value5", context5)
    version_value6 = VersionedValue("value6", context6)
    version_value7 = VersionedValue("value7", context7)
    version_value8 = VersionedValue("value8", context8)

    id = 1
    while True:
        # Take command input from the console
        command = input("Enter the command: ").strip()
        args = command.split() if command else []

        switch_name = "delhi"

        if args[0] == "run":
            cl = Client('',4000)
            cl.connect()

        if args[0] == "clear":
            print("Exiting...")
            break

        if args[0] == "put":
            if args[1] == "all":
                for i in range(1,9):
                    msg = Message(id, MessageType.PUT_KEY, f"{switch_name}_{4000}", f"{switch_name}_{args[2]}" ,
                                {"key": "1", "value": [f"value{i}"], "context": [temp[f"context{i}"].to_dict()] })
                    
                    # print({"key":args[1], "value": f"value{args[1]}", "context": temp[f"context{args[1]}"] })
                    id+=1
                    cl.socket.send(msg.serialize())
            else:
                msg = Message(id, MessageType.PUT_KEY, f"{switch_name}_{4000}", f"{switch_name}_{args[2]}" ,
                            {"key": "1", "value": [f"value{args[1]}"], "context": [temp[f"context{args[1]}"].to_dict()] })
                
                # print({"key":args[1], "value": f"value{args[1]}", "context": temp[f"context{args[1]}"] })
                id+=1
                cl.socket.send(msg.serialize())
        
        if args[0] == "get":
            msg = Message(id, MessageType.GET_KEY, f"{switch_name}_{5001}", f"{switch_name}_{args[2]}" ,
                          {"key": args[1] })
            
            # print({"key":args[1], "value": f"value{args[1]}", "context": temp[f"context{args[1]}"] })
            id+=1
            cl.socket.send(msg.serialize())

if __name__ == "__main__":
    main()
