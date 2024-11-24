import sys
import socket
import time
from multiprocessing import Process
import threading
import logging
import queue
from dataclasses import dataclass
from message import Message, MessageType
from config import *
import log

def recvall(sock: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise Exception("Connection closed")
        data += packet
    return data

class LoadBalancer(Process):
    def __init__(self, switch_name: str, switch_ip: str, port: int):
        super(LoadBalancer, self).__init__()
        self.name: str = f'{switch_name}_{port}'
        self.switch_name: str = switch_name
        self.switch_ip: str = switch_ip
        self.server_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.switch_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dict_client : dict[str,socket.socket] = {}
        self.client_request_queue: queue.Queue = queue.Queue()
        self.port = port
        self.run()
        
    def handle_client(self, client_socket: socket.socket):
        try:
            while True:
                data = recvall(client_socket, BUFFER_SIZE)
                if not data:
                    break
                message = Message.deserialize(data)
                message.dest = self.name
                switchn = self.name.split("_")[0]
                message.source = switchn + "_client_" +  message.source
                self.client_request_queue.put(message)
        except Exception as e:
            logging.error(f"Error in handle_client: {e}", exc_info=True)
        finally:
            client_socket.close()
            logging.info(f"{self.name} client handler thread exiting")

    def send_to_client(self,msg:Message) :
        self.dict_client[msg.dest.split("_")[2]].send(msg.serialize())
    def receive_from_switch(self) -> None:
        """Thread to receive messages from Switch."""
        while True:
            try:
                response, _ = self.switch_socket.recvfrom(1024)
                msg: Message = Message.deserialize(response)
                logging.info(f"Received Message: {msg}")
                ret = self.send_to_client(msg)
            except Exception as e:
                logging.error(f"Error in receive_from_switch: {e}")
                break

    def handle_query(self):
        while True:
            try:
                msg = self.client_request_queue.get()
                self.send_to_switch(msg)
            except Exception as e:
                logging.exception(f"Error in handle_query: {e}")
                
    def run(self):
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(5)
        logging.info(f"LoadBalancer listening on port {self.port}")
        
        # Connect to switch
        self.connect_switch()
        
        # Start query handler thread
        query_handler = threading.Thread(target=self.handle_query)
        query_handler.daemon = True
        query_handler.start()
        receiver_thread = threading.Thread(target=self.receive_from_switch)
        receiver_thread.daemon = True
        receiver_thread.start()
        # Accept client connections
        while True:
            try:
                client_socket, addr = self.server_socket.accept()
                logging.info(f"{self.name} accepted connection from {addr}")
                self.dict_client[addr[1]] =client_socket
                client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_handler.daemon = True
                client_handler.start()
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")

    def connect_switch(self) -> None:
        while True:
            try:
                self.switch_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.switch_socket.bind(('', LB_TO_SWITCH_PORT))
                self.switch_socket.connect((self.switch_ip, SWITCH_PORT))
                logging.info(f"Connected to switch at {self.switch_ip}:{SWITCH_PORT}")
                break
            except ConnectionRefusedError:
                logging.warning("Failed to connect to switch, retrying...")
                time.sleep(0.2)
    
    def send_to_switch(self, msg: Message):
        try:
            self.switch_socket.send(msg.serialize())
        except Exception as e:
            raise Exception(f"Couldn't send to switch: {e}")
    
    def send_test(self, id: str, dest: str):
        test_message = Message(id, MessageType.GET, self.name, dest)
        self.send_to_switch(test_message)

if __name__ == "__main__":
    logging = log.getLogger(logging.DEBUG)
    lb = LoadBalancer(sys.argv[1], '', LB_PORT)
    while True:
        id, dest = input().split()
        lb.send_test(id, dest)