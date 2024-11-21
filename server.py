import socket
import queue
import time
import threading
import random
import logging
import argparse
from abc import ABC, abstractmethod

from enum import IntEnum
from multiprocessing import Process
from ring import Ring
from message import MessageType, Message
from storage import Storage
from threading import Lock
from typing import Any, Final, Optional
from config import *
from operation import Operation
import log

SWITCH_PORT = 2000

class Server(Process):
    def __init__(self, switch_name: str, switch_ip: str, port: int, seeds: list[str]):
        super(Server, self).__init__()

        self.name: str = f'{switch_name}_{port}'
        self.port: int = port
        self.switch_name: str = switch_name
        self.switch_ip: str = switch_ip
        self.seeds: list[str] = seeds

        self.operations: dict[str, Operation] = {}

        # Connect to Switch
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect_to_switch()

        self.ring: Ring = Ring([])
        self.ring.init(self.name, config.T, self.seeds)
        
        # self.storage: Storage = Storage('localhost','root','Jdsp9595@',f'{self.name}_storage')
        self.cmdQueue: queue.Queue[Message] = queue.Queue()

        self.lock = Lock()

    def connect_to_switch(self) -> None: # Connect to switch
        """At booting, connect to the Switch."""
        while True:
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.bind(("", self.port))
                self.socket.connect(("", SWITCH_PORT))
                break
            except ConnectionRefusedError:
                time.sleep(0.2)

    def receive_from_switch(self) -> None:
        """Thread to receive messages from Switch."""
        while True:
            try:
                response, _ = self.socket.recvfrom(BUFFER_SIZE)
                msg: Message = Message.deserialize(response)
                logging.info(f"Received Message: {msg}")
                # with self.lock:
                self.cmdQueue.put(msg)
            except Exception as e:
                raise e
            
    def process_incoming_message(self, msg: Message) -> bool:
        """Processes incoming messages and distinguishes requests from responses."""
        req_type = msg.msg_type
        msgid = msg.id
        key = msg.kwargs["key"]

        if req_type in {MessageType.GET, MessageType.PUT}:
            return False

        elif req_type in {MessageType.GET_RES, MessageType.PUT_ACK}:
            if msgid in self.operations:
                op = self.operations[msgid]
                res = msg.kwargs["res"] if "res" in msg.kwargs else None
                op.handle_response(res)
            return True

        else:
            kw = {}
            if req_type == MessageType.GET_KEY:
                kw["res"] = self.get(key)
                response = Message(msgid, MessageType.GET_RES, self.name, msg.source, kw)
            elif req_type == MessageType.PUT_KEY:
                self.put(key)
                response = Message(msgid, MessageType.PUT_ACK, self.name, msg.source, kw)
            
            self.send(response)
            return True

    def exec_request(self, req: Message) -> None:
        """Execute the GET or PUT request."""
        op = self.operations.get(req.id)
        prefList = self.ring.getPrefList(req.kwargs["key"])

        if op:
            target = prefList if op.isCord else [random.choice(prefList)]
            for server in target:
                self.send(op.response_msg(self.name, server))

            op.cv.wait()  # Synchronize and wait for responses
            op.syn_reconcile()
            self.send(op.reply_msg(self.name))
            del self.operations[req.id]

    def ini_operation(self, req: Message, op_thread: threading.Thread) -> None:
        """Add & initialize the operation for given request."""
        key = req.kwargs["key"]

        # Check wether we've key or not
        op = None
        if self.check_key(key):

            if req.msg_type == MessageType.GET:
                res = self.get(key)
                op = Operation(op_thread, req, True, res, 0)
            elif req.msg_type == MessageType.PUT:
                self.put(key)
                op = Operation(op_thread, req, True, [], 1)

        else:

            if req.msg_type == MessageType.GET:
                op = Operation(op_thread, req, False)
            elif req.msg_type == MessageType.PUT:
                op = Operation(op_thread, req, False)
        
        self.operations[req.id] = op    # Add operation
        op.start()                      # start operation thread

    def command_handler(self) -> None: # thread
        while True:

            req = self.cmdQueue.get()

            if req.msg_type in {MessageType.GOSSIP_REQ, MessageType.GOSSIP_RES}:
                self.handle_gossips(req)
            else:
                # handle all request not adding new ops to self.operations
                handled = self.process_incoming_message(req)

                if not handled: # GET or PUT
                    op_thread = threading.Thread(target=self.exec_request, args=(req))
                    self.ini_operation(req,op_thread)  # make opeartion

    def check_key(self, key: int, context: dict[str, Any] = {}) -> bool:
        # What if we are in the preference list
        # but we don't have key handle that
        raise NotImplementedError
    
    def get(self, key: int, context: dict[str, Any] = {}):
        raise NotImplementedError

    def put(self, key: int, context: dict[str, Any] = {}):
        raise NotImplementedError

    def send(self, msg: Message): # To Server or Load Blanacer through Switch
        while True:
            try:
                self.socket.sendall(msg.serialize())
                break
            except:
                # if not sent, wait and again send
                time.sleep(0.05)

    def gossip(self): # Thread
        logging.info(f"Starting Gossip...")
        while True:
            logging.debug(f"Known Servers: {list(self.ring.serverSet)}, Ring State: {str(list(self.ring.state))}")
            time.sleep(config.I)
            if len(self.ring.serverSet) == 0:
                logging.warning("No Server To Gossip")
                continue
            gossipList = random.sample(list(self.ring.serverSet), min(len(self.ring.serverSet), config.G))
            for server in gossipList:
                message = Message(-1, MessageType.GOSSIP_REQ, self.name, server, ring=self.ring)
                self.send(message)

    def handle_gossips(self, msg: Message):
        # merge incoming ring
        newServers = self.ring.merge(msg.kwargs['ring'])
        for serv in newServers:
            logging.critical(f"Identified New Server {serv}")
        # send own ring
        if msg.msg_type == MessageType.GOSSIP_REQ:
            message = Message(-1, MessageType.GOSSIP_RES, self.name, msg.source, ring=self.ring)
            self.send(message)

    def handle_hinted_handoffs(): # Thread
        raise NotImplementedError

    def cleanup_operations(self):
        while True:
            time.sleep(60)  # Run cleanup every 60 seconds
            to_remove = [key for key, op in self.operations.items() if op.is_complete()]
            # with self.lock:
            for key in to_remove:
                del self.operations[key]

    def run(self):        
        # 1. Start the recv thread to recv from Switch
        # thread to handle messages from the coordinator
        recvThread = threading.Thread(target=self.receive_from_switch)
        recvThread.start()

        # 2. thread to process the command queue
        cmdThread = threading.Thread(target=self.command_handler)
        cmdThread.start()
        
        gossipThread = threading.Thread(target=self.gossip)
        gossipThread.start()

        cmdThread.join()
        recvThread.join()
        gossipThread.join()

if __name__=="__main__":    
    logging = log.getLogger(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-sw', help = "Switch Name", required=True)
    parser.add_argument('-port', help = "Port Number", required=True, type=int)
    parser.add_argument('-seeds', nargs='+', help='List of seed ports', required=True)
    args = parser.parse_args()

    switch_name = args.sw

    seeds = [f"{switch_name}_{port}" for port in args.seeds if port != args.port]

    server = Server(switch_name, '', args.port, seeds)
    server.run()