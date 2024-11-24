import socket
import time
import random
import logging
import argparse
import os
import json
from threading import Thread, Condition
from queue import Queue
from ring import Ring
from message import MessageType, Message
from storage import Storage, VersionedValue, VectorClock, KeyVersion
from typing import  Optional
from config import *
from gui import *
from operation import Operation
import log

SWITCH_PORT = 2000

class Server():
    def __init__(self, switch_name: str, switch_ip: str, port: int, seeds: list[str]):
        super(Server, self).__init__()

        self.name: str = f'{switch_name}_{port}'
        self.port: int = port
        self.switch_name: str = switch_name
        self.switch_ip: str = switch_ip
        self.seeds: list[str] = seeds

        self.operations: dict[str, Operation] = {}

        self.ring: Ring = Ring([])
        self.ring.init(self.name, config.T, self.seeds)
        
        self.loadState()

        # Connect to Switch
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect_to_switch()
        
        # Unique to each server $path_to_database={datacenter_name}/{server_name}_storage.db
        self.storage = Storage(f"{self.switch_name}/{self.name}_storage.db")
        self.kv = KeyVersion(f"{self.switch_name}/{self.name}_key_versions.db")

        self.cmdQueue: Queue[Message] = Queue()
        self.cv = Condition()

        self.recvThread = Thread(target=self.receive_from_switch)
        self.cmdThread = Thread(target=self.command_handler)
        self.gossipThread = Thread(target=self.gossip)
        self.guiThread = Thread(target=self.runGUI)

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
                response = Message.receive_all(self.socket.recvfrom)

                msg: Message = Message.deserialize(response)
                logging.info(f"Received Message: {msg}")
                with self.cv:
                    self.cmdQueue.put(msg)
                    self.cv.notify()
            except Exception as e:
                logging.error(f"error: {e}")
            
    def serialize_res(self, set: set[VersionedValue]) -> list[list]:
        """Make GET RESPONSES serializable."""
        serialized = []
        value = []
        vector_clocks = []
        for versioned_value in set:
            value.append(versioned_value.value)
            vector_clocks.append(versioned_value.vector_clock.to_dict())

        serialized.append(value)
        serialized.append(vector_clocks)
        return serialized

    def deserialize_res(self, serialized: list[list]) -> set[VersionedValue]:
        """Deserialize a list of lists into a set of VersionedValue objects."""
        deserialized = set()

        if (not len(serialized[0]) == len(serialized[1])) and len(serialized[0])==1:

            new_vc = VectorClock()
            for vc in serialized[1]:
                new_vc = new_vc.merge(vc)
            
            deserialized.add(VersionedValue(serialized[0],new_vc))
        
        else:

            for i in range(0,len(serialized[0])):
                deserialized.add(VersionedValue(serialized[0][i], VectorClock(serialized[1][i])))

        return deserialized
    
    def deserialize_put(self, value: list, vcs: list) -> VersionedValue:
        """Deserialize a list of lists into a set of VersionedValue objects."""

        if len(value)==1:
            
            if len(vcs) == 1:
                new_vc = VectorClock(vcs[0])
            else:
                new_vc = VectorClock()
                for vc in vcs:
                    new_vc = new_vc.merge(VectorClock(vc))
                
            return VersionedValue(value[0],new_vc)
        
        else:

            return None

    def process_incoming_message(self, msg: Message) -> bool:
        """Processes incoming messages and distinguishes requests from responses."""
        req_type = msg.msg_type
        msgid = msg.id
        key = msg.kwargs["key"]

        logging.info(f"Proccesing message: {msgid}")
        if req_type in {MessageType.GET, MessageType.PUT}:
            return False

        elif req_type in {MessageType.GET_RES, MessageType.PUT_ACK}:
            if msgid in self.operations:
                op = self.operations[msgid]
                res = self.deserialize_res(msg.kwargs["res"]) if "res" in msg.kwargs else None
                op.handle_response(res)
            return True

        else:
            kw = {"key":key}

            if req_type == MessageType.GET_KEY:

                res = self.get(key)
                # Only send if we've a legit GET_RESPONSE
                if not res==None:
                    kw["res"] = self.serialize_res(res)
                    logging.info(f"{res}")
                    response = Message(msgid, MessageType.GET_RES, self.name, msg.source, kw)
                    self.send(response)

            elif req_type == MessageType.PUT_KEY:
                # Put the same version got from the coordinator
                
                versioned_value = self.deserialize_put(msg.kwargs["value"], msg.kwargs["context"] )

                if versioned_value:
                    self.put(key, versioned_value.value, versioned_value.vector_clock)
                    response = Message(msgid, MessageType.PUT_ACK, self.name, msg.source, kw)
                    self.send(response)
                else:
                    logging.error(f"More than one value given for : {msgid}.")

            return True

    def exec_request(self, req: Message) -> None:
        """Execute the GET or PUT request."""
        op = self.operations.get(req.id)
        prefList = self.ring.getPrefList(req.kwargs["key"])

        with op.cv:
            target = prefList if op.isCord else [random.choice(prefList)]
            for server in target:
                if not self.name == server:
                    self.send(op.response_msg(self.name, server))

            if op.type == MessageType.GET:
                logging.info(f"Waiting for Get Responses  (id, key): {op.id, op.key}")
            else:
                logging.info(f"Waiting for Put Acks  (id, key): {op.id, op.key}")

            op.cv.wait()  # Synchronize and wait for responses

            if op.type == MessageType.GET:
                logging.info(f"Got required Get Responses  (id, key): {op.id, op.key}")
            else:
                logging.info(f"Got required Put Acks  (id, key): {op.id, op.key}")

            op.syn_reconcile()
            self.send(op.reply_msg(self.name))
            del self.operations[req.id]
            logging.info(f"Operation Completed (id, key): {op.id, op.key}")

    def ini_operation(self, req: Message, op_thread: Thread) -> None:
        """Add & initialize the operation for given request.(GET or PUT)"""
        key = req.kwargs["key"]

        # Check wether we've key or not
        op = None
        if self.ring.check_key(key): # we're coordinator
            
            if self.storage.exists(key): # initiate operation with own response or ack

                if req.msg_type == MessageType.GET:
                    res = self.get(key)
                    op = Operation(op_thread, req, True, res=res)

                else:
                    if "context" in req.kwargs:
                        # update the our version in context(context is instanvce of VectorClock)
                        if not self.kv.exists(key):
                            self.kv.add_key(key)
                        else:
                            self.kv.inc_version(key)

                        version = self.kv.get_version(key)

                        versioned_value = self.deserialize_put(req.kwargs["value"], req.kwargs["context"] )

                        versioned_value.vector_clock.add(self.name, version)

                        self.put(key, versioned_value.value , versioned_value.vector_clock)
                        
                        op = Operation(op_thread, req, True, acks=1, value=versioned_value)
                    else:
                        logging.error(f'Operation for {key} was not created: "context" not found in message {req.id}.')

            else: # initiate response without pre-providing

                op = Operation(op_thread, req, True)

        else: # we're sub coordinator

            op = Operation(op_thread, req, False)
        
        self.operations[req.id] = op    # Add operation
        op.start()                      # start operation thread

    def command_handler(self) -> None: # thread
        while True:
            with self.cv:
                while self.cmdQueue.empty():
                    self.cv.wait()
            req = self.cmdQueue.get()

            if req.msg_type in {MessageType.GOSSIP_REQ, MessageType.GOSSIP_RES}:
                self.handle_gossips(req)
                continue

            handled = self.process_incoming_message(req)

            if not handled: # GET or PUT
                op_thread = Thread(target=self.exec_request, args=(req,))

                if req.msg_type in {MessageType.GET, MessageType.PUT}:
                    self.ini_operation(req,op_thread)  # make opeartion
                    # logging.info(f"Operation Created for (id, key)=({req.id}, {req.kwargs["key"]})")

            logging.info(f"Request Processed: {req.id}")

    def get(self, key: str) -> Optional[set[VersionedValue]]:
        """Retrieve a value by key from local storage."""
        return self.storage.get_leaf_nodes(key)

    def put(self, key: str, value: str,  context: VectorClock = None) -> None:
        """Store a key-value pair in local storage."""
        self.storage.add_version(key, value, context)

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
            time.sleep(config.I)
            logging.debug(f"Known Servers: {list(self.ring.serverSet)}, Ring State: {str(list(self.ring.state))}")
            gossipSet = self.ring.serverSet
            gossipSet.discard(self.name)

            if len(gossipSet) == 0:
                logging.warning("No Server To Gossip")
                continue
            gossipList = random.sample(list(gossipSet), min(len(gossipSet), config.G))
            for server in gossipList:
                message = Message(-1, MessageType.GOSSIP_REQ, self.name, server, {'ring': self.ring})
                self.send(message)

    def handle_gossips(self, msg: Message):
        # merge incoming ring
        addedNodes, deletedNodes = self.ring.merge(msg.kwargs['ring'])
        if addedNodes or deletedNodes:
            self.gui.updateRing(self.ring)
        for node in addedNodes:
            logging.critical(f"Added new node of server {node.server} at position {node.pos}")
        for node in deletedNodes:
            logging.critical(f"Deleted a node of server {node.server} at position {node.pos}")
        # send own ring
        if msg.msg_type == MessageType.GOSSIP_REQ:
            message = Message(-1, MessageType.GOSSIP_RES, self.name, msg.source, {'ring': self.ring})
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

    def shutdown(self):
        """Shutdown the server, delete the database, close the socket, and reset variables."""
        logging.info("[Server] Shutting down server...")

        # Step 1: Close the socket connection
        try:
            self.socket.close()
            logging.info("[Server] Socket closed.")
        except Exception as e:
            logging.info(f"[Server] Error closing socket: {e}")

        # Step 2: Delete the database file
        db_path = f"{self.switch_name}/{self.name}_storage.db"
        try:
            os.remove(db_path)
            logging.info(f"[Server] Database '{db_path}' deleted.")
        except FileNotFoundError:
            logging.error(f"[Server] Database '{db_path}' does not exist, nothing to delete.")
        except Exception as e:
            logging.error(f"[Server] Error deleting database '{db_path}': {e}")

        try:
            os.remove(f"{self.switch_name}/{self.name}_state.json")
        except Exception as e:
            logging.error(e)


        # Step 3: Reset server variables
        self.operations.clear()
        self.cmdQueue = Queue()

        # Final message
        logging.info("[Server] Shutdown complete.")
        self.exit()

    def exit(self) -> None:
        self.gui.exit()


    def run(self):        
        # 2. Start the recv thread to recv from Switch
        # thread to handle messages from the coordinator

        self.recvThread.daemon = True
        self.recvThread.start()

        self.cmdThread.daemon = True
        self.cmdThread.start()
        
        self.gossipThread.daemon = True
        self.gossipThread.start()

        self.guiThread.start()
        
        self.guiThread.join()

    def runGUI(self):
        self.gui = ServerGUI(self.name, self.shutdown, self.exit, self.deleteToken, self.addToken)
        self.gui.updateRing(self.ring)
        self.gui.mainloop()

    def loadState(self):
        filePath = f"{self.switch_name}/{self.name}_state.json"
        if os.path.exists(filePath):
            with open(filePath, 'r') as stateFile:
                state = json.load(stateFile)
                self.ring.load(state['ring'])
        else:
            self.saveState()
    
    def saveState(self):
        filePath = f"{self.switch_name}/{self.name}_state.json"
        with open(filePath, 'w') as stateFile:
            json.dump({'ring': Ring.serialize(self.ring)}, stateFile)

    def addToken(self, pos):
        self.ring.add(pos)
        self.gui.updateRing(self.ring)

    def deleteToken(self, pos):
        logging.critical(f"Deleting token at position {pos}")
        self.ring.delete(pos)
        self.gui.updateRing(self.ring)

if __name__=="__main__":    
    logging = log.getLogger(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('-sw', help = "Switch Name", required=True)
    parser.add_argument('-port', help = "Port Number", required=True, type=int)
    parser.add_argument('-seeds', nargs='+', help='List of seed ports', required=True)
    args = parser.parse_args()

    switch_name = args.sw

    seeds = []

    num_seeds = len(args.seeds) // 2
    for i in range(num_seeds):
        sw = args.seeds[2*i]
        port = args.seeds[2*i+1]
        seeds.append(f"{sw}_{port}")
    print(seeds)

    server = Server(switch_name, '', args.port, seeds)
    server.run()