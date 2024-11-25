import sys
import socket
import time
from multiprocessing import Process
import logging
import queue
from dataclasses import dataclass
from message import Message, MessageType
from storage import Storage, VersionedValue, VectorClock
import log
import json
from config import *
from gui import ClientGUI

class Client:
    def __init__(self, server_host: str, server_port: int, id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.last_ctx: dict[str, list[dict]] = {}
        self.last_value: dict[str, str] = {}
        self.to_reconcile_context = []
        self.port: int
        self.msg_id: int = 1
        self.id = id
        
        self.connect()

        self.runGUI()


    def connect(self):
        try:
            self.socket.connect((self.server_host, self.server_port))
            self.name = self.socket.getsockname()[1]
            logging.info(f"Connected to load balancer at {self.server_host}:{self.server_port}")
        except ConnectionRefusedError:
            logging.error("Failed to connect to load balancer")
            raise
    
    def close(self):
        self.socket.close()

    def send(self, msg: Message):
        try:
            self.socket.send(msg.serialize())
        except Exception as e:
            logging.error(f"Failed to send GET message: {e}")

    def recv(self) -> Message:
        try:
            response = Message.receive_all(self.socket.recvfrom)
            msg: Message = Message.deserialize(response)
            logging.info(f"Received Message: {msg}")
            return msg
        except Exception as e:
            logging.error(f"Error in recv: {e}")
    
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
    
    def _get(self, key: str):
        msg = Message(self.msg_id, MessageType.GET, "","", {"key": key} )
        self.msg_id += 1
        self.send(msg)

        response = self.recv()
        assert(response.msg_type == MessageType.GET_RES)

        ctx = response.kwargs["res"][1] if response.kwargs["res"] is not None else None
        self.last_ctx[key] = ctx

        res = self.deserialize_res(response.kwargs["res"]) if response.kwargs["res"] is not None else None
        val = self.reconcile_values(res)
        self.last_value[key] = val
        logging.info(f"GET RESPOSNSE (id, key):({self.msg_id-1, key} = {val})")

    def _put(self, key: str, values: list[str]):

        rec_val = self.addValuesToKey(key, values)
        dict = {"key":key, "value":rec_val, "context": self.last_ctx[self.id]} if self.last_ctx[self.id] is not None else {"key":key, "value":rec_val}
        msg = Message(self.msg_id, MessageType.PUT, "","",dict)
        self.msg_id +=1
        self.send(msg)

        response = self.recv()

        assert(response.msg_type == MessageType.PUT_ACK)

        self.last_value[key] = rec_val
        ctx = response.kwargs["context"]
        self.last_ctx[key] = ctx
        logging.info(f"PUT Successful (id, key):({self.msg_id-1, key})")

    def addValuesToKey(self, key, values):
        dic = json.loads(self.last_value[key])
        items = set(dic['cart'])
        for value in values:
            items.add(value)
        return json.dumps({'cart': list(items)}) 
    
    def get(self):
        self._get(self.id)
        cart = json.loads(self.last_value[self.id])['cart']
        self.gui.updateList(cart)

    def put(self, values):
        if self.id not in self.last_ctx:
            self._get(self.id)
        self._put(self.id, values)
        cart = json.loads(self.last_value[self.id])['cart']
        self.gui.updateList(cart)


    def runGUI(self):
        self.gui = ClientGUI(self.id, self.put, self.get)
        self.gui.mainloop()        

    def reconcile_values(self, versioned_values: set[VersionedValue]):
        items = set()
        if versioned_values is not None:
            for v in versioned_values:
                dic = json.loads(v.value)
                for item in dic['cart']:
                    items.add(item)

        return json.dumps({'cart': list(items)})        

if __name__ == "__main__":
    logging = log.getLogger(logging.INFO)
    client = Client('localhost', LB_PORT, sys.argv[1])