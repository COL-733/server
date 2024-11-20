import sys
import socket
from threading import Thread
from message import Message
import logging
from typing import Final
from config import *

class Switch:
    def __init__(self, name, topology):
        self.name = name
        self.topology = topology
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', SWITCH_PORT))
        self.socket.listen(5)
        self.servers: dict[str, socket.socket] = dict()

    def run(self, name):
        recvThd = Thread(target=self.recvThd, args=(name,))
        recvThd.start()

    def recvThd(self, name):
        while True:
            try:
                response, _ = self.servers[name].recvfrom(1024)
                message: Message = Message.deserialize(response)
                logging.info(f"Received Message {message}")
                self.forward(message)
            except Exception as e:
                logging.error(f"Receive Thread: Invalid Message {e}")

    def connectServerLoop(self):
        while True:
            c, addr = self.socket.accept()
            port = addr[1]
            logging.info(f"Connected to server at port {port}")
            name = self.name+"_"+str(port)
            self.servers[name] = c
            self.run(name)

    def sendToServer(self, msg: Message, dest: str):
        if self.servers.get(dest) is not None:
            self.servers[dest].send(msg.serialize())
        else:
            raise Exception(f"Server {dest} not found")
   
    def sendToSwitch(self, msg: Message, dest: str):
        raise NotImplementedError
        # if self.topology.get(dest) is not None:
        #     self.topology

    def forward(self, msg: Message):
        dest = msg.dest
        if dest.split('_')[0] == self.name:
            self.sendToServer(msg, dest)
        else:
            self.sendToSwitch(msg, dest)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    switch = Switch(sys.argv[1], {})
    switch.connectServerLoop()