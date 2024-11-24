from queue import Queue
import socket
from threading import Thread, Condition
from message import Message, MessageType
import logging
from typing import Final
from config import *
import argparse
import log
import struct 
import math
from gui import SwitchGUI

class Switch:
    def __init__(self, name, topology):
        self.name: str = name
        self.topology = topology
        self.servers: dict[str, socket.socket] = dict()
        self.request_queue = Queue()
        self.cv = Condition()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', SWITCH_PORT))
        self.socket.listen(5)

        self.routingTable: dict[str, socket.socket] = dict()
        self.switchSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.switchSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.switchSocket.bind(('', SWITCH_SWITCH_PORT))
        self.switchSocket.listen(5)

        sendThd = Thread(target=self.sendThd)
        sendThd.daemon = True
        sendThd.start()

        connectThd = Thread(target=self.connectServerLoop)
        connectThd.daemon = True
        connectThd.start()

        guiThd = Thread(target=self.runGUI)
        guiThd.start()

    def recvThd(self, socket, name):
        while True:
            try:
                response = Message.receive_all(socket.recvfrom)

                if not len(response):
                    logging.critical(f"Disconnected from server {name}")
                    break

                message: Message = Message.deserialize(response)
                logging.info(f"Received Message: {message}")
                with self.cv:
                    self.request_queue.put(message)
                    self.cv.notify()

            except Exception as e:
                logging.error(e)

    def sendThd(self):
        while True:
            with self.cv:
                while self.request_queue.empty():
                    self.cv.wait()
                message = self.request_queue.get()
                try:
                    self.forward(message)
                except Exception as e:
                    logging.error(e)

    def connectServerLoop(self):
        logging.info(f"Listening at port {SWITCH_PORT}")
        while True:
            c, addr = self.socket.accept()
            port = addr[1]
            logging.info(f"Connected to server at port {port}")
            name = self.name+"_"+str(port)
            self.servers[name] = c
            recvThd = Thread(target=self.recvThd, args=(c, name))
            recvThd.start()

    def sendToServer(self, msg: Message, dest: str):
        if self.servers.get(dest) is not None:
            self.servers[dest].send(msg.serialize())
        else:
            logging.warning(f"Server {dest} is not connected")
   
    def sendToSwitch(self, msg: Message, dest: str):
        name = dest.split('_')[0]
        if self.routingTable.get(dest) is not None:
            try:
                self.routingTable[name].send(msg.serialize())
            except:
                logging.error(f"Cannot send message to switch {name}")
        else:
            logging.warning(f"Switch {name} is not in routing table")

    def forward(self, msg: Message):
        dest = msg.dest
        if dest.split('_')[0] == self.name:
            self.sendToServer(msg, dest)
        else:
            self.sendToSwitch(msg, dest)

    def connectSwitchLoop(self):
        while True:
            c, addr = self.switchSocket.accept()
            message_bytes = self.name.encode('utf-8')
            padded_message = message_bytes.ljust(BUFFER_SIZE, b'\x00')
            c.send(padded_message)
            res = c.recv(BUFFER_SIZE)
            padded_name = res.decode('utf-8')
            name = padded_message[:padded_name.index('\x00')]
            logging.critical(f"Connected to switch {name}")
            self.servers[name] = c
            recvThd = Thread(target=self.recvThd, args=(c, name))
            recvThd.start()        

    def addSwitch(self, addr, name):
        switchSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.routingTable[name] = switchSocket
        logging.critical(f"Added new switch {name} to routing table")
        self.gui.updateList(list(self.routingTable.keys()))

    def removeSwitch(self, name):
        del self.routingTable[name]
        self.gui.updateList(list(self.routingTable.keys()))

    def runGUI(self):
        self.gui = SwitchGUI(self.name, self.addSwitch, self.removeSwitch)
        self.gui.mainloop()

if __name__=="__main__":   
    logging = log.getLogger(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-sw', help = "Switch Name", required=True)
    args = parser.parse_args() 

    switch = Switch(args.sw, {})