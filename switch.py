from queue import Queue
import socket
from threading import Thread, Condition
from message import Message
import logging
from typing import Final
from config import *
import argparse
import log

class Switch:
    def __init__(self, name, topology):
        self.name = name
        self.topology = topology
        self.servers: dict[str, socket.socket] = dict()
        self.request_queue = Queue()
        self.cv = Condition()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', SWITCH_PORT))
        self.socket.listen(5)

        sendThd = Thread(target=self.sendThd)
        sendThd.start()

        connectThd = Thread(target=self.connectServerLoop)
        connectThd.start()

        sendThd.join()
        connectThd.join()
        

    def recvThd(self, name):
        while True:
            try:
                response, _ = self.servers[name].recvfrom(BUFFER_SIZE)
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
            recvThd = Thread(target=self.recvThd, args=(name,))
            recvThd.start()

    def sendToServer(self, msg: Message, dest: str):
        if self.servers.get(dest) is not None:
            self.servers[dest].send(msg.serialize())
        else:
            raise Exception(f"Server {dest} is not connected")
   
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
    logging = log.getLogger(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', help = "Switch Name", required=True)
    args = parser.parse_args() 

    switch = Switch(args.s, {})