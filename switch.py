import socket
from threading import Thread
from message import Message
import sys
PORT = 2000

class Switch:
    def __init__(self, name, topology):
        self.name = name
        self.topology = topology
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', PORT))
        self.socket.listen(5)
        self.servers: dict[str, socket.socket] = dict()
        self.connectServerLoop()

    def run(self):
        recvThd = Thread(target=self.recvThd)
        recvThd.start()

    def recvThd(self):
        while True:
            try:
                response, _ = self.socket.recvfrom(1024)
                message: Message = Message.deserialize(response)
                self.forward(message)
            except Exception as e:
                print(f"Error: {e}")

    def connectServerLoop(self):
        while True:
            c, addr = self.socket.accept()
            print(f"Connected to {addr[1]}")
            self.servers[addr[1]] = c

    def sendToServer(self, msg: Message, dest: str):
        if self.servers.get(dest) is not None:
            self.servers[dest].send(msg.serialize())
        else:
            raise Exception(f"Server {dest} not found")
   
    def sendToSwitch(self, msg: Message, dest: str):
        raise NotImplementedError
        # if self.topology.get(dest) is not None:
        #     self.topology

    def forward(self, msg: Message, dest: str):
        if dest.split('_')[0] == self.name:
            self.sendToServer(msg, dest)
        else:
            self.sendToSwitch(msg, dest)

if __name__=="__main__":
    switch = Switch(sys.argv[1], {})