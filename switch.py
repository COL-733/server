import socket
from message import Message
PORT = 2000

class Switch:
    def __init__(self, name, topology):
        self.name = name
        self.topology = topology
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', PORT))
        self.socket.listen(5)
        self.servers = dict()
        self.connectLoop()

    def connectLoop(self):
        while True:
            c, addr = self.socket.accept()
            self.servers[addr[1]] = c
