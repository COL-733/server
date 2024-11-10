import sys
import socket
import time
from multiprocessing import Process
from message import Message, MessageType

LB_PORT = 4000
SWITCH_PORT = 2000

class LoadBalancer(Process):
    def __init__(self, switch_name: str, switch_ip: str, port: int):
        super(LoadBalancer, self).__init__()

        self.name: str = f'{switch_name}_{port}'
        self.switch_name: str = switch_name
        self.switch_ip: str = switch_ip
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connectSwitch()

    def connectSwitch(self) -> None: # Connect to switch
        while True:
            try:
                self.socket.bind(('', LB_PORT))
                self.socket.connect(('', SWITCH_PORT))
                break
            except ConnectionRefusedError:
                time.sleep(0.2)
    
    def send(self, msg: Message):
        try:
            self.socket.send(msg.serialize())
        except:
            raise Exception(f"Couldn't send to switch")
    
    def send_test(self, id, dest):
        testMessage = Message(id, MessageType.GET, self.name, dest)
        self.send(testMessage)

if __name__=="__main__":
    lb = LoadBalancer(sys.argv[1], '', SWITCH_PORT)
    while True:
        id, dest = input().split()
        lb.send_test(id, dest)
    