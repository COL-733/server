from multiprocessing import Process
from ring import Ring
from message import MessageType
from storage import Storage
import socket
# from collections import deque
import queue
import time

SWITCH_PORT = 2000
        
class Server(Process):
    def __init__(self, switch_name: str, switch_ip: str, port: int):
        self.name: str = f'{switch_name}_{port}'
        super(Server, self).__init__(info=self.name)

        self.name: str = f'{switch_name}_{port}'
        self.port: int = port
        self.switch_name: str = switch_name
        self.switch_ip: str = switch_ip

        # Connect to Switch
        self.socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connectSwitch()

        self.ring: Ring
        self.storage: Storage = Storage('localhost','root','Jdsp9595@',f'{self.name}_storage')
        self.cmdQueue: queue[MessageType]

    def connectSwitch(self) -> None: # Connect to switch
        while True:
            try:
                self.socket.connect('switch_ip',SWITCH_PORT)
                break
            except ConnectionRefusedError:
                time.sleep(0.2)

    def recv():
        pass

    def processRequests():
        pass

    def cmd_handler(): # thread
        pass

    def get():
        pass

    def put():
        pass

    def send():
        pass

    def gossip(): # Thread
        pass

    def sendHintedHandoffs(): # Thread
        pass

    def run(self):
        # command queue
        cmd_q = queue.Queue()



        # Start the recv thread
        # Start the cmd handler thread.
        pass


