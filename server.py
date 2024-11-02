from multiprocessing import Process
from ring import Ring
from message import MessageType
from storage import Storage
import socket
# from collections import deque
import queue
    
class ServerInfo:
    def __init__(self, name: str, ip: str, switch_ip: str , port: int):
        self.ip: str = ip
        self.switch: str = switch_ip # or directly socket
        self.name: str = name
        self.port: int = port
        
class Server(Process):
    def __init__(self, switch_name: str, switch_ip: str):
        super(Server, self).__init__()
        self.name: str
        self.switch: str = switch_ip
        self.socket: socket.socket 
        self.ring: Ring
        self.storage: Storage = Storage('localhost','root','Jdsp9595@',f'{self.info.name}_storage')
        self.cmdQueue: queue[MessageType] 

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

    def run():
        # Start the server process
        cmd_q = queue.
        # Start the recv thread
        # Start the cmd handler thread.
        pass


