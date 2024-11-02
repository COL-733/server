from multiprocessing import Process
from ring import Ring
from message import Cmd
from storage import Storage
# from collections import deque
from queue import Queue
    
class ServerInfo:
    def __init__(self, name: str, ip: str, switch_ip: str , port: int):
        self.ip: str = ip
        self.switch: str = switch_ip # or directly socket
        self.name: str = name
        self.port: int = port
        
class Server(Process):
    def __init__(self, info: ServerInfo, ring: Ring):
        super(Server, self).__init__(name=info.name)
        self.info: ServerInfo = info
        self.ring: Ring = ring
        self.storage: Storage = Storage('localhost','root','Jdsp9595@',f'{self.info.name}_storage')
        self.cmdQueue: Queue[Cmd] = [] 

    def _recv():
        pass

    def _processRequests():
        pass

    def _cmd_handler():
        pass

    def _get():
        pass

    def _set():
        pass

    def _send(): # Thread
        pass

    def gossip(): # Thread
        pass

    def sendHintedHandoffs(): # Thread
        pass
