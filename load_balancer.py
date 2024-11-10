import socket
import time
from multiprocessing import Process

LB_PORT = 3000
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
                self.socket.bind(('', self.port))
                self.socket.connect(('', SWITCH_PORT))
                break
            except ConnectionRefusedError:
                time.sleep(0.2)