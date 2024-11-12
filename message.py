import json
import struct
from ring import VirtualNode, Ring
from enum import IntEnum
from config import BUFFER_SIZE
from typing import Any

class MessageType(IntEnum):

    GET     = 1 # From SUB_COORDINATOR to READ_COORDINATOR or Client to SUB_COORDINATOR/WRITE_COORDINATOR
    GET_KEY = 2 # From READ_COORDINATOR to Node
    GET_RES = 3

    PUT     = 4 # From SUB_COORDINATOR to WRITE_COORDINATOR or Client to SUB_COORDINATOR/WRITE_COORDINATOR
    PUT_KEY = 5 # From WRITE_COORDINATOR to Node
    PUT_ACK = 6

    GOSSIP_REQ = 7
    GOSSIP_RES = 8

    HR_REQ = 9
    HR_RES = 10

    # Server to Switch
    ADD_SERVER_REQ = 20
    ADD_SERVER_RES = 21

    ERROR = 22
    
    
class Message:

    def __init__(self, id: int, msg_type: MessageType, source: str, dest: str, kwargs: dict[str, Any] = None):
        self.id = id
        self.msg_type = msg_type
        self.source = source
        self.dest = dest
        self.kwargs = kwargs if kwargs else {} 

    def __repr__(self):
        return f"{self.msg_type.name}, From: {self.source}, To: {self.dest}, ID: {self.id}"

    def __repr__(self):
        return f"{self.msg_type.name}, From: {self.source}, To: {self.dest}, ID: {self.id}"

    def serialize(self) -> bytes:
        # Convert message to a dictionary
        message_dict = {
            "id": self.id,
            "type": self.msg_type,
            "source": self.source,
            "dest": self.dest
        }

        if self.kwargs.get('ring') is not None:
            self.kwargs['ring'] = self.kwargs['ring'].serialize()

        for k,v in self.kwargs.items():
            message_dict[k] = v

        message_json = json.dumps(message_dict)
        message_bytes = message_json.encode('utf-8')
        message_length = len(message_bytes)
        full_message = struct.pack('!I', message_length) + message_bytes
        padded_message = full_message.ljust(BUFFER_SIZE, b'\x00')
        return padded_message

    @staticmethod
    def deserialize(message_bytes: bytes) -> 'Message':
        try:
            message_length = struct.unpack('!I', message_bytes[:4])[0]
            message_json = message_bytes[4:4 + message_length].decode('utf-8')
            message_dict = json.loads(message_json)
            msg_type = MessageType(message_dict["type"])
            id = message_dict["id"]
            source = message_dict["source"]
            dest = message_dict["dest"]
            kwargs = {k: v for k, v in message_dict.items() if k not in {"type", "source", "id", "dest", "ring"}}

            if message_dict.get('ring') is not None:
                state = [VirtualNode(server, pos) for server, pos in message_dict['ring']]
                kwargs['ring'] = Ring(state)
        except Exception as e:
            raise Exception(f"Deserialize Error: {e}, Message: {message_bytes}")

        return Message(id, msg_type, source, dest, **kwargs)