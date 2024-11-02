import json
import struct
from enum import Enum

class MessageType(Enum):
    PUT = 1
    GET = 2
    GOSSIP_REQUEST = 3
    GOSSIP_RESPONSE = 4
    HINTED_REPLICA = 5


    ADD_SERVER_REQ = 20
    ADD_SERVER_RES = 21
    
    
class Message:
    BUFFER_SIZE = 1024

    def __init__(self, msg_type: MessageType, source: str, **kwargs):
        self.msg_type = msg_type
        self.source = source
        self.kwargs = kwargs

    def serialize(self) -> bytes:
        # Convert message to a dictionary
        message_dict = {
            "type": self.msg_type,
            "source": self.source,
        }
        for k,v in self.kwargs.items():
            message_dict[k] = v

        message_json = json.dumps(message_dict)
        message_bytes = message_json.encode('utf-8')
        message_length = len(message_bytes)
        full_message = struct.pack('!I', message_length) + message_bytes
        padded_message = full_message.ljust(self.BUFFER_SIZE, b'\x00')
        return padded_message

    @staticmethod
    def deserialize(message_bytes: bytes) -> 'Message':
        message_length = struct.unpack('!I', message_bytes[:4])[0]
        message_json = message_bytes[4:4 + message_length].decode('utf-8')
        message_dict = json.loads(message_json)
        msg_type = MessageType(message_dict["type"])
        source = message_dict["source"]
        kwargs = {k: v for k, v in message_dict.items() if k not in {"type", "source"}}

        return Message(msg_type, source, **kwargs)