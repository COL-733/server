import threading

from message import MessageType, Message
from typing import Any
from config import config
from storage import VectorClock, VersionedValue

class Operation:
    def __init__(self, thread: threading.Thread, msg: Message, isCord: bool, res: list[Any] = set(), acks: int = 0):
        # threading
        self.thread: threading.Thread = thread
        self.cv: threading.Condition = threading.Condition()
        self.lock: threading.Lock = threading.Lock()

        # metadata
        self.id: int = msg.id
        self.key: int = msg.kwargs["key"]
        self.source: str = msg.source
        self.type: MessageType = msg.msg_type
        self.isCord: bool = isCord

        # To maintain the responses
        self.acks: int = acks # Initialize with 1 for own ack
        self.resList: set[VersionedValue] = res # for get initialize with own response
    
    def start(self) -> None:
        """Start the Opeartion thread."""
        self.thread.start()

    def inc_ack(self) -> None:
        """Increase the acknowledgement count."""
        self.acks += 1
    
    def add_response(self, res: set[VersionedValue]) -> None:
        """Add get response to the response list."""
        self.resList.union(res)
    
    def handle_response(self, res: set[VersionedValue] = None) -> None:
        """Handle the response according to type."""
        with self.lock:
            if self.type == MessageType.PUT:
                self.inc_ack()
            elif self.type == MessageType.GET:
                self.add_response(res)
            
            done = not self.isCord or (
                (self.type == MessageType.GET and len(self.resList) >= config.R) or
                (self.type == MessageType.PUT and self.acks >= config.W)
            )
            if done:
                self.cv.notify()

    def syn_reconcile(self) -> None:
        """Perform syntactic reconcilation if possible."""
        if self.type == MessageType.GET:
            # Only reconcile if GET
            raise NotImplementedError
    
    def response_msg(self, server_name: str, destination: str) -> Message:
        """Make response message for the Preference List nodes."""
        response = {"res": self.resList} if self.type == MessageType.GET else {}

        msg_type = MessageType.GET_RES if self.type == MessageType.GET else MessageType.PUT_ACK

        msg_type = MessageType.GET_KEY if self.type == MessageType.GET else MessageType.PUT_KEY
        
        return Message(self.id, msg_type, server_name, destination, response)

    def reply_msg(self, server_name: str) -> Message:
        """Make reply message for the source nodes."""
        response = {"res": self.resList} if self.type == MessageType.GET else {}
        msg_type = MessageType.GET_RES if self.type == MessageType.GET else MessageType.PUT_ACK
        return Message(self.id, msg_type, server_name, self.source, response)