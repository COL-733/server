import threading

from message import MessageType, Message
from typing import Any
from config import config
from storage import VectorClock, VersionedValue

class Operation:
    def __init__(self, thread: threading.Thread, msg: Message, isCord: bool, res: set[VersionedValue] = set(), value: VersionedValue = None):
        # threading
        self.thread: threading.Thread = thread
        self.cv: threading.Condition = threading.Condition()
        self.lock: threading.Lock = threading.Lock()

        # metadata
        self.id: int = msg.id
        self.key: str = msg.kwargs["key"]
        self.source: str = msg.source
        self.type: MessageType = msg.msg_type
        self.isCord: bool = isCord
        self.value: VersionedValue = value

        # To maintain the responses
        self.acks: int = 0
        self.resList: set[VersionedValue] = res
        self.res: int = 0
    
    def start(self) -> None:
        """Start the Opeartion thread."""
        self.thread.start()

    def inc_ack(self) -> None:
        """Increase the acknowledgement count."""
        self.acks += 1
    
    def add_response(self, res: set[VersionedValue]) -> None:
        """Add get response to the response list."""
        if res is not None:
            self.resList.union(res)
        self.res += 1
    
    def handle_response(self, res: set[VersionedValue] = None) -> None:
        """Handle the response according to type."""
        with self.lock:
            if self.type == MessageType.PUT:
                self.inc_ack()
            elif self.type == MessageType.GET:
                self.add_response(res)
            
            done = not self.isCord or (
                (self.type == MessageType.GET and self.res >= config.R - 1) or
                (self.type == MessageType.PUT and self.acks >= config.W - 1)
            )
            if done:
                with self.cv:
                    self.cv.notify()

    def syn_reconcile(self) -> None:
        """Perform syntactic reconcilation if possible."""
        if self.type == MessageType.GET:
            # Only reconcile if GET
            pass
    
    def response_msg(self, server_name: str, destination: str) -> Message:
        """Make response message for the Preference List nodes."""
        response = {"key":self.key, "value": self.value.value, "context":self.value.vector_clock.to_dict()} if self.type == MessageType.PUT else {"key":self.key}
        msg_type = MessageType.GET_KEY if self.type == MessageType.GET else MessageType.PUT_KEY
        
        return Message(self.id, msg_type, server_name, destination, response)

    def serialize_res(self, set: set[VersionedValue]) -> list[list]:
        """Make GET RESPONSES serializable.
        list[ list[value], list[vector_clocks] ]
        """
        serialized = []
        value = []
        vector_clocks = []
        for versioned_value in set:
            value.append(versioned_value.value)
            vector_clocks.append(versioned_value.vector_clock.to_dict())

        serialized.append(value)
        serialized.append(vector_clocks)
        return serialized
    
    def reply_msg(self, server_name: str) -> Message:
        """Make reply message for the source nodes."""
        res = self.serialize_res(self.resList) if self.resList else None
        response = {"key":self.key, "res": res} if self.type == MessageType.GET else {"key":self.key, "context":[self.value.vector_clock.to_dict()]}
        msg_type = MessageType.GET_RES if self.type == MessageType.GET else MessageType.PUT_ACK
        return Message(self.id, msg_type, server_name, self.source, response)