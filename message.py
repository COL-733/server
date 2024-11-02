from enum import Enum

class Cmd(Enum):
    PUT = 1
    GET = 2
    GOSSIP_REQUEST = 3
    GOSSIP_RESPONSE = 4
    HINTED_REPLICA = 5
    
