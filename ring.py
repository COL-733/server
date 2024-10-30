from sortedcontainers import SortedList # type: ignore
from server import ServerInfo


class VirtualNode:
    pass

class VirtualNode:
    def __init__(self, server: ServerInfo, pos: int):
        self.server: ServerInfo = server
        self.pos: int = pos

    def move(self, pos: int):
        self.pos = pos
        # TODO: Move Keys

    def __lt__(self, other: VirtualNode) -> bool:
        return self.pos < other.pos

class Ring:
    def __init__(self):
        self.state: SortedList[VirtualNode] = SortedList()
        self.servers: set[ServerInfo] = set()
        