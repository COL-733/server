import random
from sortedcontainers import SortedList # type: ignore
from server import ServerInfo, Server

MAX = (1 << 128) - 1

class VirtualNode:
    pass

class VirtualNode:
    def __init__(self, server: str, pos: int):
        self.server: str = server
        self.pos: int = pos

    def __lt__(self, other: VirtualNode) -> bool:
        return self.pos < other.pos

class Ring:
    def __init__(self, numTokens, name):
        self.state: SortedList[VirtualNode] = SortedList()
        self.name = name

        for _ in range(numTokens):
            self.state.add(VirtualNode(name, random.randint(0, MAX)))
    