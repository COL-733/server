import random
from threading import Lock
import hashlib
from sortedcontainers import SortedSet
from config import config
import json

class VirtualNode: pass
class Ring: pass

class VirtualNode:
    def __init__(self, server: str, pos=random.randint(0, config.Q)):
        self.server: str = server
        self.pos: int = pos

    def __lt__(self, other: VirtualNode) -> bool:
        return self.pos < other.pos
    
    def __hash__(self):
        return self.pos

class Ring:
    def __init__(self, state):
        self.state: SortedSet[VirtualNode] = SortedSet(state)
        self.lock = Lock()
        self.serverName: str
        self.serverSet: set[str] = set()

    def merge(self, ring: Ring):
        for node in ring.state:
            self.state.add(node)
            if node.server != self.serverName:
                self.serverSet.add(node.server)

    def serialize(self):
        return list([vNode.server, vNode.pos] for vNode in self.state)
    
    def init(self, serverName, numTokens, seeds):
        self.serverName = serverName
        self.serverSet = set(seeds)
        self.serverSet.discard(serverName)
        for _ in range(numTokens):
            self.state.add(VirtualNode(serverName))


    def _hash(self, key: str):
        md5 = hashlib.md5(key.encode())
        return int(md5.hexdigest(), 16)

    def getPrefList(self, key: str) -> list[str]:
        self.lock.acquire()
        totalTokens = len(self.state)
        hash = self._hash(key)

        coord = 0
        for i in range(totalTokens):
            if self.state[i].pos >= hash:
                coord = i
                break
        
        uniqueNodes = set()
        prefList = []
        i = 0
        while len(prefList) < self.N:
            idx = (coord+i) % totalTokens
            v = self.state[idx]
            if v.server not in uniqueNodes:
                uniqueNodes.add(v.server)
                prefList.append(idx)
            
            # if i loops over, break
            if i > config.N:
                break
        
        self.lock.release()
        return prefList

    