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
        if self.pos < other.pos:
            return True
        elif self.pos == other.pos:
            return self.server < other.server
        else:
            return False
    
    def __eq__(self, other: VirtualNode):
        return self.pos == other.pos and self.server == other.server

    def __hash__(self):
        return self.pos

    def __repr__(self):
        return str((self.server, self.pos))

class Ring:
    def __init__(self, state):
        self.state: SortedSet[VirtualNode] = SortedSet(state)
        self.lock = Lock()
        self.serverName: str
        self.serverSet: set[str] = set()

    def __repr__(self):
        return str([str(node) for node in self.state])
        

    def merge(self, ring: Ring):
        newServers = []
        for node in ring.state:
            node: VirtualNode
            self.state.add(node)
            if node.server != self.serverName:
                if node.server not in self.serverSet: newServers.append(node.server)
                self.serverSet.add(node.server)
        return newServers

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
        return int(md5.hexdigest(), 16) % config.Q

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
    def get_current_position(self, key: str) -> int:
        self.lock.acquire()
        try:
            totalTokens = len(self.state)
            hash = self._hash(key)

            coord = 0
            for i in range(totalTokens):
                if self.state[i].pos >= hash:
                    coord = i
                    break
            uniqueNodes = set()
            i = 0
            last_idx = coord
            while len(uniqueNodes) < self.N:
                idx = (coord + i) % totalTokens
                v = self.state[idx]
                if v.server not in uniqueNodes:
                    uniqueNodes.add(v.server)
                    last_idx = idx
                
                i += 1
                if i > config.N:
                    break
            
            return (last_idx + 1) % totalTokens
            
        finally:
            self.lock.release()
    def give_next_hinted(self,i: int,key:str) -> str:
        self.lock.acquire()
        try:
            j = self.get_current_position(key)
            totalTokens = len(self.state)
            if i >= totalTokens:
                i = i % totalTokens
                
            target_idx = (j + i) % totalTokens
            return self.state[target_idx].server
        finally:
            self.lock.release()
    