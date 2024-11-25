import random
from threading import Lock
import hashlib
from sortedcontainers import SortedSet
from config import config
import json

class VirtualNode: pass
class Ring: pass

class VirtualNode:
    def __init__(self, server: str, pos=None):
        self.server: str = server
        if not pos: pos = random.randint(0, config.Q)
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
    def __init__(self, state, versions=None):
        self.state: SortedSet[VirtualNode] = SortedSet(state)
        self.serverName: str
        self.serverSet: set[str] = set()
        self.versions: dict[str, int] = versions

    def __repr__(self):
        return str({
            'state': [str(node) for node in self.state],
            'versions': self.versions
            })
        
    def merge(self, ring: Ring):
        updatedServers = set()
        addedNodes = set()
        deletedNodes = set()

        for s, v in ring.versions.items():
            if self.versions.get(s) is None or self.versions[s] < v:
                updatedServers.add(s)
        
        if not updatedServers: return set(), set()
        
        for node in self.state:
            if node.server not in updatedServers:
                continue
            node: VirtualNode
            if node not in ring.state:
                self.state.remove(node)
                deletedNodes.add(node)
        
        for node in ring.state:
            if node.server not in updatedServers:
                continue
            node: VirtualNode
            if node not in self.state:
                self.state.add(node)
                addedNodes.add(node)

        for server in updatedServers:
            self.versions[server] = ring.versions[server]

        self.serverSet = self.serverSet.union(updatedServers)

        return addedNodes, deletedNodes

    def serialize(self):
        return ({
            'state': [[vNode.server, vNode.pos] for vNode in self.state],
            'versions': self.versions
            })

    @staticmethod
    def deserialize(dic):
        state = [VirtualNode(server, pos) for server, pos in dic['state']]
        version = dic['versions']
        return Ring(state, version)

    
    def init(self, serverName, numTokens, seeds):
        self.serverName = serverName
        self.serverSet = set(seeds)
        self.serverSet.discard(serverName)
        if self.versions is None:
            self.versions = {self.serverName: 1}
        for _ in range(numTokens):
            self.state.add(VirtualNode(serverName))

    def load(self, dic):
        self.state = SortedSet([VirtualNode(server, pos) for server, pos in dic['state']])
        self.versions = dic['versions']
        for s in self.state:
            self.serverSet.add(s.server)

    def _hash(self, key: str):
        md5 = hashlib.md5(key.encode())
        return int(md5.hexdigest(), 16) % config.Q

    def check_key(self, key: int):
        return self.serverName in set(self.getPrefList(str(key)))    

    def getPrefList(self, key: str) -> list[str]:
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
        while len(prefList) < config.N:
            idx = (coord+i) % totalTokens
            v = self.state[idx]
            if v.server not in uniqueNodes:
                uniqueNodes.add(v.server)
                prefList.append(idx)
            i += 1
            # if i loops over, break
            if i > config.N:
                break
        
        return [self.state[v].server for v in prefList]

    def add(self, pos):
        self.state.add(VirtualNode(self.serverName, pos))
        self.versions[self.serverName] += 1
    
    def delete(self, pos):
        n = VirtualNode(self.serverName, pos)
        if n in self.state:
            self.state.remove(n)
            self.versions[self.serverName] += 1
    