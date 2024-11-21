from typing import Final

SWITCH_PORT: Final[int] = 2000
LB_TO_SWITCH_PORT: Final[int] = 3000
LB_PORT: Final[int] = 4000

BUFFER_SIZE: Final[int] = 1024

class Config:
    def __init__(self, N, R, W, Q, T, G, I):
        self.N = N
        self.R = R
        self.W = W
        self.Q = Q      # number of keys in the ring
        self.T = T      # initial number of tokens of a server
        self.G = G      # Number of servers to gossip with
        self.I = I      # Gossip interval

config = Config(
    N = 3,
    R = 3,
    W = 3,
    Q = 1024,
    T = 5,             
    G = 1,             
    I = 3,             
)