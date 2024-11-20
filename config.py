from typing import Final

SWITCH_PORT: Final[int] = 2000
LB_TO_SWITCH_PORT: Final[int] = 3000
LB_PORT: Final[int] = 4000

BUFFER_SIZE: Final[int] = 1 << 16

class Config:
    def __init__(self, N, R, W, Q, T):
        self.N = N
        self.R = R
        self.W = W
        self.Q = Q
        self.T = T

config = Config(
    N = 3,
    R = 3,
    W = 3,
    Q = (1 << 128) - 1,     # number of keys in the ring
    T = 5,                  # initial number of tokens of a server
)