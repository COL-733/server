from typing import Final

SWITCH_PORT: Final[int] = 2000
LB_TO_SWITCH_PORT: Final[int] = 3000
LB_PORT: Final[int] = 4000

class Config:
    def __init__(self, N, R, W):
        self.N = N
        self.R = R
        self.W = W

config = Config(
    N = 3,
    R = 3,
    W = 3,
)