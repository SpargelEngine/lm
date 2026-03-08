from typing import final, override

import torch
from torch import nn


@final
class RMSNorm(nn.Module):
    """
    Root Mean Square Normalization (arXiv:1910.07467).

    Attributes:
        dimension: Dimension of input tensor.
        epsilon: Epsilon value for numerical stability.
    """

    def __init__(self, dimension: int, epsilon: float = 1e-6):
        super().__init__()
        self.epsilon = epsilon
        self.dimension = dimension
        self.weight = nn.Parameter(torch.ones(self.dimension))

    @override
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        var = x.square().mean(-1, keepdim=True)
        y = x * torch.rsqrt(var + self.epsilon)
        y = self.weight * y
        return y
