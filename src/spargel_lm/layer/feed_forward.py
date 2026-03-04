from typing import final, override
import torch
import torch.nn.functional as F
from torch import nn

from spargel_lm.layer.linear import Linear
from spargel_lm.torch_typing import apply_module

@final
class FeedForward(nn.Module):
    """
    Feed Forward (2-Layer Perceptron) (arXiv:1706.03762v7).

    Attributes:
        dimension: Dimension of input tensor.
        hidden_dimension: Dimension of hidden vector.
    """

    def __init__(self, dimension: int, hidden_dimension: int):
        super().__init__()
        self.dimension = dimension
        self.hidden_dimension = hidden_dimension
        self.linear_1 = Linear(input_dimension=self.dimension, output_dimension=self.hidden_dimension, bias=True)
        self.linear_2 = Linear(input_dimension=self.hidden_dimension, output_dimension=self.dimension, bias=True)

    @override
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = apply_module(self.linear_1)(x)
        y = F.relu(y)
        y = apply_module(self.linear_2)(y)
        return y
