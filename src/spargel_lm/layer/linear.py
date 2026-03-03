from typing import Literal, final, override
import torch
import torch.nn.functional as F
from torch import nn

type LinearInit = Literal["empty", "uniform"]


@final
class Linear(nn.Module):
    """
    Linear Layer.

    Attributes:
        input_dimension: Dimension of input.
        output_dimension: Dimension of output.
        initialize: Parameter initialization.
    """

    def __init__(
        self,
        input_dimension: int,
        output_dimension: int,
        initialize: LinearInit = "uniform",
    ):
        super().__init__()
        self.input_dimension = input_dimension
        self.output_dimension = output_dimension
        self.initialize = initialize
        self.weight = nn.Parameter(
            torch.empty(self.output_dimension, self.input_dimension)
        )
        self.reset_parameters()

    def reset_parameters(self):
        if self.initialize == "uniform":
            # TODO(tianjiao): We should make `a` and `b` configurable.
            nn.init.uniform_(self.weight, a=-1.0, b=1.0)

    @override
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = F.linear(x, self.weight)
        return y
