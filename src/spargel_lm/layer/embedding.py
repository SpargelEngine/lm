import torch
import torch.nn.functional as F
from torch import nn


class Embedding(nn.Module):
    """
    Embedding Layer.

    Attributes:
        vocab_size: Vocabulary size.
        dim: Dimension of embedding.
    """
    def __init__(self, vocab_size: int, dim: int):
        super().__init__()
        self.vocab_size = vocab_size
        self.dim = dim
        self.weight = nn.Parameter(torch.empty(self.vocab_size, self.dim))
        self.reset_parameters()

    def reset_parameters(self):
        nn.init.normal_(self.weight, mean=0.0, std=1.0)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = F.embedding(x, self.weight)
        return y
