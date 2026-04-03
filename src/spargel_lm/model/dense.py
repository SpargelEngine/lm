from typing import final, override

import torch
from torch import nn

from spargel_lm.layer.attention import MultiHeadAttention
from spargel_lm.layer.embedding import Embedding
from spargel_lm.layer.feed_forward import FeedForward
from spargel_lm.layer.rms_norm import RMSNorm
from spargel_lm.torch_typing import apply_module


@final
class DenseBlock(nn.Module):
    def __init__(
        self,
        hidden_dim: int,
        num_heads: int,
        head_dim: int,
        mlp_dim: int,
        causal: bool = True,
    ):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.mlp_dim = mlp_dim
        self.causal = causal

        self.prenorm_1 = RMSNorm(dimension=self.hidden_dim)
        self.prenorm_2 = RMSNorm(dimension=self.hidden_dim)

        self.attention = MultiHeadAttention(
            feature_dim=self.hidden_dim,
            num_heads=self.num_heads,
            qk_dim=self.head_dim,
            v_dim=self.head_dim,
        )
        self.feed_forward = FeedForward(
            dimension=self.hidden_dim, hidden_dimension=self.mlp_dim
        )

    @override
    def forward(self, x: torch.Tensor, mask: torch.Tensor | None = None):
        """
        Args:
            x (batch_size, seq_len, hidden_dim)
            mask (batch_size, seq_len)
        """
        y = x + apply_module(self.attention)(
            apply_module(self.prenorm_1)(x),
            causal=self.causal,
            mask=mask,
        )
        y = y + apply_module(self.feed_forward)(apply_module(self.prenorm_2)(y))
        return y


@final
class DenseModel(nn.Module):
    """
    Basic Dense Decoder-Only Transformer with MHA.

    Attributes:
        vocab_size: Size of vocabulary.
        hidden_dim: Dimension of embedding.
        num_blocks: Number of transformer blocks.
        num_heads: Number of attention heads.
        head_dim: Dimension per attention head.
        mlp_dim: Hidden dimension of feed-forward layer.
        max_position_embeddings: Number of learned absolute positions.
        causal: Use causal mask or not.
    """

    def __init__(
        self,
        vocab_size: int,
        hidden_dim: int,
        num_blocks: int,
        num_heads: int,
        head_dim: int,
        mlp_dim: int,
        max_position_embeddings: int = 2048,
        causal: bool = True,
    ):
        super().__init__()
        self.vocab_size = vocab_size
        self.hidden_dim = hidden_dim
        self.num_blocks = num_blocks
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.mlp_dim = mlp_dim
        self.max_position_embeddings = max_position_embeddings
        self.causal = causal

        self.embedding = Embedding(vocab_size=self.vocab_size, dim=self.hidden_dim)
        self.pos_embedding = Embedding(
            vocab_size=self.max_position_embeddings, dim=self.hidden_dim
        )
        self.blocks = nn.ModuleList(
            [
                DenseBlock(
                    hidden_dim=self.hidden_dim,
                    num_heads=self.num_heads,
                    head_dim=self.head_dim,
                    mlp_dim=self.mlp_dim,
                    causal=self.causal,
                )
                for _ in range(0, self.num_blocks)
            ]
        )
        # TODO(tianjiao): Use `bias` or not.
        self.proj = nn.Linear(
            self.hidden_dim,
            self.vocab_size,
            bias=False,
        )

    @override
    def forward(self, x: torch.Tensor, mask: torch.Tensor | None = None):
        seq_len = x.size(1)
        if seq_len > self.max_position_embeddings:
            raise ValueError(
                f"Sequence length {seq_len} exceeds "
                f"max_position_embeddings={self.max_position_embeddings}."
            )

        y = apply_module(self.embedding)(x)
        positions = torch.arange(seq_len, device=x.device, dtype=torch.long)[None, :]
        y = y + apply_module(self.pos_embedding)(positions)

        for block in self.blocks:
            y = apply_module(block)(y, mask=mask)

        # TODO(tianjiao): RMSNorm here?

        y = apply_module(self.proj)(y)

        return y
