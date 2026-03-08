import math
from typing import final, override

import torch
import torch.nn.functional as F
from torch import nn

from spargel_lm.layer.linear import Linear
from spargel_lm.torch_typing import apply_module


def generic_attention(
    q: torch.Tensor,
    k: torch.Tensor,
    v: torch.Tensor,
    softmax_scale: float | None = None,
    mask: torch.Tensor | None = None,
) -> torch.Tensor:
    """
    Compute (scaled) dot-product attention (arXiv:1706.03762v7).

    Args:
        q (..., num_q, qk_dim)
        k (..., num_kv, qk_dim)
        v (..., num_kv, v_dim)
        softmax_scale: The scaling factor before softmax.
        mask (..., num_q, num_kv) dtype=bool : `True` means masked

    Returns:
        (..., num_q, v_dim)
    """

    assert q.size(-1) == k.size(-1)
    assert k.size(-2) == v.size(-2)

    # shape: (..., num_q, num_kv)
    scores = torch.einsum("...ij, ...kj -> ...ik", q, k)

    if mask is not None:
        assert mask.dtype == torch.bool

        # `mask` should be broadcast-able to (..., num_q, num_kv).
        # TODO(tianjiao): The value should depend on dtype.
        scores = scores.masked_fill(mask, -torch.inf)

    if softmax_scale is not None:
        scores = scores * softmax_scale

    scores = F.softmax(scores, dim=-1)

    # scores = scores.nan_to_num(0.0)

    if mask is not None:
        scores = scores.masked_fill(mask, 0.0)

    # shape: (..., num_q, v_dim)
    result = torch.einsum("...ij, ...jk -> ...ik", scores, v)

    return result


@final
class MultiHeadAttention(nn.Module):
    """
    Multi-Head Attention (arXiv:1706.03762v7).

    Attributes:
        dim: Feature dimension.
        num_heads: Number of attention heads.
        qk_head_dim: Dimension of query/key.
        v_head_dim: Dimension of value.
    """

    def __init__(self, dim: int, num_heads: int, qk_head_dim: int, v_head_dim: int):
        super().__init__()
        self.dim = dim
        self.num_heads = num_heads
        self.qk_head_dim = qk_head_dim
        self.v_head_dim = v_head_dim

        self.softmax_scale = math.pow(self.qk_head_dim, -0.5)

        self.W_q = Linear(
            input_dimension=self.dim, output_dimension=self.num_heads * self.qk_head_dim
        )
        self.W_k = Linear(
            input_dimension=self.dim, output_dimension=self.num_heads * self.qk_head_dim
        )
        self.W_v = Linear(
            input_dimension=self.dim, output_dimension=self.num_heads * self.v_head_dim
        )
        self.W_o = Linear(
            input_dimension=self.num_heads * self.v_head_dim, output_dimension=self.dim
        )

    # TODO(tianjiao): Support block mask.
    @override
    def forward(
        self, x: torch.Tensor, causal: bool = True, mask: torch.Tensor | None = None
    ):
        """
        Args:
            x (batch_size, seq_len, dim): Input tensor.
            causal: Apply causal mask.
            mask (batch_size, seq_len) dtype=bool: `True` means masked. This is padding mask.
        """
        batch_size, seq_len, dim = x.size()
        assert dim == self.dim

        # q, k : (batch_size, seq_len, num_heads * qk_head_dim)
        # v : (batch_size, seq_len, num_heads * v_head_dim)
        q = apply_module(self.W_q)(x)
        k = apply_module(self.W_k)(x)
        v = apply_module(self.W_v)(x)

        # q, k : (batch_size, seq_len, num_heads, qk_head_dim)
        # v : (batch_size, seq_len, num_heads, v_head_dim)
        q = q.view(batch_size, seq_len, self.num_heads, self.qk_head_dim)
        k = k.view(batch_size, seq_len, self.num_heads, self.qk_head_dim)
        v = v.view(batch_size, seq_len, self.num_heads, self.v_head_dim)

        # q, k : (batch_size, num_heads, seq_len, qk_head_dim)
        # v : (batch_size, num_heads, seq_len, v_head_dim)
        q = q.transpose(1, 2)
        k = k.transpose(1, 2)
        v = v.transpose(1, 2)

        if causal or (mask is not None):
            # (1, 1, seq_len, seq_len)
            final_mask = torch.zeros(
                seq_len, seq_len, dtype=torch.bool, device=x.device
            )[None, None, :, :]

            if causal:
                # (1, 1, seq_len, seq_len)
                causal_mask = torch.triu(
                    torch.ones(seq_len, seq_len, dtype=torch.bool, device=x.device),
                    diagonal=1,
                )[None, None, :, :]
                final_mask = final_mask | causal_mask

            if mask is not None:
                # (batch_size, 1, seq_len, seq_len)
                mask = mask[:, None, None, :] | mask[:, None, :, None]
                final_mask = final_mask | mask
        else:
            final_mask = None

        # It happens that `num_q == num_kv`.
        # result : (batch_size, num_heads, seq_len, v_head_dim)
        result = generic_attention(
            q, k, v, softmax_scale=self.softmax_scale, mask=final_mask
        )

        # result : (batch_size, seq_len, num_heads, v_head_dim)
        result = result.transpose(1, 2)
        # NOTE: Use `reshape` since `transpose` can make tensor layout non-contiguous.
        # result : (batch_size, seq_len, num_heads * v_head_dim)
        result = result.reshape(batch_size, seq_len, self.num_heads * self.v_head_dim)

        return apply_module(self.W_o)(result)


class MultiQueryAttention(nn.Module):
    """
    Multi-Query Attention (arXiv:1911.02150v1).
    """

    ...


class GroupedQueryAttention(nn.Module):
    """
    Grouped-Query Attention (arXiv:2305.13245v3).
    """

    ...


class MultiLatentAttention(nn.Module):
    """
    Multi-Head Latent Attention (arXiv:2405.04434v5).
    """

    ...
