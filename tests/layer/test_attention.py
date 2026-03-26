import torch
from torch import nn

from spargel_lm.layer.attention import MultiHeadAttention


def _build_attention_mask(
    batch_size: int,
    seq_len: int,
    num_heads: int,
    causal: bool,
    padding_mask: torch.Tensor | None = None,
) -> torch.Tensor | None:
    if not causal and padding_mask is None:
        return None

    attention_mask = torch.zeros(batch_size, seq_len, seq_len, dtype=torch.bool)

    if causal:
        attention_mask |= torch.triu(
            torch.ones(seq_len, seq_len, dtype=torch.bool), diagonal=1
        )

    if padding_mask is not None:
        attention_mask |= padding_mask[:, None, :] | padding_mask[:, :, None]

    return attention_mask.repeat_interleave(num_heads, dim=0)


def _make_reference_module(layer: MultiHeadAttention) -> nn.MultiheadAttention:
    reference = nn.MultiheadAttention(
        embed_dim=layer.dim,
        num_heads=layer.num_heads,
        dropout=0.0,
        bias=False,
        batch_first=True,
    ).double()

    with torch.no_grad():
        reference.in_proj_weight.copy_(
            torch.cat([layer.W_q.weight, layer.W_k.weight, layer.W_v.weight], dim=0)
        )
        reference.out_proj.weight.copy_(layer.W_o.weight)

    return reference


def test_multi_head_attention_matches_torch_multihead_attention():
    layer = MultiHeadAttention(dim=8, num_heads=2, qk_head_dim=4, v_head_dim=4).double()
    reference = _make_reference_module(layer)

    x = torch.randn(2, 4, 8, dtype=torch.float64)

    actual = layer(x, causal=False)
    expected, _ = reference(x, x, x, need_weights=False)

    torch.testing.assert_close(actual, expected)


def test_multi_head_attention_matches_torch_multihead_attention_with_masks():
    layer = MultiHeadAttention(dim=8, num_heads=2, qk_head_dim=4, v_head_dim=4).double()
    reference = _make_reference_module(layer)

    x = torch.randn(2, 5, 8, dtype=torch.float64)
    padding_mask = torch.tensor(
        [[False, False, False, True, True], [False, True, False, False, True]]
    )
    attention_mask = _build_attention_mask(
        batch_size=x.size(0),
        seq_len=x.size(1),
        num_heads=layer.num_heads,
        causal=True,
        padding_mask=padding_mask,
    )

    actual = layer(x, causal=True, mask=padding_mask)
    expected, _ = reference(x, x, x, attn_mask=attention_mask, need_weights=False)

    torch.testing.assert_close(actual, expected)
