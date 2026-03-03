import torch
from spargel_lm.layer.attention import generic_attention

q = torch.ones(2, 2)
k = torch.ones(2, 2)
v = torch.ones(2, 2)

mask = torch.tensor(
    [[False, True],
     [True, True]],
    dtype=torch.bool,
)

generic_attention(q, k, v)
x = generic_attention(q, k, v, mask=mask)
print(x)
print(x.masked_fill(mask, 0.0))
