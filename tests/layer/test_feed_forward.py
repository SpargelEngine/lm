import torch
from torch import nn

from spargel_lm.layer.feed_forward import FeedForward


def test_feed_forward_matches_torch_sequential():
    layer = FeedForward(dimension=4, hidden_dimension=6).double()
    reference = nn.Sequential(
        nn.Linear(in_features=4, out_features=6, bias=True),
        nn.ReLU(),
        nn.Linear(in_features=6, out_features=4, bias=True),
    ).double()

    with torch.no_grad():
        reference[0].weight.copy_(layer.linear_1.weight)
        reference[0].bias.copy_(layer.linear_1.bias)
        reference[2].weight.copy_(layer.linear_2.weight)
        reference[2].bias.copy_(layer.linear_2.bias)

    x = torch.randn(2, 3, 4, dtype=torch.float64)
    torch.testing.assert_close(layer(x), reference(x))
