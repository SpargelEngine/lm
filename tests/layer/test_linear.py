import torch
from torch import nn

from spargel_lm.layer.linear import Linear


def test_linear_matches_torch_linear_without_bias():
    layer = Linear(input_dimension=3, output_dimension=4, bias=False).double()
    reference = nn.Linear(in_features=3, out_features=4, bias=False).double()

    with torch.no_grad():
        reference.weight.copy_(layer.weight)

    x = torch.randn(2, 5, 3, dtype=torch.float64)
    torch.testing.assert_close(layer(x), reference(x))


def test_linear_matches_torch_linear_with_bias():
    layer = Linear(input_dimension=3, output_dimension=4, bias=True).double()
    reference = nn.Linear(in_features=3, out_features=4, bias=True).double()

    with torch.no_grad():
        reference.weight.copy_(layer.weight)
        reference.bias.copy_(layer.bias)

    x = torch.randn(2, 5, 3, dtype=torch.float64)
    torch.testing.assert_close(layer(x), reference(x))
