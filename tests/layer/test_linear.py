import torch

from spargel_lm.layer.linear import Linear


def test_linear_shape():
    layer = Linear(input_dimension=3, output_dimension=4)

    x = torch.randn(2, 5, 3)
    y = layer(x)

    assert y.shape == (2, 5, 4)
