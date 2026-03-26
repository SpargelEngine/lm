import torch
from torch import nn

from spargel_lm.layer.rms_norm import RMSNorm


def test_rms_norm_matches_torch_rms_norm():
    layer = RMSNorm(dimension=4, epsilon=1e-5).double()
    reference = nn.RMSNorm(normalized_shape=4, eps=1e-5).double()

    with torch.no_grad():
        reference.weight.copy_(layer.weight)

    x = torch.randn(2, 3, 4, dtype=torch.float64)
    torch.testing.assert_close(layer(x), reference(x))


def test_rms_norm_zero_input_is_finite():
    layer = RMSNorm(dimension=5, epsilon=1e-6)

    x = torch.zeros(4, 5)
    y = layer(x)

    assert torch.all(torch.isfinite(y))
    assert torch.equal(y, torch.zeros_like(y))
