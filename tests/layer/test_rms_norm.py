import torch
import torch.nn.functional as F

from spargel_lm.layer.rms_norm import RMSNorm


def test_rms_norm_preserves_input_shape():
    layer = RMSNorm(dimension=4, epsilon=1e-5)

    x = torch.randn(2, 3, 4)
    y = layer(x)

    assert y.shape == x.shape


def test_rms_norm_matches_reference_formula():
    layer = RMSNorm(dimension=3, epsilon=1e-5)
    layer.weight.data = torch.tensor([1.5, 2.0, 0.5])

    x = torch.tensor([[1.0, 2.0, 3.0], [0.5, -1.0, 2.0]])
    y = layer(x)

    expected = F.rms_norm(
        x,
        normalized_shape=(layer.dimension,),
        weight=layer.weight,
        eps=layer.epsilon,
    )

    assert torch.allclose(y, expected)


def test_rms_norm_zero_input_is_finite():
    layer = RMSNorm(dimension=5, epsilon=1e-6)

    x = torch.zeros(4, 5)
    y = layer(x)

    assert torch.all(torch.isfinite(y))
    assert torch.equal(y, torch.zeros_like(y))
