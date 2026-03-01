import torch

from spargel_lm.layer.embedding import Embedding


def test_embedding_shape():
    layer = Embedding(3, 2)

    input = torch.tensor([0, 2, 1])
    output = layer(input)
    assert output.shape == (3, 2)

    input = torch.tensor([1, 0, 2, 1, 1, 2, 0])
    output = layer(input)
    assert output.shape == (7, 2)
