import torch
from torch import nn

from spargel_lm.layer.embedding import Embedding


def test_embedding_matches_torch_embedding():
    layer = Embedding(vocab_size=5, dim=3).double()
    reference = nn.Embedding(num_embeddings=5, embedding_dim=3).double()

    with torch.no_grad():
        reference.weight.copy_(layer.weight)

    input_ids = torch.tensor([[0, 2, 1], [4, 1, 3]])
    torch.testing.assert_close(layer(input_ids), reference(input_ids))
