import torch

from spargel_lm.model.dense import DenseModel
from spargel_lm.trainer import pretrain_step


def test_dense_model_can_overfit_a_single_example():
    torch.manual_seed(0)

    inputs = torch.tensor([[0, 1, 2, 3], [1, 3, 2, 2]], dtype=torch.long)
    labels = inputs.clone()

    model = DenseModel(
        vocab_size=8,
        hidden_dim=16,
        num_blocks=1,
        num_heads=1,
        head_dim=8,
        mlp_dim=32,
        max_position_embeddings=inputs.size(1),
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-2)

    final_loss = float("inf")
    for _ in range(50):
        result = pretrain_step(
            optimizer=optimizer,
            model=model,
            inputs=inputs,
            labels=labels,
        )
        final_loss = result.loss

    assert final_loss < 1e-3

    model.eval()
    with torch.no_grad():
        logits = model(inputs)

    predictions = logits.argmax(dim=-1)
    assert torch.equal(predictions, labels)
