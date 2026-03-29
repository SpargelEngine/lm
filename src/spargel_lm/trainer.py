from dataclasses import dataclass

import torch
import torch.nn as nn
import torch.optim as optim


@dataclass
class TrainStepResult:
    loss: float


def pretrain_step(
    optimizer: optim.Optimizer,
    model: nn.Module,
    inputs: torch.Tensor,
    labels: torch.Tensor,
):
    """
    Args:
        model : ...
        inputs : shape=(batch, seq_len)
        labels : shape=(batch, seq_len), dtype=long
    """
    criterion = nn.CrossEntropyLoss()
    logits = model(inputs)
    loss = criterion(logits.reshape(-1, logits.size(-1)), labels.reshape(-1))

    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    return TrainStepResult(loss=loss.item())
