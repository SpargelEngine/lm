from typing import Callable, Protocol, Self
import torch


class _Module[**P, R](Protocol):
    """
    Protocol allowing us to unwrap `forward`.
    """

    def forward(self: Self, *args: P.args, **kwargs: P.kwargs) -> R: ...


def apply_module[**P, R](m: _Module[P, R]) -> Callable[P, R]:
    """
    Returns the provided module unchanged, but with type hints preserved.

    Args:
        m: An instance of a subclass of `torch.nn.Module` to apply.

    Returns:
        m unchanged.
    """
    # Optional
    assert issubclass(type(m), torch.nn.Module), (
        f"{type(m)} is not a subclass of torch.nn.Module"
    )
    return m  # pyright: ignore[reportReturnType]
