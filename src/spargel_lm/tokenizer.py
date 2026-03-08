from typing import Sequence, override


class Tokenizer:
    """Base class for tokenizers"""

    def encode(self, s: str) -> Sequence[int]: ...

    def decode(self, tokens: Sequence[int]) -> str: ...


class UnicodeTokenizer(Tokenizer):
    chars: Sequence[str]

    _ctoi: dict[str, int]
    _itoc: dict[int, str]

    def __init__(self, chars: Sequence[str]):
        self.chars = chars

        self._ctoi = {ch: i for i, ch in enumerate(chars)}
        self._itoc = {i: ch for i, ch in enumerate(chars)}

    @override
    def encode(self, s):
        return [self._ctoi[ch] for ch in s]

    @override
    def decode(self, tokens):
        return "".join([self._itoc[token] for token in tokens])
