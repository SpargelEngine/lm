from spargel_lm.bpe import (
    byte_pair_merge,
    decode,
    encode,
    merge,
    merge_,
    pair_counter,
    train_bpe,
)


def test_pair_counter():
    c = pair_counter([])
    assert not c.values()

    c = pair_counter([1])
    assert not c.values()

    c = pair_counter([1, 2])
    assert c.total() == 1
    assert c[(1, 2)] == 1

    c = pair_counter([1, 2, 3])
    assert c.total() == 2
    assert c[(1, 2)] == 1
    assert c[(2, 3)] == 1

    c = pair_counter([1, 1, 1])
    assert c.total() == 2
    assert c[(1, 1)] == 2


def test_merge():
    assert merge([1, 2, 3], (1, 2), 4) == [4, 3]


def test_merge_():
    x = [1, 2, 3]
    merge_(x, (1, 2), 4)
    assert x == [4, 3]


def test_train_bpe():
    vocab = train_bpe([b"abab", b"ab"], 257)
    assert len(vocab) == 257
    assert vocab[256] == b"ab"


def test_encode_roundtrip():
    vocab = train_bpe([b"abab"], 258, verbose=False)
    ranks = {token: idx for idx, token in vocab.items()}

    tokens = encode(b"ababab", ranks)

    assert tokens == [257, 256]
    assert decode(tokens, vocab) == b"ababab"


def test_byte_pair_merge_respects_rank_priority():
    ranks = {bytes([i]): i for i in range(256)}
    ranks[b"ab"] = 256
    ranks[b"bc"] = 257

    assert byte_pair_merge(b"abc", ranks) == [0, 2]
    assert encode(b"abc", ranks) == [256, 99]


def test_encode_empty():
    ranks = {bytes([i]): i for i in range(256)}

    assert encode(b"", ranks) == []
