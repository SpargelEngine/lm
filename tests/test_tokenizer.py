from random import Random

from spargel_lm.tokenizer import UnicodeTokenizer


def test_unicode_tokenizer_encode():
    tokenizer = UnicodeTokenizer(" _0Aa我😀")

    assert tokenizer.encode("我 _a0_ A😀A") == [5, 0, 1, 4, 2, 1, 0, 3, 6, 3]


def test_unicode_tokenizer_decode():
    tokenizer = UnicodeTokenizer(" _0Aa我😀")

    assert tokenizer.decode([0, 2, 4, 6, 5, 3, 1]) == " 0a😀我A_"


def test_unicode_tokenizer_encode_decode():
    random = Random(0)
    chars = [chr(i) for i in set(random.randint(0, 65535) for _ in range(1000))]
    tokenizer = UnicodeTokenizer(chars)

    s = "".join(random.choices(chars, k=1000))
    tokens = tokenizer.encode(s)
    assert tokenizer.decode(tokens) == s


def test_unicode_tokenizer_decode_encode():
    random = Random(0)
    chars = [chr(i) for i in set(random.randint(0, 65535) for _ in range(1000))]
    tokenizer = UnicodeTokenizer(chars)

    tokens = [random.randint(0, len(chars) - 1) for _ in range(1000)]
    s = tokenizer.decode(tokens)
    assert tokenizer.encode(s) == tokens
