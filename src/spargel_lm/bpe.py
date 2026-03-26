from collections import Counter
from collections.abc import Iterable


def pair_counter(seq: list[int]) -> Counter:
    return Counter(zip(seq, seq[1:]))


def merge(seq: list[int], pair: tuple[int, int], idx: int) -> list[int]:
    new_seq = []
    i = 0
    while i < len(seq):
        if seq[i] == pair[0] and i + 1 < len(seq) and seq[i + 1] == pair[1]:
            new_seq.append(idx)
            i += 2
        else:
            new_seq.append(seq[i])
            i += 1
    return new_seq


def merge_(seq: list[int], pair: tuple[int, int], idx: int):
    i = 0
    j = 0
    while i + 1 < len(seq):
        if seq[i] == pair[0] and seq[i + 1] == pair[1]:
            seq[j] = idx
            i += 2
        else:
            seq[j] = seq[i]
            i += 1
        j += 1
    if i == len(seq) - 1:
        seq[j] = seq[i]
        j += 1
    del seq[j:]


def train_bpe(
    seqs: Iterable[bytes],
    num_vocab: int,
    *,
    verbose: bool = True,
) -> dict[int, bytes]:
    assert num_vocab >= 256

    vocabs: dict[int, bytes] = {i: bytes([i]) for i in range(0, 256)}

    corpus: list[list[int]] = [list(seq) for seq in seqs]

    i = 256
    while i < num_vocab:
        counter = Counter()
        for seq in corpus:
            counter.update(pair_counter(seq))

        pair, freq = counter.most_common(1)[0]

        vocabs[i] = vocabs[pair[0]] + vocabs[pair[1]]

        for seq in corpus:
            merge_(seq, pair, i)

        if verbose:
            print(f"[bpe] merge {pair[0]} + {pair[1]} -> {i} with {freq=}")

        i += 1

    return vocabs


def decode(seq: list[int], vocabs: dict[int, bytes]) -> bytes:
    return b"".join(vocabs[i] for i in seq)


# NOTE(tianjiao): 1M tokens should be enough.
MAX_RANK: int = 1_000_000
MAX_OFFSET: int = 1_000_000_000


def byte_pair_merge(piece: bytes, ranks: dict[bytes, int]) -> list[int]:
    """
    Perform merges with the give ranks.

    Note: rank means priority for a merge, i.e. the lower the rank is, the more urgent the merge is.

    The input bytes are merged into segments.

    Args:
        ranks: a dictionary specifying ranks for some byte sequences
               Note: it is assume that `ranks[s] <= ranks[t]` if `s` is a prefix of `t`, and
                     every key in `ranks` can be written as the sum of another two keys in `ranks`.
        piece: the input byte sequence

    Returns:
        The start position of each segment.
    """
    # every item represents a part of `piece` with rank
    parts: list[tuple[int, int]] = []

    # index of the the merge point (in `parts`) with minimal rank, i.e. maximal merge priority
    min_rank: tuple[int, int] = (MAX_OFFSET, MAX_RANK)

    # iterate over the adjacent bytes
    for i in range(len(piece) - 1):
        # if the byte-pair does not exist in `ranks`, assign inf to rank
        rank = ranks.get(piece[i : i + 2], MAX_RANK)
        parts.append((i, rank))
    parts.append((len(piece) - 1, MAX_RANK))
    # add a virtual merge point at the end
    parts.append((len(piece), MAX_RANK))

    min_rank = parts[min(range(len(parts)), key=lambda i: parts[i][1])]

    # sanity check
    assert len(parts) == len(piece) + 1

    # get the rank of byte-pair formed by merge points `i`, `i+1`, `i+2`
    # note: this is called when `i` and `i+1` will be merged, or when `i+1` and `i+2` will be merged
    def get_rank(i: int) -> int:
        if i + 3 < len(parts):
            p = piece[parts[i][0] : parts[i + 3][0]]
            return ranks.get(p, MAX_RANK)
        else:
            return MAX_RANK

    # loop condition: there are byte-pairs that can be merged
    while not min_rank[1] == MAX_RANK:
        # the offset of the merge point with minimal rank
        i = min_rank[0]

        # we need to recompute the rank at the previous byte if there is one
        if i > 0:
            # only the rank is modified
            parts[i - 1] = (parts[i - 1][0], get_rank(i - 1))
        parts[i] = (parts[i][0], get_rank(i))
        # remove the next merge point, as it has been merged with `i`
        parts.pop(i + 1)

        # find the next merge point with minimal rank
        min_index = min(range(len(parts)), key=lambda i: parts[i][1])
        min_rank = (min_index, parts[min_index][1])

    return [segment[0] for segment in parts[:-1]]


def encode(piece: bytes, ranks: dict[bytes, int]) -> list[int]:
    if not piece:
        return []

    starts = byte_pair_merge(piece, ranks)

    tokens = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(piece)
        tokens.append(ranks[piece[start:end]])

    return tokens
