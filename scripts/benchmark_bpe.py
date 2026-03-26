#!/usr/bin/env python3

import argparse
import gc
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from spargel_lm.bpe import train_bpe


@dataclass(frozen=True, slots=True)
class Case:
    name: str
    sequences: list[bytes]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
    )
    parser.add_argument(
        "--num-vocab",
        type=int,
        default=512,
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        action="append",
        default=[],
        help=(
            "Optional fixed chunk size in bytes for extra multi-sequence cases. "
            "Pass multiple times to test multiple chunk sizes."
        ),
    )
    return parser.parse_args()


def build_cases(data: bytes, chunk_sizes: list[int]) -> list[Case]:
    cases = [Case(name="whole", sequences=[data])]

    line_sequences = data.splitlines(keepends=True)
    if line_sequences and len(line_sequences) > 1:
        cases.append(Case(name="lines", sequences=line_sequences))

    for chunk_size in sorted(set(chunk_sizes)):
        if chunk_size <= 0:
            raise ValueError(f"chunk size must be positive, got {chunk_size}")
        sequences = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        if len(sequences) > 1:
            cases.append(Case(name=f"chunks:{chunk_size}", sequences=sequences))

    return cases


def benchmark_case(
    case: Case,
    *,
    num_vocab: int,
    repeats: int,
    warmups: int,
) -> list[float]:
    for _ in range(warmups):
        train_bpe(case.sequences, num_vocab, verbose=False)

    gc.collect()
    timings = []
    for _ in range(repeats):
        start = time.perf_counter()
        train_bpe(case.sequences, num_vocab, verbose=False)
        timings.append(time.perf_counter() - start)
    return timings


def summarize_lengths(sequences: list[bytes]) -> str:
    lengths = [len(seq) for seq in sequences]
    mean = statistics.fmean(lengths)
    return (
        f"count={len(sequences):>4} "
        f"mean_len={mean:>8.1f} "
        f"min_len={min(lengths):>6} "
        f"max_len={max(lengths):>6}"
    )


def print_results(case: Case, timings: list[float]) -> None:
    mean = statistics.fmean(timings)
    best = min(timings)
    worst = max(timings)
    if len(timings) > 1:
        stdev = statistics.stdev(timings)
        stdev_text = f"{stdev:.6f}s"
    else:
        stdev_text = "n/a"

    print(
        f"{case.name:<12} "
        f"{summarize_lengths(case.sequences)} "
        f"mean={mean:.6f}s "
        f"best={best:.6f}s "
        f"worst={worst:.6f}s "
        f"stdev={stdev_text}"
    )


def main() -> None:
    args = parse_args()
    if args.num_vocab < 256:
        raise ValueError(f"num_vocab must be at least 256, got {args.num_vocab}")
    if args.repeats <= 0:
        raise ValueError(f"repeats must be positive, got {args.repeats}")
    if args.warmups < 0:
        raise ValueError(f"warmups cannot be negative, got {args.warmups}")

    data = args.data.read_bytes()
    cases = build_cases(data, args.chunk_size)

    print(
        f"dataset={args.data} bytes={len(data)} "
        f"num_vocab={args.num_vocab} repeats={args.repeats} warmups={args.warmups}"
    )
    for case in cases:
        timings = benchmark_case(
            case,
            num_vocab=args.num_vocab,
            repeats=args.repeats,
            warmups=args.warmups,
        )
        print_results(case, timings)


if __name__ == "__main__":
    main()
