"""Sort full RetailRocket CSV by integer timestamp without loading it into RAM.

Usage: python scripts/sort_events.py --input data/raw/events.csv \
    --output data/raw/events_sorted.csv --chunk-rows 100000

The five-column CSV header must match the source contract exactly. Ties preserve
original row order. Temporary chunks live under output.parent; merge fan-in is
bounded to 64 files, including for very small --chunk-rows values. The completed
file is published atomically without replacing an existing output: Windows
rename fails on an existing destination; POSIX uses a no-clobber hard link on
the same filesystem. POSIX needs hard-link support, and all platforms need
temporary disk space for merge passes.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import csv
import heapq
import json
import os
from pathlib import Path
import re
import sys
import tempfile


FIELDS = ("timestamp", "visitorid", "event", "itemid", "transactionid")
MERGE_FAN_IN = 64


def _chunk_path(directory: Path, generation: int, index: int) -> Path:
    return directory / f"round-{generation}-{index:08d}.csv"


def _write_chunk(rows: list, path: Path) -> None:
    rows.sort(key=lambda item: (item[0], item[1]))
    with path.open("w", newline="", encoding="utf-8") as sink:
        writer = csv.writer(sink)
        for timestamp, ordinal, values in rows:
            writer.writerow([ordinal, *values])


def _read_chunk(reader):
    for values in reader:
        ordinal = int(values[0])
        raw = values[1:]
        yield int(raw[0]), ordinal, raw


def _merge_chunks(paths: list[Path], output: Path) -> None:
    with ExitStack() as stack:
        streams = [csv.reader(stack.enter_context(path.open(newline="", encoding="utf-8"))) for path in paths]
        sink = stack.enter_context(output.open("w", newline="", encoding="utf-8"))
        writer = csv.writer(sink)
        iterators = [_read_chunk(reader) for reader in streams]
        for timestamp, ordinal, values in heapq.merge(*iterators, key=lambda item: (item[0], item[1])):
            writer.writerow([ordinal, *values])


def _publish_no_clobber(completed: Path, destination: Path) -> None:
    if os.name == "nt":
        # Windows rename fails if the destination exists, unlike POSIX rename.
        os.rename(completed, destination)
    else:
        # Hard linking is atomic and cannot replace a competing destination.
        os.link(completed, destination)


def sort_events(input_path: str | Path, output_path: str | Path, *, chunk_rows: int = 100000) -> dict:
    source = Path(input_path)
    destination = Path(output_path)
    if type(chunk_rows) is not int or chunk_rows < 1:
        raise ValueError("chunk_rows must be a positive integer")
    if source.resolve() == destination.resolve():
        raise ValueError("Input and output paths must differ")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    total_rows = chunk_count = merge_passes = 0
    # Creation, merge and cleanup are confined to this fresh directory.
    with tempfile.TemporaryDirectory(prefix=".sort-events-", dir=destination.parent) as temporary:
        directory = Path(temporary).resolve()
        if not directory.is_relative_to(destination.parent.resolve()):
            raise RuntimeError("Temporary directory escaped output parent")
        rows = []
        with source.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.reader(stream)
            header = next(reader, None)
            if header != list(FIELDS):
                raise ValueError("CSV header must be exactly: " + ",".join(FIELDS))
            for ordinal, values in enumerate(reader):
                if len(values) != len(FIELDS):
                    raise ValueError(f"CSV data row {ordinal + 1} must contain exactly five columns")
                if not re.fullmatch(r"[0-9]+", values[0]) or int(values[0]) <= 0:
                    raise ValueError(f"CSV data row {ordinal + 1}: timestamp must be positive integer milliseconds")
                rows.append((int(values[0]), ordinal, values))
                total_rows += 1
                if len(rows) >= chunk_rows:
                    _write_chunk(rows, _chunk_path(directory, 0, chunk_count))
                    chunk_count += 1
                    rows.clear()
        if rows:
            _write_chunk(rows, _chunk_path(directory, 0, chunk_count))
            chunk_count += 1
            rows.clear()
        original_chunk_count = chunk_count
        generation = 0
        while chunk_count > 1:
            next_count = 0
            for start in range(0, chunk_count, MERGE_FAN_IN):
                inputs = [_chunk_path(directory, generation, i)
                          for i in range(start, min(start + MERGE_FAN_IN, chunk_count))]
                output = _chunk_path(directory, generation + 1, next_count)
                _merge_chunks(inputs, output)
                for path in inputs:
                    path.unlink()
                next_count += 1
            generation += 1
            chunk_count = next_count
            merge_passes += 1
        completed = directory / "completed.csv"
        with completed.open("w", newline="", encoding="utf-8") as sink:
            writer = csv.writer(sink)
            writer.writerow(FIELDS)
            if chunk_count:
                with _chunk_path(directory, generation, 0).open(newline="", encoding="utf-8") as sorted_source:
                    for timestamp, ordinal, values in _read_chunk(csv.reader(sorted_source)):
                        writer.writerow(values)
            sink.flush()
            os.fsync(sink.fileno())
        # A competing writer after the initial existence check must not lose
        # its output. os.replace alone would silently overwrite that file.
        _publish_no_clobber(completed, destination)
    return {"status": "success", "rows": total_rows, "initial_chunks": original_chunk_count,
            "merge_passes": merge_passes, "chunk_rows": chunk_rows,
            "stable_ties": True, "input": str(source), "output": str(destination)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--chunk-rows", type=int, default=100000)
    args = parser.parse_args(argv)
    if args.chunk_rows < 1:
        parser.error("--chunk-rows must be positive")
    try:
        result = sort_events(args.input, args.output, chunk_rows=args.chunk_rows)
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
