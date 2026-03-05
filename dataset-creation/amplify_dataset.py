#!/usr/bin/env python3
"""
Amplify a movie-style dataset 100x with modified title (roman numeral),
overview (ordinal prefix), and keywords (shuffle + trim). Embeds title, overview,
and keywords as key-value text using SentenceTransformer (all-MiniLM-L6-v2).
Output: JSONL with L2-normalized embeddings; one JSON object per line.

Input: CSV (with header) or JSON/JSONL. CSV must have the same columns as the
movie record format (id, title, overview, keywords, etc.); no embedding column
(e.g. datasets/TMDB_movie_dataset_v11.csv). CSV is read in a streaming way.

Usage:
  python amplify_dataset.py datasets/TMDB_movie_dataset_v11.csv -o amplified_output
  python amplify_dataset.py input.csv -o out --s3-bucket my-bucket [--s3-prefix amplified/]
  python amplify_dataset.py input.csv -o out --limit 10
"""

import argparse
import csv
import json
import math
import os
import random
import sys
from pathlib import Path
from typing import Optional

# Prefer orjson for 2-5x faster JSON serialization (choke point in flush)
try:
    import orjson

    def _dumps(rec: dict) -> str:
        return orjson.dumps(rec).decode("utf-8")
except ImportError:

    def _dumps(rec: dict) -> str:
        return json.dumps(rec, ensure_ascii=False)


# Flush to disk every N records (smaller = less RAM for buffer; tune if OOM "Killed")
FLUSH_SIZE = 10_000
# Start a new output file every 1 million records (TMDB_0001.jsonl, TMDB_0002.jsonl, ...)
RECORDS_PER_FILE = 1_000_000
# Embedding batch size (smaller = less memory during encode; reduce if OOM)
EMBED_BATCH_SIZE = 32
# CSV chunk size when using pandas (faster C-backed parsing); None = use stdlib csv
CSV_CHUNK_SIZE = 100_000
SENTENCE_TRANSFORMER_MODEL = "all-MiniLM-L6-v2"
# Keys we overwrite per variant (avoid 100 full dict copies per base)
BASE_KEYS_SKIP = {"title", "overview", "keywords", "id"}

# Roman numerals 1--100
ROMAN_100 = [
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
    "XXI",
    "XXII",
    "XXIII",
    "XXIV",
    "XXV",
    "XXVI",
    "XXVII",
    "XXVIII",
    "XXIX",
    "XXX",
    "XXXI",
    "XXXII",
    "XXXIII",
    "XXXIV",
    "XXXV",
    "XXXVI",
    "XXXVII",
    "XXXVIII",
    "XXXIX",
    "XL",
    "XLI",
    "XLII",
    "XLIII",
    "XLIV",
    "XLV",
    "XLVI",
    "XLVII",
    "XLVIII",
    "XLIX",
    "L",
    "LI",
    "LII",
    "LIII",
    "LIV",
    "LV",
    "LVI",
    "LVII",
    "LVIII",
    "LIX",
    "LX",
    "LXI",
    "LXII",
    "LXIII",
    "LXIV",
    "LXV",
    "LXVI",
    "LXVII",
    "LXVIII",
    "LXIX",
    "LXX",
    "LXXI",
    "LXXII",
    "LXXIII",
    "LXXIV",
    "LXXV",
    "LXXVI",
    "LXXVII",
    "LXXVIII",
    "LXXIX",
    "LXXX",
    "LXXXI",
    "LXXXII",
    "LXXXIII",
    "LXXXIV",
    "LXXXV",
    "LXXXVI",
    "LXXXVII",
    "LXXXVIII",
    "LXXXIX",
    "XC",
    "XCI",
    "XCII",
    "XCIII",
    "XCIV",
    "XCV",
    "XCVI",
    "XCVII",
    "XCVIII",
    "XCIX",
    "C",
]

# Ordinals for overview prefix (first, second, ... one hundredth)
ORDINALS_100 = [
    "first",
    "second",
    "third",
    "fourth",
    "fifth",
    "sixth",
    "seventh",
    "eighth",
    "ninth",
    "tenth",
    "eleventh",
    "twelfth",
    "thirteenth",
    "fourteenth",
    "fifteenth",
    "sixteenth",
    "seventeenth",
    "eighteenth",
    "nineteenth",
    "twentieth",
    "twenty-first",
    "twenty-second",
    "twenty-third",
    "twenty-fourth",
    "twenty-fifth",
    "twenty-sixth",
    "twenty-seventh",
    "twenty-eighth",
    "twenty-ninth",
    "thirtieth",
    "thirty-first",
    "thirty-second",
    "thirty-third",
    "thirty-fourth",
    "thirty-fifth",
    "thirty-sixth",
    "thirty-seventh",
    "thirty-eighth",
    "thirty-ninth",
    "fortieth",
    "forty-first",
    "forty-second",
    "forty-third",
    "forty-fourth",
    "forty-fifth",
    "forty-sixth",
    "forty-seventh",
    "forty-eighth",
    "forty-ninth",
    "fiftieth",
    "fifty-first",
    "fifty-second",
    "fifty-third",
    "fifty-fourth",
    "fifty-fifth",
    "fifty-sixth",
    "fifty-seventh",
    "fifty-eighth",
    "fifty-ninth",
    "sixtieth",
    "sixty-first",
    "sixty-second",
    "sixty-third",
    "sixty-fourth",
    "sixty-fifth",
    "sixty-sixth",
    "sixty-seventh",
    "sixty-eighth",
    "sixty-ninth",
    "seventieth",
    "seventy-first",
    "seventy-second",
    "seventy-third",
    "seventy-fourth",
    "seventy-fifth",
    "seventy-sixth",
    "seventy-seventh",
    "seventy-eighth",
    "seventy-ninth",
    "eightieth",
    "eighty-first",
    "eighty-second",
    "eighty-third",
    "eighty-fourth",
    "eighty-fifth",
    "eighty-sixth",
    "eighty-seventh",
    "eighty-eighth",
    "eighty-ninth",
    "ninetieth",
    "ninety-first",
    "ninety-second",
    "ninety-third",
    "ninety-fourth",
    "ninety-fifth",
    "ninety-sixth",
    "ninety-seventh",
    "ninety-eighth",
    "ninety-ninth",
    "one hundredth",
]


def process_keywords(keywords_str: str, rng: random.Random) -> str:
    """Shuffle keywords (comma-separated), keep min 3 or remove last 15% (keep 85%)."""
    if not keywords_str or not keywords_str.strip():
        return ""
    parts = [p.strip() for p in keywords_str.split(",") if p.strip()]
    if not parts:
        return ""
    rng.shuffle(parts)
    # Keep at least 3; otherwise keep 85% (remove last 15%)
    keep = max(3, int(math.floor(len(parts) * 0.85)))
    kept = parts[:keep]
    return ", ".join(kept)


def build_key_value_text(title: str, overview: str, keywords: str) -> str:
    """Format the three modified fields as key-value text for embedding."""
    return f"title: {title}\noverview: {overview}\nkeywords: {keywords}"


def embed_batch_sentence_transformer(texts: list[str], model) -> list[list[float]]:
    """Embed a batch using SentenceTransformer; returns L2-normalized embedding vectors."""
    import numpy as np

    emb = model.encode(
        texts,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    if isinstance(emb, np.ndarray) and emb.ndim == 1:
        emb = emb.reshape(1, -1)
    return [row.tolist() for row in emb]


def _s3_client():
    """Return boto3 S3 client for smart_open. Raises ImportError if boto3 missing."""
    import boto3

    return boto3.client("s3")


def open_s3_stream(bucket: str, key: str):
    """Open a write stream to s3://bucket/key. Uses smart_open (multipart under the hood)."""
    try:
        from smart_open import open as smart_open
    except ImportError:
        raise SystemExit("Streaming to S3 requires smart_open. Install with: pip install smart_open[s3]")
    try:
        client = _s3_client()
    except ImportError:
        raise SystemExit("Streaming to S3 requires boto3. Install with: pip install boto3 smart_open[s3]")
    uri = f"s3://{bucket}/{key}"
    return smart_open(uri, "w", encoding="utf-8", transport_params={"client": client})


def _record_from_csv_row(row: dict) -> dict:
    """Convert a CSV row dict to a record; coerce id to int (CSV has no embedding column)."""
    rec = dict(row)
    if "id" in rec and rec["id"] != "":
        try:
            rec["id"] = int(rec["id"])
        except (ValueError, TypeError):
            pass
    return rec


def _record_from_pd_row(row: dict) -> dict:
    """Convert a pandas row dict to a record; normalize NaN and coerce id to int."""
    try:
        import pandas as pd
    except ImportError:
        return _record_from_csv_row(row)
    rec = {}
    for k, v in row.items():
        rec[k] = "" if (pd.isna(v) or v is None) else v
    if "id" in rec and rec["id"] != "":
        try:
            rec["id"] = int(float(rec["id"]))
        except (ValueError, TypeError):
            pass
    return rec


def _load_csv_pandas(path: Path):
    """Yield records from CSV using pandas chunked read (C-backed, faster for large files)."""
    import pandas as pd

    for chunk in pd.read_csv(
        path,
        chunksize=CSV_CHUNK_SIZE,
        encoding="utf-8",
        dtype=str,
        na_filter=False,
        low_memory=False,
    ):
        for rec in chunk.to_dict("records"):
            yield _record_from_pd_row(rec)


def load_records(path: str):
    """Yield records from a CSV file (streaming) or JSON/JSONL file. CSV has header; same fields as JSON except no embedding."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            import pandas as pd

            # Use pandas chunked read for faster CSV parsing (C-backed)
            yield from _load_csv_pandas(path)
        except ImportError:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield _record_from_csv_row(row)
        return
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if raw.startswith("["):
        for rec in json.loads(raw):
            yield rec
    else:
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def run(
    input_path: str,
    output_dir: str,
    seed: int = 42,
    s3_bucket: Optional[str] = None,
    s3_prefix: str = "",
    limit: Optional[int] = None,
    embed_batch_size: int = EMBED_BATCH_SIZE,
    flush_size: int = FLUSH_SIZE,
):
    rng = random.Random(seed)
    stream_to_s3 = bool(s3_bucket)
    if not stream_to_s3:
        os.makedirs(output_dir, exist_ok=True)
    elif s3_prefix:
        s3_prefix = s3_prefix.rstrip("/") + "/"

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise SystemExit("Requires: pip install sentence-transformers")
    print(f"Loading embedding model: {SENTENCE_TRANSFORMER_MODEL} (output: normalized JSONL)", flush=True)
    st_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
    embed_fn = lambda batch: embed_batch_sentence_transformer(batch, st_model)

    total_written = 0
    part = 1
    current_file = None
    buffer = []

    def open_next_file():
        nonlocal current_file, part
        if current_file is not None:
            current_file.close()
            current_file = None
            part += 1
        name = f"TMDB_{part:04d}.jsonl"
        if stream_to_s3:
            key = f"{s3_prefix}{name}"
            current_file = open_s3_stream(s3_bucket, key)
            print(f"Streaming to s3://{s3_bucket}/{key}", flush=True)
        else:
            current_file = open(Path(output_dir) / name, "w", encoding="utf-8")
        return current_file

    def flush():
        nonlocal buffer, current_file, total_written, part
        if not buffer:
            return
        if current_file is None:
            open_next_file()
        # Batch write: one writelines() instead of N write() calls (fewer syscalls)
        lines = [_dumps(rec) + "\n" for rec in buffer]
        current_file.writelines(lines)
        n_prev = total_written
        total_written += len(buffer)
        if total_written // 10_000 > n_prev // 10_000:
            print(f"Record {total_written} written")
        buffer.clear()
        # Start a new file every 1 million records (RECORDS_PER_FILE)
        if total_written % RECORDS_PER_FILE == 0:
            open_next_file()

    try:
        for n, base in enumerate(load_records(input_path)):
            if limit is not None and n >= limit:
                break
            base_id = int(base.get("id", 0))
            base_title = base.get("title", "")
            base_overview = base.get("overview", "")
            base_keywords = base.get("keywords", "")

            # One template dict (exclude fields we overwrite) to avoid 100 full dict(base) copies
            base_rest = {k: base[k] for k in base if k not in BASE_KEYS_SKIP}
            variants = []
            kv_texts = []
            for i in range(100):
                roman = ROMAN_100[i]
                ord_word = ORDINALS_100[i]
                title = f"{base_title} {roman}"
                overview = f"This is the {ord_word} overview of {title}, {base_overview}"
                keywords = process_keywords(base_keywords, rng)
                kv_texts.append(build_key_value_text(title, overview, keywords))
                rec = {**base_rest, "title": title, "overview": overview, "keywords": keywords, "id": base_id * 100 + (i + 1)}
                variants.append(rec)

            # Embed in batches
            all_embeddings = []
            for start in range(0, len(kv_texts), embed_batch_size):
                batch = kv_texts[start : start + embed_batch_size]
                all_embeddings.extend(embed_fn(batch))

            for rec, emb in zip(variants, all_embeddings):
                rec["embedding"] = emb
                buffer.append(rec)

            if len(buffer) >= flush_size:
                flush()
                print(f"Written so far: {total_written}")
    finally:
        interrupt = None
        try:
            flush()
        except KeyboardInterrupt as e:
            interrupt = e
        try:
            if current_file is not None:
                current_file.close()
        except KeyboardInterrupt as e:
            interrupt = e
        if interrupt is not None:
            raise interrupt

    print(f"Done. Total records written: {total_written}")


def main():
    ap = argparse.ArgumentParser(description="Amplify dataset 100x with SentenceTransformer embeddings (normalized JSONL)")
    ap.add_argument("input", help="Input CSV or JSON/JSONL file")
    ap.add_argument("-o", "--output-dir", default="amplified_output", help="Output directory for JSONL part files")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for keyword shuffle")
    ap.add_argument("--s3-bucket", default=None, help="Stream output directly to this S3 bucket (no local part files)")
    ap.add_argument("--s3-prefix", default="", help="S3 key prefix (e.g. 'amplified/') for streamed files")
    ap.add_argument("--limit", type=int, default=None, metavar="N", help="Process only first N input records (for testing)")
    ap.add_argument(
        "--embed-batch",
        type=int,
        default=EMBED_BATCH_SIZE,
        metavar="N",
        help=f"Embedding batch size (default {EMBED_BATCH_SIZE}; reduce if OOM)",
    )
    ap.add_argument(
        "--flush-size",
        type=int,
        default=FLUSH_SIZE,
        metavar="N",
        help=f"Flush buffer every N records (default {FLUSH_SIZE}; reduce if OOM)",
    )
    args = ap.parse_args()
    try:
        run(
            args.input,
            args.output_dir,
            seed=args.seed,
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix or "",
            limit=args.limit,
            embed_batch_size=args.embed_batch,
            flush_size=args.flush_size,
        )
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
