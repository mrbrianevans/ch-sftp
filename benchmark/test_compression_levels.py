"""Compress a data file at every zstd level and upload each result to S3.

Each level is processed sequentially: compress to a local file, upload that
file, then remove the local compressed copy. Use the bundled 100 KB sample for
quick iteration; pass the full .dat path when ready for production-sized runs.
"""

import argparse
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import boto3
import zstandard as zstd
from zstandard.backend_cffi import lib

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "Prod195_4252_sc_sample.dat"
FULL_INPUT = SCRIPT_DIR / "Prod195_4252_sc.dat"

MIN_LEVEL = lib.ZSTD_minCLevel()
MAX_LEVEL = lib.ZSTD_maxCLevel()
DEFAULT_MIN_LEVEL = -4  # --fast=4
DEFAULT_MAX_LEVEL = 22  # ultra


def get_s3_client():
    """Create a boto3 S3 client supporting S3-compatible endpoints."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
    )


def s3_key_for_level(input_path: Path, level: int) -> str:
    """S3 object key under the zstd/ prefix, one object per compression level."""
    return f"zstd/{input_path.stem}_level{level}{input_path.suffix}"


def compress_to_file(input_path: Path, output_path: Path, level: int) -> None:
    compressor = zstd.ZstdCompressor(level=level)
    with input_path.open("rb") as src, output_path.open("wb") as dst:
        compressor.copy_stream(src, dst)


def upload_file(s3, bucket: str, local_path: Path, key: str) -> None:
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={
            "ContentEncoding": "zstd",
            "ContentType": "text/plain",
            "ContentDisposition": "attachment",
        },
    )


def format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.2f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def parse_levels(levels_str: str) -> list[int]:
    """Parse a comma-separated list of zstd levels, e.g. '-4,1,3,8,19,22'."""
    levels = []
    for part in levels_str.split(","):
        part = part.strip()
        if not part:
            continue
        levels.append(int(part))
    if not levels:
        raise ValueError("no levels provided")
    return levels


def validate_levels(levels: list[int]) -> None:
    for level in levels:
        if level < MIN_LEVEL or level > MAX_LEVEL:
            raise ValueError(
                f"level {level} is out of range [{MIN_LEVEL}, {MAX_LEVEL}]"
            )


def format_levels_summary(levels: list[int]) -> str:
    if len(levels) == 1:
        return f"{levels[0]} (1 level)"
    if levels == list(range(levels[0], levels[-1] + 1)):
        return f"{levels[0]} .. {levels[-1]} ({len(levels)} levels)"
    return f"{', '.join(str(level) for level in levels)} ({len(levels)} levels)"


def run(
    input_path: Path,
    levels: list[int],
    keep_local: bool,
    skip_upload: bool,
) -> None:
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    bucket = os.getenv("S3_BUCKET")
    if not skip_upload and not bucket:
        raise RuntimeError("S3_BUCKET environment variable is required")

    original_size = input_path.stat().st_size
    s3 = None if skip_upload else get_s3_client()
    total = len(levels)

    print(f"=== zstd compression level test started at {datetime.now()} ===")
    print(f"Input: {input_path} ({format_bytes(original_size)})")
    print(f"Levels: {format_levels_summary(levels)}")
    if skip_upload:
        print("Upload: skipped (local compression only)")
    else:
        print(f"Bucket: s3://{bucket}/zstd/")
    print()
    upload_col = "upload_s" if not skip_upload else "upload"
    print(
        f"{'level':>8}  {'compressed':>12}  {'ratio':>7}  "
        f"{'compress_s':>11}  {upload_col:>9}  s3_key"
    )
    print("-" * 90)

    with tempfile.TemporaryDirectory(prefix="zstd_test_", dir=SCRIPT_DIR) as tmp_dir:
        tmp = Path(tmp_dir)
        for idx, level in enumerate(levels, 1):
            local_compressed = tmp / f"{input_path.stem}_level{level}.zst"
            key = s3_key_for_level(input_path, level)

            compress_start = time.perf_counter()
            try:
                compress_to_file(input_path, local_compressed, level)
            except Exception as e:
                print(f"{level:>8}  FAILED compress: {e}")
                continue
            compress_elapsed = time.perf_counter() - compress_start

            compressed_size = local_compressed.stat().st_size
            ratio = (compressed_size / original_size * 100) if original_size else 0

            if skip_upload:
                upload_display = "skipped"
            else:
                upload_start = time.perf_counter()
                try:
                    upload_file(s3, bucket, local_compressed, key)
                except Exception as e:
                    print(
                        f"{level:>8}  {format_bytes(compressed_size):>12}  {ratio:>6.1f}%  "
                        f"{compress_elapsed:>10.3f}s  {'FAILED':>9}  {key}"
                    )
                    print(f"         upload error: {e}")
                    continue
                upload_display = f"{time.perf_counter() - upload_start:.3f}s"

            print(
                f"{level:>8}  {format_bytes(compressed_size):>12}  {ratio:>6.1f}%  "
                f"{compress_elapsed:>10.3f}s  {upload_display:>9}  {key}"
            )

            if keep_local:
                kept = SCRIPT_DIR / local_compressed.name
                local_compressed.replace(kept)

            if idx % 100 == 0:
                print(f"--- progress: {idx}/{total} levels ---")

    print()
    print(f"=== Finished at {datetime.now()} ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test every zstd compression level: local compress then S3 upload."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=(
            "Source file to compress (default: 100 KB sample). "
            f"Use {FULL_INPUT.name} for the full dataset."
        ),
    )
    parser.add_argument(
        "--levels",
        metavar="LEVELS",
        help=(
            "Comma-separated levels to test (overrides --min-level/--max-level), "
            "e.g. --levels=-4,1,3,8,19,22"
        ),
    )
    parser.add_argument(
        "--min-level",
        type=int,
        default=DEFAULT_MIN_LEVEL,
        help=f"First level in range to test (default: {DEFAULT_MIN_LEVEL})",
    )
    parser.add_argument(
        "--max-level",
        type=int,
        default=DEFAULT_MAX_LEVEL,
        help=f"Last level in range to test (default: {DEFAULT_MAX_LEVEL})",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep compressed files in the zstd/ directory after upload",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Compress locally only; do not upload to S3",
    )
    args = parser.parse_args()

    if args.levels:
        try:
            levels = parse_levels(args.levels)
            validate_levels(levels)
        except ValueError as e:
            print(f"Invalid --levels: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if args.min_level > args.max_level:
            print("--min-level must be <= --max-level", file=sys.stderr)
            sys.exit(1)
        levels = list(range(args.min_level, args.max_level + 1))
        try:
            validate_levels(levels)
        except ValueError as e:
            print(e, file=sys.stderr)
            sys.exit(1)

    try:
        run(
            args.input,
            levels,
            args.keep_local,
            args.skip_upload,
        )
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
