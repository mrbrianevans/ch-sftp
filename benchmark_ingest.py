"""Benchmark SFTP download, zstd compression, and S3 upload as separate steps.

Each phase runs sequentially (no streaming) so you can see where time is spent.
"""

import argparse
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path

import boto3
import paramiko
import zstandard as zstd

SFTP_HOST = "bulk-live.companieshouse.gov.uk"
SFTP_PORT = 22
ZSTD_LEVEL = 15


def get_s3_client():
    """Create a boto3 S3 client supporting S3-compatible endpoints."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
    )


def get_s3_key(sftp_path: str) -> str:
    """Return an S3 key that mirrors the SFTP path."""
    if not sftp_path:
        return "unknown"
    return sftp_path.lstrip("/")


def connect_sftp():
    username = os.getenv("SFTP_USERNAME")
    key_path = os.getenv("SFTP_KEY")
    if not username or not key_path:
        raise RuntimeError(
            "SFTP_USERNAME and SFTP_KEY environment variables are required"
        )

    print(f"Connecting to SFTP {SFTP_HOST}...")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    transport.connect(username=username, pkey=pkey)
    return paramiko.SFTPClient.from_transport(transport), transport


def format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.2f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def mb_per_sec(byte_count: int, elapsed: float) -> float:
    if elapsed <= 0:
        return 0.0
    return byte_count / (1024 * 1024) / elapsed


def benchmark_file(sftp_path: str, keep_local: bool = False) -> None:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET environment variable is required")

    sftp, transport = connect_sftp()
    s3 = get_s3_client()
    s3_key = get_s3_key(sftp_path)

    try:
        remote_stat = sftp.stat(sftp_path)
        original_size = remote_stat.st_size
        if original_size <= 0:
            raise ValueError(f"Remote file has no content: {sftp_path}")

        print(f"Remote path: {sftp_path}")
        print(f"Remote size: {format_bytes(original_size)} ({original_size:,} bytes)")
        print(f"S3 target:   s3://{bucket}/{s3_key}")
        print()

        with tempfile.TemporaryDirectory(prefix="bench_ingest_") as tmp_dir:
            tmp = Path(tmp_dir)
            local_raw = tmp / "downloaded.dat"
            local_compressed = tmp / "compressed.zst"

            # Step 1: SFTP download
            print("Step 1: SFTP download")
            download_start = time.perf_counter()
            sftp.get(sftp_path, str(local_raw))
            download_elapsed = time.perf_counter() - download_start
            local_size = local_raw.stat().st_size
            if local_size != original_size:
                raise RuntimeError(
                    f"Download size mismatch: expected {original_size}, got {local_size}"
                )
            download_mbps = mb_per_sec(original_size, download_elapsed)
            print(
                f"   elapsed: {download_elapsed:.2f}s  \n"
                f"throughput: {download_mbps:.2f} MB/s  \n"
                f"      size: {format_bytes(original_size)}"
            )
            print()

            # Step 2: zstd compression (level 15, after download)
            print(f"Step 2: zstd compression (level {ZSTD_LEVEL})")
            compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL, threads=-1)
            compress_start = time.perf_counter()
            with local_raw.open("rb") as src, local_compressed.open("wb") as dst:
                compressor.copy_stream(src, dst)
            compress_elapsed = time.perf_counter() - compress_start
            compressed_size = local_compressed.stat().st_size
            compress_mbps = mb_per_sec(original_size, compress_elapsed)
            ratio = (compressed_size / original_size * 100) if original_size else 0
            print(
                f"         elapsed: {compress_elapsed:.2f}s  \n"
                f"input throughput: {compress_mbps:.2f} MB/s  \n"
                f"      compressed: {format_bytes(compressed_size)} ({ratio:.1f}% of original)"
            )
            print()

            # Step 3: S3 upload
            print("Step 3: S3 upload")
            upload_start = time.perf_counter()
            s3.upload_file(
                str(local_compressed),
                bucket,
                s3_key,
                ExtraArgs={
                    "ContentEncoding": "zstd",
                    "ContentType": "text/plain",
                    "ContentDisposition": "attachment",
                    "Metadata": {
                        "original-sftp-path": sftp_path,
                        "original-size-bytes": str(original_size),
                        "compressed-size-bytes": str(compressed_size),
                    },
                },
            )
            upload_elapsed = time.perf_counter() - upload_start
            upload_mbps = mb_per_sec(compressed_size, upload_elapsed)
            print(
                f"  elapsed: {upload_elapsed:.2f}s  "
                f"throughput: {upload_mbps:.2f} MB/s  "
                f"size: {format_bytes(compressed_size)}"
            )
            print()

            if keep_local:
                kept_raw = Path(f"{Path(sftp_path).name}.raw")
                kept_zst = Path(f"{Path(sftp_path).name}.zst")
                local_raw.replace(kept_raw)
                local_compressed.replace(kept_zst)
                print(f"Kept local files: {kept_raw}, {kept_zst}")
                print()

            total_elapsed = download_elapsed + compress_elapsed + upload_elapsed
            steps = [
                ("SFTP download", download_elapsed, download_mbps, original_size),
                ("zstd compress", compress_elapsed, compress_mbps, original_size),
                ("S3 upload", upload_elapsed, upload_mbps, compressed_size),
            ]
            bottleneck = max(steps, key=lambda s: s[1])

            print("=== Summary ===")
            print(f"{'step':<16} {'elapsed':>10} {'throughput':>14} {'share':>8}")
            print("-" * 52)
            for name, elapsed, throughput, size_bytes in steps:
                share = (elapsed / total_elapsed * 100) if total_elapsed else 0
                marker = "  <-- bottleneck" if name == bottleneck[0] else ""
                print(
                    f"{name:<16} {elapsed:>9.2f}s {throughput:>11.2f} MB/s "
                    f"{share:>7.1f}%{marker}"
                )
            print("-" * 52)
            print(f"{'total':<16} {total_elapsed:>9.2f}s")
            print(
                f"Bottleneck: {bottleneck[0]} "
                f"({bottleneck[1]:.2f}s, {bottleneck[1] / total_elapsed * 100:.1f}% of total)"
            )

    finally:
        sftp.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark sequential SFTP download, zstd level-15 compression, "
            "and S3 upload for a single file."
        )
    )
    parser.add_argument(
        "sftp_path",
        help="Remote SFTP path to download (e.g. /free/prod195/2025/01/01/file.dat)",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep downloaded and compressed files in the current directory",
    )
    args = parser.parse_args()

    print(f"=== Ingest benchmark started at {datetime.now()} ===")
    try:
        benchmark_file(args.sftp_path, keep_local=args.keep_local)
    except Exception as e:
        print(f"FATAL: {e}")
        raise
    print(f"=== Finished at {datetime.now()} ===")


if __name__ == "__main__":
    main()