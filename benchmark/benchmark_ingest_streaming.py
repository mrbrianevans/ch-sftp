"""Benchmark streaming SFTP download -> zstd compress -> S3 upload for a single file.

Mirrors the pipeline in ingest_data_files.py: data flows through without being
written to disk locally. Times the end-to-end operation and reports throughput
based on the original (uncompressed) file size.
"""

import argparse
import os
import time
from datetime import datetime

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
    return 'custom/'+sftp_path.lstrip("/")


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


def benchmark_streaming(sftp_path: str) -> None:
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
        print(f"Pipeline:    SFTP read -> zstd level {ZSTD_LEVEL} -> S3 upload (streaming)")
        print()

        print("Streaming download + compress + upload...")
        start = time.perf_counter()
        with sftp.open(sftp_path, "rb") as remote_file:
            compressor = zstd.ZstdCompressor(level=ZSTD_LEVEL, threads=-1)
            compressed_stream = compressor.stream_reader(remote_file)

            s3.upload_fileobj(
                compressed_stream,
                bucket,
                s3_key,
                ExtraArgs={
                    "ContentEncoding": "zstd",
                    "ContentType": "text/plain",
                    "ContentDisposition": "attachment",
                    "Metadata": {
                        "original-sftp-path": sftp_path,
                        "original-size-bytes": str(original_size),
                    },
                },
            )
        elapsed = time.perf_counter() - start
        throughput = mb_per_sec(original_size, elapsed)

        print()
        print("=== Result ===")
        print(f"elapsed:    {elapsed:.2f}s")
        print(f"throughput: {throughput:.2f} MB/s (based on original {format_bytes(original_size)})")
        print(f"uploaded:   s3://{bucket}/{s3_key}")

    finally:
        sftp.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark streaming SFTP download, zstd level-15 compression, "
            "and S3 upload for a single file."
        )
    )
    parser.add_argument(
        "sftp_path",
        help="Remote SFTP path to process (e.g. /free/prod195/2025/01/01/file.dat)",
    )
    args = parser.parse_args()

    print(f"=== Streaming ingest benchmark started at {datetime.now()} ===")
    try:
        benchmark_streaming(args.sftp_path)
    except Exception as e:
        print(f"FATAL: {e}")
        raise
    print(f"=== Finished at {datetime.now()} ===")


if __name__ == "__main__":
    main()