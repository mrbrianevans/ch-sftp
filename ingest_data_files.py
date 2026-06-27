import argparse
import os
from datetime import datetime
from time import sleep

import duckdb
import paramiko
import boto3
import zstandard as zstd


def get_s3_client():
    """Create a boto3 S3 client supporting S3-compatible endpoints.
    (Copied from crawler/export_catalogue.py)
    """
    access_key = os.getenv("S3_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_ACCESS_KEY")
    endpoint = os.getenv("S3_ENDPOINT")
    region = os.getenv("S3_REGION")
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        endpoint_url=endpoint,
    )


def get_s3_key(sftp_path: str) -> str:
    """Return an S3 key that exactly mirrors the SFTP path.

    The original filename and extension are preserved exactly.
    Compression is indicated via the Content-Encoding: zstd header
    (not via a .zst file extension).
    """
    if not sftp_path:
        return "unknown"
    clean = sftp_path.lstrip("/")
    return clean


def ingest_latest_data_files(limit: int = 100):
    """Download the newest un-ingested data files (non-zip), zstd compress them
    in a streaming fashion, upload to S3, and mark them ingested in the catalogue.
    """
    if limit < 1:
        limit = 100
    # Connect to Postgres using in-memory DuckDB + postgres extension (same pattern as crawl_sftp.py)
    conn = duckdb.connect(":memory:")
    pg_host = os.getenv("PGHOST")
    pg_port = os.getenv("PGPORT") or "5432"
    pg_database = os.getenv("PGDATABASE")
    pg_user = os.getenv("PGUSER")
    pg_password = os.getenv("PGPASSWORD")

    conn.execute(f"""
INSTALL postgres;
LOAD postgres;
CREATE SECRET postgres_catalogue (
    TYPE postgres,
    HOST '{pg_host}',
    PORT {pg_port},
    DATABASE '{pg_database}',
    USER '{pg_user}',
    PASSWORD '{pg_password}'
);
ATTACH '' AS catalogue (TYPE postgres, SECRET 'postgres_catalogue');
USE catalogue.sftp;
""")

    # Query per the spec in this file's header comments.
    # Uses the enriched_files view (product_code, production_date, file_extension, etc.)
    print("Querying enriched_files view for files pending ingest...")
    query = f"""
        SELECT
            path,
            product_code,
            production_date,
            filename,
            file_extension,
            run_number,
            size_bytes
        FROM enriched_files
        WHERE ingested_at IS NULL
          AND (file_extension IS NULL OR file_extension != 'zip') and size_bytes > 0
        ORDER BY production_date DESC, size_bytes ASC NULLS LAST, path
        LIMIT {limit}
    """
    pending = conn.execute(query).fetchall()

    if not pending:
        print("No pending files found (ingested_at IS NULL + non-zip).")
        conn.close()
        return

    print(f"Found {len(pending)} file(s) to ingest (newest production_date first).")

    # Connect to SFTP (exact same setup as crawler/crawl_sftp.py)
    sftp_host = "bulk-live.companieshouse.gov.uk"
    sftp_port = 22
    username = os.getenv("SFTP_USERNAME")
    key_path = os.getenv("SFTP_KEY")
    if not username or not key_path:
        raise RuntimeError(
            "SFTP_USERNAME and SFTP_KEY environment variables are required"
        )

    print(f"Connecting to SFTP {sftp_host}...")
    transport = paramiko.Transport((sftp_host, sftp_port))
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # S3 client (same as export_catalogue.py)
    s3 = get_s3_client()
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET environment variable is required for data ingest")

    # Process each file: streaming download -> zstd compress -> streaming S3 upload
    processed = 0
    for idx, row in enumerate(pending, 1):
        start = datetime.now()
        path, product_code, prod_date, filename, file_ext, run_num, size_bytes = row
        s3_key = get_s3_key(path)

        print(
            f"[{idx}/{len(pending)}] {path} "
            f"(product={product_code}, date={prod_date}, run={run_num}, size={size_bytes} bytes)"
        )
        print(f"    -> s3://{bucket}/{s3_key}")

        try:
            # paramiko SFTPFile acts as a binary reader; zstd stream_reader pulls on demand
            with sftp.open(path, "rb") as remote_file:
                # High compression level (19) — prioritizes smallest possible file size
                # over compression speed. Suitable for archival bulk data.
                compressor = zstd.ZstdCompressor(level=19) # TODO: test compression level for optimal
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
                            "original-sftp-path": path or "",
                            "product-code": product_code or "",
                            "run-number": str(run_num or ""),
                            "original-size-bytes": str(size_bytes or ""),
                            "production-date": str(prod_date or ""),
                        },
                    },
                )

            elapsed = (datetime.now() - start).total_seconds()
            mb_per_sec = (size_bytes / (1024 * 1024) / elapsed) if elapsed > 0 else 0
            print(
                f"    ✓ streamed + uploaded to S3 (took {elapsed:.2f}s, {mb_per_sec:.2f} MB/s)"
            )

            # Mark as ingested only after successful upload (idempotent, resumable)
            conn.execute(
                "UPDATE files SET ingested_at = CURRENT_TIMESTAMP WHERE path = ?",
                (path,),
            )
            conn.commit()
            processed += 1
            print("    ✓ updated ingested_at in Postgres")
            sleep(2)  # pause between files

        except Exception as e:
            print(f"    ✗ FAILED: {e}")
            sleep(200)  # pause longer on failure
            # Leave ingested_at NULL so it will be retried on next run
            continue

    # Cleanup
    sftp.close()
    transport.close()
    conn.close()

    print(f"Finished. Successfully ingested {processed} of {len(pending)} files.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "limit",
        nargs="?",
        type=int,
        default=100,
        help="max files to ingest (default: 100)",
    )
    args = parser.parse_args()

    print(f"=== Companies House SFTP data ingest started at {datetime.now()} ===")
    try:
        ingest_latest_data_files(limit=args.limit)
    except Exception as e:
        print(f"FATAL: {e}")
        raise
    print(f"=== Finished at {datetime.now()} ===")
