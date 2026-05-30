import os
import tempfile

import boto3


def get_s3_client():
    """Create a boto3 S3 client supporting S3-compatible endpoints"""
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

# TODO: test whether its necessary to set the custom content-encoding header, or if the cloudflare proxy would automate it if i just upload a raw json file.

def export_catalogue_to_s3(conn):
    """Export the files table to a gzipped JSON array and upload to S3 with Content-Encoding: gzip."""
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET not set — skipping JSON export to S3")
        return

    key = "all-files.json.gz"
    s3 = get_s3_client()

    # Use a temporary file for the gzipped export
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Export as a single JSON array using DuckDB (very efficient)
        conn.execute(f"""
            COPY (
                SELECT * FROM files ORDER BY path
            ) TO '{tmp_path}'
            (FORMAT JSON, COMPRESSION GZIP, ARRAY TRUE)
        """)

        # Upload with the critical headers for frontend serving
        s3.upload_file(
            tmp_path,
            bucket,
            key,
            ExtraArgs={
                "ContentEncoding": "gzip",
                "ContentType": "application/json",
                "CacheControl": "public, max-age=3600",
            },
        )
        print(
            f"✓ Exported and uploaded catalogue to s3://{bucket}/{key} (Content-Encoding: gzip)"
        )

    except Exception as e:
        print(f"Failed to export/upload catalogue to S3: {e}")
        raise
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
