import json
import os
import tempfile
from collections import defaultdict

import boto3


CACHE_CONTROL = (
    "public, max-age=180, must-revalidate, "
    "s-maxage=10800, stale-while-revalidate=86400"
)


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


def _upload_json_file(s3, local_path, bucket, key):
    """Upload a JSON file to S3 with standard frontend-serving headers (no compression)."""
    s3.upload_file(
        local_path,
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/json",
            "CacheControl": CACHE_CONTROL,
        },
    )
    print(f"✓ Exported and uploaded catalogue to s3://{bucket}/{key}")


def _export_all_files(conn, s3, bucket):
    """Export every catalogue row as a JSON array to all-files.json."""
    key = "all-files.json"

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Escape single quotes for DuckDB SQL string literals (Windows paths are fine)
        sql_path = tmp_path.replace("'", "''")
        conn.execute(f"""
            COPY (
                SELECT
                    path,
                    size_bytes,
                    strftime(last_modified, '%xT%X') AS last_modified,
                    ingested_at IS NOT NULL AS is_ingested
                FROM files
                ORDER BY path
            ) TO '{sql_path}'
            (FORMAT JSON, ARRAY TRUE)
        """)
        _upload_json_file(s3, tmp_path, bucket, key)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _build_latest_catalogue(conn):
    """Build per-product latest-path catalogue with aggregate metadata.

    Returns a list of product objects, each with:
      - product_code
      - total_size_bytes  (all files for the product)
      - total_runs        (distinct production dates)
      - doc_paths         (.doc / .docx paths for the product)
      - latest_production_date
      - latest_paths      (files under the latest production date only;
                            same fields as all-files.json entries)
    """
    # Product-level aggregates: size across all files, runs = distinct production dates
    stats_rows = conn.execute("""
        SELECT
            product_code,
            COALESCE(SUM(size_bytes), 0)::BIGINT AS total_size_bytes,
            COUNT(DISTINCT production_date) AS total_runs
        FROM enriched_files
        WHERE product_code IS NOT NULL
        GROUP BY product_code
        ORDER BY product_code
    """).fetchall()

    # Documentation files (.doc / .docx) per product
    doc_rows = conn.execute("""
        SELECT product_code, path
        FROM enriched_files
        WHERE product_code IS NOT NULL
          AND file_extension IN ('doc', 'docx')
        ORDER BY product_code, path
    """).fetchall()

    docs_by_product = defaultdict(list)
    for product_code, path in doc_rows:
        docs_by_product[product_code].append(path)

    # Latest production date per product (data files only)
    latest_date_rows = conn.execute("""
        SELECT product_code, MAX(production_date) AS latest_production_date
        FROM enriched_files
        WHERE product_code IS NOT NULL
          AND production_date IS NOT NULL
        GROUP BY product_code
    """).fetchall()
    latest_date_by_product = {
        product_code: latest_date
        for product_code, latest_date in latest_date_rows
    }

    # Paths belonging to each product's latest production date
    latest_path_rows = conn.execute("""
        WITH latest AS (
            SELECT product_code, MAX(production_date) AS latest_production_date
            FROM enriched_files
            WHERE product_code IS NOT NULL
              AND production_date IS NOT NULL
            GROUP BY product_code
        )
        SELECT
            e.product_code,
            e.path,
            e.size_bytes,
            strftime(e.last_modified, '%xT%X') AS last_modified,
            e.ingested_at IS NOT NULL AS is_ingested
        FROM enriched_files e
        INNER JOIN latest l
            ON e.product_code = l.product_code
           AND e.production_date = l.latest_production_date
        ORDER BY e.product_code, e.path
    """).fetchall()

    latest_paths_by_product = defaultdict(list)
    for product_code, path, size_bytes, last_modified, is_ingested in latest_path_rows:
        latest_paths_by_product[product_code].append(
            {
                "path": path,
                "size_bytes": size_bytes,
                "last_modified": last_modified,
                "is_ingested": bool(is_ingested),
            }
        )

    products = []
    for product_code, total_size_bytes, total_runs in stats_rows:
        latest_date = latest_date_by_product.get(product_code)
        products.append(
            {
                "product_code": product_code,
                "total_size_bytes": int(total_size_bytes or 0),
                "total_runs": int(total_runs or 0),
                "doc_paths": docs_by_product.get(product_code, []),
                "latest_production_date": (
                    latest_date.isoformat() if latest_date is not None else None
                ),
                "latest_paths": latest_paths_by_product.get(product_code, []),
            }
        )
    return products


def _export_latest(conn, s3, bucket):
    """Export latest-per-product catalogue (with metadata) to latest.json."""
    key = "latest.json"
    products = _build_latest_catalogue(conn)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        tmp_path = tmp.name
        json.dump(products, tmp, ensure_ascii=False, separators=(",", ":"))

    try:
        _upload_json_file(s3, tmp_path, bucket, key)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def export_catalogue_to_s3(conn):
    """Export catalogue JSON files to S3 (uncompressed) for frontend consumption.

    Uploads:
      - all-files.json  — every file path in the catalogue
      - latest.json     — latest paths per product plus size/run/doc metadata
    """
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET not set — skipping JSON export to S3")
        return

    s3 = get_s3_client()

    try:
        _export_all_files(conn, s3, bucket)
        _export_latest(conn, s3, bucket)
    except Exception as e:
        print(f"Failed to export/upload catalogue to S3: {e}")
        raise
