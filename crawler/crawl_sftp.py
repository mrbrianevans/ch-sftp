import paramiko
import os
import duckdb
from datetime import datetime
import time
import re

from export_catalogue import export_catalogue_to_s3


def crawl_sftp(host, port, username, key_path, base_path="/"):
    # Set up SFTP
    transport = paramiko.Transport((host, port))
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Use in-memory DuckDB + Postgres extension to write to the remote catalogue
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
INSTALL postgres;
LOAD postgres;
CREATE SECRET postgres_catalogue (
    TYPE postgres,
    HOST '{os.getenv("PGHOST")}',
    PORT {os.getenv("PGPORT")},
    DATABASE '{os.getenv("PGDATABASE")}',
    USER '{os.getenv("PGUSER")}',
    PASSWORD '{os.getenv("PGPASSWORD")}'
);
ATTACH '' AS catalogue (TYPE postgres, SECRET 'postgres_catalogue');
USE catalogue.sftp;
""")

    def get_latest_date_from_db():
        # pick up from latest crawled directories
        try:
            rows = conn.execute("""SELECT 
                                     substring(path, position('prod' IN path), position('/20' IN path) - position('prod' IN path)) AS product,
                                     MAX(substring(path, 1, position('/20' IN path) + 10)) AS latest
                              FROM files
                              WHERE path LIKE '/free/prod%/20%/%/%/%'
                              GROUP BY 1;""").fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception:
            return {}

    latest_crawled_dates = get_latest_date_from_db()
    print(f"Latest crawled dates: {latest_crawled_dates}")

    def save_file_metadata(full_path, size, mtime):
        record = {
            "path": full_path,
            "size_bytes": size,
            "last_modified": datetime.fromtimestamp(mtime).isoformat(),
        }

        # Insert into Postgres via DuckDB (ignore if already exists)
        conn.execute(
            """
            INSERT INTO files (path, size_bytes, last_modified)
            VALUES (?, ?, ?)
            ON CONFLICT (path) DO NOTHING
        """,
            (record["path"], record["size_bytes"], record["last_modified"]),
        )
        conn.commit()  # ensure durability

    depth = 0

    def recurse(dir_path):
        nonlocal depth
        depth += 1
        for entry in sftp.listdir_attr(dir_path):
            if entry.filename == "bulkimage" or depth > 1:
                continue  # skip huge directory. TODO include this. check it works with resumable.
            full_path = dir_path + "/" + entry.filename
            if entry.longname.startswith("d"):  # Directory
                if full_path.startswith("/free/prod"):
                    match = re.match(r"^/free/(prod[A-Z0-9]+)/\d{4}", full_path)
                    prod_code = match.group(1) if match else None
                    latest = latest_crawled_dates.get(prod_code, "")
                    if prod_code and latest and full_path < latest[0 : len(full_path)]:
                        print(
                            f"Skipping {full_path} as it is older than latest crawled date {latest}"
                        )
                        continue
                recurse(full_path)
            else:  # File
                print("Found path", full_path, entry.st_size)
                save_file_metadata(full_path, entry.st_size, entry.st_mtime)
        time.sleep(1)

    recurse(base_path)

    # Export catalogue to S3 as gzipped JSON (for frontend consumption)
    export_catalogue_to_s3(conn)

    # Clean up
    sftp.close()
    transport.close()
    conn.close()


if __name__ == "__main__":
    username = os.getenv("SFTP_USERNAME")
    key_path = os.getenv("SFTP_KEY")
    print(f"Crawling SFTP to postgres")
    print(f"Start time: {datetime.now()}")
    crawl_sftp(
        host="bulk-live.companieshouse.gov.uk",
        port=22,
        username=username,
        key_path=key_path,
        base_path="/free",
    )
    print(f"Finish time: {datetime.now()}")
