 # Companies Catalogue SFTP mirror
 
Cataloging companies house SFTP server bulk data products.

## Stages

- Crawl SFTP server (`crawler`)
  - saves a catalogue of all files on the server.
- Summarise catalogue 
- Save latest files of each data product.
  - get file in original format
  - upload to storage bucket compressed

## Scheduled Crawl (GitHub Actions)

A GitHub Action runs the crawler daily and writes directly to Postgres (see [`.github/workflows/daily-sftp-crawl.yml`](.github/workflows/daily-sftp-crawl.yml)).

The crawler uses an **in-memory DuckDB** instance with the Postgres extension (`ATTACH ... AS catalogue (TYPE postgres)`) to insert into a Postgres table called `sftp.files`. There is no local `.duckdb` file.

### Required Repository Secrets

Configure these in your GitHub repository under **Settings → Secrets and variables → Actions**:

| Secret                | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| `SFTP_USERNAME`       | SFTP username for `bulk-live.companieshouse.gov.uk`                         |
| `SFTP_PRIVATE_KEY`    | **Full contents** of your RSA private key file (paste the entire `-----BEGIN RSA PRIVATE KEY----- ... -----END RSA PRIVATE KEY-----` block) |
| `PGHOST`              | Postgres hostname                                                           |
| `PGPORT`              | Postgres port (optional, defaults to 5432)                                  |
| `PGUSER`              | Postgres user                                                               |
| `PGPASSWORD`          | Postgres password                                                           |

The action will fail (intentionally) if the required secrets are missing.

You can also trigger a manual run from the **Actions** tab → "Daily SFTP Catalogue Crawl" → "Run workflow".


## Data file path structure

Example file paths:

```
/free/prod215/2020/08/22/ext_rec_ind_7834
/free/prod215/2026/03/31/rec_ind_9872.txt
/free/prod215/2026/04/05/ext_rec_ind_9877.txt
/free/prod216/2025/08/05/Prod216_4017_ew_1.dat
/free/prod216/2025/08/05/Prod216_4017_ew_2.dat
/free/prod223/2024/12/04/Prod223_3843.zip
/free/prodSURRENDNAMES/2024/07/24/ProdSURRENDNAMES_3749.txt
```

The general pattern is `/free/{product code}/{year}/{month}/{day}/{Product Code}_{Run number}.{extension}`.

The logical layers are:
Product -> Date of production -> Files.

## Zstd compression

To minimise storage costs in the storage bucket, files are compressed with Zstd before being uploaded.
Cloudflare decompresses them and optionally recompresses them when serving to clients, through the use of Encoding headers.
Uploading it with `Content-Encoding` set tells Cloudflare to decompress it when serving.
Clients can then set their own `Accept-Encoding` on the request and Cloudflare will honour it.

### Compression level
I tried a few compression levels on a representative sample of data.

Decompression speed is much the same across all levels of compression, so this shouldn't affect download speed for users.

Testing on Prod195_4252_sc.dat (418.94 MB) with `test_compression_levels.py`.

**Updated Zstd Compression Test Results** (Original: 418.94 MB)

| Level | Compressed   | Ratio   | Compress Time | Effective MB/s |
|-------|--------------|---------|---------------|----------------|
| -4    | 120.67 MB    | 28.8%   | 0.842 s       | 498            |
| 1     | 80.45 MB     | 19.2%   | 1.016 s       | 412            |
| 3     | 74.23 MB     | 17.7%   | 1.439 s       | 291            |
| 8     | 63.16 MB     | 15.1%   | 6.338 s       | 66             |
| 9     | 61.55 MB     | 14.7%   | 6.583 s       | 64             |
| 13    | 60.02 MB     | 14.3%   | 27.785 s      | 15.1           |
| 15    | 58.78 MB     | 14.0%   | 60.834 s      | 6.9            |
| 18    | 54.03 MB     | 12.9%   | 142.124 s     | 2.95           |
| 19    | 52.55 MB     | 12.5%   | 301.486 s     | 1.39           |

### Streaming vs sequential

Initially I was fully streaming from SFTP -> Compression -> S3 Upload, but from benchmarking found it was quicker to do the download from SFTP using `.get` without any blocking pipeline, and then do the compression and upload afterward.

When running locally on my laptop this was a 10x increase in throughput from <0.5MB/s to 5MB/s.

Benchmarks in `benchmark/benchmark_ingest.py` and `benchmark/benchmark_ingest_streaming.py`.