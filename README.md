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
