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