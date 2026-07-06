import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { parseArgs } from "node:util";
import zlib from "node:zlib";

import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import SftpClient from "ssh2-sftp-client";

const REPO_ROOT = join(import.meta.dir, "..");
const INGEST_TEMP_DIR = join(REPO_ROOT, "temp");
const SFTP_HOST = "bulk-live.companieshouse.gov.uk";
const SFTP_PORT = 22;
const ZSTD_LEVEL = 9;

function createZstdCompressor() {
  return zlib.createZstdCompress({
    params: {
      [zlib.constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL,
    },
  });
}

interface PendingFile {
  path: string;
  productCode: string | null;
  productionDate: string | null;
  filename: string | null;
  fileExtension: string | null;
  runNumber: number | null;
  sizeBytes: number;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} environment variable is required`);
  }
  return value;
}

function sqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function getS3Key(sftpPath: string): string {
  if (!sftpPath) {
    return "unknown";
  }
  return sftpPath.replace(/^\/+/, "");
}

function getS3Client(): S3Client {
  return new S3Client({
    region: process.env.S3_REGION,
    endpoint: requireEnv("S3_ENDPOINT"),
    credentials: {
      accessKeyId: requireEnv("S3_ACCESS_KEY_ID"),
      secretAccessKey: requireEnv("S3_SECRET_ACCESS_KEY"),
    },
  });
}

async function connectCatalogue(): Promise<DuckDBConnection> {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();

  const pgHost = requireEnv("PGHOST");
  const pgPort = process.env.PGPORT || "5432";
  const pgDatabase = requireEnv("PGDATABASE");
  const pgUser = requireEnv("PGUSER");
  const pgPassword = requireEnv("PGPASSWORD");

  await connection.run("INSTALL postgres; LOAD postgres;");
  await connection.run(`CREATE SECRET postgres_catalogue (
    TYPE postgres,
    HOST '${sqlLiteral(pgHost)}',
    PORT ${pgPort},
    DATABASE '${sqlLiteral(pgDatabase)}',
    USER '${sqlLiteral(pgUser)}',
    PASSWORD '${sqlLiteral(pgPassword)}'
  );`);
  await connection.run(
    "ATTACH '' AS catalogue (TYPE postgres, SECRET 'postgres_catalogue');",
  );
  await connection.run("USE catalogue.sftp;");

  return connection;
}

async function fetchPendingFiles(
  connection: DuckDBConnection,
  limit: number,
  maxSizeBytes?: number,
): Promise<PendingFile[]> {
  const sizeFilter =
    maxSizeBytes != null ? `AND size_bytes <= ${maxSizeBytes}` : "";

  const reader = await connection.runAndReadAll(`
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
      AND (file_extension IS NULL OR file_extension != 'zip')
      AND size_bytes > 0
      ${sizeFilter}
    ORDER BY production_date DESC, size_bytes ASC NULLS LAST, path
    LIMIT ${limit}
  `);

  return reader.getRows().map((row) => ({
    path: String(row[0]),
    productCode: row[1] == null ? null : String(row[1]),
    productionDate: row[2] == null ? null : String(row[2]),
    filename: row[3] == null ? null : String(row[3]),
    fileExtension: row[4] == null ? null : String(row[4]),
    runNumber: row[5] == null ? null : Number(row[5]),
    sizeBytes: Number(row[6]),
  }));
}

async function connectSftp(): Promise<SftpClient> {
  const username = requireEnv("SFTP_USERNAME");
  const keyPath = requireEnv("SFTP_KEY");

  const sftp = new SftpClient();
  console.log(`Connecting to SFTP ${SFTP_HOST}...`);
  await sftp.connect({
    host: SFTP_HOST,
    port: SFTP_PORT,
    username,
    privateKey: await Bun.file(keyPath).text(),
    keepaliveInterval: 15_000,
  });

  return sftp;
}

function mbPerSec(byteCount: number, elapsedSeconds: number): number {
  if (elapsedSeconds <= 0) {
    return 0;
  }
  return byteCount / (1024 * 1024) / elapsedSeconds;
}

async function compressAndUpload(
  s3: S3Client,
  bucket: string,
  file: PendingFile,
  downloadPath: string,
): Promise<void> {
  const s3Key = getS3Key(file.path);
  const compressedPath = join(join(downloadPath, ".."), "compressed.zst");

  const pipelineStart = performance.now();
  await pipeline(
    createReadStream(downloadPath),
    createZstdCompressor(),
    createWriteStream(compressedPath),
  );
  const pipelineElapsed = (performance.now() - pipelineStart) / 1000;
  console.log(
    `    ✓ compressed to disk (took ${pipelineElapsed.toFixed(2)}s, ${mbPerSec(file.sizeBytes, pipelineElapsed).toFixed(2)} MB/s)`,
  );

  const uploadStart = performance.now();
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: s3Key,
      Body: await Bun.file(compressedPath).bytes(),
      ContentEncoding: "zstd",
      ContentType: "text/plain",
      ContentDisposition: "attachment",
      Metadata: {
        "original-sftp-path": file.path,
        "product-code": file.productCode ?? "",
        "run-number": String(file.runNumber ?? ""),
        "original-size-bytes": String(file.sizeBytes),
        "production-date": file.productionDate ?? "",
      },
    }),
  );
  const uploadElapsed = (performance.now() - uploadStart) / 1000;
  console.log(
    `    ✓ uploaded to S3 (took ${uploadElapsed.toFixed(2)}s, ${mbPerSec(file.sizeBytes, pipelineElapsed + uploadElapsed).toFixed(2)} MB/s end-to-end)`,
  );
}

async function markIngested(
  connection: DuckDBConnection,
  path: string,
): Promise<void> {
  await connection.run(
    "UPDATE files SET ingested_at = CURRENT_TIMESTAMP WHERE path = $path",
    { path },
  );
}

async function ingestLatestDataFiles(
  limit = 100,
  maxSizeBytes?: number,
): Promise<void> {
  if (limit < 1) {
    limit = 100;
  }

  const connection = await connectCatalogue();

  try {
    console.log("Querying enriched_files view for files pending ingest...");
    const pending = await fetchPendingFiles(connection, limit, maxSizeBytes);

    if (pending.length === 0) {
      console.log("No pending files found (ingested_at IS NULL + non-zip).");
      return;
    }

    console.log(
      `Found ${pending.length} file(s) to ingest (newest production_date first).`,
    );

    const sftp = await connectSftp();
    const s3 = getS3Client();
    const bucket = requireEnv("S3_BUCKET");

    let processed = 0;
    let consecutiveFailures = 0;
    const maxConsecutiveFailures = 5;

    try {
      for (const [index, file] of pending.entries()) {
        const ordinal = index + 1;
        console.log(
          `[${ordinal}/${pending.length}] ${file.path} ` +
            `(product=${file.productCode}, date=${file.productionDate}, run=${file.runNumber}, size=${file.sizeBytes} bytes)`,
        );
        console.log(`    -> s3://${bucket}/${getS3Key(file.path)}`);

        const fileTmpDir = await mkdtemp(join(INGEST_TEMP_DIR, "ingest_"));
        const downloadPath = join(fileTmpDir, "download");

        try {
          const downloadStart = performance.now();
          await sftp.fastGet(file.path, downloadPath, { concurrency: 64 });
          const downloadElapsed = (performance.now() - downloadStart) / 1000;
          console.log(
            `    ✓ downloaded from SFTP via fastGet (took ${downloadElapsed.toFixed(2)}s, ${mbPerSec(file.sizeBytes, downloadElapsed).toFixed(2)} MB/s)`,
          );

          await compressAndUpload(s3, bucket, file, downloadPath);
          await markIngested(connection, file.path);
          processed += 1;
          consecutiveFailures = 0;
          console.log("    ✓ updated ingested_at in Postgres");
        } catch (error) {
          consecutiveFailures += 1;
          console.log(`    ✗ FAILED: ${error}`);
          if (consecutiveFailures > maxConsecutiveFailures) {
            throw new Error(
              `Stopping ingest after ${consecutiveFailures} consecutive failures (limit: ${maxConsecutiveFailures})`,
            );
          }
          await Bun.sleep(200_000);
        } finally {
          await rm(fileTmpDir, { recursive: true, force: true });
        }
      }
    } finally {
      await sftp.end();
    }

    console.log(
      `Finished. Successfully ingested ${processed} of ${pending.length} files.`,
    );
  } finally {
    connection.closeSync();
  }
}

if (import.meta.main) {
  const startedAt = new Date();
  console.log(
    `=== Companies House SFTP data ingest started at ${startedAt.toISOString()} ===`,
  );

  try {
    const { values, positionals } = parseArgs({
      args: process.argv.slice(2),
      options: {
        "max-size-mb": { type: "string" },
      },
      allowPositionals: true,
    });

    let limit = 100;
    if (positionals[0] !== undefined) {
      const parsedLimit = Number(positionals[0]);
      if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
        throw new Error("limit must be a positive integer");
      }
      limit = parsedLimit;
    }

    let maxSizeBytes: number | undefined;
    if (values["max-size-mb"] !== undefined) {
      const maxSizeMb = Number(values["max-size-mb"]);
      if (!Number.isFinite(maxSizeMb) || maxSizeMb <= 0) {
        throw new Error("--max-size-mb requires a positive number");
      }
      maxSizeBytes = Math.floor(maxSizeMb * 1024 * 1024);
    }

    await mkdir(INGEST_TEMP_DIR, { recursive: true });
    await ingestLatestDataFiles(limit, maxSizeBytes);
  } catch (error) {
    console.log(`FATAL: ${error}`);
    throw error;
  }

  console.log(
    `=== Finished at ${new Date().toISOString()} ===`,
  );
}