import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { ObjectId } from 'mongodb';
import { ReportRow } from '@reportsys/report-generators';
import { ReportIncludeFormat } from '@reportsys/contracts';
import { ReportJobDocument, SnapshotResult } from './types';

export const sanitizeFilters = (input: unknown): Record<string, unknown> => {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return {};
  }

  const output: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(input)) {
    if (key.startsWith('$') || key.includes('.')) {
      continue;
    }

    if (value && typeof value === 'object' && !Array.isArray(value)) {
      output[key] = sanitizeFilters(value);
      continue;
    }

    output[key] = value;
  }

  return output;
};

export const normalizeValue = (value: unknown): unknown => {
  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((item) => normalizeValue(item));
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, child]) => [
        key,
        normalizeValue(child)
      ])
    );
  }

  return value;
};

export const nowMs = (): number => Date.now();

export const snapshotNameFor = (jobId: string): string =>
  `snapshot-${jobId}-${Date.now()}-${randomUUID()}.ndjson`;

export const writeSnapshot = async (
  rows: AsyncIterable<ReportRow>,
  tmpDir: string,
  fileName: string,
  maxBytes: number,
  highWaterMark: number,
  onProgress?: (rowsCount: number) => void
): Promise<SnapshotResult> => {
  await mkdir(tmpDir, { recursive: true });

  const path = join(tmpDir, fileName);
  const writer = createWriteStream(path, {
    encoding: 'utf8',
    highWaterMark
  });

  let rowCount = 0;
  let bytes = 0;

  const writerError = new Promise<never>((_resolve, reject) => {
    writer.once('error', reject);
  });

  try {
    for await (const row of rows) {
      const line = `${JSON.stringify(row)}\n`;
      const lineBytes = Buffer.byteLength(line);
      bytes += lineBytes;

      if (bytes > maxBytes) {
        throw new Error(
          `Temporary snapshot size exceeded REPORT_TMP_MAX_BYTES (${maxBytes} bytes)`
        );
      }

      if (!writer.write(line)) {
        await Promise.race([once(writer, 'drain'), writerError]);
      }

      rowCount += 1;
      onProgress?.(rowCount);
    }

    writer.end();
    await Promise.race([once(writer, 'finish'), writerError]);

    return {
      path,
      rowCount,
      bytes
    };
  } catch (error) {
    writer.destroy();
    await rm(path, { force: true }).catch(() => undefined);
    throw error;
  }
};

export const snapshotRows = async function* (
  path: string,
  highWaterMark: number
): AsyncGenerator<ReportRow> {
  const input = createReadStream(path, {
    encoding: 'utf8',
    highWaterMark
  });

  const lineReader = createInterface({
    input,
    crlfDelay: Infinity
  });

  try {
    for await (const line of lineReader) {
      if (!line.trim()) {
        continue;
      }

      yield JSON.parse(line) as ReportRow;
    }
  } finally {
    lineReader.close();
    input.close();
  }
};

export const ensureIncludeFormats = (
  report: Pick<ReportJobDocument, 'format' | 'includeFormats'>
): ReportIncludeFormat[] => {
  if (report.format !== 'zip') {
    return [];
  }

  if (!report.includeFormats?.length) {
    throw new Error('format=zip requires includeFormats');
  }

  return report.includeFormats;
};
