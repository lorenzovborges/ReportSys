import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { ObjectId } from 'mongodb';
import { afterEach, describe, expect, it } from 'vitest';
import {
  ensureIncludeFormats,
  normalizeValue,
  sanitizeFilters,
  snapshotRows,
  writeSnapshot
} from './helpers';

const tempDirs: string[] = [];

const rowsToAsync = async function* (
  rows: Array<Record<string, unknown>>
): AsyncGenerator<Record<string, unknown>> {
  for (const row of rows) {
    yield row;
  }
};

describe('worker helpers', () => {
  afterEach(async () => {
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it('sanitizeFilters removes dangerous operators recursively', () => {
    const sanitized = sanitizeFilters({
      status: 'paid',
      nested: {
        ok: true,
        $where: '1 == 1'
      },
      $or: [{ a: 1 }],
      'bad.field': 2
    });

    expect(sanitized).toEqual({
      status: 'paid',
      nested: {
        ok: true
      }
    });
  });

  it('normalizeValue converts ObjectId and Date in nested structures', () => {
    const objectId = new ObjectId('65e5b6f1de74f57fcb0f97a5');
    const date = new Date('2024-02-01T10:20:30.000Z');

    const normalized = normalizeValue({
      id: objectId,
      when: date,
      list: [objectId, date],
      nested: {
        value: objectId
      }
    });

    expect(normalized).toEqual({
      id: '65e5b6f1de74f57fcb0f97a5',
      when: '2024-02-01T10:20:30.000Z',
      list: ['65e5b6f1de74f57fcb0f97a5', '2024-02-01T10:20:30.000Z'],
      nested: {
        value: '65e5b6f1de74f57fcb0f97a5'
      }
    });
  });

  it('writeSnapshot enforces REPORT_TMP_MAX_BYTES', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'reportsys-helpers-'));
    tempDirs.push(dir);

    await expect(
      writeSnapshot(
        rowsToAsync([{ line: '1234567890' }, { line: 'x' }]),
        dir,
        'snapshot.ndjson',
        10,
        16
      )
    ).rejects.toThrow(/REPORT_TMP_MAX_BYTES/i);
  });

  it('snapshotRows reads NDJSON and ignores empty lines', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'reportsys-helpers-'));
    tempDirs.push(dir);
    const filePath = join(dir, 'input.ndjson');

    await writeFile(filePath, '{"id":1}\n\n{"id":2}\n', 'utf8');

    const rows: Array<Record<string, unknown>> = [];
    for await (const row of snapshotRows(filePath, 1024)) {
      rows.push(row);
    }

    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it('writeSnapshot stores rows and returns metadata', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'reportsys-helpers-'));
    tempDirs.push(dir);

    const snapshot = await writeSnapshot(
      rowsToAsync([{ id: 1 }, { id: 2 }]),
      dir,
      'snapshot.ndjson',
      1024 * 1024,
      1024
    );

    expect(snapshot.rowCount).toBe(2);
    expect(snapshot.bytes).toBeGreaterThan(0);

    const raw = await readFile(snapshot.path, 'utf8');
    expect(raw.trim().split('\n')).toHaveLength(2);
  });

  it('ensureIncludeFormats validates zip payload', () => {
    expect(() =>
      ensureIncludeFormats({
        format: 'zip',
        includeFormats: undefined
      } as never)
    ).toThrow(/requires includeFormats/i);

    expect(
      ensureIncludeFormats({
        format: 'zip',
        includeFormats: ['csv', 'json']
      } as never)
    ).toEqual(['csv', 'json']);
  });
});
