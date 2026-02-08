import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import {
  generateSingleFormatStream,
  generateZipFromEntries,
  ReportRow
} from './index';

const readAll = async (stream: NodeJS.ReadableStream): Promise<Buffer> => {
  const chunks: Buffer[] = [];

  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
};

const rowsToAsync = async function* (
  rows: ReportRow[]
): AsyncGenerator<Record<string, unknown>> {
  for (const row of rows) {
    yield row;
  }
};

const countLocalHeaders = (buffer: Buffer): number => {
  let count = 0;

  for (let index = 0; index <= buffer.length - 4; index += 1) {
    if (
      buffer[index] === 0x50 &&
      buffer[index + 1] === 0x4b &&
      buffer[index + 2] === 0x03 &&
      buffer[index + 3] === 0x04
    ) {
      count += 1;
    }
  }

  return count;
};

describe('report-generators', () => {
  it('generates CSV with header and escaped values', async () => {
    const generated = generateSingleFormatStream({
      format: 'csv',
      rows: rowsToAsync([
        {
          id: 1,
          name: 'A,B',
          notes: 'quote "x"'
        }
      ])
    });

    const payload = (await readAll(generated.stream)).toString('utf-8');
    expect(payload).toBe('id,name,notes\n1,"A,B","quote ""x"""\n');
    expect(generated.contentType).toBe('text/csv');
  });

  it('generates JSON array for empty rows', async () => {
    const generated = generateSingleFormatStream({
      format: 'json',
      rows: rowsToAsync([])
    });

    const payload = (await readAll(generated.stream)).toString('utf-8');
    expect(payload).toBe('[]');
    expect(generated.contentType).toBe('application/json');
  });

  it('generates JSON array for one or many rows', async () => {
    const generated = generateSingleFormatStream({
      format: 'json',
      rows: rowsToAsync([
        { id: 1, status: 'paid' },
        { id: 2, status: 'pending' }
      ])
    });

    const payload = (await readAll(generated.stream)).toString('utf-8');
    expect(payload).toBe('[{"id":1,"status":"paid"},{"id":2,"status":"pending"}]');
  });

  it('generates zip with exactly the provided entries', async () => {
    const generated = generateZipFromEntries([
      { name: 'report.csv', stream: Readable.from('id\n1\n') },
      { name: 'report.json', stream: Readable.from('[{"id":1}]') }
    ]);

    const payload = await readAll(generated.stream);

    expect(countLocalHeaders(payload)).toBe(2);
    expect(payload.includes(Buffer.from('report.csv'))).toBe(true);
    expect(payload.includes(Buffer.from('report.json'))).toBe(true);
  });

  it('fails PDF generation when row count exceeds configured limit', async () => {
    const generated = generateSingleFormatStream({
      format: 'pdf',
      rows: rowsToAsync([{ id: 1 }, { id: 2 }]),
      options: {
        pdfMaxRows: 1
      }
    });

    await expect(readAll(generated.stream)).rejects.toThrow(/PDF row limit exceeded/i);
  });

  it('generates XLSX stream as non-empty payload', async () => {
    const generated = generateSingleFormatStream({
      format: 'xlsx',
      rows: rowsToAsync([{ id: 1, amount: 10 }])
    });

    const payload = await readAll(generated.stream);
    expect(payload.length).toBeGreaterThan(0);
    expect(payload.slice(0, 2).toString('utf8')).toBe('PK');
  });
});
