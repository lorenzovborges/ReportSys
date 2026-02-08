import { PassThrough, Readable, Transform } from 'node:stream';
import archiver from 'archiver';
import ExcelJS from 'exceljs';
import PDFDocument from 'pdfkit';
import { ReportFormat, ReportIncludeFormat } from '@reportsys/contracts';

export type ReportRow = Record<string, unknown>;

export type StreamOptions = {
  highWaterMark?: number;
  pdfMaxRows?: number;
};

export type GeneratedReport = {
  stream: Readable;
  contentType: string;
  extension: string;
};

export type ZipEntry = {
  name: string;
  stream: Readable;
};

export type GenerateSingleFormatInput = {
  format: ReportIncludeFormat;
  rows: AsyncIterable<ReportRow>;
  options?: StreamOptions;
};

export type GenerateReportInput = {
  format: ReportFormat;
  rows: AsyncIterable<ReportRow>;
  options?: StreamOptions;
};

const escapeCsv = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }

  const raw =
    typeof value === 'string'
      ? value
      : value instanceof Date
        ? value.toISOString()
        : JSON.stringify(value);

  if (raw.includes(',') || raw.includes('"') || raw.includes('\n')) {
    return `"${raw.replace(/"/g, '""')}"`;
  }

  return raw;
};

const toObjectStream = (
  rows: AsyncIterable<ReportRow>,
  options?: StreamOptions
): Readable =>
  Readable.from(rows, {
    objectMode: true,
    highWaterMark: options?.highWaterMark
  });

const createCsvStream = (rows: AsyncIterable<ReportRow>, options?: StreamOptions): Readable => {
  const source = toObjectStream(rows, options);
  let headers: string[] | null = null;

  const transform = new Transform({
    writableObjectMode: true,
    readableObjectMode: false,
    writableHighWaterMark: options?.highWaterMark,
    readableHighWaterMark: options?.highWaterMark,
    transform(chunk, _encoding, callback): void {
      const row = chunk as ReportRow;

      if (!headers) {
        headers = Object.keys(row);
        this.push(`${headers.join(',')}\n`);
      }

      const values = (headers ?? []).map((header) => escapeCsv(row[header]));
      this.push(`${values.join(',')}\n`);
      callback();
    }
  });

  return source.pipe(transform);
};

const createJsonArrayStream = (
  rows: AsyncIterable<ReportRow>,
  options?: StreamOptions
): Readable => {
  const source = toObjectStream(rows, options);
  let isFirst = true;

  const transform = new Transform({
    writableObjectMode: true,
    readableObjectMode: false,
    writableHighWaterMark: options?.highWaterMark,
    readableHighWaterMark: options?.highWaterMark,
    transform(chunk, _encoding, callback): void {
      if (isFirst) {
        this.push(`[${JSON.stringify(chunk)}`);
        isFirst = false;
      } else {
        this.push(`,${JSON.stringify(chunk)}`);
      }

      callback();
    },
    final(callback): void {
      if (isFirst) {
        this.push('[]');
      } else {
        this.push(']');
      }

      callback();
    }
  });

  return source.pipe(transform);
};

const createXlsxStream = (rows: AsyncIterable<ReportRow>, options?: StreamOptions): Readable => {
  const output = new PassThrough({
    highWaterMark: options?.highWaterMark
  });

  void (async () => {
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      stream: output,
      useSharedStrings: false,
      useStyles: false
    });

    const worksheet = workbook.addWorksheet('Report');
    let headers: string[] | null = null;

    for await (const row of rows) {
      if (!headers) {
        headers = Object.keys(row);
        worksheet.addRow(headers).commit();
      }

      const values = (headers ?? []).map((header) => row[header] ?? null);
      worksheet.addRow(values).commit();
    }

    await workbook.commit();
  })().catch((error) => {
    output.destroy(error as Error);
  });

  return output;
};

const createPdfStream = (rows: AsyncIterable<ReportRow>, options?: StreamOptions): Readable => {
  const output = new PassThrough({
    highWaterMark: options?.highWaterMark
  });
  const doc = new PDFDocument({ margin: 24 });
  doc.pipe(output);

  void (async () => {
    doc.fontSize(16).text('Report', { underline: true }).moveDown();
    let index = 0;

    for await (const row of rows) {
      index += 1;

      if (options?.pdfMaxRows !== undefined && index > options.pdfMaxRows) {
        throw new Error(
          `PDF row limit exceeded. max=${options.pdfMaxRows}, received>${options.pdfMaxRows}`
        );
      }

      doc.fontSize(10).text(`${index}. ${JSON.stringify(row)}`);
    }

    doc.end();
  })().catch((error) => {
    output.destroy(error as Error);
  });

  return output;
};

export const generateSingleFormatStream = ({
  format,
  rows,
  options
}: GenerateSingleFormatInput): GeneratedReport => {
  switch (format) {
    case 'csv':
      return {
        stream: createCsvStream(rows, options),
        contentType: 'text/csv',
        extension: 'csv'
      };
    case 'json':
      return {
        stream: createJsonArrayStream(rows, options),
        contentType: 'application/json',
        extension: 'json'
      };
    case 'xlsx':
      return {
        stream: createXlsxStream(rows, options),
        contentType:
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        extension: 'xlsx'
      };
    case 'pdf':
      return {
        stream: createPdfStream(rows, options),
        contentType: 'application/pdf',
        extension: 'pdf'
      };
    default:
      throw new Error(`Unsupported single format: ${String(format)}`);
  }
};

export const generateZipFromEntries = (
  entries: ZipEntry[],
  options?: StreamOptions
): GeneratedReport => {
  const output = new PassThrough({
    highWaterMark: options?.highWaterMark
  });
  const archive = archiver('zip', {
    zlib: {
      level: 9
    }
  });

  archive.on('error', (error: Error) => {
    output.destroy(error);
  });

  archive.pipe(output);

  for (const entry of entries) {
    archive.append(entry.stream, { name: entry.name });
  }

  void archive.finalize();

  return {
    stream: output,
    contentType: 'application/zip',
    extension: 'zip'
  };
};

export const generateReportStream = ({
  format,
  rows,
  options
}: GenerateReportInput): GeneratedReport => {
  if (format === 'zip') {
    return generateZipFromEntries([
      {
        name: 'report.csv',
        stream: generateSingleFormatStream({
          format: 'csv',
          rows,
          options
        }).stream
      }
    ]);
  }

  return generateSingleFormatStream({
    format,
    rows,
    options
  });
};
