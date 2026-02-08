import { Readable } from 'node:stream';
import { Collection, Document, ObjectId } from 'mongodb';
import { Job } from 'bullmq';
import { describe, expect, it, vi } from 'vitest';
import { AppConfig, createLogger } from '@reportsys/core';
import { createReportJobProcessor, runPartitionedReduce } from './job-processor';
import { ReportJobDocument, ReportQueueData } from './types';

const makeConfig = (overrides: Partial<AppConfig> = {}): AppConfig => ({
  nodeEnv: 'test',
  port: 3000,
  logLevel: 'silent',
  mongoDbName: 'reportsys',
  mongoWriteUri: 'mongodb://localhost:27017/reportsys?replicaSet=rs0',
  mongoReportReadUri:
    'mongodb://localhost:27018/reportsys?replicaSet=rs0&readPreference=secondary',
  redisUrl: 'redis://localhost:6379',
  storageDriver: 'noop',
  enableExternalStorage: false,
  integrationStrictMode: false,
  enableWebhooks: false,
  s3Endpoint: 'http://localhost:9000',
  s3Region: 'us-east-1',
  s3Bucket: 'reportsys',
  s3PublicEndpoint: 'http://localhost:9000',
  s3AccessKeyId: 'minioadmin',
  s3SecretAccessKey: 'minioadmin',
  s3ForcePathStyle: true,
  filesystemStoragePath: '/tmp/reportsys',
  signedUrlTtlSeconds: 900,
  reportMaxConcurrency: 4,
  reportBatchSize: 100,
  reportStreamHighWaterMark: 1024,
  reportPartitionDefaultChunks: 8,
  reportPartitionMaxConcurrency: 2,
  reportReduceEngineV2Enabled: false,
  reportZipMultipassEnabled: false,
  reportAllowedSourceCollections: ['reportSource'],
  reportReduceMaxGroups: 500000,
  reportSourceIndexManagementEnabled: true,
  reportTmpDir: '/tmp/reportsys',
  reportTmpMaxBytes: 1024 * 1024,
  reportPdfMaxRows: 10,
  artifactRetentionDays: 30,
  apiKeyHashPepper: 'pepper',
  devApiKey: 'local-dev-key',
  schedulePollIntervalMs: 30000,
  ...overrides
});

const readBuffer = async (stream: NodeJS.ReadableStream): Promise<Buffer> => {
  const chunks: Buffer[] = [];

  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
};

const objectIdValue = (value: unknown): bigint | null => {
  if (value instanceof ObjectId) {
    return BigInt(`0x${value.toHexString()}`);
  }

  if (typeof value === 'string' && /^[0-9a-fA-F]{24}$/.test(value)) {
    return BigInt(`0x${value.toLowerCase()}`);
  }

  return null;
};

const compareUnknown = (left: unknown, right: unknown): number => {
  const leftId = objectIdValue(left);
  const rightId = objectIdValue(right);

  if (leftId !== null && rightId !== null) {
    if (leftId === rightId) {
      return 0;
    }
    return leftId < rightId ? -1 : 1;
  }

  return String(left).localeCompare(String(right));
};

const matchesQuery = (row: Record<string, unknown>, query: Record<string, unknown>): boolean => {
  if ('$and' in query && Array.isArray(query.$and)) {
    return query.$and.every(
      (child) =>
        child &&
        typeof child === 'object' &&
        matchesQuery(row, child as Record<string, unknown>)
    );
  }

  return Object.entries(query).every(([key, value]) => {
    if (key === '$and') {
      return true;
    }

    const rowValue = row[key];

    if (key === 'tenantId' && rowValue === undefined) {
      return true;
    }

    if (key === '_id' && value && typeof value === 'object' && !Array.isArray(value)) {
      const id = objectIdValue(rowValue);
      if (id === null) {
        return false;
      }

      const operators = value as Record<string, unknown>;

      if (operators.$gte !== undefined) {
        const target = objectIdValue(operators.$gte);
        if (target === null || id < target) {
          return false;
        }
      }

      if (operators.$gt !== undefined) {
        const target = objectIdValue(operators.$gt);
        if (target === null || id <= target) {
          return false;
        }
      }

      if (operators.$lt !== undefined) {
        const target = objectIdValue(operators.$lt);
        if (target === null || id >= target) {
          return false;
        }
      }

      if (operators.$lte !== undefined) {
        const target = objectIdValue(operators.$lte);
        if (target === null || id > target) {
          return false;
        }
      }

      if (operators.$eq !== undefined) {
        return compareUnknown(rowValue, operators.$eq) === 0;
      }

      return true;
    }

    return compareUnknown(rowValue, value) === 0;
  });
};

const createSourceCollection = (
  rows: Array<Record<string, unknown>>
): Collection<Document> => {
  const aggregate = vi.fn((pipeline: Document[] = []) => {
    const list = [...rows];
    const matchStage = pipeline.find((stage) => '$match' in stage) as
      | { $match: Record<string, unknown> }
      | undefined;
    const sortStage = pipeline.find((stage) => '$sort' in stage) as
      | { $sort: Record<string, 1 | -1> }
      | undefined;

    const matched = matchStage
      ? list.filter((row) => matchesQuery(row, matchStage.$match))
      : list;
    const sorted = [...matched];
    if (sortStage && sortStage.$sort._id) {
      const direction = sortStage.$sort._id;
      sorted.sort((left, right) => compareUnknown(left._id, right._id) * direction);
    }

    return {
      async *[Symbol.asyncIterator](): AsyncGenerator<Record<string, unknown>> {
        for (const row of sorted) {
          yield row;
        }
      },
      toArray: async () => sorted
    };
  });

  const find = vi.fn((query: Record<string, unknown>) => {
    const matched = rows.filter((row) => matchesQuery(row, query));
    return {
      sort(sortSpec: { _id: 1 | -1 }) {
        const sorted = [...matched].sort(
          (left, right) => compareUnknown(left._id, right._id) * sortSpec._id
        );
        return {
          limit(limitValue: number) {
            return {
              next: async () => sorted.slice(0, limitValue)[0] ?? null
            };
          }
        };
      }
    };
  });

  return {
    aggregate,
    find
  } as unknown as Collection<Document>;
};

const rowsToAsync = async function* (
  rows: Array<Record<string, unknown>>
): AsyncGenerator<Record<string, unknown>> {
  for (const row of rows) {
    yield row;
  }
};

const createSourceCollectionWithIterables = (
  rows: Array<Record<string, unknown>>
): Collection<Document> => {
  const aggregate = vi.fn(() => ({
    async *[Symbol.asyncIterator](): AsyncGenerator<Record<string, unknown>> {
      for (const row of rows) {
        yield row;
      }
    },
    toArray: async () => rows
  }));

  const find = vi.fn(() => ({
    sort: () => ({
      limit: () => ({
        next: async () => rows.at(-1) ?? null
      })
    })
  }));

  return {
    aggregate,
    find
  } as unknown as Collection<Document>;
};

const baseReport = (overrides: Partial<ReportJobDocument> = {}): ReportJobDocument => ({
  _id: new ObjectId('65e5b6f1de74f57fcb0f97a5'),
  tenantId: 'tenant-a',
  status: 'queued',
  progress: 0,
  rowCount: 0,
  reportDefinitionId: 'sales',
  format: 'json',
  artifact: {
    mode: 'noop',
    available: false,
    reason: 'PENDING'
  },
  createdAt: new Date(),
  expireAt: new Date(Date.now() + 86_400_000),
  ...overrides
});

const createJob = (report: ReportJobDocument): Job<ReportQueueData> =>
  ({
    data: {
      reportJobId: report._id.toHexString(),
      tenantId: report.tenantId
    }
  }) as Job<ReportQueueData>;

describe('createReportJobProcessor', () => {
  it('processes raw json stream and updates job as uploaded', async () => {
    const report = baseReport({
      format: 'json'
    });

    const uploaded: { contentType: string; key: string; payload: Buffer }[] = [];
    const storage = {
      upload: vi.fn(async (input: any) => {
        const payload = await readBuffer(input.stream);
        uploaded.push({
          contentType: input.contentType,
          key: input.key,
          payload
        });
        return {
          mode: 'filesystem',
          available: true,
          key: input.key,
          sizeBytes: payload.length,
          checksum: 'abc'
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([
      { _id: new ObjectId('65e5b6f1de74f57fcb0f97a6'), status: 'paid' },
      { _id: new ObjectId('65e5b6f1de74f57fcb0f97a7'), status: 'pending' }
    ]);

    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await processor(createJob(report));

    expect(storage.upload).toHaveBeenCalledOnce();
    expect(uploaded[0]?.contentType).toBe('application/json');
    expect(uploaded[0]?.payload.toString('utf8')).toContain('"status":"paid"');

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('uploaded');
    expect(finalSet.rowCount).toBe(2);
  });

  it('generates zip multi-format entries when format=zip', async () => {
    const report = baseReport({
      format: 'zip',
      includeFormats: ['csv', 'json']
    });

    const uploadedPayloads: Buffer[] = [];
    const storage = {
      upload: vi.fn(async (input: any) => {
        const payload = await readBuffer(input.stream);
        uploadedPayloads.push(payload);
        return {
          mode: 'filesystem',
          available: true,
          key: input.key,
          sizeBytes: payload.length,
          checksum: 'abc'
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([{ status: 'paid', amount: 10 }]);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await processor(createJob(report));

    expect(storage.upload).toHaveBeenCalledOnce();
    const buffer = uploadedPayloads[0] ?? Buffer.alloc(0);
    expect(buffer.includes(Buffer.from('report.csv'))).toBe(true);
    expect(buffer.includes(Buffer.from('report.json'))).toBe(true);

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('uploaded');
    expect(finalSet.artifact.entries).toEqual(['report.csv', 'report.json']);
  });

  it('applies compression=zip for single format reports', async () => {
    const report = baseReport({
      format: 'csv',
      compression: 'zip'
    });

    let uploadKey = '';
    let uploadPayload = Buffer.alloc(0);

    const storage = {
      upload: vi.fn(async (input: any) => {
        uploadKey = input.key;
        uploadPayload = await readBuffer(input.stream);
        return {
          mode: 'filesystem',
          available: true,
          key: input.key,
          sizeBytes: uploadPayload.length,
          checksum: 'abc'
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([{ status: 'paid', amount: 10 }]);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await processor(createJob(report));

    expect(uploadKey.endsWith('.zip')).toBe(true);
    expect(uploadPayload.includes(Buffer.from('report.csv'))).toBe(true);
  });

  it('runs reduce path when reduceSpec is provided', async () => {
    const report = baseReport({
      format: 'csv',
      reduceSpec: {
        groupBy: ['status'],
        metrics: [{ op: 'count', as: 'totalOrders' }]
      },
      partitionSpec: {
        strategy: 'objectIdRange',
        chunks: 4
      }
    });

    let uploadedText = '';
    const storage = {
      upload: vi.fn(async (input: any) => {
        uploadedText = (await readBuffer(input.stream)).toString('utf8');
        return {
          mode: 'filesystem',
          available: true,
          key: input.key,
          sizeBytes: uploadedText.length,
          checksum: 'abc'
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const runPartitionedReduceFn = vi.fn().mockResolvedValue({
      rows: [{ status: 'paid', totalOrders: 2 }],
      rowsIn: 2,
      rowsOut: 1,
      chunks: 4,
      chunkMetrics: [{ index: 0, durationMs: 12, rowsOut: 1 }]
    });

    const readDb = {
      collection: vi.fn().mockReturnValue(createSourceCollection([]))
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined),
      runPartitionedReduceFn
    });

    await processor(createJob(report));

    expect(runPartitionedReduceFn).toHaveBeenCalledOnce();
    expect((runPartitionedReduceFn as any).mock.calls[0]?.[9]).toEqual({
      useStreamingAccumulator: false,
      maxGroups: 500000
    });
    expect(uploadedText).toContain('status,totalOrders');
    expect(uploadedText).toContain('paid,2');

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.rowCount).toBe(1);
  });

  it('fails gracefully when pdf exceeds configured row limit', async () => {
    const report = baseReport({
      format: 'pdf'
    });

    const storage = {
      upload: vi.fn(async (input: any) => {
        await readBuffer(input.stream);
        return {
          mode: 'filesystem',
          available: true,
          key: input.key
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([{ id: 1 }, { id: 2 }]);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig({
        reportPdfMaxRows: 1
      }),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await expect(processor(createJob(report))).rejects.toThrow(/PDF row limit exceeded/i);
    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('failed');
  });

  it('completes job when storage returns noop artifact', async () => {
    const report = baseReport({
      format: 'csv'
    });

    const storage = {
      upload: vi.fn(async (input: any) => {
        await readBuffer(input.stream);
        return {
          mode: 'noop',
          available: false,
          reason: 'OPTIONAL_INTEGRATION_FAILURE',
          key: input.key
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([{ status: 'paid' }]);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await processor(createJob(report));

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('uploaded');
    expect(finalSet.artifact.mode).toBe('noop');
    expect(finalSet.artifact.available).toBe(false);
  });

  it('fails job when required storage upload throws', async () => {
    const report = baseReport({
      format: 'csv'
    });

    const storage = {
      upload: vi.fn(async () => {
        throw new Error('S3 unavailable');
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const sourceCollection = createSourceCollection([{ status: 'paid' }]);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const processor = createReportJobProcessor({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await expect(processor(createJob(report))).rejects.toThrow('S3 unavailable');

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('failed');
  });

  it('passes reduce streaming options when REPORT_REDUCE_ENGINE_V2_ENABLED=true', async () => {
    const report = baseReport({
      format: 'csv',
      reduceSpec: {
        groupBy: ['status'],
        metrics: [{ op: 'count', as: 'totalOrders' }]
      },
      partitionSpec: {
        strategy: 'objectIdRange',
        chunks: 2
      }
    });

    const runPartitionedReduceFn = vi.fn().mockResolvedValue({
      rows: [{ status: 'paid', totalOrders: 1 }],
      rowsIn: 1,
      rowsOut: 1,
      chunks: 2,
      chunkMetrics: [{ index: 0, durationMs: 5, rowsOut: 1 }]
    });

    const storage = {
      upload: vi.fn(async (input: any) => {
        await readBuffer(input.stream);
        return {
          mode: 'filesystem',
          available: true,
          key: input.key
        };
      })
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const readDb = {
      collection: vi.fn().mockReturnValue(createSourceCollection([]))
    };

    const processor = createReportJobProcessor({
      config: makeConfig({
        reportReduceEngineV2Enabled: true,
        reportReduceMaxGroups: 123
      }),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined),
      runPartitionedReduceFn
    });

    await processor(createJob(report));

    expect((runPartitionedReduceFn as any).mock.calls[0]?.[9]).toEqual({
      useStreamingAccumulator: true,
      maxGroups: 123
    });
  });

  it('uses zip multipass strategy when REPORT_ZIP_MULTIPASS_ENABLED=true', async () => {
    const report = baseReport({
      format: 'zip',
      includeFormats: ['csv', 'json']
    });

    const sourceRows = [
      {
        _id: new ObjectId('65e5b6f1de74f57fcb0f97a6'),
        tenantId: 'tenant-a',
        status: 'paid'
      },
      {
        _id: new ObjectId('65e5b6f1de74f57fcb0f97a7'),
        tenantId: 'tenant-a',
        status: 'pending'
      }
    ];
    const sourceCollection = createSourceCollection(sourceRows);
    const readDb = {
      collection: vi.fn().mockReturnValue(sourceCollection)
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const storage = {
      upload: vi.fn(async (input: any) => {
        await readBuffer(input.stream);
        return {
          mode: 'filesystem',
          available: true,
          key: input.key
        };
      })
    };

    const processor = createReportJobProcessor({
      config: makeConfig({
        reportZipMultipassEnabled: true
      }),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await processor(createJob(report));

    expect((sourceCollection.find as any).mock.calls).toHaveLength(1);
    expect((sourceCollection.aggregate as any).mock.calls).toHaveLength(2);

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.processingStats.zipStrategy).toBe('multipass');
    expect(finalSet.processingStats.rowsIn).toBe(2);
    expect(finalSet.rowCount).toBe(2);
  });

  it('fails when sourceCollection is not in allowlist', async () => {
    const report = baseReport({
      format: 'csv',
      sourceCollection: 'orders'
    });

    const storage = {
      upload: vi.fn()
    };

    const jobsCollection = {
      findOne: vi.fn().mockResolvedValue(report),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const readDb = {
      collection: vi.fn().mockReturnValue(createSourceCollectionWithIterables([]))
    };

    const processor = createReportJobProcessor({
      config: makeConfig({
        reportAllowedSourceCollections: ['reportSource']
      }),
      logger: createLogger(makeConfig()),
      mongo: {
        readDb: readDb as never
      },
      jobsCollection,
      storage: storage as never,
      assertReadReplicaFn: vi.fn().mockResolvedValue(undefined)
    });

    await expect(processor(createJob(report))).rejects.toThrow(
      "sourceCollection 'orders' is not allowed"
    );

    const finalSet = (jobsCollection.updateOne as any).mock.calls.at(-1)?.[1]?.$set;
    expect(finalSet.status).toBe('failed');
    expect(storage.upload).not.toHaveBeenCalled();
  });
});
