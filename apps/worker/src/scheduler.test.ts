import { Collection, ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { AppConfig, createLogger } from '@reportsys/core';
import { createScheduleTicker } from './scheduler';
import { ReportJobDocument, ScheduleDocument } from './types';

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
  reportBatchSize: 2000,
  reportStreamHighWaterMark: 65536,
  reportPartitionDefaultChunks: 32,
  reportPartitionMaxConcurrency: 4,
  reportReduceEngineV2Enabled: false,
  reportZipMultipassEnabled: false,
  reportAllowedSourceCollections: ['reportSource'],
  reportReduceMaxGroups: 500000,
  reportSourceIndexManagementEnabled: true,
  reportTmpDir: '/tmp/reportsys',
  reportTmpMaxBytes: 2147483648,
  reportPdfMaxRows: 10000,
  artifactRetentionDays: 30,
  apiKeyHashPepper: 'pepper',
  devApiKey: 'local-dev-key',
  schedulePollIntervalMs: 30000,
  ...overrides
});

const makeSchedule = (): ScheduleDocument => ({
  _id: new ObjectId('65e5b6f1de74f57fcb0f97a5'),
  tenantId: 'tenant-a',
  name: 'Daily',
  cron: '*/5 * * * *',
  reportDefinitionId: 'sales',
  format: 'csv',
  timezone: 'UTC',
  enabled: true,
  createdAt: new Date(),
  updatedAt: new Date(),
  nextRunAt: new Date(Date.now() - 1000)
});

describe('createScheduleTicker', () => {
  it('enqueues due schedules and persists a report job', async () => {
    const schedule = makeSchedule();

    const schedulesCollection = {
      findOne: vi
        .fn()
        .mockResolvedValueOnce(schedule)
        .mockResolvedValueOnce(null),
      findOneAndUpdate: vi.fn().mockResolvedValue(schedule),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ScheduleDocument>;

    const jobsCollection = {
      insertOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ReportJobDocument>;

    const queue = {
      add: vi.fn().mockResolvedValue({})
    };

    const ticker = createScheduleTicker({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      queue: queue as never,
      jobsCollection,
      schedulesCollection
    });

    await ticker.tickNow();

    expect(jobsCollection.insertOne).toHaveBeenCalledOnce();
    expect(queue.add).toHaveBeenCalledOnce();
  });

  it('disables schedule when cron expression is invalid', async () => {
    const schedule = makeSchedule();

    const schedulesCollection = {
      findOne: vi
        .fn()
        .mockResolvedValueOnce(schedule)
        .mockResolvedValueOnce(null),
      findOneAndUpdate: vi.fn(),
      updateOne: vi.fn().mockResolvedValue({})
    } as unknown as Collection<ScheduleDocument>;

    const jobsCollection = {
      insertOne: vi.fn()
    } as unknown as Collection<ReportJobDocument>;

    const queue = {
      add: vi.fn()
    };

    const ticker = createScheduleTicker({
      config: makeConfig(),
      logger: createLogger(makeConfig()),
      queue: queue as never,
      jobsCollection,
      schedulesCollection,
      computeNextRunAtFn: vi.fn(() => {
        throw new Error('invalid cron');
      }) as never
    });

    await ticker.tickNow();

    expect(schedulesCollection.updateOne).toHaveBeenCalledOnce();
    expect(queue.add).not.toHaveBeenCalled();
  });
});
