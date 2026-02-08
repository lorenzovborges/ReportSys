import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => {
  const queueClose = vi.fn().mockResolvedValue(undefined);
  const queueAdd = vi.fn().mockResolvedValue(undefined);
  const workerClose = vi.fn().mockResolvedValue(undefined);
  const workerOn = vi.fn();
  const redisQuit = vi.fn().mockResolvedValue(undefined);
  const redisCtor = vi.fn().mockImplementation(() => ({
    quit: redisQuit
  }));
  const queueCtor = vi.fn().mockImplementation(() => ({
    add: queueAdd,
    close: queueClose
  }));
  const workerCtor = vi.fn().mockImplementation(() => ({
    close: workerClose,
    on: workerOn
  }));

  const jobsCollection = {};
  const schedulesCollection = {};

  const writeDb = {
    collection: vi.fn((name: string) => {
      if (name === 'reportJobs') {
        return jobsCollection;
      }
      return schedulesCollection;
    })
  };

  const readDb = {
    collection: vi.fn()
  };

  const connectMongo = vi.fn().mockResolvedValue({
    readDb,
    writeDb
  });

  return {
    queueClose,
    queueAdd,
    workerClose,
    workerOn,
    redisQuit,
    redisCtor,
    queueCtor,
    workerCtor,
    jobsCollection,
    schedulesCollection,
    connectMongo,
    assertReadReplica: vi.fn().mockResolvedValue(undefined),
    closeMongo: vi.fn().mockResolvedValue(undefined),
    createLogger: vi.fn(() => ({
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn()
    })),
    loadConfig: vi.fn(() => ({
      nodeEnv: 'test',
      port: 3000,
      logLevel: 'silent',
      mongoDbName: 'reportsys',
      mongoWriteUri: 'mongodb://localhost:27017/reportsys',
      mongoReportReadUri: 'mongodb://localhost:27018/reportsys',
      redisUrl: 'redis://localhost:6379',
      storageDriver: 'noop',
      enableExternalStorage: false,
      integrationStrictMode: false,
      enableWebhooks: false,
      s3Endpoint: 'http://localhost:9000',
      s3Region: 'us-east-1',
      s3Bucket: 'reportsys',
      s3PublicEndpoint: '',
      s3AccessKeyId: 'x',
      s3SecretAccessKey: 'y',
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
      reportPdfMaxRows: 1000,
      artifactRetentionDays: 30,
      apiKeyHashPepper: 'pepper',
      devApiKey: 'local-dev-key',
      schedulePollIntervalMs: 30000
    })),
    storageCtor: vi.fn(),
    processJobFactory: vi.fn(() => vi.fn().mockResolvedValue(undefined)),
    tickerStart: vi.fn().mockResolvedValue(undefined),
    tickerStop: vi.fn(),
    tickerFactory: vi.fn()
  };
});

vi.mock('ioredis', () => ({
  default: mocks.redisCtor
}));

vi.mock('bullmq', () => ({
  Queue: mocks.queueCtor,
  Worker: mocks.workerCtor
}));

vi.mock('@reportsys/core', () => ({
  COLLECTIONS: {
    REPORT_JOBS: 'reportJobs',
    REPORT_SCHEDULES: 'reportSchedules'
  },
  REPORT_QUEUE_NAME: 'report-jobs',
  connectMongo: mocks.connectMongo,
  assertReadReplica: mocks.assertReadReplica,
  closeMongo: mocks.closeMongo,
  createLogger: mocks.createLogger,
  loadConfig: mocks.loadConfig
}));

vi.mock('@reportsys/storage-s3', () => ({
  StorageService: mocks.storageCtor.mockImplementation(() => ({}))
}));

vi.mock('./job-processor', () => ({
  createReportJobProcessor: mocks.processJobFactory
}));

vi.mock('./scheduler', () => ({
  createScheduleTicker: mocks.tickerFactory.mockImplementation(() => ({
    start: mocks.tickerStart,
    stop: mocks.tickerStop,
    tickNow: vi.fn()
  }))
}));

describe('startWorker', () => {
  it('wires worker runtime and supports shutdown', async () => {
    const { startWorker } = await import('./index');

    const runtime = await startWorker();

    expect(mocks.connectMongo).toHaveBeenCalledOnce();
    expect(mocks.assertReadReplica).toHaveBeenCalledOnce();
    expect(mocks.processJobFactory).toHaveBeenCalledOnce();
    expect(mocks.tickerFactory).toHaveBeenCalledOnce();
    expect(mocks.tickerStart).toHaveBeenCalledOnce();
    expect(mocks.workerCtor).toHaveBeenCalledOnce();

    await runtime.shutdown();

    expect(mocks.tickerStop).toHaveBeenCalledOnce();
    expect(mocks.workerClose).toHaveBeenCalledOnce();
    expect(mocks.queueClose).toHaveBeenCalledOnce();
    expect(mocks.closeMongo).toHaveBeenCalledOnce();
    expect(mocks.redisQuit).toHaveBeenCalledTimes(2);
  });
});
