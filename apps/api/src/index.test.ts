import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => {
  const redisQuit = vi.fn().mockResolvedValue(undefined);
  const redisCtor = vi.fn().mockImplementation(() => ({
    quit: redisQuit
  }));
  const queueCtor = vi.fn().mockImplementation(() => ({}));
  const createIndex = vi.fn().mockResolvedValue(undefined);
  const collection = vi.fn(() => ({
    createIndex
  }));

  const connectMongo = vi.fn().mockResolvedValue({
    writeDb: {
      collection
    }
  });

  const appListen = vi.fn().mockResolvedValue(undefined);
  const appClose = vi.fn().mockResolvedValue(undefined);

  return {
    redisQuit,
    redisCtor,
    queueCtor,
    createIndex,
    collection,
    connectMongo,
    closeMongo: vi.fn().mockResolvedValue(undefined),
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
    createLogger: vi.fn(() => ({
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn()
    })),
    storageCtor: vi.fn(),
    createApiApp: vi.fn(() => ({
      listen: appListen,
      close: appClose
    })),
    appListen
  };
});

vi.mock('ioredis', () => ({
  default: mocks.redisCtor
}));

vi.mock('bullmq', () => ({
  Queue: mocks.queueCtor
}));

vi.mock('@reportsys/core', () => ({
  COLLECTIONS: {
    REPORT_JOBS: 'reportJobs',
    REPORT_SCHEDULES: 'reportSchedules',
    API_KEYS: 'apiKeys',
    REPORT_SOURCE: 'reportSource'
  },
  REPORT_QUEUE_NAME: 'report-jobs',
  connectMongo: mocks.connectMongo,
  closeMongo: mocks.closeMongo,
  loadConfig: mocks.loadConfig,
  createLogger: mocks.createLogger
}));

vi.mock('@reportsys/storage-s3', () => ({
  StorageService: mocks.storageCtor.mockImplementation(() => ({}))
}));

vi.mock('./app', () => ({
  createApiApp: mocks.createApiApp
}));

describe('startApi', () => {
  it('boots API wiring and calls listen', async () => {
    const { startApi } = await import('./index');
    await startApi();

    expect(mocks.connectMongo).toHaveBeenCalledOnce();
    expect(mocks.queueCtor).toHaveBeenCalledOnce();
    expect(mocks.storageCtor).toHaveBeenCalledOnce();
    expect(mocks.createApiApp).toHaveBeenCalledOnce();
    expect(mocks.createIndex).toHaveBeenCalledTimes(6);
    expect(mocks.appListen).toHaveBeenCalledOnce();
  });
});
