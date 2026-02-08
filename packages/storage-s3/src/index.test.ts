import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { createLogger, AppConfig } from '@reportsys/core';
import { StorageService } from './index';

const makeConfig = (overrides: Partial<AppConfig> = {}): AppConfig => ({
  nodeEnv: 'test',
  port: 3000,
  logLevel: 'silent',
  mongoDbName: 'reportsys',
  mongoWriteUri: 'mongodb://localhost:27017/reportsys',
  mongoReportReadUri: 'mongodb://localhost:27018/reportsys?readPreference=secondary',
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
  filesystemStoragePath: '/tmp/reportsys-test',
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

const context = {
  tenantId: 'tenant-a',
  jobId: 'job-a',
  integration: 'storage'
};

describe('StorageService', () => {
  it('completes in noop mode when external storage is disabled', async () => {
    const config = makeConfig({
      storageDriver: 's3',
      enableExternalStorage: false
    });

    const service = new StorageService(config, createLogger(config), 'optional');

    const result = await service.upload({
      key: 'tenant-a/job-a/report.csv',
      contentType: 'text/csv',
      stream: Readable.from('col1,col2\n1,2\n'),
      context
    });

    expect(result.mode).toBe('noop');
    expect(result.available).toBe(false);
    expect(result.reason).toBe('EXTERNAL_STORAGE_DISABLED');
    expect(result.sizeBytes).toBeGreaterThan(0);
  });

  it('returns signed URL for s3/minio artifacts', async () => {
    const fakeSigner = vi.fn().mockResolvedValue('https://signed.example/reports/file.csv');

    const fakeS3Client = {
      send: vi.fn().mockResolvedValue({})
    };

    const config = makeConfig({
      storageDriver: 's3',
      enableExternalStorage: true
    });

    const service = new StorageService(config, createLogger(config), 'required', {
      s3Client: fakeS3Client as never,
      signer: fakeSigner
    });

    const url = await service.createSignedDownloadUrl({
      mode: 's3',
      available: true,
      key: 'tenant-a/job-a/report.csv',
      bucket: 'reportsys'
    });

    expect(url).toBe('https://signed.example/reports/file.csv');
    expect(fakeSigner).toHaveBeenCalledOnce();
  });

  it('uploads into filesystem storage when configured', async () => {
    const tempDir = await mkdtemp(join(tmpdir(), 'reportsys-storage-test-'));

    try {
      const config = makeConfig({
        storageDriver: 'filesystem',
        enableExternalStorage: true,
        filesystemStoragePath: tempDir
      });

      const service = new StorageService(config, createLogger(config), 'required');

      const artifact = await service.upload({
        key: 'tenant-a/job-a/report.csv',
        contentType: 'text/csv',
        stream: Readable.from('id,amount\n1,2\n'),
        context
      });

      expect(artifact.mode).toBe('filesystem');
      expect(artifact.available).toBe(true);
      expect(typeof artifact.key).toBe('string');

      const content = await readFile(artifact.key as string, 'utf8');
      expect(content).toBe('id,amount\n1,2\n');
    } finally {
      await rm(tempDir, { recursive: true, force: true });
    }
  });

  it('returns filesystem URL for filesystem artifacts', async () => {
    const config = makeConfig({
      storageDriver: 'filesystem',
      enableExternalStorage: true
    });

    const service = new StorageService(config, createLogger(config), 'required');

    const url = await service.createSignedDownloadUrl({
      mode: 'filesystem',
      available: true,
      key: '/tmp/reportsys/tenant-a/job-a/report.csv'
    });

    expect(url).toBe('file:///tmp/reportsys/tenant-a/job-a/report.csv');
  });

  it('returns null for noop or unavailable artifacts', async () => {
    const config = makeConfig({
      storageDriver: 'noop',
      enableExternalStorage: false
    });

    const service = new StorageService(config, createLogger(config), 'optional');

    const noop = await service.createSignedDownloadUrl({
      mode: 'noop',
      available: false,
      reason: 'EXTERNAL_STORAGE_DISABLED'
    });

    const unavailable = await service.createSignedDownloadUrl({
      mode: 'filesystem',
      available: false,
      key: '/tmp/reportsys/report.csv'
    });

    expect(noop).toBeNull();
    expect(unavailable).toBeNull();
  });

  it('keeps signer result when s3PublicEndpoint is configured', async () => {
    const fakeSigner = vi
      .fn()
      .mockResolvedValue(
        'http://minio:9000/reportsys/tenant-a/job-a/report.csv?X-Amz-Signature=abc'
      );

    const fakeS3Client = {
      send: vi.fn().mockResolvedValue({})
    };

    const config = makeConfig({
      storageDriver: 'minio',
      enableExternalStorage: true,
      s3PublicEndpoint: 'http://localhost:9000'
    });

    const service = new StorageService(config, createLogger(config), 'required', {
      s3Client: fakeS3Client as never,
      signer: fakeSigner
    });

    const url = await service.createSignedDownloadUrl({
      mode: 'minio',
      available: true,
      key: 'tenant-a/job-a/report.csv',
      bucket: 'reportsys'
    });

    expect(url).toBe(
      'http://minio:9000/reportsys/tenant-a/job-a/report.csv?X-Amz-Signature=abc'
    );
    expect(fakeSigner).toHaveBeenCalledOnce();
  });

  it('returns null for s3 artifact when signer client is unavailable', async () => {
    const config = makeConfig({
      storageDriver: 'noop',
      enableExternalStorage: true
    });

    const service = new StorageService(config, createLogger(config), 'optional');

    const result = await service.createSignedDownloadUrl({
      mode: 's3',
      available: true,
      key: 'tenant-a/job-a/report.csv'
    });

    expect(result).toBeNull();
  });

  it('fails when required integration is unavailable', async () => {
    const config = makeConfig({
      storageDriver: 's3',
      enableExternalStorage: true,
      s3Endpoint: 'http://127.0.0.1:1'
    });

    const service = new StorageService(config, createLogger(config), 'required');

    await expect(
      service.upload({
        key: 'tenant-a/job-a/report.csv',
        contentType: 'text/csv',
        stream: Readable.from('hello'),
        context
      })
    ).rejects.toThrow();
  });

  it('falls back to noop when optional integration fails', async () => {
    const config = makeConfig({
      storageDriver: 'minio',
      enableExternalStorage: true,
      s3Endpoint: 'http://127.0.0.1:1'
    });

    const service = new StorageService(config, createLogger(config), 'optional');

    const result = await service.upload({
      key: 'tenant-a/job-a/report.csv',
      contentType: 'text/csv',
      stream: Readable.from('row-1'),
      context
    });

    expect(result.mode).toBe('noop');
    expect(result.available).toBe(false);
    expect(result.reason).toBe('OPTIONAL_INTEGRATION_FAILURE');
  });

  it('healthCheck skips for noop/filesystem and uses s3 client for s3/minio', async () => {
    const noopConfig = makeConfig({
      storageDriver: 'noop',
      enableExternalStorage: false
    });
    const noopService = new StorageService(noopConfig, createLogger(noopConfig), 'optional');
    await expect(noopService.healthCheck()).resolves.toBeUndefined();

    const fsConfig = makeConfig({
      storageDriver: 'filesystem',
      enableExternalStorage: true
    });
    const fsService = new StorageService(fsConfig, createLogger(fsConfig), 'optional');
    await expect(fsService.healthCheck()).resolves.toBeUndefined();

    const fakeS3Client = {
      send: vi.fn().mockResolvedValue({})
    };

    const s3Config = makeConfig({
      storageDriver: 's3',
      enableExternalStorage: true
    });
    const s3Service = new StorageService(s3Config, createLogger(s3Config), 'required', {
      s3Client: fakeS3Client as never
    });

    await s3Service.healthCheck();
    expect(fakeS3Client.send).toHaveBeenCalledOnce();
  });

  it('healthCheck throws when s3 mode has no initialized client', async () => {
    const config = makeConfig({
      storageDriver: 's3',
      enableExternalStorage: true
    });

    const service = new StorageService(config, createLogger(config), 'required');
    (service as any).s3Client = undefined;

    await expect(service.healthCheck()).rejects.toThrow(/S3 client not initialized/i);
  });
});
