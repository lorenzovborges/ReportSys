import { isDeepStrictEqual } from 'node:util';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ObjectId } from 'mongodb';
import { AppConfig } from '@reportsys/core';
import {
  ApiKeyDocument,
  ReportJobDocument,
  ScheduleDocument,
  createApiApp
} from './app';

type CollectionStub<T extends { _id: ObjectId }> = {
  docs: T[];
  findOne: ReturnType<typeof vi.fn>;
  insertOne: ReturnType<typeof vi.fn>;
  updateOne: ReturnType<typeof vi.fn>;
  find: ReturnType<typeof vi.fn>;
};

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

const fieldEquals = (left: unknown, right: unknown): boolean => {
  if (left instanceof ObjectId && right instanceof ObjectId) {
    return left.equals(right);
  }

  if (left instanceof Date && right instanceof Date) {
    return left.getTime() === right.getTime();
  }

  return isDeepStrictEqual(left, right);
};

const matches = (doc: Record<string, unknown>, query: Record<string, unknown>): boolean =>
  Object.entries(query).every(([key, value]) => fieldEquals(doc[key], value));

const compareValues = (left: unknown, right: unknown): number => {
  if (left instanceof Date && right instanceof Date) {
    return left.getTime() - right.getTime();
  }

  if (typeof left === 'number' && typeof right === 'number') {
    return left - right;
  }

  return String(left).localeCompare(String(right));
};

const createCollectionStub = <T extends { _id: ObjectId }>(
  seed: T[] = []
): CollectionStub<T> => {
  const docs = [...seed];

  const findOne = vi.fn(async (query: Record<string, unknown>) => {
    const found = docs.find((doc) => matches(doc as unknown as Record<string, unknown>, query));
    return found ?? null;
  });

  const insertOne = vi.fn(async (doc: T) => {
    docs.push(doc);
    return {
      acknowledged: true,
      insertedId: doc._id
    };
  });

  const updateOne = vi.fn(async (filter: Record<string, unknown>, update: Record<string, any>) => {
    const target = docs.find((doc) => matches(doc as unknown as Record<string, unknown>, filter));

    if (!target) {
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0
      };
    }

    const setData = update.$set ?? {};
    const unsetData = update.$unset ?? {};

    Object.assign(target, setData);

    for (const key of Object.keys(unsetData)) {
      delete (target as Record<string, unknown>)[key];
    }

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1
    };
  });

  const find = vi.fn((query: Record<string, unknown>) => {
    let current = docs.filter((doc) =>
      matches(doc as unknown as Record<string, unknown>, query)
    );

    return {
      sort: (sortInput: Record<string, 1 | -1>) => {
        const [field, direction] = Object.entries(sortInput)[0] ?? ['createdAt', -1];
        current = [...current].sort((left, right) => {
          const result = compareValues(
            (left as Record<string, unknown>)[field],
            (right as Record<string, unknown>)[field]
          );
          return direction < 0 ? -result : result;
        });

        return {
          toArray: async () => current
        };
      }
    };
  });

  return {
    docs,
    findOne,
    insertOne,
    updateOne,
    find
  };
};

const makeReportJob = (overrides: Partial<ReportJobDocument> = {}): ReportJobDocument => ({
  _id: new ObjectId(),
  tenantId: 'tenant-a',
  status: 'uploaded',
  progress: 100,
  rowCount: 3,
  reportDefinitionId: 'sales',
  format: 'json',
  artifact: {
    mode: 'noop',
    available: false,
    reason: 'EXTERNAL_STORAGE_DISABLED'
  },
  createdAt: new Date(),
  expireAt: new Date(Date.now() + 3600_000),
  ...overrides
});

describe('createApiApp', () => {
  const tenantId = 'tenant-a';
  const reportCollection = createCollectionStub<ReportJobDocument>();
  const scheduleCollection = createCollectionStub<ScheduleDocument>();
  const apiKeyCollection = createCollectionStub<ApiKeyDocument>();

  const queue = {
    add: vi.fn(),
    close: vi.fn()
  };

  const storage = {
    createSignedDownloadUrl: vi.fn()
  };

  let app = createApiApp({
    config: makeConfig(),
    queue: queue as never,
    storage: storage as never,
    jobsCollection: reportCollection as never,
    schedulesCollection: scheduleCollection as never,
    apiKeysCollection: apiKeyCollection as never
  });

  const authHeaders = {
    'x-api-key': 'local-dev-key',
    'x-tenant-id': tenantId
  };

  beforeEach(() => {
    reportCollection.docs.length = 0;
    scheduleCollection.docs.length = 0;
    apiKeyCollection.docs.length = 0;
    queue.add.mockReset();
    queue.close.mockReset();
    storage.createSignedDownloadUrl.mockReset();
  });

  afterEach(async () => {
    await app.close();

    app = createApiApp({
      config: makeConfig(),
      queue: queue as never,
      storage: storage as never,
      jobsCollection: reportCollection as never,
      schedulesCollection: scheduleCollection as never,
      apiKeysCollection: apiKeyCollection as never
    });
  });

  it('returns 401 when auth headers are missing', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/report-schedules'
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      message: 'Missing X-API-Key or X-Tenant-Id headers'
    });
  });

  it('accepts local dev api key', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/report-schedules',
      headers: authHeaders
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual([]);
  });

  it('returns 401 for invalid non-dev api key', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/report-schedules',
      headers: {
        'x-api-key': 'invalid',
        'x-tenant-id': tenantId
      }
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      message: 'Invalid API key'
    });
  });

  it('validates report payload for zip/includeFormats/compression', async () => {
    const withoutInclude = await app.inject({
      method: 'POST',
      url: '/v1/reports',
      headers: authHeaders,
      payload: {
        reportDefinitionId: 'sales',
        format: 'zip'
      }
    });

    expect(withoutInclude.statusCode).toBe(400);

    const includeForNonZip = await app.inject({
      method: 'POST',
      url: '/v1/reports',
      headers: authHeaders,
      payload: {
        reportDefinitionId: 'sales',
        format: 'csv',
        includeFormats: ['json']
      }
    });

    expect(includeForNonZip.statusCode).toBe(400);

    const invalidCompression = await app.inject({
      method: 'POST',
      url: '/v1/reports',
      headers: authHeaders,
      payload: {
        reportDefinitionId: 'sales',
        format: 'zip',
        includeFormats: ['csv'],
        compression: 'zip'
      }
    });

    expect(invalidCompression.statusCode).toBe(400);

    const disallowedCollection = await app.inject({
      method: 'POST',
      url: '/v1/reports',
      headers: authHeaders,
      payload: {
        reportDefinitionId: 'sales',
        format: 'csv',
        sourceCollection: 'orders'
      }
    });

    expect(disallowedCollection.statusCode).toBe(400);
    expect(disallowedCollection.json()).toEqual({
      message: "sourceCollection 'orders' is not allowed"
    });
  });

  it('creates report and enqueues job', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/reports',
      headers: authHeaders,
      payload: {
        reportDefinitionId: 'sales',
        format: 'json',
        filters: {
          status: 'paid'
        }
      }
    });

    expect(response.statusCode).toBe(202);
    expect(queue.add).toHaveBeenCalledOnce();
    const body = response.json();
    expect(body.status).toBe('queued');
    expect(typeof body.id).toBe('string');
  });

  it('returns 404 for missing report and supports download contracts', async () => {
    const missingId = new ObjectId().toHexString();
    const notFound = await app.inject({
      method: 'GET',
      url: `/v1/reports/${missingId}`,
      headers: authHeaders
    });

    expect(notFound.statusCode).toBe(404);

    const availableJob = makeReportJob({
      artifact: {
        mode: 'minio',
        available: true,
        key: 'tenant-a/job/report.csv',
        bucket: 'reportsys'
      }
    });

    reportCollection.docs.push(availableJob);
    storage.createSignedDownloadUrl.mockResolvedValue('https://signed.example/report.csv');

    const downloadAvailable = await app.inject({
      method: 'GET',
      url: `/v1/reports/${availableJob._id.toHexString()}/download`,
      headers: authHeaders
    });

    expect(downloadAvailable.statusCode).toBe(200);
    expect(downloadAvailable.json()).toEqual({
      available: true,
      url: 'https://signed.example/report.csv'
    });

    const noopJob = makeReportJob({
      artifact: {
        mode: 'noop',
        available: false,
        reason: 'EXTERNAL_STORAGE_DISABLED'
      }
    });

    reportCollection.docs.push(noopJob);

    const downloadNoop = await app.inject({
      method: 'GET',
      url: `/v1/reports/${noopJob._id.toHexString()}/download`,
      headers: authHeaders
    });

    expect(downloadNoop.statusCode).toBe(200);
    expect(downloadNoop.json()).toEqual({
      available: false,
      mode: 'noop',
      reason: 'EXTERNAL_STORAGE_DISABLED'
    });
  });

  it('supports schedule create/list/patch/delete and validation', async () => {
    const invalidCreate = await app.inject({
      method: 'POST',
      url: '/v1/report-schedules',
      headers: authHeaders,
      payload: {
        name: 'Daily',
        cron: '* * * * *',
        reportDefinitionId: 'sales',
        format: 'zip'
      }
    });

    expect(invalidCreate.statusCode).toBe(400);

    const invalidCollectionCreate = await app.inject({
      method: 'POST',
      url: '/v1/report-schedules',
      headers: authHeaders,
      payload: {
        name: 'Daily',
        cron: '* * * * *',
        reportDefinitionId: 'sales',
        format: 'csv',
        sourceCollection: 'orders'
      }
    });

    expect(invalidCollectionCreate.statusCode).toBe(400);

    const createResponse = await app.inject({
      method: 'POST',
      url: '/v1/report-schedules',
      headers: authHeaders,
      payload: {
        name: 'Daily',
        cron: '* * * * *',
        reportDefinitionId: 'sales',
        format: 'zip',
        includeFormats: ['csv', 'json'],
        timezone: 'UTC',
        enabled: true
      }
    });

    expect(createResponse.statusCode).toBe(201);
    const created = createResponse.json();
    expect(created.name).toBe('Daily');
    expect(created.includeFormats).toEqual(['csv', 'json']);

    const listResponse = await app.inject({
      method: 'GET',
      url: '/v1/report-schedules',
      headers: authHeaders
    });

    expect(listResponse.statusCode).toBe(200);
    expect(listResponse.json()).toHaveLength(1);

    const invalidPatch = await app.inject({
      method: 'PATCH',
      url: `/v1/report-schedules/${created.id}`,
      headers: authHeaders,
      payload: {
        format: 'csv',
        includeFormats: ['json']
      }
    });

    expect(invalidPatch.statusCode).toBe(400);

    const validPatch = await app.inject({
      method: 'PATCH',
      url: `/v1/report-schedules/${created.id}`,
      headers: authHeaders,
      payload: {
        name: 'Daily Updated',
        enabled: false
      }
    });

    expect(validPatch.statusCode).toBe(200);
    expect(validPatch.json().name).toBe('Daily Updated');

    const deleteResponse = await app.inject({
      method: 'DELETE',
      url: `/v1/report-schedules/${created.id}`,
      headers: authHeaders
    });

    expect(deleteResponse.statusCode).toBe(204);
  });
});
