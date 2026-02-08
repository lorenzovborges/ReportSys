import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import {
  COLLECTIONS,
  REPORT_QUEUE_NAME,
  closeMongo,
  connectMongo,
  createLogger,
  loadConfig
} from '@reportsys/core';
import { StorageService } from '@reportsys/storage-s3';
import {
  ApiKeyDocument,
  ReportJobDocument,
  ScheduleDocument,
  createApiApp
} from './app';

export const startApi = async () => {
  const config = loadConfig();
  const logger = createLogger(config);

  const mongo = await connectMongo(config);
  const redis = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });
  const queue = new Queue(REPORT_QUEUE_NAME, { connection: redis });

  const storagePolicy = config.integrationStrictMode ? 'required' : 'optional';
  const storage = new StorageService(config, logger, storagePolicy);

  const jobsCollection = mongo.writeDb.collection<ReportJobDocument>(COLLECTIONS.REPORT_JOBS);
  const schedulesCollection =
    mongo.writeDb.collection<ScheduleDocument>(COLLECTIONS.REPORT_SCHEDULES);
  const apiKeysCollection = mongo.writeDb.collection<ApiKeyDocument>(COLLECTIONS.API_KEYS);
  const reportSourceCollection =
    mongo.writeDb.collection<Record<string, unknown>>(COLLECTIONS.REPORT_SOURCE);

  const indexOperations = [
    jobsCollection.createIndex({ tenantId: 1, createdAt: -1 }),
    jobsCollection.createIndex({ status: 1, createdAt: 1 }),
    jobsCollection.createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 }),
    schedulesCollection.createIndex({ tenantId: 1, enabled: 1, nextRunAt: 1 }),
    apiKeysCollection.createIndex({ tenantId: 1, keyHash: 1 }, { unique: true })
  ];

  if (config.reportSourceIndexManagementEnabled) {
    indexOperations.push(reportSourceCollection.createIndex({ tenantId: 1, _id: 1 }));
  }

  await Promise.all(indexOperations);

  const app = createApiApp({
    config,
    queue,
    storage,
    jobsCollection,
    schedulesCollection,
    apiKeysCollection,
    onClose: async () => {
      await redis.quit();
      await closeMongo(mongo);
    }
  });

  await app.listen({ host: '0.0.0.0', port: config.port });

  logger.info({ port: config.port }, 'API listening');

  return app;
};

if (require.main === module) {
  void startApi();
}
