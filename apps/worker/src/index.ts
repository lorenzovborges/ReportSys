import { Worker, Queue } from 'bullmq';
import IORedis from 'ioredis';
import {
  COLLECTIONS,
  REPORT_QUEUE_NAME,
  assertReadReplica,
  closeMongo,
  connectMongo,
  createLogger,
  loadConfig
} from '@reportsys/core';
import { StorageService } from '@reportsys/storage-s3';
import { createReportJobProcessor } from './job-processor';
import { createScheduleTicker } from './scheduler';
import { ReportJobDocument, ReportQueueData, ScheduleDocument } from './types';

export type WorkerRuntime = {
  worker: Worker<ReportQueueData>;
  shutdown: () => Promise<void>;
};

export const startWorker = async (): Promise<WorkerRuntime> => {
  const config = loadConfig();
  const logger = createLogger(config);
  const mongo = await connectMongo(config);

  await assertReadReplica(mongo.readDb);

  const redis = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });
  const queue = new Queue<ReportQueueData>(REPORT_QUEUE_NAME, { connection: redis });
  const workerRedis = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });

  const storagePolicy = config.integrationStrictMode ? 'required' : 'optional';
  const storage = new StorageService(config, logger, storagePolicy);

  const jobsCollection = mongo.writeDb.collection<ReportJobDocument>(COLLECTIONS.REPORT_JOBS);
  const schedulesCollection =
    mongo.writeDb.collection<ScheduleDocument>(COLLECTIONS.REPORT_SCHEDULES);

  const processReportJob = createReportJobProcessor({
    config,
    logger,
    mongo,
    jobsCollection,
    storage
  });

  const worker = new Worker<ReportQueueData>(REPORT_QUEUE_NAME, processReportJob, {
    connection: workerRedis,
    concurrency: config.reportMaxConcurrency
  });

  const scheduleTicker = createScheduleTicker({
    config,
    logger,
    queue,
    jobsCollection,
    schedulesCollection
  });

  await scheduleTicker.start();

  worker.on('failed', (failedJob, error) => {
    logger.error(
      {
        jobId: failedJob?.id,
        err: error
      },
      'Queue worker failed a job'
    );
  });

  let shuttingDown = false;

  const shutdown = async (): Promise<void> => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    scheduleTicker.stop();
    await worker.close();
    await queue.close();
    await Promise.all([redis.quit(), workerRedis.quit()]);
    await closeMongo(mongo);
  };

  process.on('SIGINT', () => {
    void shutdown().finally(() => process.exit(0));
  });

  process.on('SIGTERM', () => {
    void shutdown().finally(() => process.exit(0));
  });

  logger.info({ concurrency: config.reportMaxConcurrency }, 'Worker started');

  return {
    worker,
    shutdown
  };
};

if (require.main === module) {
  void startWorker();
}
