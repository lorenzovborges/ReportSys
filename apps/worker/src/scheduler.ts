import { ObjectId, Collection } from 'mongodb';
import { Queue } from 'bullmq';
import { Logger } from 'pino';
import { AppConfig, computeNextRunAt, REPORT_QUEUE_NAME } from '@reportsys/core';
import { ReportJobDocument, ReportQueueData, ScheduleDocument } from './types';

type QueueLike = Pick<Queue<ReportQueueData>, 'add'>;

export type CreateScheduleTickerDeps = {
  config: AppConfig;
  logger: Logger;
  queue: QueueLike;
  jobsCollection: Collection<ReportJobDocument>;
  schedulesCollection: Collection<ScheduleDocument>;
  computeNextRunAtFn?: typeof computeNextRunAt;
};

export type ScheduleTicker = {
  start: () => Promise<void>;
  stop: () => void;
  tickNow: () => Promise<void>;
};

export const createScheduleTicker = (deps: CreateScheduleTickerDeps): ScheduleTicker => {
  const computeNextRunAtFn = deps.computeNextRunAtFn ?? computeNextRunAt;
  let timer: NodeJS.Timeout | null = null;
  let running = false;

  const runSchedulesTick = async (): Promise<void> => {
    while (true) {
      const now = new Date();
      const due = await deps.schedulesCollection.findOne({
        enabled: true,
        nextRunAt: { $lte: now }
      });

      if (!due || !due.nextRunAt) {
        break;
      }

      let nextRunAt: Date;
      try {
        nextRunAt = computeNextRunAtFn(due.cron, due.timezone, now);
      } catch (error) {
        deps.logger.error(
          {
            scheduleId: due._id.toHexString(),
            tenantId: due.tenantId,
            err: error
          },
          'Invalid cron expression. Disabling schedule'
        );

        await deps.schedulesCollection.updateOne(
          { _id: due._id },
          {
            $set: {
              enabled: false,
              updatedAt: now
            }
          }
        );

        continue;
      }

      const updated = await deps.schedulesCollection.findOneAndUpdate(
        {
          _id: due._id,
          enabled: true,
          nextRunAt: due.nextRunAt
        },
        {
          $set: {
            lastRunAt: now,
            nextRunAt,
            updatedAt: now
          }
        },
        {
          returnDocument: 'after'
        }
      );

      if (!updated) {
        continue;
      }

      const reportJobId = new ObjectId();
      const expireAt = new Date(
        now.getTime() + deps.config.artifactRetentionDays * 24 * 60 * 60 * 1000
      );

      await deps.jobsCollection.insertOne({
        _id: reportJobId,
        tenantId: due.tenantId,
        status: 'queued',
        progress: 0,
        rowCount: 0,
        reportDefinitionId: due.reportDefinitionId,
        format: due.format,
        filters: due.filters,
        timezone: due.timezone,
        compression: due.compression,
        includeFormats: due.includeFormats,
        reduceSpec: due.reduceSpec,
        partitionSpec: due.partitionSpec,
        sourceCollection: due.sourceCollection,
        artifact: {
          mode: deps.config.enableExternalStorage ? deps.config.storageDriver : 'noop',
          available: false,
          reason: 'PENDING'
        },
        createdAt: now,
        expireAt
      });

      await deps.queue.add(
        REPORT_QUEUE_NAME,
        {
          reportJobId: reportJobId.toHexString(),
          tenantId: due.tenantId
        },
        {
          jobId: reportJobId.toHexString(),
          attempts: 5,
          backoff: {
            type: 'exponential',
            delay: 2000
          },
          removeOnComplete: 100,
          removeOnFail: 1000
        }
      );

      deps.logger.info(
        {
          scheduleId: due._id.toHexString(),
          tenantId: due.tenantId,
          reportJobId: reportJobId.toHexString()
        },
        'Scheduled report enqueued'
      );
    }
  };

  const tickNow = async (): Promise<void> => {
    if (running) {
      return;
    }

    running = true;
    try {
      await runSchedulesTick();
    } catch (error) {
      deps.logger.error({ err: error }, 'Schedule tick failed');
      throw error;
    } finally {
      running = false;
    }
  };

  const start = async (): Promise<void> => {
    if (!timer) {
      timer = setInterval(() => {
        void tickNow().catch(() => undefined);
      }, deps.config.schedulePollIntervalMs);
    }

    await tickNow();
  };

  const stop = (): void => {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  };

  return {
    start,
    stop,
    tickNow
  };
};
