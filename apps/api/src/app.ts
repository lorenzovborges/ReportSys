import Fastify, { FastifyInstance } from 'fastify';
import { Queue } from 'bullmq';
import { Collection, ObjectId } from 'mongodb';
import { z } from 'zod';
import {
  AppConfig,
  COLLECTIONS,
  REPORT_QUEUE_NAME,
  computeNextRunAt,
  hashApiKey,
  resolveAllowedSourceCollection
} from '@reportsys/core';
import {
  CreateReportRequest,
  CreateScheduleRequest,
  PartitionSpec,
  ReportProcessingStats,
  ReduceSpec,
  ReportArtifactView,
  ReportJobView,
  ReportIncludeFormat,
  ScheduleView,
  SignedDownloadResponse,
  UpdateScheduleRequest
} from '@reportsys/contracts';
import { StorageService } from '@reportsys/storage-s3';

declare module 'fastify' {
  interface FastifyRequest {
    tenantId?: string;
  }
}

export type ReportJobDocument = {
  _id: ObjectId;
  tenantId: string;
  status: 'queued' | 'running' | 'uploading' | 'uploaded' | 'failed' | 'expired';
  progress: number;
  rowCount: number;
  reportDefinitionId: string;
  format: CreateReportRequest['format'];
  filters?: Record<string, unknown>;
  timezone?: string;
  locale?: string;
  compression?: 'none' | 'zip';
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  sourceCollection?: string;
  processingStats?: ReportProcessingStats;
  artifact: ReportArtifactView;
  error?: {
    message: string;
  };
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  expireAt: Date;
};

export type ScheduleDocument = {
  _id: ObjectId;
  tenantId: string;
  name: string;
  cron: string;
  reportDefinitionId: string;
  format: CreateScheduleRequest['format'];
  filters?: Record<string, unknown>;
  timezone: string;
  enabled: boolean;
  compression?: 'none' | 'zip';
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  sourceCollection?: string;
  nextRunAt?: Date;
  lastRunAt?: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type ApiKeyDocument = {
  _id: ObjectId;
  tenantId: string;
  keyHash: string;
  status: 'active' | 'revoked';
  scopes?: string[];
};

type QueueLike = Pick<Queue, 'add' | 'close'>;
type StorageLike = Pick<StorageService, 'createSignedDownloadUrl'>;

export type CreateApiAppDeps = {
  config: AppConfig;
  queue: QueueLike;
  storage: StorageLike;
  jobsCollection: Collection<ReportJobDocument>;
  schedulesCollection: Collection<ScheduleDocument>;
  apiKeysCollection: Collection<ApiKeyDocument>;
  onClose?: () => Promise<void>;
  computeNextRunAtFn?: typeof computeNextRunAt;
  hashApiKeyFn?: typeof hashApiKey;
};

const safeField = /^[A-Za-z0-9_]+$/;

const reduceMetricSchema = z
  .object({
    op: z.enum(['count', 'sum', 'min', 'max', 'avg']),
    field: z.string().regex(safeField).optional(),
    as: z.string().regex(safeField)
  })
  .superRefine((metric, context) => {
    if (metric.op !== 'count' && !metric.field) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: `metric '${metric.as}' with op '${metric.op}' requires field`
      });
    }
  });

const reduceSpecSchema = z
  .object({
    groupBy: z.array(z.string().regex(safeField)).default([]),
    metrics: z.array(reduceMetricSchema).min(1)
  })
  .superRefine((spec, context) => {
    const aliases = new Set<string>();

    for (const metric of spec.metrics) {
      if (aliases.has(metric.as)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: `duplicated metric alias '${metric.as}'`
        });
      }
      aliases.add(metric.as);
    }
  });

const partitionSpecSchema = z.object({
  strategy: z.literal('objectIdRange'),
  chunks: z.number().int().positive().optional()
});

const includeFormatsSchema = z.array(z.enum(['csv', 'xlsx', 'json', 'pdf'])).min(1);

const hasDuplicateValues = (values: string[] | undefined): boolean => {
  if (!values) {
    return false;
  }

  const unique = new Set(values);
  return unique.size !== values.length;
};

const createReportSchema = z
  .object({
    reportDefinitionId: z.string().min(1),
    format: z.enum(['csv', 'xlsx', 'json', 'pdf', 'zip']),
    filters: z.record(z.string(), z.unknown()).optional(),
    timezone: z.string().optional(),
    locale: z.string().optional(),
    compression: z.enum(['none', 'zip']).optional(),
    includeFormats: includeFormatsSchema.optional(),
    reduceSpec: reduceSpecSchema.optional(),
    partitionSpec: partitionSpecSchema.optional(),
    sourceCollection: z.string().regex(safeField).optional()
  })
  .superRefine((input, context) => {
    if (input.format === 'zip' && (!input.includeFormats || input.includeFormats.length === 0)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is required when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.format !== 'zip' && input.includeFormats?.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is allowed only when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.compression === 'zip' && input.format === 'zip') {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'compression=zip cannot be used with format=zip',
        path: ['compression']
      });
    }

    if (input.includeFormats && hasDuplicateValues(input.includeFormats)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats cannot contain duplicates',
        path: ['includeFormats']
      });
    }
  });

const createScheduleSchema = z
  .object({
    name: z.string().min(1),
    cron: z.string().min(1),
    reportDefinitionId: z.string().min(1),
    format: z.enum(['csv', 'xlsx', 'json', 'pdf', 'zip']),
    filters: z.record(z.string(), z.unknown()).optional(),
    timezone: z.string().default('UTC'),
    enabled: z.boolean().optional(),
    compression: z.enum(['none', 'zip']).optional(),
    includeFormats: includeFormatsSchema.optional(),
    reduceSpec: reduceSpecSchema.optional(),
    partitionSpec: partitionSpecSchema.optional(),
    sourceCollection: z.string().regex(safeField).optional()
  })
  .superRefine((input, context) => {
    if (input.format === 'zip' && (!input.includeFormats || input.includeFormats.length === 0)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is required when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.format !== 'zip' && input.includeFormats?.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is allowed only when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.compression === 'zip' && input.format === 'zip') {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'compression=zip cannot be used with format=zip',
        path: ['compression']
      });
    }

    if (hasDuplicateValues(input.includeFormats)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats cannot contain duplicates',
        path: ['includeFormats']
      });
    }
  });

const updateScheduleSchema = z
  .object({
    name: z.string().min(1).optional(),
    cron: z.string().min(1).optional(),
    reportDefinitionId: z.string().min(1).optional(),
    format: z.enum(['csv', 'xlsx', 'json', 'pdf', 'zip']).optional(),
    filters: z.record(z.string(), z.unknown()).optional(),
    timezone: z.string().optional(),
    enabled: z.boolean().optional(),
    compression: z.enum(['none', 'zip']).optional(),
    includeFormats: includeFormatsSchema.optional(),
    reduceSpec: reduceSpecSchema.optional(),
    partitionSpec: partitionSpecSchema.optional(),
    sourceCollection: z.string().regex(safeField).optional()
  })
  .superRefine((input, context) => {
    if (input.format === 'zip' && (!input.includeFormats || input.includeFormats.length === 0)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is required when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.format !== undefined && input.format !== 'zip' && input.includeFormats?.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats is allowed only when format=zip',
        path: ['includeFormats']
      });
    }

    if (input.compression === 'zip' && input.format === 'zip') {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'compression=zip cannot be used with format=zip',
        path: ['compression']
      });
    }

    if (hasDuplicateValues(input.includeFormats)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'includeFormats cannot contain duplicates',
        path: ['includeFormats']
      });
    }
  });

const toReportJobView = (job: ReportJobDocument): ReportJobView => ({
  id: job._id.toHexString(),
  tenantId: job.tenantId,
  status: job.status,
  progress: job.progress,
  rowCount: job.rowCount,
  reportDefinitionId: job.reportDefinitionId,
  format: job.format,
  artifact: job.artifact,
  processingStats: job.processingStats,
  error: job.error,
  createdAt: job.createdAt.toISOString(),
  startedAt: job.startedAt?.toISOString(),
  finishedAt: job.finishedAt?.toISOString()
});

const toScheduleView = (schedule: ScheduleDocument): ScheduleView => ({
  id: schedule._id.toHexString(),
  tenantId: schedule.tenantId,
  name: schedule.name,
  cron: schedule.cron,
  reportDefinitionId: schedule.reportDefinitionId,
  format: schedule.format,
  filters: schedule.filters,
  timezone: schedule.timezone,
  enabled: schedule.enabled,
  compression: schedule.compression,
  includeFormats: schedule.includeFormats,
  reduceSpec: schedule.reduceSpec,
  partitionSpec: schedule.partitionSpec,
  nextRunAt: schedule.nextRunAt?.toISOString(),
  lastRunAt: schedule.lastRunAt?.toISOString(),
  createdAt: schedule.createdAt.toISOString()
});

const parseObjectId = (rawId: string): ObjectId | null => {
  try {
    return new ObjectId(rawId);
  } catch {
    return null;
  }
};

const validateSourceCollectionInput = (
  sourceCollection: string | undefined,
  config: AppConfig
): string | null => {
  try {
    resolveAllowedSourceCollection(
      sourceCollection,
      config.reportAllowedSourceCollections,
      COLLECTIONS.REPORT_SOURCE
    );
    return null;
  } catch (error) {
    return (error as Error).message;
  }
};

export const createApiApp = (deps: CreateApiAppDeps): FastifyInstance => {
  const app = Fastify({ logger: false });

  const computeNextRunAtFn = deps.computeNextRunAtFn ?? computeNextRunAt;
  const hashApiKeyFn = deps.hashApiKeyFn ?? hashApiKey;

  app.addHook('preHandler', async (request, reply) => {
    const routeConfig = request.routeOptions.config as { auth?: boolean } | undefined;
    const authEnabled = routeConfig?.auth !== false;

    if (!authEnabled) {
      return;
    }

    const apiKey = request.headers['x-api-key'];
    const tenantId = request.headers['x-tenant-id'];

    if (typeof apiKey !== 'string' || typeof tenantId !== 'string') {
      reply.code(401).send({ message: 'Missing X-API-Key or X-Tenant-Id headers' });
      return;
    }

    request.tenantId = tenantId;

    if (apiKey === deps.config.devApiKey) {
      return;
    }

    const keyHash = hashApiKeyFn(apiKey, deps.config.apiKeyHashPepper);
    const keyDoc = await deps.apiKeysCollection.findOne({
      tenantId,
      keyHash,
      status: 'active'
    });

    if (!keyDoc) {
      reply.code(401).send({ message: 'Invalid API key' });
      return;
    }
  });

  app.get('/health', { config: { auth: false } }, async () => ({
    status: 'ok',
    storageDriver: deps.config.storageDriver,
    externalStorageEnabled: deps.config.enableExternalStorage
  }));

  app.post('/v1/reports', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const parsed = createReportSchema.safeParse(request.body);

    if (!parsed.success) {
      reply.code(400).send({ message: parsed.error.flatten() });
      return;
    }

    const body = parsed.data;
    const sourceCollectionError = validateSourceCollectionInput(body.sourceCollection, deps.config);

    if (sourceCollectionError) {
      reply.code(400).send({ message: sourceCollectionError });
      return;
    }

    const now = new Date();
    const expireAt = new Date(
      now.getTime() + deps.config.artifactRetentionDays * 24 * 60 * 60 * 1000
    );
    const reportId = new ObjectId();

    const doc: ReportJobDocument = {
      _id: reportId,
      tenantId,
      status: 'queued',
      progress: 0,
      rowCount: 0,
      reportDefinitionId: body.reportDefinitionId,
      format: body.format,
      filters: body.filters,
      timezone: body.timezone,
      locale: body.locale,
      compression: body.compression,
      includeFormats: body.includeFormats,
      reduceSpec: body.reduceSpec as ReduceSpec | undefined,
      partitionSpec: body.partitionSpec as PartitionSpec | undefined,
      sourceCollection: body.sourceCollection,
      artifact: {
        mode: deps.config.enableExternalStorage ? deps.config.storageDriver : 'noop',
        available: false,
        reason: 'PENDING'
      },
      createdAt: now,
      expireAt
    };

    await deps.jobsCollection.insertOne(doc);

    await deps.queue.add(
      REPORT_QUEUE_NAME,
      {
        reportJobId: reportId.toHexString(),
        tenantId
      },
      {
        jobId: reportId.toHexString(),
        attempts: 5,
        backoff: {
          type: 'exponential',
          delay: 2000
        },
        removeOnComplete: 100,
        removeOnFail: 1000
      }
    );

    reply.code(202).send({
      id: reportId.toHexString(),
      status: 'queued'
    });
  });

  app.get('/v1/reports/:jobId', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const rawJobId = (request.params as { jobId: string }).jobId;
    const jobId = parseObjectId(rawJobId);

    if (!jobId) {
      reply.code(400).send({ message: 'Invalid job id' });
      return;
    }

    const job = await deps.jobsCollection.findOne({ _id: jobId, tenantId });

    if (!job) {
      reply.code(404).send({ message: 'Job not found' });
      return;
    }

    reply.send(toReportJobView(job));
  });

  app.get('/v1/reports/:jobId/download', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const rawJobId = (request.params as { jobId: string }).jobId;
    const jobId = parseObjectId(rawJobId);

    if (!jobId) {
      reply.code(400).send({ message: 'Invalid job id' });
      return;
    }

    const job = await deps.jobsCollection.findOne({ _id: jobId, tenantId });

    if (!job) {
      reply.code(404).send({ message: 'Job not found' });
      return;
    }

    if (!job.artifact?.available) {
      const unavailable: SignedDownloadResponse = {
        available: false,
        mode: job.artifact?.mode ?? 'noop',
        reason: job.artifact?.reason ?? 'EXTERNAL_STORAGE_DISABLED'
      };

      reply.send(unavailable);
      return;
    }

    const url = await deps.storage.createSignedDownloadUrl(job.artifact);

    if (!url) {
      const unavailable: SignedDownloadResponse = {
        available: false,
        mode: job.artifact.mode,
        reason: 'DOWNLOAD_URL_UNAVAILABLE'
      };

      reply.send(unavailable);
      return;
    }

    reply.send({
      available: true,
      url
    } satisfies SignedDownloadResponse);
  });

  app.post('/v1/report-schedules', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const parsed = createScheduleSchema.safeParse(request.body);

    if (!parsed.success) {
      reply.code(400).send({ message: parsed.error.flatten() });
      return;
    }

    const body = parsed.data;
    const sourceCollectionError = validateSourceCollectionInput(body.sourceCollection, deps.config);

    if (sourceCollectionError) {
      reply.code(400).send({ message: sourceCollectionError });
      return;
    }

    const now = new Date();
    const nextRunAt = computeNextRunAtFn(body.cron, body.timezone, now);
    const scheduleId = new ObjectId();

    const schedule: ScheduleDocument = {
      _id: scheduleId,
      tenantId,
      name: body.name,
      cron: body.cron,
      reportDefinitionId: body.reportDefinitionId,
      format: body.format,
      filters: body.filters,
      timezone: body.timezone,
      enabled: body.enabled ?? true,
      compression: body.compression,
      includeFormats: body.includeFormats,
      reduceSpec: body.reduceSpec as ReduceSpec | undefined,
      partitionSpec: body.partitionSpec as PartitionSpec | undefined,
      sourceCollection: body.sourceCollection,
      nextRunAt,
      createdAt: now,
      updatedAt: now
    };

    await deps.schedulesCollection.insertOne(schedule);

    reply.code(201).send(toScheduleView(schedule));
  });

  app.get('/v1/report-schedules', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const schedules = await deps.schedulesCollection
      .find({ tenantId })
      .sort({ createdAt: -1 })
      .toArray();

    reply.send(schedules.map(toScheduleView));
  });

  app.patch('/v1/report-schedules/:scheduleId', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const rawScheduleId = (request.params as { scheduleId: string }).scheduleId;
    const scheduleId = parseObjectId(rawScheduleId);

    if (!scheduleId) {
      reply.code(400).send({ message: 'Invalid schedule id' });
      return;
    }

    const parsed = updateScheduleSchema.safeParse(request.body);

    if (!parsed.success) {
      reply.code(400).send({ message: parsed.error.flatten() });
      return;
    }

    const payload = parsed.data as UpdateScheduleRequest;
    const existing = await deps.schedulesCollection.findOne({ _id: scheduleId, tenantId });

    if (!existing) {
      reply.code(404).send({ message: 'Schedule not found' });
      return;
    }

    const nextState = {
      ...existing,
      ...payload,
      updatedAt: new Date()
    };

    if (nextState.format === 'zip' && (!nextState.includeFormats || !nextState.includeFormats.length)) {
      reply.code(400).send({ message: 'includeFormats is required when format=zip' });
      return;
    }

    if (nextState.format !== 'zip' && nextState.includeFormats?.length) {
      reply.code(400).send({ message: 'includeFormats is allowed only when format=zip' });
      return;
    }

    if (nextState.compression === 'zip' && nextState.format === 'zip') {
      reply.code(400).send({ message: 'compression=zip cannot be used with format=zip' });
      return;
    }

    if (hasDuplicateValues(nextState.includeFormats)) {
      reply.code(400).send({ message: 'includeFormats cannot contain duplicates' });
      return;
    }

    const sourceCollectionError = validateSourceCollectionInput(
      nextState.sourceCollection,
      deps.config
    );

    if (sourceCollectionError) {
      reply.code(400).send({ message: sourceCollectionError });
      return;
    }

    if (payload.cron || payload.timezone || payload.enabled !== undefined) {
      if (nextState.enabled) {
        nextState.nextRunAt = computeNextRunAtFn(
          nextState.cron,
          nextState.timezone,
          new Date()
        );
      } else {
        nextState.nextRunAt = undefined;
      }
    }

    await deps.schedulesCollection.updateOne(
      { _id: scheduleId, tenantId },
      {
        $set: {
          name: nextState.name,
          cron: nextState.cron,
          reportDefinitionId: nextState.reportDefinitionId,
          format: nextState.format,
          filters: nextState.filters,
          timezone: nextState.timezone,
          enabled: nextState.enabled,
          compression: nextState.compression,
          includeFormats: nextState.includeFormats,
          reduceSpec: nextState.reduceSpec,
          partitionSpec: nextState.partitionSpec,
          sourceCollection: nextState.sourceCollection,
          nextRunAt: nextState.nextRunAt,
          updatedAt: nextState.updatedAt
        }
      }
    );

    const updated = await deps.schedulesCollection.findOne({ _id: scheduleId, tenantId });

    reply.send(toScheduleView(updated as ScheduleDocument));
  });

  app.delete('/v1/report-schedules/:scheduleId', async (request, reply) => {
    const tenantId = request.tenantId as string;
    const rawScheduleId = (request.params as { scheduleId: string }).scheduleId;
    const scheduleId = parseObjectId(rawScheduleId);

    if (!scheduleId) {
      reply.code(400).send({ message: 'Invalid schedule id' });
      return;
    }

    const result = await deps.schedulesCollection.updateOne(
      { _id: scheduleId, tenantId },
      {
        $set: {
          enabled: false,
          nextRunAt: undefined,
          updatedAt: new Date()
        }
      }
    );

    if (!result.matchedCount) {
      reply.code(404).send({ message: 'Schedule not found' });
      return;
    }

    reply.code(204).send();
  });

  app.addHook('onClose', async () => {
    await deps.queue.close();
    if (deps.onClose) {
      await deps.onClose();
    }
  });

  return app;
};
