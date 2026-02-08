import { ObjectId } from 'mongodb';
import {
  CreateReportRequest,
  PartitionSpec,
  ReduceSpec,
  ReportArtifactView,
  ReportIncludeFormat,
  ReportProcessingStats
} from '@reportsys/contracts';
import { ReportRow } from '@reportsys/report-generators';

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

export type ReportQueueData = {
  reportJobId: string;
  tenantId: string;
};

export type ScheduleDocument = {
  _id: ObjectId;
  tenantId: string;
  name: string;
  cron: string;
  reportDefinitionId: string;
  format: CreateReportRequest['format'];
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

export type SnapshotResult = {
  path: string;
  rowCount: number;
  bytes: number;
};

export type ChunkMetric = {
  index: number;
  durationMs: number;
  rowsOut: number;
};

export type ReduceExecutionResult = {
  rows: ReportRow[];
  rowsIn: number;
  rowsOut: number;
  chunks: number;
  chunkMetrics: ChunkMetric[];
};
