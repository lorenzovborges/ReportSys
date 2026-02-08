export type ReportFormat = 'csv' | 'xlsx' | 'json' | 'pdf' | 'zip';
export type ReportIncludeFormat = Exclude<ReportFormat, 'zip'>;
export type ReportCompression = 'none' | 'zip';
export type ReduceOperation = 'count' | 'sum' | 'min' | 'max' | 'avg';

export interface ReduceMetricSpec {
  op: ReduceOperation;
  field?: string;
  as: string;
}

export interface ReduceSpec {
  groupBy: string[];
  metrics: ReduceMetricSpec[];
}

export interface PartitionSpec {
  strategy: 'objectIdRange';
  chunks?: number;
}

export type ReportStatus =
  | 'queued'
  | 'running'
  | 'uploading'
  | 'uploaded'
  | 'failed'
  | 'expired';

export type ReportArtifactMode = 's3' | 'minio' | 'filesystem' | 'noop';
export type ReportProcessingMode = 'raw' | 'reduce';
export type ReportZipStrategy = 'multipass' | 'snapshot';

export interface ReportArtifactView {
  mode: ReportArtifactMode;
  available: boolean;
  reason?: string;
  sizeBytes?: number;
  checksum?: string;
  key?: string;
  bucket?: string;
  entries?: string[];
}

export interface ReportProcessingStats {
  rowsIn: number;
  rowsOut: number;
  chunks: number;
  durationMs: number;
  throughputRowsPerSecond: number;
  memoryPeakBytes: number;
  mode: ReportProcessingMode;
  zipStrategy?: ReportZipStrategy;
}

export interface CreateReportRequest {
  reportDefinitionId: string;
  format: ReportFormat;
  filters?: Record<string, unknown>;
  timezone?: string;
  locale?: string;
  compression?: ReportCompression;
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  sourceCollection?: string;
}

export interface CreateReportResponse {
  id: string;
  status: ReportStatus;
}

export interface ReportJobView {
  id: string;
  tenantId: string;
  status: ReportStatus;
  progress: number;
  rowCount: number;
  reportDefinitionId: string;
  format: ReportFormat;
  artifact: ReportArtifactView;
  processingStats?: ReportProcessingStats;
  error?: {
    message: string;
  };
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface SignedDownloadResponse {
  available: boolean;
  url?: string;
  mode?: ReportArtifactMode;
  reason?: string;
}

export interface CreateScheduleRequest {
  name: string;
  cron: string;
  reportDefinitionId: string;
  format: ReportFormat;
  filters?: Record<string, unknown>;
  timezone?: string;
  enabled?: boolean;
  compression?: ReportCompression;
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  sourceCollection?: string;
}

export interface UpdateScheduleRequest {
  name?: string;
  cron?: string;
  reportDefinitionId?: string;
  format?: ReportFormat;
  filters?: Record<string, unknown>;
  timezone?: string;
  enabled?: boolean;
  compression?: ReportCompression;
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  sourceCollection?: string;
}

export interface ScheduleView {
  id: string;
  tenantId: string;
  name: string;
  cron: string;
  reportDefinitionId: string;
  format: ReportFormat;
  filters?: Record<string, unknown>;
  timezone: string;
  enabled: boolean;
  compression?: ReportCompression;
  includeFormats?: ReportIncludeFormat[];
  reduceSpec?: ReduceSpec;
  partitionSpec?: PartitionSpec;
  nextRunAt?: string;
  lastRunAt?: string;
  createdAt: string;
}
