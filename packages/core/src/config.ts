import { z } from 'zod';
import { ReportArtifactMode } from '@reportsys/contracts';

const boolFromEnv = z.preprocess((value) => {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes';
  }

  return value;
}, z.boolean());

const collectionListFromEnv = z.preprocess((value) => {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    return value
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  return value;
}, z.array(z.string().regex(/^[A-Za-z0-9_]+$/)).min(1));

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.string().default('info'),
  MONGODB_DB_NAME: z.string().default('reportsys'),
  MONGODB_WRITE_URI: z
    .string()
    .default('mongodb://localhost:27017/reportsys?replicaSet=rs0'),
  MONGODB_REPORT_READ_URI: z
    .string()
    .default(
      'mongodb://localhost:27018/reportsys?replicaSet=rs0&readPreference=secondary&directConnection=true'
    ),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  STORAGE_DRIVER: z
    .enum(['s3', 'minio', 'filesystem', 'noop'] satisfies [ReportArtifactMode, ...ReportArtifactMode[]])
    .default('minio'),
  ENABLE_EXTERNAL_STORAGE: boolFromEnv.default(true),
  INTEGRATION_STRICT_MODE: boolFromEnv.default(false),
  ENABLE_WEBHOOKS: boolFromEnv.default(false),
  S3_ENDPOINT: z.string().default('http://localhost:9000'),
  S3_REGION: z.string().default('us-east-1'),
  S3_BUCKET: z.string().default('reportsys'),
  S3_PUBLIC_ENDPOINT: z.string().default(''),
  S3_ACCESS_KEY_ID: z.string().default('minioadmin'),
  S3_SECRET_ACCESS_KEY: z.string().default('minioadmin'),
  S3_FORCE_PATH_STYLE: boolFromEnv.default(true),
  FILESYSTEM_STORAGE_PATH: z.string().default('/tmp/reportsys'),
  SIGNED_URL_TTL_SECONDS: z.coerce.number().int().positive().default(900),
  REPORT_MAX_CONCURRENCY: z.coerce.number().int().positive().default(4),
  REPORT_BATCH_SIZE: z.coerce.number().int().positive().default(2000),
  REPORT_STREAM_HIGH_WATER_MARK: z.coerce.number().int().positive().default(65536),
  REPORT_PARTITION_DEFAULT_CHUNKS: z.coerce.number().int().positive().default(32),
  REPORT_PARTITION_MAX_CONCURRENCY: z.coerce.number().int().positive().default(4),
  REPORT_REDUCE_ENGINE_V2_ENABLED: boolFromEnv.default(false),
  REPORT_ZIP_MULTIPASS_ENABLED: boolFromEnv.default(false),
  REPORT_ALLOWED_SOURCE_COLLECTIONS: collectionListFromEnv.default(['reportSource']),
  REPORT_REDUCE_MAX_GROUPS: z.coerce.number().int().positive().default(500000),
  REPORT_SOURCE_INDEX_MANAGEMENT_ENABLED: boolFromEnv.default(true),
  REPORT_TMP_DIR: z.string().default('/tmp/reportsys'),
  REPORT_TMP_MAX_BYTES: z.coerce.number().int().positive().default(2147483648),
  REPORT_PDF_MAX_ROWS: z.coerce.number().int().positive().default(10000),
  ARTIFACT_RETENTION_DAYS: z.coerce.number().int().positive().default(30),
  API_KEY_HASH_PEPPER: z.string().default('change-me'),
  DEV_API_KEY: z.string().default('local-dev-key'),
  SCHEDULE_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30000)
});

export type AppConfig = {
  nodeEnv: 'development' | 'test' | 'production';
  port: number;
  logLevel: string;
  mongoDbName: string;
  mongoWriteUri: string;
  mongoReportReadUri: string;
  redisUrl: string;
  storageDriver: ReportArtifactMode;
  enableExternalStorage: boolean;
  integrationStrictMode: boolean;
  enableWebhooks: boolean;
  s3Endpoint: string;
  s3Region: string;
  s3Bucket: string;
  s3PublicEndpoint: string;
  s3AccessKeyId: string;
  s3SecretAccessKey: string;
  s3ForcePathStyle: boolean;
  filesystemStoragePath: string;
  signedUrlTtlSeconds: number;
  reportMaxConcurrency: number;
  reportBatchSize: number;
  reportStreamHighWaterMark: number;
  reportPartitionDefaultChunks: number;
  reportPartitionMaxConcurrency: number;
  reportReduceEngineV2Enabled: boolean;
  reportZipMultipassEnabled: boolean;
  reportAllowedSourceCollections: string[];
  reportReduceMaxGroups: number;
  reportSourceIndexManagementEnabled: boolean;
  reportTmpDir: string;
  reportTmpMaxBytes: number;
  reportPdfMaxRows: number;
  artifactRetentionDays: number;
  apiKeyHashPepper: string;
  devApiKey: string;
  schedulePollIntervalMs: number;
};

let cache: AppConfig | null = null;

export const loadConfig = (): AppConfig => {
  if (cache) {
    return cache;
  }

  const env = envSchema.parse(process.env);

  cache = {
    nodeEnv: env.NODE_ENV,
    port: env.PORT,
    logLevel: env.LOG_LEVEL,
    mongoDbName: env.MONGODB_DB_NAME,
    mongoWriteUri: env.MONGODB_WRITE_URI,
    mongoReportReadUri: env.MONGODB_REPORT_READ_URI,
    redisUrl: env.REDIS_URL,
    storageDriver: env.STORAGE_DRIVER,
    enableExternalStorage: env.ENABLE_EXTERNAL_STORAGE,
    integrationStrictMode: env.INTEGRATION_STRICT_MODE,
    enableWebhooks: env.ENABLE_WEBHOOKS,
    s3Endpoint: env.S3_ENDPOINT,
    s3Region: env.S3_REGION,
    s3Bucket: env.S3_BUCKET,
    s3PublicEndpoint: env.S3_PUBLIC_ENDPOINT,
    s3AccessKeyId: env.S3_ACCESS_KEY_ID,
    s3SecretAccessKey: env.S3_SECRET_ACCESS_KEY,
    s3ForcePathStyle: env.S3_FORCE_PATH_STYLE,
    filesystemStoragePath: env.FILESYSTEM_STORAGE_PATH,
    signedUrlTtlSeconds: env.SIGNED_URL_TTL_SECONDS,
    reportMaxConcurrency: env.REPORT_MAX_CONCURRENCY,
    reportBatchSize: env.REPORT_BATCH_SIZE,
    reportStreamHighWaterMark: env.REPORT_STREAM_HIGH_WATER_MARK,
    reportPartitionDefaultChunks: env.REPORT_PARTITION_DEFAULT_CHUNKS,
    reportPartitionMaxConcurrency: env.REPORT_PARTITION_MAX_CONCURRENCY,
    reportReduceEngineV2Enabled: env.REPORT_REDUCE_ENGINE_V2_ENABLED,
    reportZipMultipassEnabled: env.REPORT_ZIP_MULTIPASS_ENABLED,
    reportAllowedSourceCollections: env.REPORT_ALLOWED_SOURCE_COLLECTIONS,
    reportReduceMaxGroups: env.REPORT_REDUCE_MAX_GROUPS,
    reportSourceIndexManagementEnabled: env.REPORT_SOURCE_INDEX_MANAGEMENT_ENABLED,
    reportTmpDir: env.REPORT_TMP_DIR,
    reportTmpMaxBytes: env.REPORT_TMP_MAX_BYTES,
    reportPdfMaxRows: env.REPORT_PDF_MAX_ROWS,
    artifactRetentionDays: env.ARTIFACT_RETENTION_DAYS,
    apiKeyHashPepper: env.API_KEY_HASH_PEPPER,
    devApiKey: env.DEV_API_KEY,
    schedulePollIntervalMs: env.SCHEDULE_POLL_INTERVAL_MS
  };

  return cache;
};

export const resetConfigCache = (): void => {
  cache = null;
};
