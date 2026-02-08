import { afterEach, describe, expect, it } from 'vitest';
import { loadConfig, resetConfigCache } from './config';

const originalEnv = { ...process.env };

const resetEnv = () => {
  process.env = { ...originalEnv };
  resetConfigCache();
};

describe('core/config', () => {
  afterEach(() => {
    resetEnv();
  });

  it('parses booleans and numeric values from env', () => {
    process.env.ENABLE_EXTERNAL_STORAGE = 'false';
    process.env.INTEGRATION_STRICT_MODE = '1';
    process.env.ENABLE_WEBHOOKS = 'yes';
    process.env.REPORT_BATCH_SIZE = '1234';
    process.env.REPORT_PDF_MAX_ROWS = '77';
    process.env.REPORT_REDUCE_ENGINE_V2_ENABLED = 'true';
    process.env.REPORT_ZIP_MULTIPASS_ENABLED = '1';
    process.env.REPORT_ALLOWED_SOURCE_COLLECTIONS = 'reportSource,customSource';
    process.env.REPORT_REDUCE_MAX_GROUPS = '123';
    process.env.REPORT_SOURCE_INDEX_MANAGEMENT_ENABLED = 'false';
    process.env.STORAGE_DRIVER = 'noop';
    resetConfigCache();

    const config = loadConfig();

    expect(config.enableExternalStorage).toBe(false);
    expect(config.integrationStrictMode).toBe(true);
    expect(config.enableWebhooks).toBe(true);
    expect(config.reportBatchSize).toBe(1234);
    expect(config.reportPdfMaxRows).toBe(77);
    expect(config.reportReduceEngineV2Enabled).toBe(true);
    expect(config.reportZipMultipassEnabled).toBe(true);
    expect(config.reportAllowedSourceCollections).toEqual(['reportSource', 'customSource']);
    expect(config.reportReduceMaxGroups).toBe(123);
    expect(config.reportSourceIndexManagementEnabled).toBe(false);
    expect(config.storageDriver).toBe('noop');
  });

  it('uses defaults for report read uri and partition settings', () => {
    delete process.env.MONGODB_REPORT_READ_URI;
    delete process.env.REPORT_PARTITION_DEFAULT_CHUNKS;
    delete process.env.REPORT_PARTITION_MAX_CONCURRENCY;
    delete process.env.REPORT_ALLOWED_SOURCE_COLLECTIONS;
    delete process.env.REPORT_REDUCE_ENGINE_V2_ENABLED;
    delete process.env.REPORT_ZIP_MULTIPASS_ENABLED;
    delete process.env.REPORT_REDUCE_MAX_GROUPS;
    delete process.env.REPORT_SOURCE_INDEX_MANAGEMENT_ENABLED;
    resetConfigCache();

    const config = loadConfig();

    expect(config.mongoReportReadUri).toContain('readPreference=secondary');
    expect(config.mongoReportReadUri).toContain('directConnection=true');
    expect(config.reportPartitionDefaultChunks).toBe(32);
    expect(config.reportPartitionMaxConcurrency).toBe(4);
    expect(config.reportAllowedSourceCollections).toEqual(['reportSource']);
    expect(config.reportReduceEngineV2Enabled).toBe(false);
    expect(config.reportZipMultipassEnabled).toBe(false);
    expect(config.reportReduceMaxGroups).toBe(500000);
    expect(config.reportSourceIndexManagementEnabled).toBe(true);
  });
});
