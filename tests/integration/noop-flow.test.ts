import { setTimeout as sleep } from 'node:timers/promises';
import { beforeAll, describe, expect, it } from 'vitest';
import {
  apiHeaders,
  requestJson,
  waitForJobCompletion
} from './shared';

describe.sequential(
  'integration/noop-flow',
  {
    timeout: 240_000
  },
  () => {
    const waitForApiHealth = async (): Promise<{
      status: string;
      externalStorageEnabled: boolean;
      storageDriver: string;
    }> => {
      let lastError: unknown;

      for (let attempt = 0; attempt < 60; attempt += 1) {
        try {
          const health = await requestJson<{
            status: string;
            externalStorageEnabled: boolean;
            storageDriver: string;
          }>({
            method: 'GET',
            path: '/health'
          });

          if (health.status === 200 && health.body) {
            return health.body;
          }

          lastError = new Error(`unexpected health status: ${health.status}`);
        } catch (error) {
          lastError = error;
        }

        await sleep(1000);
      }

      throw new Error(`api healthcheck did not become ready: ${String(lastError)}`);
    };

    beforeAll(async () => {
      const health = await waitForApiHealth();

      if (health.externalStorageEnabled !== false) {
        throw new Error(
          'noop integration test requires ENABLE_EXTERNAL_STORAGE=false. Run stack in noop mode first.'
        );
      }
    }, 120_000);

    it('completes report and returns explicit unavailable download in noop mode', async () => {
      const createResponse = await requestJson<{ id: string }>({
        method: 'POST',
        path: '/v1/reports',
        headers: apiHeaders(),
        body: {
          reportDefinitionId: 'integration-noop',
          format: 'csv',
          sourceCollection: 'reportSource'
        }
      });

      expect(createResponse.status).toBe(202);
      const jobId = createResponse.body?.id;
      expect(jobId).toBeDefined();

      const finalStatus = await waitForJobCompletion(jobId as string);
      expect(finalStatus.status).toBe('uploaded');
      expect(finalStatus.artifact.mode).toBe('noop');
      expect(finalStatus.artifact.available).toBe(false);

      const downloadResponse = await requestJson({
        method: 'GET',
        path: `/v1/reports/${jobId}/download`,
        headers: apiHeaders()
      });

      expect(downloadResponse.status).toBe(200);
      expect(downloadResponse.body).toEqual({
        available: false,
        mode: 'noop',
        reason: 'EXTERNAL_STORAGE_DISABLED'
      });
    });
  }
);
