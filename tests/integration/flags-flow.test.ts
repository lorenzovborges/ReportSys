import { setTimeout as sleep } from 'node:timers/promises';
import { beforeAll, afterAll, describe, expect, it } from 'vitest';
import { MongoClient } from 'mongodb';
import {
  apiHeaders,
  integrationConfig,
  requestJson,
  waitForJobCompletion
} from './shared';

const mongoUri =
  process.env.IT_MONGO_URI ??
  'mongodb://localhost:27017/reportsys?replicaSet=rs0&directConnection=true';
const mongoCandidates = (
  process.env.IT_MONGO_WRITE_URIS ??
  [
    'mongodb://localhost:27017/reportsys?replicaSet=rs0&directConnection=true',
    'mongodb://localhost:27018/reportsys?replicaSet=rs0&directConnection=true',
    'mongodb://localhost:27019/reportsys?replicaSet=rs0&directConnection=true'
  ].join(',')
)
  .split(',')
  .map((entry) => entry.trim())
  .filter(Boolean);
const mongoDbName = process.env.IT_MONGO_DB ?? 'reportsys';

describe.sequential(
  'integration/flags-flow',
  {
    timeout: 240_000
  },
  () => {
    let mongoClient: MongoClient | null = null;

    const connectMongoWithRetry = async (): Promise<MongoClient> => {
      let lastError: unknown;

      for (let attempt = 0; attempt < 30; attempt += 1) {
        for (const candidateUri of mongoCandidates) {
          const client = new MongoClient(candidateUri || mongoUri, {
            serverSelectionTimeoutMS: 3000,
            connectTimeoutMS: 3000
          });

          try {
            await client.connect();
            const hello = await client.db(mongoDbName).command({ hello: 1 });

            if (hello?.isWritablePrimary) {
              return client;
            }

            lastError = new Error(`candidate is not writable primary: ${candidateUri}`);
            await client.close().catch(() => undefined);
          } catch (error) {
            lastError = error;
            await client.close().catch(() => undefined);
          }
        }

        await sleep(1000);
      }

      throw new Error(`unable to connect to mongo for integration tests: ${String(lastError)}`);
    };

    const waitForApiHealth = async (): Promise<void> => {
      let lastError: unknown;

      for (let attempt = 0; attempt < 60; attempt += 1) {
        try {
          const health = await requestJson({
            method: 'GET',
            path: '/health'
          });

          if (health.status === 200) {
            return;
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
      await waitForApiHealth();
      mongoClient = await connectMongoWithRetry();

      const db = mongoClient.db(mongoDbName);
      const tenantId = integrationConfig.tenantId;

      await Promise.all([
        db.collection('reportJobs').deleteMany({ tenantId }),
        db.collection('reportSchedules').deleteMany({ tenantId }),
        db.collection('reportSource').deleteMany({ tenantId })
      ]);

      await db.collection('reportSource').insertMany([
        { tenantId, status: 'paid', amount: 100, region: 'br', createdAt: new Date() },
        { tenantId, status: 'paid', amount: 200, region: 'br', createdAt: new Date() },
        { tenantId, status: 'pending', amount: 50, region: 'us', createdAt: new Date() }
      ]);
    }, 120_000);

    afterAll(async () => {
      if (mongoClient) {
        await mongoClient.close();
      }
    }, 60_000);

    it('runs reduce path with processingStats from streaming reduce engine', async () => {
      const createResponse = await requestJson<{ id: string }>({
        method: 'POST',
        path: '/v1/reports',
        headers: apiHeaders(),
        body: {
          reportDefinitionId: 'integration-reduce-flags',
          format: 'csv',
          filters: {
            status: 'paid'
          },
          reduceSpec: {
            groupBy: ['status'],
            metrics: [
              { op: 'count', as: 'totalOrders' },
              { op: 'sum', field: 'amount', as: 'sumAmount' }
            ]
          },
          partitionSpec: {
            strategy: 'objectIdRange',
            chunks: 4
          },
          sourceCollection: 'reportSource'
        }
      });

      expect(createResponse.status).toBe(202);
      const jobId = createResponse.body?.id;
      expect(jobId).toBeDefined();

      const finalStatus = await waitForJobCompletion(jobId as string);
      expect(finalStatus.status).toBe('uploaded');
      expect(finalStatus.processingStats?.mode).toBe('reduce');
      expect(finalStatus.processingStats?.rowsIn).toBe(2);
      expect(finalStatus.processingStats?.rowsOut).toBe(1);
    });

    it('runs zip multipass strategy and reports zipStrategy=multipass', async () => {
      const createResponse = await requestJson<{ id: string }>({
        method: 'POST',
        path: '/v1/reports',
        headers: apiHeaders(),
        body: {
          reportDefinitionId: 'integration-zip-flags',
          format: 'zip',
          includeFormats: ['csv', 'json'],
          filters: {
            status: 'paid'
          },
          sourceCollection: 'reportSource'
        }
      });

      expect(createResponse.status).toBe(202);
      const jobId = createResponse.body?.id;
      expect(jobId).toBeDefined();

      const finalStatus = await waitForJobCompletion(jobId as string);
      expect(finalStatus.status).toBe('uploaded');
      expect(finalStatus.artifact.entries).toEqual(['report.csv', 'report.json']);
      expect(finalStatus.processingStats?.zipStrategy).toBe('multipass');
      expect(finalStatus.processingStats?.rowsIn).toBe(2);
      expect(finalStatus.processingStats?.rowsOut).toBe(2);
    });
  }
);
