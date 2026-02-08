import { MongoClient } from 'mongodb';
import { setTimeout as sleep } from 'node:timers/promises';

type BenchmarkResult = {
  jobId: string;
  status: string;
  rowCount: number;
  rowsIn: number;
  rowsOut: number;
  elapsedMs: number;
  throughputRowsPerSecond: number;
  throughputBasis: 'rowsIn' | 'rowsOut';
};

const parseNumberArg = (flag: string, fallback: number): number => {
  const pair = process.argv.find((arg) => arg.startsWith(`${flag}=`));
  if (!pair) {
    return fallback;
  }

  const raw = pair.slice(flag.length + 1);
  const numeric = Number(raw);
  return Number.isFinite(numeric) && numeric > 0 ? Math.floor(numeric) : fallback;
};

const rows = parseNumberArg('--rows', 1_000_000);
const batchSize = parseNumberArg('--batch-size', 10_000);

const baseUrl = process.env.REPORTSYS_BASE_URL ?? 'http://localhost:3000';
const apiKey = process.env.REPORTSYS_API_KEY ?? 'local-dev-key';
const tenantId = process.env.REPORTSYS_TENANT_ID ?? 'perf-tenant';
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
const dbName = process.env.IT_MONGO_DB ?? 'reportsys';

const requestJson = async <T = any>(
  method: 'GET' | 'POST',
  path: string,
  body?: unknown
): Promise<{ status: number; body: T }> => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      'x-api-key': apiKey,
      'x-tenant-id': tenantId,
      ...(body !== undefined ? { 'content-type': 'application/json' } : {})
    },
    body: body !== undefined ? JSON.stringify(body) : undefined
  });

  const raw = await response.text();
  const parsed = raw ? (JSON.parse(raw) as T) : ({} as T);
  return {
    status: response.status,
    body: parsed
  };
};

const waitForCompletion = async (jobId: string): Promise<any> => {
  for (let attempt = 0; attempt < 300; attempt += 1) {
    const response = await requestJson<any>('GET', `/v1/reports/${jobId}`);
    if (response.status !== 200) {
      throw new Error(`status query failed for ${jobId}: ${response.status}`);
    }

    if (['uploaded', 'failed', 'expired'].includes(response.body.status)) {
      return response.body;
    }

    await sleep(1000);
  }

  throw new Error(`job ${jobId} did not finish within timeout`);
};

const createAndMeasureJob = async (payload: unknown): Promise<BenchmarkResult> => {
  const create = await requestJson<{ id: string }>('POST', '/v1/reports', payload);

  if (create.status !== 202 || !create.body.id) {
    throw new Error(`unable to create job. status=${create.status}`);
  }

  const jobId = create.body.id;
  const startedAt = Date.now();
  const status = await waitForCompletion(jobId);
  const elapsedMs = Math.max(1, Date.now() - startedAt);
  const rowsOut = Number(status.rowCount ?? 0);
  const rowsIn = Number(status.processingStats?.rowsIn ?? rowsOut);
  const throughputBasis: 'rowsIn' | 'rowsOut' =
    status.processingStats?.rowsIn !== undefined ? 'rowsIn' : 'rowsOut';
  const throughputSource = throughputBasis === 'rowsIn' ? rowsIn : rowsOut;

  return {
    jobId,
    status: status.status,
    rowCount: rowsOut,
    rowsIn,
    rowsOut,
    elapsedMs,
    throughputRowsPerSecond: Number((throughputSource / (elapsedMs / 1000)).toFixed(2)),
    throughputBasis
  };
};

const connectWritableMongo = async (): Promise<MongoClient> => {
  let lastError: unknown;

  for (let attempt = 0; attempt < 30; attempt += 1) {
    for (const candidateUri of mongoCandidates) {
      const client = new MongoClient(candidateUri || mongoUri, {
        serverSelectionTimeoutMS: 3000,
        connectTimeoutMS: 3000
      });

      try {
        await client.connect();
        const hello = await client.db(dbName).command({ hello: 1 });
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

  throw new Error(`unable to connect writable mongo: ${String(lastError)}`);
};

const seedData = async (): Promise<void> => {
  const client = await connectWritableMongo();

  try {
    const db = client.db(dbName);
    const source = db.collection('reportSource');
    const jobs = db.collection('reportJobs');

    await Promise.all([source.deleteMany({ tenantId }), jobs.deleteMany({ tenantId })]);

    for (let inserted = 0; inserted < rows; inserted += batchSize) {
      const batchCount = Math.min(batchSize, rows - inserted);
      const docs = Array.from({ length: batchCount }, (_, idx) => {
        const absolute = inserted + idx;
        return {
          tenantId,
          status: absolute % 2 === 0 ? 'paid' : 'pending',
          amount: (absolute % 1000) + 1,
          bucket: `b${absolute % 10}`,
          createdAt: new Date()
        };
      });

      await source.insertMany(docs, { ordered: false });
      process.stdout.write(
        `seeded ${Math.min(inserted + batchCount, rows)}/${rows} rows\r`
      );
    }

    process.stdout.write('\n');
  } finally {
    await client.close();
  }
};

const run = async (): Promise<void> => {
  let memoryPeak = process.memoryUsage().rss;
  const memoryTimer = setInterval(() => {
    memoryPeak = Math.max(memoryPeak, process.memoryUsage().rss);
  }, 250);

  try {
    await seedData();

    const csv = await createAndMeasureJob({
      reportDefinitionId: 'perf-csv',
      format: 'csv',
      sourceCollection: 'reportSource'
    });

    const reduce = await createAndMeasureJob({
      reportDefinitionId: 'perf-reduce',
      format: 'csv',
      sourceCollection: 'reportSource',
      reduceSpec: {
        groupBy: ['status'],
        metrics: [
          { op: 'count', as: 'totalOrders' },
          { op: 'sum', field: 'amount', as: 'totalAmount' },
          { op: 'avg', field: 'amount', as: 'avgAmount' }
        ]
      },
      partitionSpec: {
        strategy: 'objectIdRange'
      }
    });

    const output = {
      timestamp: new Date().toISOString(),
      rowsSeeded: rows,
      csv,
      reduce,
      memoryPeakBytes: memoryPeak
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    clearInterval(memoryTimer);
  }
};

void run().catch((error) => {
  console.error('benchmark failed', error);
  process.exit(1);
});
