import { rm } from 'node:fs/promises';
import { Collection, Document, ObjectId } from 'mongodb';
import { Job } from 'bullmq';
import { Logger } from 'pino';
import {
  AppConfig,
  COLLECTIONS,
  MongoConnections,
  assertReadReplica,
  resolveAllowedSourceCollection
} from '@reportsys/core';
import {
  ReportRow,
  generateSingleFormatStream,
  generateZipFromEntries
} from '@reportsys/report-generators';
import { StorageService } from '@reportsys/storage-s3';
import {
  PartitionSpec,
  ReduceSpec,
  ReportArtifactView,
  ReportProcessingStats,
  ReportZipStrategy
} from '@reportsys/contracts';
import { buildObjectIdRanges, rangeToIdMatch } from './partitioner';
import {
  buildReducePartitionPipeline,
  createReduceAccumulator,
  reducePartitionRows,
  validateReduceSpec
} from './reduce-engine';
import {
  ensureIncludeFormats,
  normalizeValue,
  nowMs,
  sanitizeFilters,
  snapshotNameFor,
  snapshotRows,
  writeSnapshot
} from './helpers';
import {
  ChunkMetric,
  ReduceExecutionResult,
  ReportJobDocument,
  ReportQueueData
} from './types';

type StorageLike = Pick<StorageService, 'upload'>;

export type CreateReportJobProcessorDeps = {
  config: AppConfig;
  logger: Logger;
  mongo: Pick<MongoConnections, 'readDb'>;
  jobsCollection: Collection<ReportJobDocument>;
  storage: StorageLike;
  assertReadReplicaFn?: typeof assertReadReplica;
  runPartitionedReduceFn?: typeof runPartitionedReduce;
};

export type PartitionedReduceOptions = {
  useStreamingAccumulator?: boolean;
  maxGroups?: number;
};

export const runPartitionedReduce = async (
  collection: Collection<Document>,
  tenantId: string,
  filters: Record<string, unknown>,
  spec: ReduceSpec,
  partitionSpec: PartitionSpec | undefined,
  batchSize: number,
  partitionDefaultChunks: number,
  partitionMaxConcurrency: number,
  captureMemoryPeak: () => void,
  options?: PartitionedReduceOptions
): Promise<ReduceExecutionResult> => {
  validateReduceSpec(spec);

  const baseMatch: Record<string, unknown> = {
    tenantId,
    ...filters
  };

  const [minDoc, maxDoc] = await Promise.all([
    collection.find(baseMatch, { projection: { _id: 1 } }).sort({ _id: 1 }).limit(1).next(),
    collection.find(baseMatch, { projection: { _id: 1 } }).sort({ _id: -1 }).limit(1).next()
  ]);

  if (!minDoc || !maxDoc) {
    return {
      rows: [],
      rowsIn: 0,
      rowsOut: 0,
      chunks: 0,
      chunkMetrics: []
    };
  }

  const minId = minDoc._id;
  const maxId = maxDoc._id;

  if (!(minId instanceof ObjectId) || !(maxId instanceof ObjectId)) {
    throw new Error('objectIdRange partition requires ObjectId _id field in source collection');
  }

  const requestedChunks = partitionSpec?.chunks ?? partitionDefaultChunks;
  const ranges = buildObjectIdRanges(minId, maxId, requestedChunks);
  const workerCount = Math.max(1, Math.min(partitionMaxConcurrency, ranges.length));

  const chunkMetrics: ChunkMetric[] = [];
  const useStreamingAccumulator = options?.useStreamingAccumulator === true;
  const accumulator = useStreamingAccumulator
    ? createReduceAccumulator(spec, {
        maxGroups: options?.maxGroups
      })
    : null;
  const partialRows: Array<Record<string, unknown>> = [];

  let cursor = 0;

  const runChunkWorker = async (): Promise<void> => {
    while (true) {
      const nextIndex = cursor;
      cursor += 1;

      if (nextIndex >= ranges.length) {
        return;
      }

      const range = ranges[nextIndex];
      const startedAt = process.hrtime.bigint();

      const pipeline = buildReducePartitionPipeline(
        tenantId,
        filters,
        rangeToIdMatch(range),
        spec
      );

      const chunkRows: Array<Record<string, unknown>> = [];
      let chunkRowCount = 0;
      const chunkCursor = collection.aggregate(pipeline, {
        allowDiskUse: true,
        batchSize
      }) as AsyncIterable<Record<string, unknown>>;

      for await (const row of chunkCursor) {
        chunkRowCount += 1;
        if (accumulator) {
          accumulator.consume(row);
        } else {
          chunkRows.push(row);
        }
        captureMemoryPeak();
      }

      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
      chunkMetrics.push({
        index: range.index,
        durationMs: Math.round(durationMs),
        rowsOut: chunkRowCount
      });

      if (!accumulator) {
        for (const row of chunkRows) {
          partialRows.push(row);
        }
      }
    }
  };

  await Promise.all(Array.from({ length: workerCount }, () => runChunkWorker()));

  chunkMetrics.sort((a, b) => a.index - b.index);

  const reduced = accumulator ? accumulator.finalize() : reducePartitionRows(spec, partialRows);

  return {
    rows: reduced.rows.map((row) => normalizeValue(row) as ReportRow),
    rowsIn: reduced.rowsIn,
    rowsOut: reduced.rowsOut,
    chunks: ranges.length,
    chunkMetrics
  };
};

const buildRawReportPipeline = (
  tenantId: string,
  filters: Record<string, unknown>,
  maxObjectId: ObjectId | null
): Document[] => {
  const matchClauses: Document[] = [
    {
      tenantId,
      ...filters
    }
  ];

  if (maxObjectId) {
    matchClauses.push({
      _id: {
        $lte: maxObjectId
      }
    });
  }

  const match =
    matchClauses.length === 1
      ? matchClauses[0]
      : {
          $and: matchClauses
        };

  return [
    {
      $match: match
    },
    {
      $sort: {
        _id: 1
      }
    }
  ];
};

const readDatasetMaxObjectId = async (
  collection: Collection<Document>,
  tenantId: string,
  filters: Record<string, unknown>
): Promise<ObjectId | null> => {
  const maxDoc = await collection
    .find(
      {
        tenantId,
        ...filters
      },
      {
        projection: { _id: 1 }
      }
    )
    .sort({ _id: -1 })
    .limit(1)
    .next();

  if (!maxDoc || !(maxDoc._id instanceof ObjectId)) {
    return null;
  }

  return maxDoc._id;
};

type RawRowsInput = {
  collection: Collection<Document>;
  tenantId: string;
  filters: Record<string, unknown>;
  maxObjectId: ObjectId | null;
  batchSize: number;
  captureMemoryPeak: () => void;
  onRow?: () => void;
};

const rawRowsFromCollection = ({
  collection,
  tenantId,
  filters,
  maxObjectId,
  batchSize,
  captureMemoryPeak,
  onRow
}: RawRowsInput): AsyncIterable<ReportRow> =>
  (async function* (): AsyncGenerator<ReportRow> {
    const pipeline = buildRawReportPipeline(tenantId, filters, maxObjectId);
    const cursor = collection.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize
    });

    for await (const row of cursor) {
      onRow?.();
      captureMemoryPeak();
      yield normalizeValue(row) as ReportRow;
    }
  })();

export const createReportJobProcessor = (deps: CreateReportJobProcessorDeps) => {
  const assertReadReplicaFn = deps.assertReadReplicaFn ?? assertReadReplica;
  const runPartitionedReduceFn = deps.runPartitionedReduceFn ?? runPartitionedReduce;

  return async (job: Job<ReportQueueData>): Promise<void> => {
    const { reportJobId, tenantId } = job.data;
    const objectId = new ObjectId(reportJobId);

    const report = await deps.jobsCollection.findOne({
      _id: objectId,
      tenantId
    });

    if (!report) {
      deps.logger.warn({ tenantId, jobId: reportJobId }, 'Report job not found');
      return;
    }

    await deps.jobsCollection.updateOne(
      { _id: objectId },
      {
        $set: {
          status: 'running',
          progress: 10,
          startedAt: new Date()
        },
        $unset: {
          error: ''
        }
      }
    );

    let snapshotPath: string | null = null;

    const startedAt = nowMs();
    let memoryPeakBytes = process.memoryUsage().rss;

    const captureMemoryPeak = (): void => {
      memoryPeakBytes = Math.max(memoryPeakBytes, process.memoryUsage().rss);
    };

    try {
      await assertReadReplicaFn(deps.mongo.readDb);

      const filters = sanitizeFilters(report.filters);
      const sourceCollectionName = resolveAllowedSourceCollection(
        report.sourceCollection,
        deps.config.reportAllowedSourceCollections,
        COLLECTIONS.REPORT_SOURCE
      );
      const sourceCollection = deps.mongo.readDb.collection<Document>(sourceCollectionName);

      let rowsIn = 0;
      let rowsOut = 0;
      let chunks = 1;
      let chunkMetrics: ChunkMetric[] = [];
      let mode: ReportProcessingStats['mode'] = 'raw';
      let zipStrategy: ReportZipStrategy | undefined;

      let rows: AsyncIterable<ReportRow> | null = null;
      const useZipMultipass =
        deps.config.reportZipMultipassEnabled && report.format === 'zip' && !report.reduceSpec;

      if (report.reduceSpec) {
        mode = 'reduce';
        const reduced = await runPartitionedReduceFn(
          sourceCollection,
          tenantId,
          filters,
          report.reduceSpec,
          report.partitionSpec,
          deps.config.reportBatchSize,
          deps.config.reportPartitionDefaultChunks,
          deps.config.reportPartitionMaxConcurrency,
          captureMemoryPeak,
          {
            useStreamingAccumulator: deps.config.reportReduceEngineV2Enabled,
            maxGroups: deps.config.reportReduceMaxGroups
          }
        );

        rowsIn = reduced.rowsIn;
        rowsOut = reduced.rowsOut;
        chunks = reduced.chunks;
        chunkMetrics = reduced.chunkMetrics;
        rows = (async function* (): AsyncGenerator<ReportRow> {
          for (const row of reduced.rows) {
            yield row;
          }
        })();
      } else if (!useZipMultipass) {
        rows = rawRowsFromCollection({
          collection: sourceCollection,
          tenantId,
          filters,
          maxObjectId: null,
          batchSize: deps.config.reportBatchSize,
          captureMemoryPeak,
          onRow: () => {
            rowsIn += 1;
            rowsOut = rowsIn;
          }
        });
      }

      const streamOptions = {
        highWaterMark: deps.config.reportStreamHighWaterMark,
        pdfMaxRows: deps.config.reportPdfMaxRows
      };

      let generated;
      let artifactEntries: string[] | undefined;

      if (report.format === 'zip') {
        const includeFormats = ensureIncludeFormats(report);

        if (useZipMultipass) {
          const maxObjectId = await readDatasetMaxObjectId(sourceCollection, tenantId, filters);
          let firstPassRows = 0;

          const entries = includeFormats.map((format, index) => {
            const single = generateSingleFormatStream({
              format,
              rows: rawRowsFromCollection({
                collection: sourceCollection,
                tenantId,
                filters,
                maxObjectId,
                batchSize: deps.config.reportBatchSize,
                captureMemoryPeak,
                onRow:
                  index === 0
                    ? () => {
                        firstPassRows += 1;
                        rowsIn = firstPassRows;
                        rowsOut = firstPassRows;
                      }
                    : undefined
              }),
              options: streamOptions
            });

            return {
              name: `report.${single.extension}`,
              stream: single.stream
            };
          });

          artifactEntries = entries.map((entry) => entry.name);
          generated = generateZipFromEntries(entries, streamOptions);
          zipStrategy = 'multipass';
        } else {
          if (!rows) {
            throw new Error('Unable to initialize rows for zip snapshot generation');
          }

          const snapshot = await writeSnapshot(
            rows,
            deps.config.reportTmpDir,
            snapshotNameFor(reportJobId),
            deps.config.reportTmpMaxBytes,
            deps.config.reportStreamHighWaterMark,
            () => {
              captureMemoryPeak();
            }
          );

          snapshotPath = snapshot.path;

          if (!report.reduceSpec) {
            rowsIn = snapshot.rowCount;
            rowsOut = snapshot.rowCount;
          } else {
            rowsOut = snapshot.rowCount;
          }

          const entries = includeFormats.map((format) => {
            const single = generateSingleFormatStream({
              format,
              rows: snapshotRows(snapshot.path, deps.config.reportStreamHighWaterMark),
              options: streamOptions
            });

            return {
              name: `report.${single.extension}`,
              stream: single.stream
            };
          });

          artifactEntries = entries.map((entry) => entry.name);
          generated = generateZipFromEntries(entries, streamOptions);
          zipStrategy = 'snapshot';
        }
      } else {
        if (!rows) {
          throw new Error('Unable to initialize rows for report generation');
        }

        const single = generateSingleFormatStream({
          format: report.format,
          rows,
          options: streamOptions
        });

        if (report.compression === 'zip') {
          const entryName = `report.${single.extension}`;
          artifactEntries = [entryName];
          generated = generateZipFromEntries(
            [
              {
                name: entryName,
                stream: single.stream
              }
            ],
            streamOptions
          );
        } else {
          generated = single;
        }
      }

      await deps.jobsCollection.updateOne(
        { _id: objectId },
        {
          $set: {
            status: 'uploading',
            progress: 75
          }
        }
      );

      const key = `${tenantId}/${reportJobId}/report.${generated.extension}`;

      const uploadedArtifact = await deps.storage.upload({
        key,
        contentType: generated.contentType,
        stream: generated.stream,
        context: {
          tenantId,
          jobId: reportJobId,
          integration: 'object-storage'
        }
      });

      const artifact: ReportArtifactView = artifactEntries?.length
        ? {
            ...uploadedArtifact,
            entries: artifactEntries
          }
        : uploadedArtifact;

      const durationMs = Math.max(1, nowMs() - startedAt);
      const throughputRowsPerSecond = Number((rowsOut / (durationMs / 1000)).toFixed(2));
      const processingStats: ReportProcessingStats = {
        rowsIn,
        rowsOut,
        chunks,
        durationMs,
        throughputRowsPerSecond,
        memoryPeakBytes,
        mode,
        ...(zipStrategy ? { zipStrategy } : {})
      };

      await deps.jobsCollection.updateOne(
        { _id: objectId },
        {
          $set: {
            status: 'uploaded',
            progress: 100,
            rowCount: rowsOut,
            artifact,
            processingStats,
            finishedAt: new Date()
          },
          $unset: {
            error: ''
          }
        }
      );

      deps.logger.info(
        {
          tenantId,
          jobId: reportJobId,
          chunks,
          chunkMs: chunkMetrics,
          rowsIn,
          rowsOut,
          mode,
          zipStrategy,
          artifactMode: artifact.mode,
          available: artifact.available,
          durationMs,
          throughputRowsPerSecond,
          bytesUploaded: artifact.sizeBytes ?? 0,
          memoryPeakBytes
        },
        'Report job completed'
      );
    } catch (error) {
      await deps.jobsCollection.updateOne(
        { _id: objectId },
        {
          $set: {
            status: 'failed',
            progress: 100,
            finishedAt: new Date(),
            error: {
              message: (error as Error).message
            }
          }
        }
      );

      deps.logger.error(
        {
          tenantId,
          jobId: reportJobId,
          err: error
        },
        'Report job failed'
      );

      throw error;
    } finally {
      if (snapshotPath) {
        await rm(snapshotPath, { force: true }).catch(() => undefined);
      }
    }
  };
};
