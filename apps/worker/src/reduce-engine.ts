import { Document } from 'mongodb';
import { ReduceMetricSpec, ReduceSpec } from '@reportsys/contracts';

type AverageState = {
  sum: number;
  count: number;
};

type ReduceGroupState = {
  group: Record<string, unknown>;
  values: Map<string, unknown>;
  averages: Map<string, AverageState>;
  inputCount: number;
};

export type ReduceSummary = {
  rows: Array<Record<string, unknown>>;
  rowsIn: number;
  rowsOut: number;
};

export type ReduceAccumulatorOptions = {
  maxGroups?: number;
};

export type ReduceAccumulator = {
  consume: (row: Record<string, unknown>) => void;
  finalize: () => ReduceSummary;
  groupCount: () => number;
};

const AVG_SUM_PREFIX = '__avg_sum__';
const AVG_COUNT_PREFIX = '__avg_count__';
const INPUT_COUNT_FIELD = '__input_count';

const numericFrom = (value: unknown, fieldName: string): number => {
  if (value === null || value === undefined) {
    return 0;
  }

  const numeric = Number(value);

  if (!Number.isFinite(numeric)) {
    throw new Error(`Non numeric value found in reduce field: ${fieldName}`);
  }

  return numeric;
};

const buildGroupIdExpression = (groupBy: string[]): Record<string, string> | null => {
  if (!groupBy.length) {
    return null;
  }

  return Object.fromEntries(groupBy.map((field) => [field, `$${field}`]));
};

const buildMetricAccumulator = (metric: ReduceMetricSpec): Record<string, unknown> => {
  switch (metric.op) {
    case 'count':
      return {
        [metric.as]: { $sum: 1 }
      };
    case 'sum':
      return {
        [metric.as]: { $sum: `$${metric.field}` }
      };
    case 'min':
      return {
        [metric.as]: { $min: `$${metric.field}` }
      };
    case 'max':
      return {
        [metric.as]: { $max: `$${metric.field}` }
      };
    case 'avg':
      return {
        [`${AVG_SUM_PREFIX}${metric.as}`]: { $sum: `$${metric.field}` },
        [`${AVG_COUNT_PREFIX}${metric.as}`]: {
          $sum: {
            $cond: [{ $ne: [`$${metric.field}`, null] }, 1, 0]
          }
        }
      };
    default:
      throw new Error(`Unsupported reduce operation: ${String(metric.op)}`);
  }
};

export const validateReduceSpec = (spec: ReduceSpec): void => {
  if (!Array.isArray(spec.metrics) || spec.metrics.length === 0) {
    throw new Error('reduceSpec.metrics must contain at least one metric');
  }

  const aliases = new Set<string>();

  for (const groupField of spec.groupBy) {
    if (!/^[A-Za-z0-9_]+$/.test(groupField)) {
      throw new Error(`Invalid groupBy field: ${groupField}`);
    }
  }

  for (const metric of spec.metrics) {
    if (!metric.as || !/^[A-Za-z0-9_]+$/.test(metric.as)) {
      throw new Error(`Invalid metric alias: ${metric.as}`);
    }

    if (aliases.has(metric.as)) {
      throw new Error(`Duplicated metric alias: ${metric.as}`);
    }

    aliases.add(metric.as);

    if (metric.op !== 'count') {
      if (!metric.field || !/^[A-Za-z0-9_]+$/.test(metric.field)) {
        throw new Error(`Metric ${metric.as} requires a valid field`);
      }
    }
  }
};

export const buildReducePartitionPipeline = (
  tenantId: string,
  filters: Record<string, unknown>,
  idMatch: Record<string, unknown>,
  spec: ReduceSpec
): Document[] => {
  const groupAcc = spec.metrics.reduce<Record<string, unknown>>((acc, metric) => {
    Object.assign(acc, buildMetricAccumulator(metric));
    return acc;
  }, {});

  groupAcc[INPUT_COUNT_FIELD] = { $sum: 1 };

  const groupIdExpr = buildGroupIdExpression(spec.groupBy);

  return [
    {
      $match: {
        tenantId,
        ...filters,
        ...idMatch
      }
    },
    {
      $group: {
        _id: groupIdExpr,
        ...groupAcc
      }
    }
  ];
};

const decodeGroup = (groupBy: string[], rawGroup: unknown): Record<string, unknown> => {
  if (!groupBy.length) {
    return {};
  }

  if (!rawGroup || typeof rawGroup !== 'object') {
    return Object.fromEntries(groupBy.map((field) => [field, null]));
  }

  const source = rawGroup as Record<string, unknown>;
  return Object.fromEntries(groupBy.map((field) => [field, source[field] ?? null]));
};

const pickComparable = (value: unknown): number | string | null => {
  if (value === null || value === undefined) {
    return null;
  }

  if (value instanceof Date) {
    return value.getTime();
  }

  if (typeof value === 'number' || typeof value === 'string') {
    return value;
  }

  return null;
};

const mergeMetricValue = (
  state: ReduceGroupState,
  metric: ReduceMetricSpec,
  doc: Record<string, unknown>
): void => {
  if (metric.op === 'avg') {
    const sum = numericFrom(doc[`${AVG_SUM_PREFIX}${metric.as}`], `${metric.as}.sum`);
    const count = numericFrom(doc[`${AVG_COUNT_PREFIX}${metric.as}`], `${metric.as}.count`);
    const previous = state.averages.get(metric.as) ?? { sum: 0, count: 0 };
    state.averages.set(metric.as, {
      sum: previous.sum + sum,
      count: previous.count + count
    });
    return;
  }

  if (metric.op === 'count' || metric.op === 'sum') {
    const current = numericFrom(doc[metric.as], metric.as);
    const previous = numericFrom(state.values.get(metric.as), metric.as);
    state.values.set(metric.as, previous + current);
    return;
  }

  if (metric.op === 'min' || metric.op === 'max') {
    const current = doc[metric.as];
    const previous = state.values.get(metric.as);

    if (previous === undefined) {
      state.values.set(metric.as, current ?? null);
      return;
    }

    if (current === null || current === undefined) {
      return;
    }

    const currentComparable = pickComparable(current);
    const previousComparable = pickComparable(previous);

    if (currentComparable === null || previousComparable === null) {
      return;
    }

    if (
      (metric.op === 'min' && currentComparable < previousComparable) ||
      (metric.op === 'max' && currentComparable > previousComparable)
    ) {
      state.values.set(metric.as, current);
    }
  }
};

const toReduceSummary = (spec: ReduceSpec, state: Map<string, ReduceGroupState>): ReduceSummary => {
  const rows: Array<Record<string, unknown>> = [];
  let rowsIn = 0;

  const sortedEntries = [...state.entries()].sort((left, right) =>
    left[0].localeCompare(right[0])
  );

  for (const [, value] of sortedEntries) {
    const output: Record<string, unknown> = {
      ...value.group
    };

    rowsIn += value.inputCount;

    for (const metric of spec.metrics) {
      if (metric.op === 'avg') {
        const avg = value.averages.get(metric.as) ?? { sum: 0, count: 0 };
        output[metric.as] = avg.count === 0 ? null : avg.sum / avg.count;
        continue;
      }

      output[metric.as] = value.values.get(metric.as) ?? null;
    }

    rows.push(output);
  }

  return {
    rows,
    rowsIn,
    rowsOut: rows.length
  };
};

export const createReduceAccumulator = (
  spec: ReduceSpec,
  options?: ReduceAccumulatorOptions
): ReduceAccumulator => {
  const state = new Map<string, ReduceGroupState>();

  return {
    consume(row: Record<string, unknown>): void {
      const group = decodeGroup(spec.groupBy, row._id);
      const key = JSON.stringify(group);

      let current = state.get(key);

      if (!current) {
        if (options?.maxGroups !== undefined && state.size >= options.maxGroups) {
          throw new Error(
            `Reduce group cardinality exceeded REPORT_REDUCE_MAX_GROUPS (${options.maxGroups})`
          );
        }

        current = {
          group,
          values: new Map<string, unknown>(),
          averages: new Map<string, AverageState>(),
          inputCount: 0
        } satisfies ReduceGroupState;

        state.set(key, current);
      }

      current.inputCount += numericFrom(row[INPUT_COUNT_FIELD], INPUT_COUNT_FIELD);

      for (const metric of spec.metrics) {
        mergeMetricValue(current, metric, row);
      }
    },
    finalize(): ReduceSummary {
      return toReduceSummary(spec, state);
    },
    groupCount(): number {
      return state.size;
    }
  };
};

export const reducePartitionRows = (
  spec: ReduceSpec,
  partialRows: Array<Record<string, unknown>>
): ReduceSummary => {
  const accumulator = createReduceAccumulator(spec);

  for (const row of partialRows) {
    accumulator.consume(row);
  }

  return accumulator.finalize();
};
