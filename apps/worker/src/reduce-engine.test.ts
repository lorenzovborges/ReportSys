import { describe, expect, it } from 'vitest';
import {
  buildReducePartitionPipeline,
  createReduceAccumulator,
  reducePartitionRows,
  validateReduceSpec
} from './reduce-engine';

describe('reduce-engine', () => {
  it('validates duplicate aliases', () => {
    expect(() =>
      validateReduceSpec({
        groupBy: ['region'],
        metrics: [
          { op: 'count', as: 'total' },
          { op: 'sum', field: 'amount', as: 'total' }
        ]
      })
    ).toThrow(/Duplicated metric alias/i);
  });

  it('builds partition pipeline with tenant filters and id range', () => {
    const pipeline = buildReducePartitionPipeline(
      'tenant-a',
      { status: 'paid' },
      { _id: { $gte: 'a', $lt: 'b' } },
      {
        groupBy: ['region'],
        metrics: [
          { op: 'count', as: 'orders' },
          { op: 'sum', field: 'amount', as: 'amountTotal' }
        ]
      }
    );

    expect(pipeline).toHaveLength(2);
    expect(pipeline[0]).toMatchObject({
      $match: {
        tenantId: 'tenant-a',
        status: 'paid',
        _id: { $gte: 'a', $lt: 'b' }
      }
    });

    expect(pipeline[1]).toMatchObject({
      $group: {
        _id: { region: '$region' },
        orders: { $sum: 1 },
        amountTotal: { $sum: '$amount' }
      }
    });
  });

  it('reduces count/sum/min/max/avg across partition partials', () => {
    const spec = {
      groupBy: ['region'],
      metrics: [
        { op: 'count', as: 'orders' },
        { op: 'sum', field: 'amount', as: 'totalAmount' },
        { op: 'min', field: 'amount', as: 'minAmount' },
        { op: 'max', field: 'amount', as: 'maxAmount' },
        { op: 'avg', field: 'amount', as: 'avgAmount' }
      ]
    } as const;

    const partialRows = [
      {
        _id: { region: 'us' },
        orders: 2,
        totalAmount: 30,
        minAmount: 10,
        maxAmount: 20,
        __avg_sum__avgAmount: 30,
        __avg_count__avgAmount: 2,
        __input_count: 2
      },
      {
        _id: { region: 'us' },
        orders: 1,
        totalAmount: 15,
        minAmount: 15,
        maxAmount: 15,
        __avg_sum__avgAmount: 15,
        __avg_count__avgAmount: 1,
        __input_count: 1
      },
      {
        _id: { region: 'br' },
        orders: 1,
        totalAmount: 5,
        minAmount: 5,
        maxAmount: 5,
        __avg_sum__avgAmount: 5,
        __avg_count__avgAmount: 1,
        __input_count: 1
      }
    ];

    const reduced = reducePartitionRows(spec, partialRows);
    const sorted = [...reduced.rows].sort((a, b) => String(a.region).localeCompare(String(b.region)));

    expect(reduced.rowsIn).toBe(4);
    expect(reduced.rowsOut).toBe(2);

    expect(sorted[0]).toEqual({
      region: 'br',
      orders: 1,
      totalAmount: 5,
      minAmount: 5,
      maxAmount: 5,
      avgAmount: 5
    });

    expect(sorted[1]).toEqual({
      region: 'us',
      orders: 3,
      totalAmount: 45,
      minAmount: 10,
      maxAmount: 20,
      avgAmount: 15
    });
  });

  it('enforces max reduce group cardinality in accumulator', () => {
    const accumulator = createReduceAccumulator(
      {
        groupBy: ['region'],
        metrics: [{ op: 'count', as: 'orders' }]
      },
      {
        maxGroups: 1
      }
    );

    accumulator.consume({
      _id: { region: 'us' },
      orders: 1,
      __input_count: 1
    });

    expect(() =>
      accumulator.consume({
        _id: { region: 'br' },
        orders: 1,
        __input_count: 1
      })
    ).toThrow(/REPORT_REDUCE_MAX_GROUPS/i);
  });

  it('finalizes accumulator in deterministic key order', () => {
    const accumulator = createReduceAccumulator({
      groupBy: ['region'],
      metrics: [{ op: 'count', as: 'orders' }]
    });

    accumulator.consume({
      _id: { region: 'us' },
      orders: 2,
      __input_count: 2
    });
    accumulator.consume({
      _id: { region: 'br' },
      orders: 1,
      __input_count: 1
    });

    const summary = accumulator.finalize();

    expect(summary.rows).toEqual([
      { region: 'br', orders: 1 },
      { region: 'us', orders: 2 }
    ]);
    expect(summary.rowsIn).toBe(3);
    expect(summary.rowsOut).toBe(2);
  });
});
