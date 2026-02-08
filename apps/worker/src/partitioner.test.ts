import { ObjectId } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { buildObjectIdRanges } from './partitioner';

const toBigInt = (id: ObjectId): bigint => BigInt(`0x${id.toHexString()}`);

describe('buildObjectIdRanges', () => {
  it('creates one open-ended range when chunks=1', () => {
    const min = new ObjectId('000000000000000000000010');
    const max = new ObjectId('0000000000000000000000ff');

    const ranges = buildObjectIdRanges(min, max, 1);

    expect(ranges).toHaveLength(1);
    expect(ranges[0]?.startInclusive.toHexString()).toBe(min.toHexString());
    expect(ranges[0]?.endExclusive).toBeNull();
  });

  it('covers full interval without overlap', () => {
    const min = new ObjectId('000000000000000000000100');
    const max = new ObjectId('0000000000000000000005ff');

    const ranges = buildObjectIdRanges(min, max, 8);

    expect(ranges.length).toBeGreaterThan(1);
    expect(ranges[0]?.startInclusive.toHexString()).toBe(min.toHexString());
    expect(ranges.at(-1)?.endExclusive).toBeNull();

    for (let index = 0; index < ranges.length - 1; index += 1) {
      const current = ranges[index];
      const next = ranges[index + 1];

      expect(current?.endExclusive?.toHexString()).toBe(next?.startInclusive.toHexString());
      expect(toBigInt(current!.startInclusive)).toBeLessThan(toBigInt(next!.startInclusive));
    }

    const lastStart = toBigInt(ranges.at(-1)!.startInclusive);
    expect(lastStart).toBeLessThanOrEqual(toBigInt(max));
  });

  it('returns empty when max < min', () => {
    const min = new ObjectId('ffffffffffffffffffffffff');
    const max = new ObjectId('000000000000000000000001');

    expect(buildObjectIdRanges(min, max, 4)).toEqual([]);
  });
});
