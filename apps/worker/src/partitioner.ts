import { ObjectId } from 'mongodb';

export type ObjectIdRange = {
  index: number;
  startInclusive: ObjectId;
  endExclusive: ObjectId | null;
};

const objectIdToBigInt = (id: ObjectId): bigint => BigInt(`0x${id.toHexString()}`);

const bigIntToObjectId = (value: bigint): ObjectId => {
  const hex = value.toString(16).padStart(24, '0').slice(-24);
  return new ObjectId(hex);
};

export const buildObjectIdRanges = (
  minId: ObjectId,
  maxId: ObjectId,
  requestedChunks: number
): ObjectIdRange[] => {
  const min = objectIdToBigInt(minId);
  const max = objectIdToBigInt(maxId);

  if (max < min) {
    return [];
  }

  const chunks = Math.max(1, requestedChunks);

  if (chunks === 1) {
    return [
      {
        index: 0,
        startInclusive: minId,
        endExclusive: null
      }
    ];
  }

  const span = max - min + 1n;
  const chunkSize = (span + BigInt(chunks) - 1n) / BigInt(chunks);
  const ranges: ObjectIdRange[] = [];

  for (let index = 0; index < chunks; index += 1) {
    const start = min + BigInt(index) * chunkSize;

    if (start > max) {
      break;
    }

    const nextStart = start + chunkSize;

    ranges.push({
      index,
      startInclusive: bigIntToObjectId(start),
      endExclusive: nextStart <= max ? bigIntToObjectId(nextStart) : null
    });
  }

  return ranges;
};

export const rangeToIdMatch = (range: ObjectIdRange): Record<string, unknown> => {
  if (!range.endExclusive) {
    return {
      _id: {
        $gte: range.startInclusive
      }
    };
  }

  return {
    _id: {
      $gte: range.startInclusive,
      $lt: range.endExclusive
    }
  };
};
