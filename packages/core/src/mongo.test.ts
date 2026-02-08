import { describe, expect, it, vi } from 'vitest';
import { Db } from 'mongodb';
import { assertReadReplica } from './mongo';

describe('core/mongo', () => {
  it('throws when read replica points to primary', async () => {
    const db = {
      command: vi.fn().mockResolvedValue({
        isWritablePrimary: true
      })
    } as unknown as Db;

    await expect(assertReadReplica(db)).rejects.toThrow(/pointing to primary/i);
  });

  it('passes when read replica is secondary/hidden', async () => {
    const db = {
      command: vi.fn().mockResolvedValue({
        isWritablePrimary: false
      })
    } as unknown as Db;

    await expect(assertReadReplica(db)).resolves.toBeUndefined();
  });
});
