import { describe, expect, it } from 'vitest';
import { computeNextRunAt } from './cron';

describe('core/cron', () => {
  it('computes next run date for valid cron expression', () => {
    const current = new Date('2026-02-08T10:00:00.000Z');
    const next = computeNextRunAt('*/5 * * * *', 'UTC', current);

    expect(next.getTime()).toBeGreaterThan(current.getTime());
    expect(next.toISOString()).toBe('2026-02-08T10:05:00.000Z');
  });

  it('throws for invalid cron expression', () => {
    expect(() => computeNextRunAt('invalid cron', 'UTC', new Date())).toThrow();
  });
});
