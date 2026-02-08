import { CronExpressionParser } from 'cron-parser';

export const computeNextRunAt = (
  cronExpression: string,
  timezone: string,
  fromDate: Date
): Date => {
  const cron = CronExpressionParser.parse(cronExpression, {
    tz: timezone,
    currentDate: fromDate
  });

  return cron.next().toDate();
};
