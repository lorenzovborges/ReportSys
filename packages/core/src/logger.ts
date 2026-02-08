import pino, { Logger } from 'pino';
import { AppConfig } from './config';

export const createLogger = (config: AppConfig): Logger =>
  pino({
    level: config.logLevel,
    base: {
      service: 'reportsys'
    },
    timestamp: pino.stdTimeFunctions.isoTime
  });
