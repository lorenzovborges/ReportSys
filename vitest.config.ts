import { defineConfig } from 'vitest/config';
import { resolve } from 'node:path';

export default defineConfig({
  resolve: {
    alias: {
      '@reportsys/contracts': resolve(__dirname, 'packages/contracts/src'),
      '@reportsys/core': resolve(__dirname, 'packages/core/src'),
      '@reportsys/report-generators': resolve(__dirname, 'packages/report-generators/src'),
      '@reportsys/storage-s3': resolve(__dirname, 'packages/storage-s3/src')
    }
  },
  test: {
    include: [
      'apps/**/src/**/*.test.ts',
      'packages/**/src/**/*.test.ts',
      'tests/**/*.test.ts'
    ],
    exclude: ['**/node_modules/**', '**/dist/**', '**/coverage/**'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'json-summary'],
      include: ['apps/**/src/**/*.ts', 'packages/**/src/**/*.ts'],
      exclude: [
        '**/*.test.ts',
        '**/dist/**',
        '**/node_modules/**',
        '**/coverage/**',
        '**/infrastructure/**',
        '**/postman/**',
        '**/tests/**'
      ],
      thresholds: {
        statements: 60,
        branches: 55,
        functions: 65,
        lines: 60
      }
    }
  }
});
