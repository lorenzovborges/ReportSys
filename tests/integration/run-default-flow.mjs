import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '../..');

const run = (command, args, env = {}) => {
  const result = spawnSync(command, args, {
    cwd: root,
    stdio: 'inherit',
    env: {
      ...process.env,
      ...env
    }
  });

  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with status ${result.status}`);
  }
};

const resolveApiBaseUrl = (env = {}) => {
  const result = spawnSync('docker', ['compose', 'port', 'api', '3000'], {
    cwd: root,
    stdio: 'pipe',
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env
    }
  });

  if (result.status !== 0) {
    throw new Error(`docker compose port api 3000 failed with status ${result.status}`);
  }

  const endpoint = result.stdout.trim();
  const portMatch = endpoint.match(/:(\d+)$/);

  if (!portMatch) {
    throw new Error(`could not parse API port from docker compose output: '${endpoint}'`);
  }

  return `http://127.0.0.1:${portMatch[1]}`;
};

const main = async () => {
  run('docker', ['compose', 'up', '-d', '--build']);
  const apiBaseUrl = resolveApiBaseUrl();
  run('npx', [
    'vitest',
    'run',
    'tests/integration/default-flow.test.ts',
    '--config',
    'vitest.config.ts'
  ], {
    REPORTSYS_BASE_URL: apiBaseUrl
  });
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
