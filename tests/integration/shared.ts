import { setTimeout as sleep } from 'node:timers/promises';

export type IntegrationRequestOptions = {
  method: 'GET' | 'POST' | 'PATCH' | 'DELETE';
  path: string;
  body?: unknown;
  headers?: Record<string, string>;
};

export type IntegrationResponse<T = any> = {
  status: number;
  body: T | null;
  raw: string;
};

const apiHostPort = process.env.API_HOST_PORT ?? '3000';

export const integrationConfig = {
  baseUrl: process.env.REPORTSYS_BASE_URL ?? `http://127.0.0.1:${apiHostPort}`,
  apiKey: process.env.REPORTSYS_API_KEY ?? 'local-dev-key',
  tenantId: process.env.REPORTSYS_TENANT_ID ?? 'integration-tenant'
};

export const requestJson = async <T = any>({
  method,
  path,
  body,
  headers
}: IntegrationRequestOptions): Promise<IntegrationResponse<T>> => {
  const response = await fetch(`${integrationConfig.baseUrl}${path}`, {
    method,
    headers: {
      ...(body !== undefined ? { 'content-type': 'application/json' } : {}),
      ...(headers ?? {})
    },
    body: body !== undefined ? JSON.stringify(body) : undefined
  });

  const raw = await response.text();
  let parsed: T | null = null;

  if (raw) {
    try {
      parsed = JSON.parse(raw) as T;
    } catch {
      parsed = null;
    }
  }

  return {
    status: response.status,
    body: parsed,
    raw
  };
};

export const apiHeaders = (): Record<string, string> => ({
  'x-api-key': integrationConfig.apiKey,
  'x-tenant-id': integrationConfig.tenantId
});

export const waitForJobCompletion = async (
  jobId: string,
  maxAttempts = 90,
  waitMs = 1000
): Promise<any> => {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const statusResponse = await requestJson({
      method: 'GET',
      path: `/v1/reports/${jobId}`,
      headers: apiHeaders()
    });

    if (statusResponse.status !== 200) {
      throw new Error(
        `failed to query job status. status=${statusResponse.status} body=${statusResponse.raw}`
      );
    }

    const body = statusResponse.body as any;
    if (!body) {
      throw new Error('empty job status response');
    }

    if (['uploaded', 'failed', 'expired'].includes(body.status)) {
      return body;
    }

    await sleep(waitMs);
  }

  throw new Error(`job ${jobId} did not finish within timeout`);
};
