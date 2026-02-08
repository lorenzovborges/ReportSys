import { createHash } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { PassThrough, Transform, Writable } from 'node:stream';
import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ClientConfig
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { ReportArtifactMode, ReportArtifactView } from '@reportsys/contracts';
import { AppConfig } from '@reportsys/core';
import { Logger } from 'pino';

export type IntegrationPolicy = 'required' | 'optional';

export type UploadContext = {
  tenantId: string;
  jobId: string;
  integration: string;
};

export type UploadInput = {
  key: string;
  contentType: string;
  stream: NodeJS.ReadableStream;
  context: UploadContext;
};

type SignerFn = (
  client: S3Client,
  command: GetObjectCommand,
  options: { expiresIn: number }
) => Promise<string>;

type Dependencies = {
  s3Client?: S3Client;
  signer?: SignerFn;
};

const countAndHash = () => {
  const hash = createHash('sha256');
  let sizeBytes = 0;

  const transform = new Transform({
    transform(chunk, _encoding, callback): void {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      sizeBytes += buffer.length;
      hash.update(buffer);
      callback(null, buffer);
    }
  });

  return {
    transform,
    getDigest: () => ({ sizeBytes, checksum: hash.digest('hex') })
  };
};

export class StorageService {
  private readonly s3Client?: S3Client;

  private readonly s3SignerClient?: S3Client;

  private readonly signer: SignerFn;

  constructor(
    private readonly config: AppConfig,
    private readonly logger: Logger,
    private readonly policy: IntegrationPolicy,
    dependencies?: Dependencies
  ) {
    this.signer = dependencies?.signer ?? getSignedUrl;

    if (config.storageDriver === 's3' || config.storageDriver === 'minio') {
      const clientConfig: S3ClientConfig = {
        endpoint: config.s3Endpoint,
        region: config.s3Region,
        forcePathStyle: config.s3ForcePathStyle,
        credentials: {
          accessKeyId: config.s3AccessKeyId,
          secretAccessKey: config.s3SecretAccessKey
        }
      };

      this.s3Client = dependencies?.s3Client ?? new S3Client(clientConfig);

      if (config.s3PublicEndpoint) {
        this.s3SignerClient = new S3Client({
          ...clientConfig,
          endpoint: config.s3PublicEndpoint
        });
      }
    }
  }

  private resolveMode(): ReportArtifactMode {
    if (!this.config.enableExternalStorage) {
      return 'noop';
    }

    return this.config.storageDriver;
  }

  private async consumeIntoNoop(
    input: UploadInput,
    reason: string
  ): Promise<ReportArtifactView> {
    const { transform, getDigest } = countAndHash();

    const blackhole = new Writable({
      write(_chunk, _encoding, callback): void {
        callback();
      }
    });

    await pipeline(input.stream, transform, blackhole);

    const digest = getDigest();

    this.logger.info(
      {
        tenantId: input.context.tenantId,
        jobId: input.context.jobId,
        integration: input.context.integration,
        mode: 'noop',
        reason
      },
      'Storage disabled or bypassed. Job completed in noop mode'
    );

    return {
      mode: 'noop',
      available: false,
      reason,
      key: input.key,
      checksum: digest.checksum,
      sizeBytes: digest.sizeBytes
    };
  }

  private async drainStream(stream: NodeJS.ReadableStream): Promise<void> {
    try {
      await pipeline(
        stream,
        new Writable({
          write(_chunk, _encoding, callback): void {
            callback();
          }
        })
      );
    } catch {
      // noop: best-effort drain only
    }
  }

  private async uploadToFilesystem(input: UploadInput): Promise<ReportArtifactView> {
    const absolutePath = resolve(this.config.filesystemStoragePath, input.key);
    await mkdir(dirname(absolutePath), { recursive: true });

    const { transform, getDigest } = countAndHash();
    const fileWriter = createWriteStream(absolutePath);
    await pipeline(input.stream, transform, fileWriter);

    const digest = getDigest();

    return {
      mode: 'filesystem',
      available: true,
      key: absolutePath,
      checksum: digest.checksum,
      sizeBytes: digest.sizeBytes
    };
  }

  private async uploadToS3Like(
    mode: 's3' | 'minio',
    input: UploadInput
  ): Promise<ReportArtifactView> {
    if (!this.s3Client) {
      throw new Error('S3 client not configured');
    }

    const { transform, getDigest } = countAndHash();
    const body = new PassThrough();

    const uploader = new Upload({
      client: this.s3Client,
      params: {
        Bucket: this.config.s3Bucket,
        Key: input.key,
        ContentType: input.contentType,
        Body: body
      }
    });

    const pipePromise = pipeline(input.stream, transform, body);
    await Promise.all([uploader.done(), pipePromise]);

    const digest = getDigest();

    return {
      mode,
      available: true,
      bucket: this.config.s3Bucket,
      key: input.key,
      checksum: digest.checksum,
      sizeBytes: digest.sizeBytes
    };
  }

  async upload(input: UploadInput): Promise<ReportArtifactView> {
    const mode = this.resolveMode();

    if (mode === 'noop') {
      return this.consumeIntoNoop(input, 'EXTERNAL_STORAGE_DISABLED');
    }

    try {
      if (mode === 'filesystem') {
        return await this.uploadToFilesystem(input);
      }

      return await this.uploadToS3Like(mode, input);
    } catch (error) {
      if (this.policy === 'optional') {
        this.logger.warn(
          {
            tenantId: input.context.tenantId,
            jobId: input.context.jobId,
            integration: input.context.integration,
            mode,
            err: error
          },
          'Optional integration failure. Falling back to noop'
        );

        await this.drainStream(input.stream);

        return {
          mode: 'noop',
          available: false,
          reason: 'OPTIONAL_INTEGRATION_FAILURE',
          key: input.key
        };
      }

      this.logger.error(
        {
          tenantId: input.context.tenantId,
          jobId: input.context.jobId,
          integration: input.context.integration,
          mode,
          err: error
        },
        'Required integration failure'
      );

      throw error;
    }
  }

  async createSignedDownloadUrl(artifact: ReportArtifactView): Promise<string | null> {
    if (!artifact.available || !artifact.key) {
      return null;
    }

    if (artifact.mode === 'filesystem') {
      return `file://${artifact.key}`;
    }

    if (artifact.mode === 'noop') {
      return null;
    }

    const signerClient = this.s3SignerClient ?? this.s3Client;

    if (!signerClient) {
      return null;
    }

    const command = new GetObjectCommand({
      Bucket: artifact.bucket ?? this.config.s3Bucket,
      Key: artifact.key
    });

    return this.signer(signerClient, command, {
      expiresIn: this.config.signedUrlTtlSeconds
    });
  }

  async healthCheck(): Promise<void> {
    const mode = this.resolveMode();

    if (mode === 'noop' || mode === 'filesystem') {
      return;
    }

    if (!this.s3Client) {
      throw new Error('S3 client not initialized');
    }

    await this.s3Client.send(
      new PutObjectCommand({
        Bucket: this.config.s3Bucket,
        Key: '.healthcheck',
        Body: 'ok'
      })
    );
  }
}
