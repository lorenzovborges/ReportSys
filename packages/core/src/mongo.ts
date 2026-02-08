import { Db, MongoClient, ReadPreference } from 'mongodb';
import { AppConfig } from './config';

export type MongoConnections = {
  writeClient: MongoClient;
  readClient: MongoClient;
  writeDb: Db;
  readDb: Db;
};

export const connectMongo = async (config: AppConfig): Promise<MongoConnections> => {
  const writeClient = new MongoClient(config.mongoWriteUri);
  const readClient = new MongoClient(config.mongoReportReadUri, {
    readPreference: ReadPreference.SECONDARY
  });

  await Promise.all([writeClient.connect(), readClient.connect()]);

  return {
    writeClient,
    readClient,
    writeDb: writeClient.db(config.mongoDbName),
    readDb: readClient.db(config.mongoDbName)
  };
};

export const closeMongo = async (connections: MongoConnections): Promise<void> => {
  await Promise.all([connections.writeClient.close(), connections.readClient.close()]);
};

export const assertReadReplica = async (readDb: Db): Promise<void> => {
  const hello = await readDb.command({ hello: 1 });

  if (hello?.isWritablePrimary) {
    throw new Error('Read replica connection is pointing to primary, refusing to run report job');
  }
};
