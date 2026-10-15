
const { Client } = require('pg');
const { createClient } = require('redis');

const postgresConfig = {
  host: process.env.POSTGRES_HOST || 'timescaledb',
  port: Number.parseInt(process.env.POSTGRES_PORT || '5432', 10),
  database: process.env.POSTGRES_DB || 'iotdata',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};

const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';

const redisBatchSize = Number.parseInt(
  process.env.REDIS_BATCH_SIZE || '1000',
  10
);

if (!Number.isInteger(redisBatchSize) || redisBatchSize < 1) {
  throw new Error('REDIS_BATCH_SIZE must be a positive integer');
}

const plugMedianQuery = `
  SELECT
    window_size,
    slice_index_in_day,
    house_id,
    household_id,
    plug_id,
    percentile_cont(0.5)
      WITHIN GROUP (ORDER BY average_load) AS median_load
  FROM plug_average
  GROUP BY
    window_size,
    slice_index_in_day,
    house_id,
    household_id,
    plug_id
`;

const houseMedianQuery = `
  SELECT
    window_size,
    slice_index_in_day,
    house_id,
    percentile_cont(0.5)
      WITHIN GROUP (ORDER BY average_load) AS median_load
  FROM house_average
  GROUP BY
    window_size,
    slice_index_in_day,
    house_id
`;

function plugMedianKey(row) {
  return (
    `median:plug:${row.window_size}:` +
    `${row.slice_index_in_day}:` +
    `${row.house_id}:` +
    `${row.household_id}:` +
    `${row.plug_id}`
  );
}

function houseMedianKey(row) {
  return (
    `median:house:${row.window_size}:` +
    `${row.slice_index_in_day}:` +
    `${row.house_id}`
  );
}

// Wait until TimescaleDB accepts connections.
async function waitForPostgres(database) {
  console.log('[median-cache-loader] Waiting for TimescaleDB...');

  while (true) {
    try {
      await database.connect();

      console.log('[median-cache-loader] Connected to TimescaleDB.');
      return;
    } catch (error) {
      console.log(
        `[median-cache-loader] TimescaleDB not ready: ${
          error.code || error.message
        }. Retrying in 2 seconds...`
      );

      // A failed pg.Client connection should be discarded before retrying.
      // Create a fresh client for the next attempt.
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Replace the failed client's connection attempt with a fresh client.
      database.removeAllListeners();
      database._ending = true;
      throw error;
    }
  }
}

async function writeMedianRows(redisClient, rows, keyFactory) {
  let writtenCount = 0;
  let batchCount = 0;
  let batch = redisClient.multi();

  for (const row of rows) {
    if (row.median_load === null || row.median_load === undefined) {
      continue;
    }

    const median = Number(row.median_load);
    const key = keyFactory(row);

    if (!Number.isFinite(median)) {
      throw new Error(`Invalid median value for ${key}: ${row.median_load}`);
    }

    batch.set(key, String(median));
    batchCount++;

    if (batchCount >= redisBatchSize) {
      await batch.exec();
      writtenCount += batchCount;

      console.log(
        `[median-cache-loader] Written ${writtenCount} keys`
      );

      batch = redisClient.multi();
      batchCount = 0;
    }
  }

  if (batchCount > 0) {
    await batch.exec();
    writtenCount += batchCount;
  }

  return writtenCount;
}

async function main() {
  let database = new Client(postgresConfig);

  const redisClient = createClient({
    url: redisUrl,
  });

  redisClient.on('error', (error) => {
    console.error(
      '[median-cache-loader] Redis error:',
      error.message
    );
  });

  try {
    // Connect to TimescaleDB.
    console.log('[median-cache-loader] Connecting to TimescaleDB...');

    while (true) {
      try {
        await database.connect();
        console.log('[median-cache-loader] Connected to TimescaleDB.');
        break;
      } catch (error) {
        console.log(
          `[median-cache-loader] TimescaleDB not ready: ${
            error.code || error.message
          }. Retrying in 2 seconds...`
        );

        // Discard the failed client and create a fresh one.
        database = new Client(postgresConfig);

        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    // Connect to Redis.
    console.log('[median-cache-loader] Connecting to Redis...');
    await redisClient.connect();
    await redisClient.ping();

    // Load plug median values.
    console.log('[median-cache-loader] Querying plug medians...');

    const plugResult = await database.query(plugMedianQuery);

    console.log(
      `[median-cache-loader] Retrieved ${plugResult.rowCount} plug rows`
    );

    const writtenPlugKeys = await writeMedianRows(
      redisClient,
      plugResult.rows,
      plugMedianKey
    );

    console.log(
      `[median-cache-loader] Loaded ${writtenPlugKeys} plug median keys`
    );

    // Load house median values.
    console.log('[median-cache-loader] Querying house medians...');

    const houseResult = await database.query(houseMedianQuery);

    console.log(
      `[median-cache-loader] Retrieved ${houseResult.rowCount} house rows`
    );

    const writtenHouseKeys = await writeMedianRows(
      redisClient,
      houseResult.rows,
      houseMedianKey
    );

    console.log(
      `[median-cache-loader] Loaded ${writtenHouseKeys} house median keys`
    );

    console.log(
      '[median-cache-loader] Initial cache loading completed successfully'
    );
  } finally {
    if (redisClient.isOpen) {
      await redisClient.quit().catch((error) => {
        console.error(
          '[median-cache-loader] Failed to close Redis:',
          error.message
        );
      });
    }

    await database.end().catch((error) => {
      console.error(
        '[median-cache-loader] Failed to close PostgreSQL:',
        error.message
      );
    });
  }
}

main().catch((error) => {
  console.error('[median-cache-loader] Failed:', error);
  process.exitCode = 1;
});
