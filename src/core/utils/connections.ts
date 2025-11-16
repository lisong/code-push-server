import { logger } from 'kv-logger';
import { createClient } from 'redis';
import { Sequelize } from 'sequelize';
import { config } from '../config';

export const sequelize = new Sequelize(
    config.db.database,
    config.db.username,
    config.db.password,
    config.db,
);

const redisTlsUrl = config.redis.tlsUrl; // REDIS_TLS_URL 또는 REDIS_URL
const isTlsSupported = !!redisTlsUrl && redisTlsUrl.startsWith('rediss://');

export const redisClient = redisTlsUrl
    ? createClient({
          url: redisTlsUrl,
          socket: {
              tls: isTlsSupported,
              rejectUnauthorized: false, // `Redis Client Error: self-signed certificate in certificate chain` 오류 우회; 헤로쿠 공식문서도 이 옵션 사용으로 명시되어 있음
              reconnectStrategy: (retries: number) => {
                  if (retries > 10) {
                      return new Error('Retry count exhausted');
                  }
                  return retries * 100;
              },
          },
      })
    : createClient({
          socket: {
              host: config.redis.host,
              port: config.redis.port,
              reconnectStrategy: (retries: number) => {
                  if (retries > 10) {
                      return new Error('Retry count exhausted');
                  }

                  return retries * 100;
              },
          },
          password: config.redis.password,
          database: config.redis.db,
      });

// 에러 로깅 (Unhandled 'error' 로 인한 앱크래시 방지)
redisClient.on('error', (err) => {
    logger.error('Redis Client Error', { message: err?.message, stack: err?.stack });
});

// connect 시도 (커넥션 실패시 앱크래시 방지)
redisClient.connect().catch((err) => {
    logger.error('Redis connect error', { message: err?.message, stack: err?.stack });
});
