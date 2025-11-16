import os from 'os';
import {
    setLogTransports,
    ConsoleLogTransport,
    logger,
    LogLevel,
    withLogLevelFilter,
} from 'kv-logger';

function toBool(str: string): boolean {
    return str === 'true' || str === '1';
}

function toNumber(str: string, defaultValue: number): number {
    const num = Number(str);
    if (Number.isNaN(num)) {
        return defaultValue;
    }
    return num;
}

export const config = {
    // Config for log
    log: {
        // debug, info, warn, error
        level: process.env.LOG_LEVEL || 'info',
        // text, json
        format: process.env.LOG_FORMAT || 'text',
    },
    // Config for database, only support mysql.
    db: {
        username: process.env.RDS_USERNAME || 'codepush',
        password: process.env.RDS_PASSWORD || 'codepush',
        database: process.env.RDS_DATABASE || 'codepush',
        host: process.env.RDS_HOST || '127.0.0.1',
        port: toNumber(process.env.RDS_PORT, 3306),
        dialect: 'mysql',
        logging: false,
        dialectOptions: toBool(process.env.DB_SSL_REQUIRED)
            ? {
                  ssl: {
                      require: false,
                      // 관리형 MySQL에서 self-signed 또는 CA 정보 안 주는 경우가 많아서, 일단 빠르게 붙는 용도로 false. 추후 CA 넣어서 true로 바꾸는 게 베스트.
                      rejectUnauthorized: toBool(process.env.DB_SSL_REJECT_UNAUTHORIZED) ?? false,
                  },
              }
            : {},
    },
    // Config for qiniu (http://www.qiniu.com/) cloud storage when storageType value is "qiniu".
    qiniu: {
        accessKey: process.env.QINIU_ACCESS_KEY,
        secretKey: process.env.QINIU_SECRET_KEY,
        bucketName: process.env.QINIU_BUCKET_NAME,
        downloadUrl: process.env.QINIU_DOWNLOAD_URL || process.env.DOWNLOAD_URL,
    },
    // Config for Amazon s3 (https://aws.amazon.com/cn/s3/) storage when storageType value is "s3".
    s3: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        sessionToken: process.env.AWS_SESSION_TOKEN, // (optional)
        bucketName: process.env.AWS_BUCKET_NAME,
        region: process.env.AWS_REGION,
        // binary files download host address.
        downloadUrl: process.env.AWS_DOWNLOAD_URL || process.env.DOWNLOAD_URL,
        prefix: process.env.AWS_S3_KEY_PREFIX, // (optional)
    },
    // Config for Aliyun OSS (https://www.aliyun.com/product/oss) when storageType value is "oss".
    oss: {
        accessKeyId: process.env.OSS_ACCESS_KEY_ID,
        secretAccessKey: process.env.OSS_SECRET_ACCESS_KEY,
        endpoint: process.env.OSS_ENDPOINT,
        bucketName: process.env.OSS_BUCKET_NAME,
        // Key prefix in object key
        prefix: process.env.OSS_PREFIX,
        // binary files download host address
        downloadUrl: process.env.OSS_DOWNLOAD_URL || process.env.DOWNLOAD_URL,
    },
    // Config for tencentyun COS (https://cloud.tencent.com/product/cos) when storageType value is "oss".
    tencentcloud: {
        accessKeyId: process.env.COS_ACCESS_KEY_ID,
        secretAccessKey: process.env.COS_SECRET_ACCESS_KEY,
        bucketName: process.env.COS_BUCKET_NAME,
        region: process.env.COS_REGION,
        // binary files download host address
        downloadUrl: process.env.COS_DOWNLOAD_URL || process.env.DOWNLOAD_URL,
    },
    // Config for local storage when storageType value is "local".
    local: {
        // Binary files storage dir, Do not use tmpdir and it's public download dir.
        storageDir: process.env.STORAGE_DIR || os.tmpdir(),
        // Binary files download host address which Code Push Server listen to. the files storage in storageDir.
        downloadUrl:
            process.env.LOCAL_DOWNLOAD_URL ||
            process.env.DOWNLOAD_URL ||
            'http://172.30.1.85:3000/download',
        // public static download spacename.
        public: '/download',
    },
    jwt: {
        // Recommended: 63 random alpha-numeric characters
        // Generate using: https://www.grc.com/passwords.htm
        tokenSecret: process.env.TOKEN_SECRET || 'INSERT_RANDOM_TOKEN_KEY',
    },
    common: {
        // determine whether new account registrations are allowed
        allowRegistration: toBool(process.env.ALLOW_REGISTRATION),
        /*
         * tryLoginTimes is control login error times to avoid force attack.
         * if value is 0, no limit for login auth, it may not safe for account. when it's a number, it means you can
         * try that times today. but it need config redis server.
         */
        tryLoginTimes: toNumber(process.env.TRY_LOGIN_TIMES, 4),
        // create patch updates's number. default value is 3
        diffNums: toNumber(process.env.DIFF_NUMS, 3),
        // data dir for caclulate diff files. it's optimization.
        dataDir: process.env.DATA_DIR || os.tmpdir(),
        // storageType which is your binary package files store. options value is ("local" | "qiniu" | "s3"| "oss" || "tencentcloud")
        storageType: (process.env.STORAGE_TYPE || 'local') as
            | 'local'
            | 'qiniu'
            | 's3'
            | 'oss'
            | 'tencentcloud',
        // options value is (true | false), when it's true, it will cache updateCheck results in redis.
        updateCheckCache: toBool(process.env.UPDATE_CHECK_CACHE),
        // options value is (true | false), when it's true, it will cache rollout results in redis
        rolloutClientUniqueIdCache: toBool(process.env.ROLLOUT_CLIENT_UNIQUE_ID_CACHE),
    },
    // Config for smtp email，register module need validate user email project source https://github.com/nodemailer/nodemailer
    smtpConfig: {
        host: process.env.SMTP_HOST,
        port: toNumber(process.env.SMTP_PORT, 465),
        secure: true,
        auth: {
            user: process.env.SMTP_USERNAME,
            pass: process.env.SMTP_PASSWORD,
        },
    },
    // Config for redis (register module, tryLoginTimes module)
    redis: {
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: toNumber(process.env.REDIS_PORT, 6379),
        password: process.env.REDIS_PASSWORD,
        db: toNumber(process.env.REDIS_DB, 0),
        tlsUrl: process.env.REDIS_TLS_URL || process.env.REDIS_URL,
    },
} as const;

// config logger - make sure its ready before anyting else
setLogTransports(
    withLogLevelFilter(config.log.level as LogLevel)(
        new ConsoleLogTransport(config.log.format as 'text' | 'json'),
    ),
);

const env = process.env.NODE_ENV || 'development';
logger.info(`use config`, {
    env,
    storageType: config.common.storageType,
});
