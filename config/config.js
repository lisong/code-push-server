var config = {};
config.development = {
  // Config for database, only support mysql.
  db: {
    username: process.env.RDS_USERNAME || "root",
    password: process.env.RDS_PASSWORD || null,
    database: process.env.DATA_BASE || "codepush",
    host: process.env.RDS_HOST || "127.0.0.1",
    port: process.env.RDS_PORT || 3306,
    dialect: "mysql"
  },
  // Config for qiniu (http://www.qiniu.com/) cloud storage when storageType value is "qiniu".
  qiniu: {
    accessKey: "",
    secretKey: "",
    bucketName: "",
    downloadUrl: "" // Binary files download host address.
  },
  // Config for Amazon s3 (https://aws.amazon.com/cn/s3/) storage when storageType value is "s3".
  s3: {
    bucketName: process.env.BUCKET_NAME,
    region: process.env.REGION,
    downloadUrl: process.env.DOWNLOAD_URL, // binary files download host address.
  },
  // Config for Aliyun OSS (https://www.aliyun.com/product/oss) when storageType value is "oss".
  oss: {
    accessKeyId: "",
    secretAccessKey: "",
    endpoint: "",
    bucketName: "",
    prefix: "", // Key prefix in object key
    downloadUrl: "", // binary files download host address.
  },
  // Config for local storage when storageType value is "local".
  local: {
    // Binary files storage dir, Do not use tmpdir and it's public download dir.
    storageDir: process.env.STORAGE_DIR || "/Users/tablee/workspaces/storage",
    // Binary files download host address which Code Push Server listen to. the files storage in storageDir.
    downloadUrl: process.env.LOCAL_DOWNLOAD_URL || "http://localhost:3000/download",
    // public static download spacename.
    public: process.env.PUBLIC || '/download'
  },
  jwt: {
    // Recommended: 63 random alpha-numeric characters
    // Generate using: https://www.grc.com/passwords.htm
    tokenSecret: process.env.TOKEN_SECRET ||'INSERT_RANDOM_TOKEN_KEY'
  },
  common: {
    /*
     * tryLoginTimes is control login error times to avoid force attack.
     * if value is 0, no limit for login auth, it may not safe for account. when it's a number, it means you can
     * try that times today. but it need config redis server.
     */
    tryLoginTimes: 0,
    // CodePush Web(https://github.com/lisong/code-push-web) login address.
    //codePushWebUrl: "http://localhost:3001/login",
    // create patch updates's number. default value is 3
    diffNums: 3,
    // data dir for caclulate diff files. it's optimization.
    dataDir: process.env.DATA_DIR || "/Users/tablee/workspaces/data",
    // storageType which is your binary package files store. options value is ("local" | "qiniu" | "s3")
    storageType: process.env.STORAGE_TYPE || "local",
    // options value is (true | false), when it's true, it will cache updateCheck results in redis.
    updateCheckCache: false
  },
  // Config for smtp email，register module need validate user email project source https://github.com/nodemailer/nodemailer
  smtpConfig:{
    host: "smtp.mxhichina.com",
    port: 465,
    secure: true,
    auth: {
      user: "",
      pass: ""
    }
  },
  // Config for redis (register module, tryLoginTimes module)
  redis: {
    default: {
      host: "127.0.0.1",
      port: 6379,
      retry_strategy: function (options) {
        if (options.error.code === 'ECONNREFUSED') {
          // End reconnecting on a specific error and flush all commands with a individual error
          return new Error('The server refused the connection');
        }
        if (options.total_retry_time > 1000 * 60 * 60) {
            // End reconnecting after a specific timeout and flush all commands with a individual error
            return new Error('Retry time exhausted');
        }
        if (options.times_connected > 10) {
            // End reconnecting with built in error
            return undefined;
        }
        // reconnect after
        return Math.max(options.attempt * 100, 3000);
      }
    }
  }
}
config.production = Object.assign({}, config.development);
module.exports = config;
