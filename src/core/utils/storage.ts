import fs from 'fs';
import path from 'path';
import ALYOSSStream from 'aliyun-oss-upload-stream';
import ALY from 'aliyun-sdk';
import AWS from 'aws-sdk';
import COS from 'cos-nodejs-sdk-v5';
import fsextra from 'fs-extra';
import { Logger } from 'kv-logger';
import _ from 'lodash';
import qiniu from 'qiniu';
import { AppError } from '../app-error';
import { config } from '../config';

function uploadFileToLocal(key: string, filePath: string, logger: Logger): Promise<void> {
    return new Promise((resolve, reject) => {
        logger.info(`try uploadFileToLocal`, {
            key,
        });

        const storageDir = _.get(config, 'local.storageDir');
        if (!storageDir) {
            throw new AppError('please set config local storageDir');
        }
        if (key.length < 3) {
            logger.error(`generate key is too short, key value:${key}`);
            throw new AppError('generate key is too short.');
        }
        try {
            logger.debug(`uploadFileToLocal check directory ${storageDir} fs.W_OK`);
            fs.accessSync(storageDir, fs.constants.W_OK);
            logger.debug(`uploadFileToLocal directory ${storageDir} fs.W_OK is ok`);
        } catch (err) {
            throw new AppError(err);
        }
        const subDir = key.substring(0, 2).toLowerCase();
        const finalDir = path.join(storageDir, subDir);
        const fileName = path.join(finalDir, key);
        if (fs.existsSync(fileName)) {
            logger.info(`uploadFileToLocal file exists, skip copy`, {
                key,
            });

            resolve();
            return;
        }
        let stats = fs.statSync(storageDir);
        if (!stats.isDirectory()) {
            throw new AppError(`${storageDir} must be directory`);
        }
        if (!fs.existsSync(`${finalDir}`)) {
            fs.mkdirSync(`${finalDir}`);
            logger.info(`uploadFileToLocal mkdir:${finalDir}`, {
                key,
            });
        }
        try {
            fs.accessSync(filePath, fs.constants.R_OK);
        } catch (err) {
            throw new AppError(err);
        }
        stats = fs.statSync(filePath);
        if (!stats.isFile()) {
            throw new AppError(`${filePath} must be file`);
        }
        fsextra.copy(filePath, fileName, (err) => {
            if (err) {
                reject(new AppError(err));
                return;
            }
            logger.info(`uploadFileToLocal copy file success.`, {
                key,
            });
            resolve();
        });
    });
}

function uploadFileToS3(key: string, filePath: string, logger: Logger): Promise<void> {
    return new Promise((resolve, reject) => {
        logger.info('try uploadFileToS3', { key });

        // config.s3.prefix에서 prefix 읽기
        let prefix = _.get(config, 's3.prefix', '') as string | undefined;
        prefix = (prefix || '').trim();

        if (prefix.length > 0 && !prefix.endsWith('/')) {
            prefix += '/';
        }

        const finalKey = prefix ? `${prefix}${key}` : key;
        logger.info('uploadFileToS3 resolved finalKey', { key: finalKey });

        AWS.config.update({
            accessKeyId: _.get(config, 's3.accessKeyId'),
            secretAccessKey: _.get(config, 's3.secretAccessKey'),
            sessionToken: _.get(config, 's3.sessionToken'),
            region: _.get(config, 's3.region'),
        });
        const s3 = new AWS.S3();
        fs.readFile(filePath, (err, data) => {
            if (err) {
                reject(new AppError(err));
                return;
            }
            s3.upload(
                {
                    Key: finalKey, // prefix 적용된 key
                    Body: data,
                    ACL: 'public-read',
                    Bucket: _.get(config, 's3.bucketName'),
                },
                (error: Error) => {
                    if (error) {
                        reject(new AppError(error));
                    } else {
                        logger.info('uploadFileToS3 success', { key: finalKey });
                        resolve();
                    }
                },
            );
        });
    });
}

function uploadFileToOSS(key: string, filePath: string, logger: Logger): Promise<void> {
    logger.info('try uploadFileToOSS', { key });
    const ossStream = ALYOSSStream(
        new ALY.OSS({
            accessKeyId: _.get(config, 'oss.accessKeyId'),
            secretAccessKey: _.get(config, 'oss.secretAccessKey'),
            endpoint: _.get(config, 'oss.endpoint'),
            apiVersion: '2013-10-15',
        }),
    );
    if (!_.isEmpty(_.get(config, 'oss.prefix', ''))) {
        // eslint-disable-next-line no-param-reassign
        key = `${_.get(config, 'oss.prefix')}/${key}`;
    }
    const upload = ossStream.upload({
        Bucket: _.get(config, 'oss.bucketName'),
        Key: key,
    });

    return new Promise((resolve, reject) => {
        upload.on('error', (error) => {
            reject(new AppError(JSON.stringify(error)));
        });

        upload.on('uploaded', () => {
            logger.info('uploadFileToOSS success', { key });
            resolve();
        });
        fs.createReadStream(filePath).pipe(upload);
    });
}

function getUploadTokenQiniu(mac: qiniu.auth.digest.Mac, bucket: string, key: string) {
    const options = {
        scope: `${bucket}:${key}`,
    };
    const putPolicy = new qiniu.rs.PutPolicy(options);
    return putPolicy.uploadToken(mac);
}

function uploadFileToQiniu(key: string, filePath: string, logger: Logger): Promise<void> {
    return new Promise((resolve, reject) => {
        logger.info('try uploadFileToQiniu', { key });
        const accessKey = _.get(config, 'qiniu.accessKey');
        const secretKey = _.get(config, 'qiniu.secretKey');
        const bucket = _.get(config, 'qiniu.bucketName', '');
        const mac = new qiniu.auth.digest.Mac(accessKey, secretKey);
        const conf = new qiniu.conf.Config();
        const bucketManager = new qiniu.rs.BucketManager(mac, conf);
        bucketManager.stat(bucket, key, (respErr, respBody, respInfo) => {
            if (respErr) {
                reject(new AppError(respErr.message));
                return;
            }
            if (respInfo.statusCode === 200) {
                logger.info('uploadFileToQiniu file exists, skip upload', { key });
                resolve();
                return;
            }

            let uploadToken: string;
            try {
                uploadToken = getUploadTokenQiniu(mac, bucket, key);
            } catch (e) {
                reject(new AppError(e.message));
                return;
            }
            const formUploader = new qiniu.form_up.FormUploader(conf);
            const putExtra = new qiniu.form_up.PutExtra();
            formUploader.putFile(
                uploadToken,
                key,
                filePath,
                putExtra,
                (resErr, resBody, resInfo) => {
                    if (resErr) {
                        // 上传失败， 处理返回代码
                        return reject(new AppError(resErr));
                    }
                    // 上传成功， 处理返回值
                    if (resInfo.statusCode === 200) {
                        logger.info('uploadFileToQiniu success', { key });
                        return resolve();
                    }
                    return reject(new AppError(resBody.error));
                },
            );
        });
    });
}

function uploadFileToTencentCloud(key: string, filePath: string, logger: Logger): Promise<void> {
    return new Promise((resolve, reject) => {
        logger.info('try uploadFileToTencentCloud', { key });
        const cosIn = new COS({
            SecretId: _.get(config, 'tencentcloud.accessKeyId'),
            SecretKey: _.get(config, 'tencentcloud.secretAccessKey'),
        });
        cosIn.sliceUploadFile(
            {
                Bucket: _.get(config, 'tencentcloud.bucketName'),
                Region: _.get(config, 'tencentcloud.region'),
                Key: key,
                FilePath: filePath,
            },
            (err) => {
                if (err) {
                    reject(new AppError(err.message));
                } else {
                    logger.info('uploadFileToTencentCloud success', { key });
                    resolve();
                }
            },
        );
    });
}

export function uploadFileToStorage(key: string, filePath: string, logger: Logger): Promise<void> {
    const { storageType } = config.common;
    switch (storageType) {
        case 'local':
            return uploadFileToLocal(key, filePath, logger);
        case 's3':
            return uploadFileToS3(key, filePath, logger);
        case 'oss':
            return uploadFileToOSS(key, filePath, logger);
        case 'qiniu':
            return uploadFileToQiniu(key, filePath, logger);
        case 'tencentcloud':
            return uploadFileToTencentCloud(key, filePath, logger);
        default:
            throw new AppError(`${storageType} storageType does not support.`);
    }
}
