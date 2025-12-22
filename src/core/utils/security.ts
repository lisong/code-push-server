import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import bcrypt from 'bcryptjs';
import { logger } from 'kv-logger';
import _ from 'lodash';
import { generator } from 'rand-token';
import recursive from 'recursive-readdir';
import slash from 'slash';
import { AppError } from '../app-error';
import { ANDROID, IOS } from '../const';

export function md5(str: string) {
    const md5sum = crypto.createHash('md5');
    md5sum.update(str);
    return md5sum.digest('hex');
}

export function passwordHashSync(password: string) {
    return bcrypt.hashSync(password, bcrypt.genSaltSync(12));
}

export function passwordVerifySync(password: string, hash: string) {
    return bcrypt.compareSync(password, hash);
}

const randTokenGen = generator({
    chars: '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
    source: 'crypto',
});

export function randToken(num: number) {
    return randTokenGen.generate(num);
}

export function parseToken(token: string) {
    return { identical: token.substring(token.length - 9), token: token.substring(0, 28) };
}

function fileSha256(file: string): Promise<string> {
    return new Promise((resolve, reject) => {
        const rs = fs.createReadStream(file);
        const hash = crypto.createHash('sha256');
        rs.on('data', hash.update.bind(hash));
        rs.on('error', (e) => {
            reject(e);
        });
        rs.on('end', () => {
            resolve(hash.digest('hex'));
        });
    });
}

function stringSha256Sync(contents: string) {
    const sha256 = crypto.createHash('sha256');
    sha256.update(contents);
    return sha256.digest('hex');
}

function sortJsonToArr(json: Record<string, string>) {
    const rs: { path: string; hash: string }[] = [];
    _.forIn(json, (value, key) => {
        rs.push({ path: key, hash: value });
    });
    return _.sortBy(rs, (o) => o.path);
}

// some files are ignored in calc hash in client sdk
// https://github.com/Microsoft/react-native-code-push/pull/974/files#diff-21b650f88429c071b217d46243875987R15
function isHashIgnored(relativePath: string) {
    if (!relativePath) {
        return true;
    }

    const IgnoreMacOSX = '__MACOSX/';
    const IgnoreDSStore = '.DS_Store';

    return (
        relativePath.startsWith(IgnoreMacOSX) ||
        relativePath === IgnoreDSStore ||
        relativePath.endsWith(IgnoreDSStore)
    );
}

function isPackageHashIgnored(relativePath: string) {
    if (!relativePath) {
        return true;
    }

    // .codepushrelease contains code sign JWT
    // it should be ignored in package hash but need to be included in package manifest
    const IgnoreCodePushMetadata = '.codepushrelease';
    return (
        relativePath === IgnoreCodePushMetadata ||
        relativePath.endsWith(IgnoreCodePushMetadata) ||
        isHashIgnored(relativePath)
    );
}

export function packageHashSync(jsonData: Record<string, string>) {
    const sortedArr = sortJsonToArr(jsonData);
    const manifestData = _.filter(sortedArr, (v) => {
        return !isPackageHashIgnored(v.path);
    }).map((v) => {
        return `${v.path}:${v.hash}`;
    });
    let manifestString = JSON.stringify(manifestData.sort());
    manifestString = _.replace(manifestString, /\\\//g, '/');
    logger.debug('packageHashSync manifestString', {
        manifestString,
    });
    return stringSha256Sync(manifestString);
}

function sha256AllFiles(files: string[]): Promise<Record<string, string>> {
    return new Promise((resolve) => {
        const results: Record<string, string> = {};
        const { length } = files;
        let count = 0;
        files.forEach((file) => {
            fileSha256(file).then((hash) => {
                results[file] = hash;
                count += 1;
                if (count === length) {
                    resolve(results);
                }
            });
        });
    });
}

export function uploadPackageType(directoryPath: string) {
    return new Promise<number>((resolve, reject) => {
        recursive(directoryPath, (err, files) => {
            if (err) {
                logger.error(new AppError(err.message));
                reject(new AppError(err.message));
            } else if (files.length === 0) {
                logger.debug(`uploadPackageType empty files`);
                reject(new AppError('empty files'));
            } else {
                const aregex = /android\.bundle/;
                const aregexIOS = /main\.jsbundle/;
                let packageType = 0;
                _.forIn(files, (value: string) => {
                    if (aregex.test(value)) {
                        packageType = ANDROID;
                        return false;
                    }
                    if (aregexIOS.test(value)) {
                        packageType = IOS;
                        return false;
                    }

                    return undefined;
                });
                logger.debug(`uploadPackageType packageType: ${packageType}`);
                resolve(packageType);
            }
        });
    });
}

export function calcAllFileSha256(directoryPath: string): Promise<Record<string, string>> {
    return new Promise((resolve, reject) => {
        recursive(directoryPath, (error, files) => {
            if (error) {
                logger.error(error);
                reject(new AppError(error.message));
            } else {
                // filter files that should be ignored
                // eslint-disable-next-line no-param-reassign
                files = files.filter((file) => {
                    const relative = path.relative(directoryPath, file);
                    return !isHashIgnored(relative);
                });

                if (files.length === 0) {
                    logger.debug(`calcAllFileSha256 empty files in directory`, { directoryPath });
                    reject(new AppError('empty files'));
                } else {
                    sha256AllFiles(files).then((results) => {
                        const data: Record<string, string> = {};
                        _.forIn(results, (value, key) => {
                            let relativePath = path.relative(directoryPath, key);
                            relativePath = slash(relativePath);
                            data[relativePath] = value;
                        });
                        logger.debug(`calcAllFileSha256 files:`, data);
                        resolve(data);
                    });
                }
            }
        });
    });
}
