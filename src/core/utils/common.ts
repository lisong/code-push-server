/* eslint-disable no-cond-assign */
import fs from 'fs';
import { pipeline } from 'stream';
import util from 'util';
import extract from 'extract-zip';
import fsextra from 'fs-extra';
import { Logger } from 'kv-logger';
import _ from 'lodash';
import fetch from 'node-fetch';
import validator from 'validator';
import { AppError } from '../app-error';
import { config } from '../config';

const streamPipeline = util.promisify(pipeline);

/** WEB_UI_ALLOW !== true 이면 UI 숨김 */
export function shouldHideWebUI(): boolean {
    const allowWebUiInProd = config.common.webUIAllow;

    return !allowWebUiInProd;
}

function cleanVersion(versionNo?: string) {
    if (typeof versionNo !== 'string') {
        return versionNo as any;
    }
    // 2.10.2-dev-01 -> 2.10.2
    return versionNo.replace(/-.*$/, '');
}

/**
 * SEMVER 스트링을 마이너와 패치 부분 자리수가 늘어난 스트링으로 변환
 * - 다수의 패치 횟수 보장을 위한 작업
 */
export function parseVersion(versionNo: string) {
    let version = '0';
    let data = null;
    const normalized = cleanVersion(versionNo);
    if ((data = normalized.match(/^([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/))) {
        // "1.2.3"
        version = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
    } else if ((data = normalized.match(/^([0-9]{1,3}).([0-9]{1,5})$/))) {
        // "1.2"
        version = data[1] + _.padStart(data[2], 5, '0') + _.padStart('0', 10, '0');
    }
    return version;
}

export function validatorVersion(versionNo: string) {
    let flag = false;
    let min = '0';
    let max = '9999999999999999999';
    let data = null;
    if (versionNo === '*') {
        // "*"
        flag = true;
    } else if ((data = versionNo.match(/^([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/))) {
        // "1.2.3"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
        max =
            data[1] +
            _.padStart(data[2], 5, '0') +
            _.padStart(`${parseInt(data[3], 10) + 1}`, 10, '0');
    } else if ((data = versionNo.match(/^([0-9]{1,3}).([0-9]{1,5})(\.\*){0,1}$/))) {
        // "1.2" "1.2.*"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart('0', 10, '0');
        max =
            data[1] + _.padStart(`${parseInt(data[2], 10) + 1}`, 5, '0') + _.padStart('0', 10, '0');
    } else if ((data = versionNo.match(/^~([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/))) {
        // "~1.2.3"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
        max =
            data[1] + _.padStart(`${parseInt(data[2], 10) + 1}`, 5, '0') + _.padStart('0', 10, '0');
    } else if ((data = versionNo.match(/^\^([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/))) {
        // "^1.2.3"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
        max =
            _.toString(parseInt(data[1], 10) + 1) +
            _.padStart('0', 5, '0') +
            _.padStart('0', 10, '0');
    } else if (
        (data = versionNo.match(
            /^([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})\s?-\s?([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/,
        ))
    ) {
        // "1.2.3 - 1.2.7"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
        max =
            data[4] +
            _.padStart(data[5], 5, '0') +
            _.padStart(`${parseInt(data[6], 10) + 1}`, 10, '0');
    } else if (
        (data = versionNo.match(
            /^>=([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})\s?<([0-9]{1,3}).([0-9]{1,5}).([0-9]{1,10})$/,
        ))
    ) {
        // ">=1.2.3 <1.2.7"
        flag = true;
        min = data[1] + _.padStart(data[2], 5, '0') + _.padStart(data[3], 10, '0');
        max = data[4] + _.padStart(data[5], 5, '0') + _.padStart(data[6], 10, '0');
    }
    return [flag, min, max];
}

export async function createFileFromRequest(url: string, filePath: string, logger: Logger) {
    try {
        await fs.promises.stat(filePath);
        return;
    } catch (err) {
        if (err.code !== 'ENOENT') {
            throw err;
        }
    }

    logger.debug(`createFileFromRequest url:${url}`);
    const response = await fetch(url);
    if (!response.ok) {
        throw new AppError(`unexpected response ${response.statusText}`);
    }
    await streamPipeline(response.body, fs.createWriteStream(filePath));
}

export function copySync(sourceDst: string, targertDst: string) {
    return fsextra.copySync(sourceDst, targertDst, { overwrite: true });
}

export function copy(sourceDst: string, targertDst: string) {
    return fsextra.copy(sourceDst, targertDst, { overwrite: true });
}

function deleteFolder(folderPath: string) {
    return fsextra.remove(folderPath);
}

export function deleteFolderSync(folderPath: string) {
    return fsextra.removeSync(folderPath);
}

export async function createEmptyFolder(folderPath: string) {
    await deleteFolder(folderPath);
    await fsextra.mkdirs(folderPath);
}

export function createEmptyFolderSync(folderPath: string) {
    deleteFolderSync(folderPath);
    fsextra.mkdirsSync(folderPath);
}

export async function unzipFile(zipFile: string, outputPath: string, logger: Logger) {
    try {
        logger.debug(`unzipFile check zipFile ${zipFile} fs.R_OK`);
        fs.accessSync(zipFile, fs.constants.R_OK);
        logger.debug(`Pass unzipFile file ${zipFile}`);
    } catch (err) {
        throw new AppError(err.message);
    }

    try {
        await extract(zipFile, { dir: outputPath });
        logger.debug(`unzipFile success`);
    } catch (err) {
        throw new AppError(`it's not a zipFile`);
    }
    return outputPath;
}

export function getBlobDownloadUrl(blobUrl: string): string {
    let fileName = blobUrl;
    const { storageType } = config.common;
    const { downloadUrl } = config[storageType];
    if (storageType === 'local') {
        fileName = `${blobUrl.substring(0, 2).toLowerCase()}/${blobUrl}`;
    }
    if (!validator.isURL(downloadUrl)) {
        throw new AppError(`Please config ${storageType}.downloadUrl in config.js`);
    }
    return `${downloadUrl}/${fileName}`;
}

export function diffCollectionsSync(
    collection1: Record<string, string>,
    collection2: Record<string, string>,
) {
    const diff: string[] = [];
    const collection1Only: string[] = [];
    const collection2Keys = new Set(Object.keys(collection2));
    if (collection1 instanceof Object) {
        const keys = Object.keys(collection1);
        for (let i = 0; i < keys.length; i += 1) {
            const key = keys[i];
            if (!collection2Keys.has(key)) {
                collection1Only.push(key);
            } else {
                collection2Keys.delete(key);
                if (!_.eq(collection1[key], collection2[key])) {
                    diff.push(key);
                }
            }
        }
    }
    return {
        diff,
        collection1Only,
        collection2Only: Array.from(collection2Keys),
    };
}
