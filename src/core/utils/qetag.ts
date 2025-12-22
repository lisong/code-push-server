import { Buffer } from 'buffer';
import crypto from 'crypto';
import fs from 'fs';
import { Stream, Readable } from 'stream';
import { Logger } from 'kv-logger';
import { AppError } from '../app-error';

// 计算文件的eTag，参数为buffer或者readableStream或者文件路径
function getEtag(buffer: string | Stream | Buffer, callback: (etag: string) => void) {
    // 判断传入的参数是buffer还是stream还是filepath
    let mode = 'buffer';

    if (typeof buffer === 'string') {
        // eslint-disable-next-line no-param-reassign
        buffer = fs.createReadStream(buffer);
        mode = 'stream';
    } else if (buffer instanceof Stream) {
        mode = 'stream';
    }

    // sha1算法
    const sha1 = (content) => {
        const sha1Hash = crypto.createHash('sha1');
        sha1Hash.update(content);
        return sha1Hash.digest();
    };

    // 以4M为单位分割
    const blockSize = 4 * 1024 * 1024;
    const sha1String = [];
    let prefix = 0x16;
    let blockCount = 0;

    const calcEtag = () => {
        if (!sha1String.length) {
            return 'Fto5o-5ea0sNMlW_75VgGJCv2AcJ';
        }
        let sha1Buffer = Buffer.concat(sha1String, blockCount * 20);

        // 如果大于4M，则对各个块的sha1结果再次sha1
        if (blockCount > 1) {
            prefix = 0x96;
            sha1Buffer = sha1(sha1Buffer);
        }

        sha1Buffer = Buffer.concat([Buffer.from([prefix]), sha1Buffer], sha1Buffer.length + 1);

        return sha1Buffer.toString('base64').replace(/\//g, '_').replace(/\+/g, '-');
    };

    switch (mode) {
        case 'buffer': {
            const buf = buffer as Buffer;
            const bufferSize = buf.length;
            blockCount = Math.ceil(bufferSize / blockSize);

            for (let i = 0; i < blockCount; i += 1) {
                sha1String.push(sha1(buf.slice(i * blockSize, (i + 1) * blockSize)));
            }
            process.nextTick(() => {
                callback(calcEtag());
            });
            break;
        }
        case 'stream': {
            const stream = buffer as Readable;
            stream.on('readable', () => {
                for (;;) {
                    const chunk = stream.read(blockSize);
                    if (!chunk) {
                        break;
                    }
                    sha1String.push(sha1(chunk));
                    blockCount += 1;
                }
            });
            stream.on('end', () => {
                callback(calcEtag());
            });

            break;
        }
        default:
            // cannot be here
            break;
    }
}

// TODO: support only files (string)?
export function qetag(buffer: string | Stream | Buffer, logger: Logger): Promise<string> {
    if (typeof buffer === 'string') {
        // it's a file
        try {
            logger.debug(`Check upload file ${buffer} fs.R_OK`);
            fs.accessSync(buffer, fs.constants.R_OK);
            logger.debug(`Check upload file ${buffer} fs.R_OK pass`);
        } catch (e) {
            logger.error(e);
            return Promise.reject(new AppError(e.message));
        }
    }
    logger.debug(`generate file identical`);
    return new Promise((resolve) => {
        getEtag(buffer, (data) => {
            if (typeof buffer === 'string') {
                logger.debug('identical:', {
                    file: buffer,
                    etag: data,
                });
            }
            resolve(data);
        });
    });
}
