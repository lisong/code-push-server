import fs from 'fs';
import path from 'path';
import { Logger } from 'kv-logger';
import _ from 'lodash';
import { AppError } from '../app-error';
import { config } from '../config';
import { createEmptyFolder, copy } from '../utils/common';
import { calcAllFileSha256, packageHashSync } from '../utils/security';

const MANIFEST_FILENAME = 'manifest.json';
const CONTENTS_NAME = 'contents';

class DataCenterManager {
    getDataDir() {
        return config.common.dataDir;
    }

    hasPackageStoreSync(packageHash: string) {
        const dataDir = this.getDataDir();
        const packageHashPath = path.join(dataDir, packageHash);
        const manifestFile = path.join(packageHashPath, MANIFEST_FILENAME);
        const contentPath = path.join(packageHashPath, CONTENTS_NAME);
        return fs.existsSync(manifestFile) && fs.existsSync(contentPath);
    }

    getPackageInfo(packageHash: string) {
        if (this.hasPackageStoreSync(packageHash)) {
            const dataDir = this.getDataDir();
            const packageHashPath = path.join(dataDir, packageHash);
            const manifestFile = path.join(packageHashPath, MANIFEST_FILENAME);
            const contentPath = path.join(packageHashPath, CONTENTS_NAME);
            return this.buildPackageInfo(packageHash, packageHashPath, contentPath, manifestFile);
        }
        throw new AppError("can't get PackageInfo");
    }

    buildPackageInfo(
        packageHash: string,
        packageHashPath: string,
        contentPath: string,
        manifestFile: string,
    ) {
        return {
            packageHash,
            path: packageHashPath,
            contentPath,
            manifestFilePath: manifestFile,
        };
    }

    validateStore(providePackageHash: string, logger: Logger) {
        const dataDir = this.getDataDir();
        const packageHashPath = path.join(dataDir, providePackageHash);
        const manifestFile = path.join(packageHashPath, MANIFEST_FILENAME);
        const contentPath = path.join(packageHashPath, CONTENTS_NAME);
        if (!this.hasPackageStoreSync(providePackageHash)) {
            logger.debug(`validateStore providePackageHash not exist`);
            return Promise.resolve(false);
        }
        return calcAllFileSha256(contentPath).then((manifestJson) => {
            const packageHash = packageHashSync(manifestJson);
            let manifestJsonLocal;
            try {
                manifestJsonLocal = JSON.parse(fs.readFileSync(manifestFile, { encoding: 'utf8' }));
            } catch (e) {
                logger.debug(`validateStore manifestFile contents invilad`);
                return false;
            }
            const packageHashLocal = packageHashSync(manifestJsonLocal);
            if (
                _.eq(providePackageHash, packageHash) &&
                _.eq(providePackageHash, packageHashLocal)
            ) {
                logger.debug(`validateStore store files is ok`);
                return true;
            }
            logger.debug(`validateStore store files broken`);
            return false;
        });
    }

    storePackage(sourceDst: string, force: boolean, logger: Logger) {
        return calcAllFileSha256(sourceDst).then((manifestJson) => {
            const packageHash = packageHashSync(manifestJson);
            const dataDir = this.getDataDir();
            const packageHashPath = path.join(dataDir, packageHash);
            const manifestFile = path.join(packageHashPath, MANIFEST_FILENAME);
            const contentPath = path.join(packageHashPath, CONTENTS_NAME);
            return this.validateStore(packageHash, logger).then((isValidate) => {
                if (!force && isValidate) {
                    return this.buildPackageInfo(
                        packageHash,
                        packageHashPath,
                        contentPath,
                        manifestFile,
                    );
                }
                return createEmptyFolder(packageHashPath).then(() => {
                    return copy(sourceDst, contentPath).then(() => {
                        const manifestString = JSON.stringify(manifestJson);
                        fs.writeFileSync(manifestFile, manifestString);
                        return this.buildPackageInfo(
                            packageHash,
                            packageHashPath,
                            contentPath,
                            manifestFile,
                        );
                    });
                });
            });
        });
    }
}

export const dataCenterManager = new DataCenterManager();
