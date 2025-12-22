/* eslint-disable max-lines */
import { EventEmitter } from 'events';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { Request } from 'express';
import formidable from 'formidable';
import { Logger } from 'kv-logger';
import _ from 'lodash';
import { Op } from 'sequelize';
import slash from 'slash';
import yazl from 'yazl';
import { Apps } from '../../models/apps';
import { Deployments, generateDeploymentsLabelId } from '../../models/deployments';
import { DeploymentsHistory } from '../../models/deployments_history';
import { DeploymentsVersions } from '../../models/deployments_versions';
import { Packages, PackagesInterface } from '../../models/packages';
import { PackagesDiff } from '../../models/packages_diff';
import { PackagesMetrics } from '../../models/packages_metrics';
import { AppError } from '../app-error';
import {
    DIFF_MANIFEST_FILE_NAME,
    IS_DISABLED_NO,
    IS_DISABLED_YES,
    IS_MANDATORY_NO,
    IS_MANDATORY_YES,
    RELEASE_METHOD_PROMOTE,
    RELEASE_METHOD_UPLOAD,
} from '../const';
import {
    getBlobDownloadUrl,
    createFileFromRequest,
    unzipFile,
    copySync,
    diffCollectionsSync,
    createEmptyFolder,
    createEmptyFolderSync,
    deleteFolderSync,
    validatorVersion,
} from '../utils/common';
import { sequelize } from '../utils/connections';
import { qetag } from '../utils/qetag';
import { randToken, uploadPackageType } from '../utils/security';
import { uploadFileToStorage } from '../utils/storage';
import { dataCenterManager } from './datacenter-manager';

class PackageManager {
    getMetricsbyPackageId(packageId) {
        return PackagesMetrics.findOne({ where: { package_id: packageId } });
    }

    findPackageInfoByDeploymentIdAndLabel(deploymentId, label) {
        return Packages.findOne({ where: { deployment_id: deploymentId, label } });
    }

    findLatestPackageInfoByDeployVersion(deploymentsVersionsId, logger: Logger) {
        return DeploymentsVersions.findByPk(deploymentsVersionsId).then((deploymentsVersions) => {
            if (!deploymentsVersions || deploymentsVersions.current_package_id < 0) {
                const e = new AppError('not found last packages');
                logger.debug(e);
                throw e;
            }
            return Packages.findByPk(deploymentsVersions.current_package_id);
        });
    }

    parseReqFile(req: Request, logger: Logger) {
        logger.debug('parseReqFile');
        return new Promise<{ packageInfo: string; package: formidable.File }>((resolve, reject) => {
            const form = formidable();
            form.parse(req, (err, fields, files) => {
                if (err) {
                    reject(new AppError('upload error'));
                    return;
                }

                if (_.isEmpty(fields.packageInfo) || _.isEmpty(_.get(files, 'package'))) {
                    logger.debug('parseReqFile upload info lack');
                    reject(new AppError('upload info lack'));
                    return;
                }

                logger.debug('parseReqFile is ok');
                resolve({
                    packageInfo: JSON.parse(fields.packageInfo as string),
                    package: files.package as formidable.File,
                });
            });
        });
    }

    createDeploymentsVersionIfNotExist(
        deploymentId,
        appVersion,
        minVersion,
        maxVersion,
        t,
        logger: Logger,
    ) {
        return DeploymentsVersions.findOrCreate({
            where: {
                deployment_id: deploymentId,
                app_version: appVersion,
                min_version: minVersion,
                max_version: maxVersion,
            },
            defaults: { current_package_id: 0 },
            transaction: t,
        }).then(([data, created]) => {
            if (created) {
                logger.debug(
                    `createDeploymentsVersionIfNotExist findOrCreate version ${appVersion}`,
                );
            }
            logger.debug(`createDeploymentsVersionIfNotExist version data:`, data.get());
            return data;
        });
    }

    isMatchPackageHash(packageId, packageHash, logger: Logger) {
        if (_.lt(packageId, 0)) {
            logger.debug(`isMatchPackageHash packageId is 0`);
            return Promise.resolve(false);
        }
        return Packages.findByPk(packageId).then((data) => {
            if (data && _.eq(data.get('package_hash'), packageHash)) {
                logger.debug(`isMatchPackageHash data:`, data.get());
                logger.debug(`isMatchPackageHash packageHash exist`);
                return true;
            }
            logger.debug(`isMatchPackageHash package is null`);
            return false;
        });
    }

    createPackage(
        deploymentId,
        appVersion,
        packageHash,
        manifestHash: string,
        blobHash: string,
        params,
        logger: Logger,
    ) {
        const releaseMethod = params.releaseMethod || RELEASE_METHOD_UPLOAD;
        const releaseUid = params.releaseUid || 0;
        const isMandatory = params.isMandatory || 0;
        const size = params.size || 0;
        const rollout = params.rollout || 100;
        const description = params.description || '';
        const originalLabel = params.originalLabel || '';
        const isDisabled = params.isDisabled || 0;
        const originalDeployment = params.originalDeployment || '';
        return generateDeploymentsLabelId(deploymentId).then((labelId) => {
            return sequelize.transaction((t) => {
                return this.createDeploymentsVersionIfNotExist(
                    deploymentId,
                    appVersion,
                    params.min_version,
                    params.max_version,
                    t,
                    logger,
                ).then((deploymentsVersions) => {
                    return Packages.create(
                        {
                            deployment_version_id: deploymentsVersions.id,
                            deployment_id: deploymentId,
                            description,
                            package_hash: packageHash,
                            blob_url: blobHash,
                            size,
                            manifest_blob_url: manifestHash,
                            release_method: releaseMethod,
                            label: `v${labelId}`,
                            released_by: releaseUid,
                            is_mandatory: isMandatory,
                            is_disabled: isDisabled,
                            rollout,
                            original_label: originalLabel,
                            original_deployment: originalDeployment,
                        },
                        { transaction: t },
                    ).then((packages) => {
                        deploymentsVersions.set('current_package_id', packages.id);
                        return Promise.all([
                            deploymentsVersions.save({ transaction: t }),
                            Deployments.update(
                                { last_deployment_version_id: deploymentsVersions.id },
                                { where: { id: deploymentId }, transaction: t },
                            ),
                            PackagesMetrics.create({ package_id: packages.id }, { transaction: t }),
                            DeploymentsHistory.create(
                                { deployment_id: deploymentId, package_id: packages.id },
                                { transaction: t },
                            ),
                        ]).then(() => packages);
                    });
                });
            });
        });
    }

    downloadPackageAndExtract(workDirectoryPath, packageHash, blobHash, logger: Logger) {
        return dataCenterManager.validateStore(packageHash, logger).then((isValidate) => {
            if (isValidate) {
                return dataCenterManager.getPackageInfo(packageHash);
            }
            const downloadURL = getBlobDownloadUrl(blobHash);
            return createFileFromRequest(
                downloadURL,
                path.join(workDirectoryPath, blobHash),
                logger,
            ).then(() => {
                return unzipFile(
                    path.join(workDirectoryPath, blobHash),
                    path.join(workDirectoryPath, 'current'),
                    logger,
                ).then((outputPath) => {
                    return dataCenterManager.storePackage(outputPath, true, logger);
                });
            });
        });
    }

    zipDiffPackage(fileName, files, baseDirectoryPath, hotCodePushFile) {
        return new Promise<{ isTemporary: boolean; path: string }>((resolve, reject) => {
            const zipFile = new yazl.ZipFile();
            const writeStream = fs.createWriteStream(fileName);
            writeStream.on('error', (error) => {
                reject(error);
            });
            (zipFile as unknown as EventEmitter).on('error', (error) => {
                reject(error);
            });
            zipFile.outputStream
                .pipe(writeStream)
                .on('error', (error) => {
                    reject(error);
                })
                .on('close', () => {
                    resolve({ isTemporary: true, path: fileName });
                });
            for (let i = 0; i < files.length; i += 1) {
                const file = files[i];
                zipFile.addFile(path.join(baseDirectoryPath, file), slash(file));
            }
            zipFile.addFile(hotCodePushFile, DIFF_MANIFEST_FILE_NAME);
            zipFile.end();
        });
    }

    generateOneDiffPackage(
        workDirectoryPath,
        packageId,
        originDataCenter,
        oldPackageDataCenter,
        diffPackageHash,
        diffManifestBlobHash,
        logger: Logger,
    ) {
        return PackagesDiff.findOne({
            where: {
                package_id: packageId,
                diff_against_package_hash: diffPackageHash,
            },
        }).then((diffPackage) => {
            if (!_.isEmpty(diffPackage)) {
                return undefined;
            }
            logger.debug('generateOneDiffPackage', {
                packageId,
                originDataCenter,
                oldPackageDataCenter,
            });
            const downloadURL = getBlobDownloadUrl(diffManifestBlobHash);
            return createFileFromRequest(
                downloadURL,
                path.join(workDirectoryPath, diffManifestBlobHash),
                logger,
            ).then(() => {
                const dataCenterContentPath = path.join(workDirectoryPath, 'dataCenter');
                copySync(originDataCenter.contentPath, dataCenterContentPath);
                // const oldPackageDataCenterContentPath = oldPackageDataCenter.contentPath;
                const originManifestJson = JSON.parse(
                    fs.readFileSync(originDataCenter.manifestFilePath, 'utf8'),
                );
                const diffManifestJson = JSON.parse(
                    fs.readFileSync(path.join(workDirectoryPath, diffManifestBlobHash), 'utf8'),
                );
                const json = diffCollectionsSync(originManifestJson, diffManifestJson);
                const files = _.concat(json.diff, json.collection1Only);
                const hotcodepush = { deletedFiles: json.collection2Only, patchedFiles: [] };
                const hotCodePushFile = path.join(
                    workDirectoryPath,
                    `${diffManifestBlobHash}_hotcodepush`,
                );
                fs.writeFileSync(hotCodePushFile, JSON.stringify(hotcodepush));
                const fileName = path.join(workDirectoryPath, `${diffManifestBlobHash}.zip`);
                return this.zipDiffPackage(
                    fileName,
                    files,
                    dataCenterContentPath,
                    hotCodePushFile,
                ).then((data) => {
                    return qetag(data.path, logger).then((diffHash) => {
                        return uploadFileToStorage(diffHash, fileName, logger).then(() => {
                            const stats = fs.statSync(fileName);
                            return PackagesDiff.create({
                                package_id: packageId,
                                diff_against_package_hash: diffPackageHash,
                                diff_blob_url: diffHash,
                                diff_size: stats.size,
                            });
                        });
                    });
                });
            });
        });
    }

    createDiffPackagesByLastNums(
        appId,
        originalPackage: PackagesInterface,
        num: number,
        logger: Logger,
    ) {
        const packageId = originalPackage.id;
        return Promise.all([
            Packages.findAll({
                where: {
                    deployment_version_id: originalPackage.deployment_version_id,
                    id: { [Op.lt]: packageId },
                },
                order: [['id', 'desc']],
                limit: num,
            }),
            Packages.findAll({
                where: {
                    deployment_version_id: originalPackage.deployment_version_id,
                    id: { [Op.lt]: packageId },
                },
                order: [['id', 'asc']],
                limit: 2,
            }),
        ])
            .then(([lastNumsPackages, basePackages]) => {
                return _.uniqBy(_.unionBy(lastNumsPackages, basePackages, 'id'), 'package_hash');
            })
            .then((lastNumsPackages) => {
                return this.createDiffPackages(originalPackage, lastNumsPackages, logger);
            });
    }

    createDiffPackages(originalPackage, destPackages, logger: Logger) {
        if (!_.isArray(destPackages)) {
            return Promise.reject(new AppError('第二个参数必须是数组'));
        }
        if (destPackages.length <= 0) {
            return null;
        }
        const packageHash = _.get(originalPackage, 'package_hash');
        // const manifest_blob_url = _.get(originalPackage, 'manifest_blob_url');
        const blobUrl = _.get(originalPackage, 'blob_url');
        const workDirectoryPath = path.join(os.tmpdir(), `codepush_${randToken(32)}`);
        logger.debug('createDiffPackages using dir', { workDirectoryPath });
        return createEmptyFolder(workDirectoryPath)
            .then(() =>
                this.downloadPackageAndExtract(workDirectoryPath, packageHash, blobUrl, logger),
            )
            .then((originDataCenter) =>
                Promise.all(
                    destPackages.map((v) => {
                        const diffWorkDirectoryPath = path.join(
                            workDirectoryPath,
                            _.get(v, 'package_hash'),
                        );
                        createEmptyFolderSync(diffWorkDirectoryPath);
                        return this.downloadPackageAndExtract(
                            diffWorkDirectoryPath,
                            _.get(v, 'package_hash'),
                            _.get(v, 'blob_url'),
                            logger,
                        ).then((oldPackageDataCenter) =>
                            this.generateOneDiffPackage(
                                diffWorkDirectoryPath,
                                originalPackage.id,
                                originDataCenter,
                                oldPackageDataCenter,
                                v.package_hash,
                                v.manifest_blob_url,
                                logger,
                            ),
                        );
                    }),
                ),
            )
            .finally(() => deleteFolderSync(workDirectoryPath));
    }

    // eslint-disable-next-line max-lines-per-function
    releasePackage(
        appId,
        deploymentId,
        packageInfo,
        filePath: string,
        releaseUid: number,
        logger: Logger,
    ) {
        const { appVersion } = packageInfo;
        const versionInfo = validatorVersion(appVersion);
        if (!versionInfo[0]) {
            logger.debug(`releasePackage targetBinaryVersion ${appVersion} not support.`);
            return Promise.reject(new AppError(`targetBinaryVersion ${appVersion} not support.`));
        }
        const { description } = packageInfo; // 描述
        const { isDisabled } = packageInfo; // 是否立刻下载
        const { rollout } = packageInfo; // 灰度百分比
        const { isMandatory } = packageInfo; // 是否强制更新，无法跳过
        const tmpDir = os.tmpdir();
        const directoryPathParent = path.join(tmpDir, `codepuh_${randToken(32)}`);
        const directoryPath = path.join(directoryPathParent, 'current');
        logger.debug(`releasePackage generate an random dir path: ${directoryPath}`);
        return Promise.all([
            qetag(filePath, logger),
            createEmptyFolder(directoryPath).then(() => {
                return unzipFile(filePath, directoryPath, logger);
            }),
        ])
            .then(([blobHash]) => {
                return uploadPackageType(directoryPath).then((type) => {
                    return Apps.findByPk(appId).then((appInfo) => {
                        if (type > 0 && appInfo.os > 0 && appInfo.os !== type) {
                            const e = new AppError('it must be publish it by ios type');
                            logger.debug(e);
                            throw e;
                        } else {
                            // 不验证
                            logger.debug(`Unknown package type:`, {
                                type,
                                os: appInfo.os,
                            });
                        }
                        return blobHash;
                    });
                });
            })
            .then((blobHash) => {
                return dataCenterManager
                    .storePackage(directoryPath, false, logger)
                    .then((dataCenter) => {
                        const { packageHash } = dataCenter;
                        const manifestFile = dataCenter.manifestFilePath;
                        return DeploymentsVersions.findOne({
                            where: { deployment_id: deploymentId, app_version: appVersion },
                        })
                            .then((deploymentsVersions) => {
                                if (!deploymentsVersions) {
                                    return false;
                                }
                                return this.isMatchPackageHash(
                                    deploymentsVersions.get('current_package_id'),
                                    packageHash,
                                    logger,
                                );
                            })
                            .then((isExist) => {
                                if (isExist) {
                                    const e = new AppError(
                                        "The uploaded package is identical to the contents of the specified deployment's current release.",
                                    );
                                    logger.debug(e.message);
                                    throw e;
                                }
                                return qetag(manifestFile, logger);
                            })
                            .then((manifestHash) => {
                                return Promise.all([
                                    uploadFileToStorage(manifestHash, manifestFile, logger),
                                    uploadFileToStorage(blobHash, filePath, logger),
                                ]).then(() => [packageHash, manifestHash, blobHash]);
                            });
                    });
            })
            .then(([packageHash, manifestHash, blobHash]) => {
                const stats = fs.statSync(filePath);
                const params = {
                    releaseMethod: RELEASE_METHOD_UPLOAD,
                    releaseUid,
                    isMandatory: isMandatory ? IS_MANDATORY_YES : IS_MANDATORY_NO,
                    isDisabled: isDisabled ? IS_DISABLED_YES : IS_DISABLED_NO,
                    rollout,
                    size: stats.size,
                    description,
                    min_version: versionInfo[1],
                    max_version: versionInfo[2],
                };
                return this.createPackage(
                    deploymentId,
                    appVersion,
                    packageHash,
                    manifestHash,
                    blobHash,
                    params,
                    logger,
                );
            })
            .finally(() => deleteFolderSync(directoryPathParent));
    }

    modifyReleasePackage(packageId, params) {
        const appVersion = _.get(params, 'appVersion');
        const description = _.get(params, 'description');
        const isMandatory = _.get(params, 'isMandatory');
        const isDisabled = _.get(params, 'isDisabled');
        const rollout = _.get(params, 'rollout');
        return Packages.findByPk(packageId)
            .then((packageInfo) => {
                if (!packageInfo) {
                    throw new AppError(`packageInfo not found`);
                }
                if (!_.isNull(appVersion)) {
                    const versionInfo = validatorVersion(appVersion);
                    if (!versionInfo[0]) {
                        throw new AppError(`--targetBinaryVersion ${appVersion} not support.`);
                    }
                    return Promise.all([
                        DeploymentsVersions.findOne({
                            where: {
                                deployment_id: packageInfo.deployment_id,
                                app_version: appVersion,
                            },
                        }),
                        DeploymentsVersions.findByPk(packageInfo.deployment_version_id),
                    ])
                        .then(([v1, v2]) => {
                            if (v1 && !_.eq(v1.id, v2.id)) {
                                throw new AppError(`${appVersion} already exist.`);
                            }
                            if (!v2) {
                                throw new AppError(`packages not found.`);
                            }
                            return DeploymentsVersions.update(
                                {
                                    app_version: appVersion,
                                    min_version: versionInfo[1],
                                    max_version: versionInfo[2],
                                },
                                { where: { id: v2.id } },
                            );
                        })
                        .then(() => {
                            return packageInfo;
                        });
                }
                return packageInfo;
            })
            .then((packageInfo) => {
                const newParams = {
                    description: description || packageInfo.description,
                } as PackagesInterface;
                if (_.isInteger(rollout)) {
                    newParams.rollout = rollout;
                }
                if (_.isBoolean(isMandatory)) {
                    newParams.is_mandatory = isMandatory ? IS_MANDATORY_YES : IS_MANDATORY_NO;
                }
                if (_.isBoolean(isDisabled)) {
                    newParams.is_disabled = isDisabled ? IS_DISABLED_YES : IS_DISABLED_NO;
                }
                return Packages.update(newParams, { where: { id: packageId } });
            });
    }

    // eslint-disable-next-line max-lines-per-function
    promotePackage(sourceDeploymentInfo, destDeploymentInfo, params, logger: Logger) {
        const appVersion = _.get(params, 'appVersion', null);
        const label = _.get(params, 'label', null);
        return new Promise((resolve, reject) => {
            if (label) {
                Packages.findOne({
                    where: { deployment_id: sourceDeploymentInfo.id, label },
                })
                    .then((sourcePack) => {
                        if (!sourcePack) {
                            throw new AppError('label does not exist.');
                        }
                        return DeploymentsVersions.findByPk(sourcePack.deployment_version_id).then(
                            (deploymentsVersions) => {
                                if (!deploymentsVersions) {
                                    throw new AppError('deploymentsVersions does not exist.');
                                }
                                resolve([sourcePack, deploymentsVersions]);
                            },
                        );
                    })
                    .catch((e) => {
                        reject(e);
                    });
                return;
            }
            const lastDeploymentVersionId = _.get(
                sourceDeploymentInfo,
                'last_deployment_version_id',
                0,
            );
            if (_.lte(lastDeploymentVersionId, 0)) {
                throw new AppError(`does not exist last_deployment_version_id.`);
            }

            DeploymentsVersions.findByPk(lastDeploymentVersionId)
                .then((deploymentsVersions) => {
                    const sourcePackId = _.get(deploymentsVersions, 'current_package_id', 0);
                    if (_.lte(sourcePackId, 0)) {
                        throw new AppError(`packageInfo not found.`);
                    }
                    return Packages.findByPk(sourcePackId).then((sourcePack) => {
                        if (!sourcePack) {
                            throw new AppError(`packageInfo not found.`);
                        }
                        resolve([sourcePack, deploymentsVersions]);
                    });
                })
                .catch((e) => {
                    reject(e);
                });
        })
            .then(([sourcePack, deploymentsVersions]) => {
                const appFinalVersion = appVersion || deploymentsVersions.app_version;
                logger.debug('sourcePack', sourcePack);
                logger.debug('deploymentsVersions', deploymentsVersions);
                logger.debug('appFinalVersion', appFinalVersion);
                return DeploymentsVersions.findOne({
                    where: {
                        deployment_id: destDeploymentInfo.id,
                        app_version: appFinalVersion,
                    },
                })
                    .then((destDeploymentsVersions) => {
                        if (!destDeploymentsVersions) {
                            return false;
                        }
                        return this.isMatchPackageHash(
                            destDeploymentsVersions.get('current_package_id'),
                            sourcePack.package_hash,
                            logger,
                        );
                    })
                    .then((isExist) => {
                        if (isExist) {
                            throw new AppError(
                                "The uploaded package is identical to the contents of the specified deployment's current release.",
                            );
                        }
                        return [sourcePack, appFinalVersion];
                    });
            })
            .then(([sourcePack, appFinalVersion]) => {
                const versionInfo = validatorVersion(appFinalVersion);
                if (!versionInfo[0]) {
                    logger.debug(`targetBinaryVersion ${appVersion} not support.`);
                    throw new AppError(`targetBinaryVersion ${appVersion} not support.`);
                }

                const createParams = {
                    releaseMethod: RELEASE_METHOD_PROMOTE,
                    releaseUid: params.promoteUid || 0,
                    rollout: params.rollout || 100,
                    size: sourcePack.size,
                    description: params.description || sourcePack.description,
                    originalLabel: sourcePack.label,
                    originalDeployment: sourceDeploymentInfo.name,
                    min_version: versionInfo[1],
                    max_version: versionInfo[2],
                    isMandatory: 0,
                    isDisabled: 0,
                };
                if (_.isBoolean(params.isMandatory)) {
                    createParams.isMandatory = params.isMandatory
                        ? IS_MANDATORY_YES
                        : IS_MANDATORY_NO;
                } else {
                    createParams.isMandatory = sourcePack.is_mandatory;
                }
                if (_.isBoolean(params.isDisabled)) {
                    createParams.isDisabled = params.isDisabled ? IS_DISABLED_YES : IS_DISABLED_NO;
                } else {
                    createParams.isDisabled = sourcePack.is_disabled;
                }
                return this.createPackage(
                    destDeploymentInfo.id,
                    appFinalVersion,
                    sourcePack.package_hash,
                    sourcePack.manifest_blob_url,
                    sourcePack.blob_url,
                    createParams,
                    logger,
                );
            });
    }

    rollbackPackage(
        deploymentVersionId: number,
        targetLabel: string | undefined,
        rollbackUid: number,
        logger: Logger,
    ) {
        return DeploymentsVersions.findByPk(deploymentVersionId).then((deploymentsVersions) => {
            if (!deploymentsVersions) {
                throw new AppError('您之前还没有发布过版本');
            }
            return Packages.findByPk(deploymentsVersions.current_package_id)
                .then((currentPackageInfo): Promise<[PackagesInterface, PackagesInterface[]]> => {
                    if (targetLabel) {
                        return Packages.findAll({
                            where: {
                                deployment_version_id: deploymentVersionId,
                                label: targetLabel,
                            },
                            limit: 1,
                        }).then((rollbackPackageInfos) => {
                            return [currentPackageInfo, rollbackPackageInfos];
                        });
                    }
                    return this.getCanRollbackPackages(deploymentVersionId).then(
                        (rollbackPackageInfos) => {
                            return [currentPackageInfo, rollbackPackageInfos];
                        },
                    );
                })
                .then(([currentPackageInfo, rollbackPackageInfos]) => {
                    if (currentPackageInfo && rollbackPackageInfos.length > 0) {
                        for (let i = rollbackPackageInfos.length - 1; i >= 0; i -= 1) {
                            if (
                                rollbackPackageInfos[i].package_hash !==
                                currentPackageInfo.package_hash
                            ) {
                                return rollbackPackageInfos[i];
                            }
                        }
                    }
                    throw new AppError('没有可供回滚的版本');
                })
                .then((rollbackPackage) => {
                    const params = {
                        releaseMethod: 'Rollback',
                        releaseUid: rollbackUid,
                        isMandatory: rollbackPackage.is_mandatory,
                        isDisabled: rollbackPackage.is_disabled,
                        rollout: rollbackPackage.rollout,
                        size: rollbackPackage.size,
                        description: rollbackPackage.description,
                        originalLabel: rollbackPackage.label,
                        originalDeployment: '',
                        min_version: deploymentsVersions.min_version,
                        max_version: deploymentsVersions.max_version,
                    };
                    return this.createPackage(
                        deploymentsVersions.deployment_id,
                        deploymentsVersions.app_version,
                        rollbackPackage.package_hash,
                        rollbackPackage.manifest_blob_url,
                        rollbackPackage.blob_url,
                        params,
                        logger,
                    );
                });
        });
    }

    getCanRollbackPackages(deploymentVersionId) {
        return Packages.findAll({
            where: {
                deployment_version_id: deploymentVersionId,
                release_method: {
                    [Op.in]: [RELEASE_METHOD_UPLOAD, RELEASE_METHOD_PROMOTE],
                },
            },
            order: [['id', 'desc']],
            limit: 2,
        });
    }
}

export const packageManager = new PackageManager();
