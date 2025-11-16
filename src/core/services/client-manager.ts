import { Logger } from 'kv-logger';
import _ from 'lodash';
import { Op } from 'sequelize';
import { Deployments } from '../../models/deployments';
import { DeploymentsVersions } from '../../models/deployments_versions';
import { LogReportDeploy } from '../../models/log_report_deploy';
import { LogReportDownload } from '../../models/log_report_download';
import { Packages } from '../../models/packages';
import { PackagesDiff } from '../../models/packages_diff';
import { PackagesMetrics } from '../../models/packages_metrics';
import { AppError } from '../app-error';
import { config } from '../config';
import { DEPLOYMENT_FAILED, DEPLOYMENT_SUCCEEDED } from '../const';
import { parseVersion, getBlobDownloadUrl } from '../utils/common';
import { redisClient } from '../utils/connections';

const UPDATE_CHECK = 'UPDATE_CHECK';
const CHOSEN_MAN = 'CHOSEN_MAN';
const EXPIRED = 600;

interface UpdateCheckInfo {
    packageId: number;
    downloadURL: string;
    downloadUrl: string;
    description: string;
    isAvailable: boolean;
    isDisabled: boolean;
    isMandatory: boolean;
    appVersion: string;
    targetBinaryRange: string;
    packageHash: string;
    label: string;
    packageSize: number;
    updateAppVersion: boolean;
    shouldRunBinaryVersion: boolean;
    rollout: number;
}

class ClientManager {
    private getUpdateCheckCacheKey(deploymentKey, appVersion, label, packageHash) {
        return [UPDATE_CHECK, deploymentKey, appVersion, label, packageHash].join(':');
    }

    clearUpdateCheckCache(deploymentKey, appVersion, label, packageHash, logger: Logger) {
        logger.info('clear cache Deployments key', {
            key: deploymentKey,
        });
        const redisCacheKey = this.getUpdateCheckCacheKey(
            deploymentKey,
            appVersion,
            label,
            packageHash,
        );
        return redisClient.keys(redisCacheKey).then((data) => {
            if (_.isArray(data)) {
                return Promise.all(
                    data.map((key) => {
                        return redisClient.del(key);
                    }),
                );
            }
            return null;
        });
    }

    updateCheckFromCache(
        deploymentKey: string,
        appVersion: string,
        label: string,
        packageHash: string,
        clientUniqueId: string,
        logger: Logger,
    ) {
        if (!config.common.updateCheckCache) {
            return this.updateCheck(
                deploymentKey,
                appVersion,
                label,
                packageHash,
                clientUniqueId,
                logger,
            );
        }
        const redisCacheKey = this.getUpdateCheckCacheKey(
            deploymentKey,
            appVersion,
            label,
            packageHash,
        );
        return redisClient.get(redisCacheKey).then((data) => {
            if (data) {
                try {
                    logger.debug('updateCheckFromCache read from cache');
                    const obj = JSON.parse(data) as UpdateCheckInfo;
                    return obj;
                } catch (e) {
                    // do nothing
                }
            }
            return this.updateCheck(
                deploymentKey,
                appVersion,
                label,
                packageHash,
                clientUniqueId,
                logger,
            ).then((rs) => {
                try {
                    logger.debug('updateCheckFromCache read from db');
                    const strRs = JSON.stringify(rs);
                    redisClient.setEx(redisCacheKey, EXPIRED, strRs);
                } catch (e) {
                    // do nothing
                }
                return rs;
            });
        });
    }

    private getChosenManCacheKey(packageId, rollout, clientUniqueId) {
        return [CHOSEN_MAN, packageId, rollout, clientUniqueId].join(':');
    }

    private random(rollout) {
        const r = Math.ceil(Math.random() * 10000);
        if (r < rollout * 100) {
            return Promise.resolve(true);
        }
        return Promise.resolve(false);
    }

    chosenMan(packageId, rollout, clientUniqueId: string) {
        if (rollout >= 100) {
            return Promise.resolve(true);
        }
        const rolloutClientUniqueIdCache = _.get(
            config,
            'common.rolloutClientUniqueIdCache',
            false,
        );
        if (rolloutClientUniqueIdCache === false) {
            return this.random(rollout);
        }
        const redisCacheKey = this.getChosenManCacheKey(packageId, rollout, clientUniqueId);
        return redisClient.get(redisCacheKey).then((data) => {
            if (data === '1') {
                return true;
            }
            if (data === '2') {
                return false;
            }
            return this.random(rollout).then((r) => {
                return redisClient
                    .setEx(redisCacheKey, 60 * 60 * 24 * 7, r ? '1' : '2')
                    .then(() => {
                        return r;
                    });
            });
        });
    }

    // eslint-disable-next-line max-lines-per-function
    private updateCheck(
        deploymentKey: string,
        appVersion: string,
        label: string,
        packageHash: string,
        clientUniqueId: string,
        logger: Logger,
    ) {
        const rs: UpdateCheckInfo = {
            packageId: 0,
            downloadURL: '',
            downloadUrl: '',
            description: '',
            isAvailable: false,
            isDisabled: true,
            isMandatory: false,
            appVersion,
            targetBinaryRange: '',
            packageHash: '',
            label: '',
            packageSize: 0,
            updateAppVersion: false,
            shouldRunBinaryVersion: false,
            rollout: 100,
        };
        if (_.isEmpty(deploymentKey) || _.isEmpty(appVersion)) {
            return Promise.reject(new AppError('please input deploymentKey and appVersion'));
        }
        return Deployments.findOne({ where: { deployment_key: deploymentKey } })
            .then((dep) => {
                if (_.isEmpty(dep)) {
                    throw new AppError('Not found deployment, check deployment key is right.');
                }
                const version = parseVersion(appVersion);
                logger.info('updateCheck.parsedVersion', { version });
                return DeploymentsVersions.findAll({
                    where: {
                        deployment_id: dep.id,
                        min_version: { [Op.lte]: version },
                        max_version: { [Op.gt]: version },
                    },
                }).then((deploymentsVersionsMore) => {
                    const item = _.last(_.sortBy(deploymentsVersionsMore, 'created_at'));
                    logger.debug({
                        item,
                    });
                    return item;
                });
            })
            .then((deploymentsVersions) => {
                const packageId = _.get(deploymentsVersions, 'current_package_id', 0);
                if (_.eq(packageId, 0)) {
                    return undefined;
                }
                return Packages.findByPk(packageId)
                    .then((packages) => {
                        if (
                            packages &&
                            _.eq(packages.deployment_id, deploymentsVersions.deployment_id) &&
                            !_.eq(packages.package_hash, packageHash)
                        ) {
                            rs.packageId = packageId;
                            rs.targetBinaryRange = deploymentsVersions.app_version;
                            rs.downloadURL = getBlobDownloadUrl(packages.blob_url);
                            rs.downloadUrl = rs.downloadURL;
                            rs.description = _.get(packages, 'description', '');
                            rs.isAvailable = !_.eq(packages.is_disabled, 1);
                            rs.isDisabled = !!_.eq(packages.is_disabled, 1);
                            rs.isMandatory = !!_.eq(packages.is_mandatory, 1);
                            rs.appVersion = appVersion;
                            rs.packageHash = _.get(packages, 'package_hash', '');
                            rs.label = _.get(packages, 'label', '');
                            rs.packageSize = _.get(packages, 'size', 0);
                            rs.rollout = _.get(packages, 'rollout', 100);
                        }
                        return packages;
                    })
                    .then((packages) => {
                        // 尝试增量更新
                        if (
                            packageHash &&
                            !_.isEmpty(packages) &&
                            !_.eq(_.get(packages, 'package_hash', ''), packageHash)
                        ) {
                            return PackagesDiff.findOne({
                                where: {
                                    package_id: packages.id,
                                    diff_against_package_hash: packageHash,
                                },
                            }).then((diffPackage) => {
                                if (!_.isEmpty(diffPackage)) {
                                    rs.downloadURL = getBlobDownloadUrl(
                                        _.get(diffPackage, 'diff_blob_url'),
                                    );
                                    rs.downloadUrl = getBlobDownloadUrl(
                                        _.get(diffPackage, 'diff_blob_url'),
                                    );
                                    rs.packageSize = _.get(diffPackage, 'diff_size', 0);
                                }
                            });
                        }
                        return undefined;
                    });
            })
            .then(() => {
                return rs;
            });
    }

    private getPackagesInfo(deploymentKey, label) {
        if (_.isEmpty(deploymentKey) || _.isEmpty(label)) {
            return Promise.reject(new AppError('please input deploymentKey and label'));
        }
        return Deployments.findOne({ where: { deployment_key: deploymentKey } })
            .then((dep) => {
                if (_.isEmpty(dep)) {
                    throw new AppError('does not found deployment');
                }
                return Packages.findOne({ where: { deployment_id: dep.id, label } });
            })
            .then((packages) => {
                if (_.isEmpty(packages)) {
                    throw new AppError('does not found packages');
                }
                return packages;
            });
    }

    reportStatusDownload(deploymentKey, label, clientUniqueId) {
        return this.getPackagesInfo(deploymentKey, label).then((packages) => {
            return Promise.all([
                PackagesMetrics.findOne({ where: { package_id: packages.id } }).then((metrics) => {
                    if (metrics) {
                        return metrics.increment('downloaded');
                    }
                    return undefined;
                }),
                LogReportDownload.create({
                    package_id: packages.id,
                    client_unique_id: clientUniqueId,
                }),
            ]);
        });
    }

    reportStatusDeploy(deploymentKey: string, label, clientUniqueId: string, others) {
        return this.getPackagesInfo(deploymentKey, label).then((packages) => {
            const statusText = _.get(others, 'status');
            let status = 0;
            if (_.eq(statusText, 'DeploymentSucceeded')) {
                status = DEPLOYMENT_SUCCEEDED;
            } else if (_.eq(statusText, 'DeploymentFailed')) {
                status = DEPLOYMENT_FAILED;
            }
            const packageId = packages.id;
            const previousDeploymentKey = _.get(others, 'previousDeploymentKey');
            const previousLabel = _.get(others, 'previousLabelOrAppVersion');
            if (status > 0) {
                return Promise.all([
                    LogReportDeploy.create({
                        package_id: packageId,
                        client_unique_id: clientUniqueId,
                        previous_label: previousLabel,
                        previous_deployment_key: previousDeploymentKey,
                        status,
                    }),
                    PackagesMetrics.findOne({ where: { package_id: packageId } }).then(
                        (metrics) => {
                            if (_.isEmpty(metrics)) {
                                return undefined;
                            }
                            if (_.eq(status, DEPLOYMENT_SUCCEEDED)) {
                                return metrics.increment(['installed', 'active'], { by: 1 });
                            }
                            return metrics.increment(['installed', 'failed'], { by: 1 });
                        },
                    ),
                ]).then(() => {
                    if (previousDeploymentKey && previousLabel) {
                        return Deployments.findOne({
                            where: { deployment_key: previousDeploymentKey },
                        })
                            .then((dep) => {
                                if (_.isEmpty(dep)) {
                                    return undefined;
                                }
                                return Packages.findOne({
                                    where: { deployment_id: dep.id, label: previousLabel },
                                }).then((p) => {
                                    if (_.isEmpty(p)) {
                                        return undefined;
                                    }
                                    return PackagesMetrics.findOne({
                                        where: { package_id: p.id },
                                    });
                                });
                            })
                            .then((metrics) => {
                                if (metrics) {
                                    return metrics.decrement('active');
                                }
                                return undefined;
                            });
                    }
                    return undefined;
                });
            }
            return undefined;
        });
    }
}

export const clientManager = new ClientManager();
