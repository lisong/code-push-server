import { Logger } from 'kv-logger';
import _ from 'lodash';
import moment from 'moment';
import { Deployments } from '../../models/deployments';
import { DeploymentsHistory } from '../../models/deployments_history';
import {
    DeploymentsVersions,
    DeploymentsVersionsInterface,
} from '../../models/deployments_versions';
import { Packages, PackagesInterface } from '../../models/packages';
import { PackagesDiff } from '../../models/packages_diff';
import { PackagesMetrics } from '../../models/packages_metrics';
import { Users, UsersInterface } from '../../models/users';
import { AppError } from '../app-error';
import { getBlobDownloadUrl } from '../utils/common';
import { sequelize } from '../utils/connections';
import { randToken } from '../utils/security';

class DeploymentsManager {
    getAllPackageIdsByDeploymentsId(deploymentsId: number) {
        return Packages.findAll({ where: { deployment_id: deploymentsId } });
    }

    existDeloymentName(appId: number, name: string) {
        return Deployments.findOne({
            where: { appid: appId, name },
        }).then((data) => {
            if (!_.isEmpty(data)) {
                throw new AppError(`${name} name does Exist!`);
            } else {
                return data;
            }
        });
    }

    addDeloyment(name: string, appId: number, uid: number) {
        return Users.findByPk(uid).then((user) => {
            if (_.isEmpty(user)) {
                throw new AppError("can't find user");
            }
            return this.existDeloymentName(appId, name).then(() => {
                const { identical } = user;
                const deploymentKey = randToken(28) + identical;
                return Deployments.create({
                    appid: appId,
                    name,
                    deployment_key: deploymentKey,
                    last_deployment_version_id: 0,
                    label_id: 0,
                });
            });
        });
    }

    renameDeloymentByName(deploymentName: string, appId: number, newName: string) {
        return this.existDeloymentName(appId, newName).then(() => {
            return Deployments.update(
                { name: newName },
                { where: { name: deploymentName, appid: appId } },
            ).then(([affectedCount]) => {
                if (_.gt(affectedCount, 0)) {
                    return { name: newName };
                }
                throw new AppError(`does not find the deployment "${deploymentName}"`);
            });
        });
    }

    deleteDeloymentByName(deploymentName: string, appId: number) {
        return Deployments.destroy({
            where: { name: deploymentName, appid: appId },
        }).then((rowNum) => {
            if (_.gt(rowNum, 0)) {
                return { name: `${deploymentName}` };
            }
            throw new AppError(`does not find the deployment "${deploymentName}"`);
        });
    }

    findDeloymentByName(deploymentName: string, appId: number, logger: Logger) {
        logger.debug('findDeloymentByName', {
            name: deploymentName,
            appId,
        });
        return Deployments.findOne({
            where: { name: deploymentName, appid: appId },
        });
    }

    findPackagesAndOtherInfos(packageId: number) {
        return Packages.findOne({
            where: { id: packageId },
        })
            .then((packageInfo) => {
                if (!packageInfo) {
                    return null;
                }
                return Promise.all([
                    Promise.resolve(packageInfo),
                    PackagesDiff.findAll({
                        where: { package_id: packageId },
                    }).then((diffs) => {
                        if (diffs.length > 0) {
                            return _.reduce(
                                diffs,
                                (result, v) => {
                                    result[v.diff_against_package_hash] = {
                                        size: v.diff_size,
                                        url: getBlobDownloadUrl(v.diff_blob_url),
                                    };
                                    return result;
                                },
                                {} as Record<string, { size: number; url: string }>,
                            );
                        }
                        return null;
                    }),
                    Users.findOne({
                        where: { id: packageInfo.released_by },
                    }),
                    DeploymentsVersions.findByPk(packageInfo.deployment_version_id),
                ]);
            })
            .then(([packageInfo, packageDiffMap, userInfo, deploymentsVersions]) => {
                return {
                    packageInfo,
                    packageDiffMap,
                    userInfo,
                    deploymentsVersions,
                };
            });
    }

    findDeloymentsPackages(deploymentsVersionsId) {
        return DeploymentsVersions.findOne({
            where: { id: deploymentsVersionsId },
        }).then((deploymentsVersionsInfo) => {
            if (deploymentsVersionsInfo) {
                return this.findPackagesAndOtherInfos(deploymentsVersionsInfo.current_package_id);
            }
            return null;
        });
    }

    formatPackage(packageVersion: {
        packageInfo: PackagesInterface;
        userInfo: UsersInterface;
        packageDiffMap: Record<string, { size: number; url: string }>;
        deploymentsVersions: DeploymentsVersionsInterface;
    }) {
        if (!packageVersion) {
            return null;
        }
        return {
            description: packageVersion.packageInfo.description,
            isDisabled: false,
            isMandatory: packageVersion.packageInfo.is_mandatory === 1,
            rollout: 100,
            appVersion: packageVersion.deploymentsVersions.app_version,
            packageHash: packageVersion.packageInfo.package_hash,
            blobUrl: getBlobDownloadUrl(packageVersion.packageInfo.blob_url),
            size: packageVersion.packageInfo.size,
            manifestBlobUrl: getBlobDownloadUrl(packageVersion.packageInfo.manifest_blob_url),
            diffPackageMap: packageVersion.packageDiffMap,
            releaseMethod: packageVersion.packageInfo.release_method,
            uploadTime: moment(packageVersion.packageInfo.updated_at).valueOf(),
            originalLabel: packageVersion.packageInfo.original_label,
            originalDeployment: packageVersion.packageInfo.original_deployment,
            label: packageVersion.packageInfo.label,
            releasedBy: packageVersion.userInfo.email,
        };
    }

    listDeloyments(appId: number) {
        return Deployments.findAll({ where: { appid: appId } }).then((deploymentsInfos) => {
            if (_.isEmpty(deploymentsInfos)) {
                return [];
            }
            return Promise.all(
                deploymentsInfos.map((v) => {
                    return this.listDeloyment(v);
                }),
            );
        });
    }

    listDeloyment(deploymentInfo) {
        return this.findDeloymentsPackages([deploymentInfo.last_deployment_version_id])
            .then(this.formatPackage)
            .then((packageInfo) => {
                return {
                    createdTime: moment(deploymentInfo.created_at).valueOf(),
                    id: `${deploymentInfo.id}`,
                    key: deploymentInfo.deployment_key,
                    name: deploymentInfo.name,
                    package: packageInfo,
                };
            });
    }

    getDeploymentHistory(deploymentId) {
        return DeploymentsHistory.findAll({
            where: { deployment_id: deploymentId },
            order: [['id', 'desc']],
            limit: 15,
        })
            .then((history) => {
                return _.map(history, (v) => {
                    return v.package_id;
                });
            })
            .then((packageIds) => {
                return Promise.all(
                    packageIds.map((v) => {
                        return this.findPackagesAndOtherInfos(v).then(this.formatPackage);
                    }),
                );
            });
    }

    deleteDeploymentHistory(deploymentId) {
        return sequelize.transaction((t) => {
            return Promise.all([
                Deployments.update(
                    { last_deployment_version_id: 0, label_id: 0 },
                    { where: { id: deploymentId }, transaction: t },
                ),
                DeploymentsHistory.findAll({
                    where: { deployment_id: deploymentId },
                    order: [['id', 'desc']],
                    limit: 1000,
                }).then((rs) => {
                    return Promise.all(
                        rs.map((v) => {
                            return v.destroy({ transaction: t });
                        }),
                    );
                }),
                DeploymentsVersions.findAll({
                    where: { deployment_id: deploymentId },
                    order: [['id', 'desc']],
                    limit: 1000,
                }).then((rs) => {
                    return Promise.all(
                        rs.map((v) => {
                            return v.destroy({ transaction: t });
                        }),
                    );
                }),
                Packages.findAll({
                    where: { deployment_id: deploymentId },
                    order: [['id', 'desc']],
                    limit: 1000,
                }).then((rs) => {
                    return Promise.all(
                        rs.map((v) => {
                            return v.destroy({ transaction: t }).then(() => {
                                return Promise.all([
                                    PackagesMetrics.destroy({
                                        where: { package_id: v.get('id') },
                                        transaction: t,
                                    }),
                                    PackagesDiff.destroy({
                                        where: { package_id: v.get('id') },
                                        transaction: t,
                                    }),
                                ]);
                            });
                        }),
                    );
                }),
            ]);
        });
    }
}

export const deploymentsManager = new DeploymentsManager();
