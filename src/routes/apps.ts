/* eslint-disable max-lines */
import express from 'express';
import _ from 'lodash';
import validator from 'validator';
import { AppError } from '../core/app-error';
import { config } from '../core/config';
import {
    IOS,
    IOS_NAME,
    ANDROID,
    ANDROID_NAME,
    WINDOWS,
    WINDOWS_NAME,
    REACT_NATIVE,
    REACT_NATIVE_NAME,
    CORDOVA,
    CORDOVA_NAME,
} from '../core/const';
import { checkToken, Req } from '../core/middleware';
import { accountManager } from '../core/services/account-manager';
import { appManager } from '../core/services/app-manager';
import { clientManager } from '../core/services/client-manager';
import { collaboratorsManager } from '../core/services/collaborators-manager';
import { deploymentsManager } from '../core/services/deployments-manager';
import { packageManager } from '../core/services/package-manager';
import { deleteFolderSync } from '../core/utils/common';
import { PackagesInterface } from '../models/packages';

function delay(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

export const appsRouter = express.Router();

appsRouter.get('/', checkToken, (req: Req, res, next) => {
    const { logger } = req;
    const uid = req.users.id;
    logger.info('try list apps', { uid });
    appManager
        .listApps(uid)
        .then((data) => {
            logger.info('list apps success', { uid });
            res.send({ apps: data });
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('list apps failed', { uid, error: e.message });
                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
});

appsRouter.get('/:appName/deployments', checkToken, (req: Req<{ appName: string }>, res, next) => {
    const { logger, params } = req;
    const uid = req.users.id;
    const appName = _.trim(params.appName);
    logger.info('try list deployments', {
        uid,
        appName,
    });
    accountManager
        .collaboratorCan(uid, appName, logger)
        .then((col) => {
            return deploymentsManager.listDeloyments(col.appid);
        })
        .then((data) => {
            logger.info('list deployments success', {
                uid,
                appName,
            });
            res.send({ deployments: data });
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('list deployments failed', {
                    uid,
                    appName,
                    error: e.message,
                });
                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
});

appsRouter.get(
    '/:appName/deployments/:deploymentName',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const uid = req.users.id;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        logger.info('try to get deployment', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager.findDeloymentByName(deploymentName, col.appid, logger);
            })
            .then((deploymentInfo) => {
                if (_.isEmpty(deploymentInfo)) {
                    throw new AppError('does not find the deployment');
                }
                logger.info('get deployment success', {
                    uid,
                    appName,
                    deploymentName,
                });

                // TODO: check if this works as expected
                res.send({ deployment: deploymentsManager.listDeloyment(deploymentInfo) });
                return true;
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('get deployment failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.post(
    '/:appName/deployments',
    checkToken,
    (req: Req<{ appName: string }, { name: string }>, res, next) => {
        const { logger, params, body } = req;
        const uid = req.users.id;
        const appName = _.trim(params.appName);
        const { name } = body;
        logger.info('try to create deployment', {
            uid,
            appName,
            deploymentName: name,
        });
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager.addDeloyment(name, col.appid, uid);
            })
            .then((data) => {
                logger.info('create deployment success', {
                    uid,
                    appName,
                    deploymentName: data.name,
                });

                res.send({ deployment: { name: data.name, key: data.deployment_key } });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('create deployment failed', {
                        uid,
                        appName,
                        deploymentName: name,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.get(
    '/:appName/deployments/:deploymentName/metrics',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const uid = req.users.id;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        logger.info('try to get deployment metrics', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager
                    .findDeloymentByName(deploymentName, col.appid, logger)
                    .then((deploymentInfo) => {
                        if (_.isEmpty(deploymentInfo)) {
                            throw new AppError('does not find the deployment');
                        }
                        return deploymentInfo;
                    });
            })
            .then((deploymentInfo) => {
                return deploymentsManager.getAllPackageIdsByDeploymentsId(deploymentInfo.id);
            })
            .then((packagesInfos) => {
                return packagesInfos.reduce(
                    (prev, v) => {
                        return prev.then((result) => {
                            return packageManager
                                .getMetricsbyPackageId(v.get('id'))
                                .then((metrics) => {
                                    if (metrics) {
                                        result[v.get('label')] = {
                                            active: metrics.get('active'),
                                            downloaded: metrics.get('downloaded'),
                                            failed: metrics.get('failed'),
                                            installed: metrics.get('installed'),
                                        };
                                    }
                                    return result;
                                });
                        });
                    },
                    Promise.resolve(
                        {} as Record<
                            string,
                            {
                                active: number;
                                downloaded: number;
                                failed: number;
                                installed: number;
                            }
                        >,
                    ),
                );
            })
            .then((rs) => {
                logger.info('get deployment metrics success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send({ metrics: rs });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('get deployment metrics failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.send({ metrics: null });
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.get(
    '/:appName/deployments/:deploymentName/history',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const uid = req.users.id;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        logger.info('try to get deployment history', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager
                    .findDeloymentByName(deploymentName, col.appid, logger)
                    .then((deploymentInfo) => {
                        if (_.isEmpty(deploymentInfo)) {
                            throw new AppError('does not find the deployment');
                        }
                        return deploymentInfo;
                    });
            })
            .then((deploymentInfo) => {
                return deploymentsManager.getDeploymentHistory(deploymentInfo.id);
            })
            .then((rs) => {
                logger.info('get deployment history success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send({ history: _.pull(rs, null) });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('get deployment history failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.delete(
    '/:appName/deployments/:deploymentName/history',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const uid = req.users.id;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        logger.info('try to delete deployment history', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager
                    .findDeloymentByName(deploymentName, col.appid, logger)
                    .then((deploymentInfo) => {
                        if (_.isEmpty(deploymentInfo)) {
                            throw new AppError('does not find the deployment');
                        }
                        return deploymentInfo;
                    });
            })
            .then((deploymentInfo) => {
                return deploymentsManager.deleteDeploymentHistory(deploymentInfo.id);
            })
            .then(() => {
                logger.info('delete deployment history success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send('ok');
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('delete deployment history failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.patch(
    '/:appName/deployments/:deploymentName',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }, { name: string }>, res, next) => {
        const { logger, params, body } = req;
        const { name } = body;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        const uid = req.users.id;
        logger.info('try to update deployment', {
            uid,
            appName,
            deploymentName,
            newName: name,
        });
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager.renameDeloymentByName(deploymentName, col.appid, name);
            })
            .then((data) => {
                logger.info('update deployment success', {
                    uid,
                    appName,
                    deploymentName,
                    newName: name,
                });

                res.send({ deployment: data });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('update deployment failed', {
                        uid,
                        appName,
                        deploymentName,
                        newName: name,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.delete(
    '/:appName/deployments/:deploymentName',
    checkToken,
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        const uid = req.users.id;
        logger.info('try to delete deployment', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager.deleteDeloymentByName(deploymentName, col.appid);
            })
            .then((data) => {
                logger.info('delete deployment success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send({ deployment: data });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('delete deployment failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.post(
    '/:appName/deployments/:deploymentName/release',
    checkToken,
    // eslint-disable-next-line max-lines-per-function
    (req: Req<{ appName: string; deploymentName: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        const uid = req.users.id;
        logger.info('try to release', {
            uid,
            appName,
            deploymentName,
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                logger.info('release user check ok', {
                    uid,
                    appName,
                    deploymentName,
                });

                return deploymentsManager
                    .findDeloymentByName(deploymentName, col.appid, logger)
                    .then((deploymentInfo) => {
                        if (_.isEmpty(deploymentInfo)) {
                            throw new AppError('does not find the deployment');
                        }
                        logger.info('release deployment check ok', {
                            uid,
                            appName,
                            deploymentName,
                        });

                        return packageManager
                            .parseReqFile(req, logger)
                            .then((data) => {
                                if (data.package.mimetype !== 'application/zip') {
                                    throw new AppError(
                                        `upload file type is invalidate: ${data.package.mimetype}`,
                                    );
                                }
                                logger.info('release packagee parse ok', {
                                    uid,
                                    appName,
                                    deploymentName,
                                });

                                return packageManager
                                    .releasePackage(
                                        deploymentInfo.appid,
                                        deploymentInfo.id,
                                        data.packageInfo,
                                        data.package.filepath,
                                        uid,
                                        logger,
                                    )
                                    .finally(() => {
                                        deleteFolderSync(data.package.filepath);
                                    });
                            })
                            .then((packages) => {
                                if (packages) {
                                    delay(1000).then(() => {
                                        packageManager
                                            .createDiffPackagesByLastNums(
                                                deploymentInfo.appid,
                                                packages,
                                                config.common.diffNums,
                                                logger,
                                            )
                                            .catch((e) => {
                                                logger.error(e);
                                            });
                                    });
                                }
                                // clear cache if exists.
                                if (config.common.updateCheckCache) {
                                    delay(2500).then(() => {
                                        clientManager.clearUpdateCheckCache(
                                            deploymentInfo.deployment_key,
                                            '*',
                                            '*',
                                            '*',
                                            logger,
                                        );
                                    });
                                }
                                return null;
                            });
                    });
            })
            .then(() => {
                logger.info('release success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send('{"msg": "succeed"}');
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('release failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.patch(
    '/:appName/deployments/:deploymentName/release',
    checkToken,
    // eslint-disable-next-line max-lines-per-function
    (
        req: Req<{ appName: string; deploymentName: string }, { packageInfo: { label: string } }>,
        res,
        next,
    ) => {
        const { logger, params, body } = req;
        const appName = _.trim(params.appName);
        const deploymentName = _.trim(params.deploymentName);
        const uid = req.users.id;
        logger.info('try modify release package', {
            uid,
            appName,
            deploymentName,
            body: JSON.stringify(body),
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                return deploymentsManager
                    .findDeloymentByName(deploymentName, col.appid, logger)
                    .then((deploymentInfo) => {
                        if (_.isEmpty(deploymentInfo)) {
                            throw new AppError('does not find the deployment');
                        }
                        const label = _.get(body, 'packageInfo.label');
                        if (label) {
                            return packageManager
                                .findPackageInfoByDeploymentIdAndLabel(deploymentInfo.id, label)
                                .then((data) => {
                                    return [deploymentInfo, data] as const;
                                });
                        }
                        const deploymentVersionId = deploymentInfo.last_deployment_version_id;
                        return packageManager
                            .findLatestPackageInfoByDeployVersion(deploymentVersionId, logger)
                            .then((data) => {
                                return [deploymentInfo, data] as const;
                            });
                    })
                    .then(([deploymentInfo, packageInfo]) => {
                        if (!packageInfo) {
                            throw new AppError('does not find the packageInfo');
                        }
                        return packageManager
                            .modifyReleasePackage(packageInfo.id, body.packageInfo)
                            .then(() => {
                                // clear cache if exists.
                                if (config.common.updateCheckCache) {
                                    delay(2500).then(() => {
                                        clientManager.clearUpdateCheckCache(
                                            deploymentInfo.deployment_key,
                                            '*',
                                            '*',
                                            '*',
                                            logger,
                                        );
                                    });
                                }
                            });
                    });
            })
            .then(() => {
                logger.info('modify release package success', {
                    uid,
                    appName,
                    deploymentName,
                });

                res.send('');
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('modify release package failed', {
                        uid,
                        appName,
                        deploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.post(
    '/:appName/deployments/:sourceDeploymentName/promote/:destDeploymentName',
    checkToken,
    // eslint-disable-next-line max-lines-per-function
    (
        req: Req<
            { appName: string; sourceDeploymentName: string; destDeploymentName: string },
            { packageInfo: unknown }
        >,
        res,
        next,
    ) => {
        const { logger, params, body } = req;
        const appName = _.trim(params.appName);
        const sourceDeploymentName = _.trim(params.sourceDeploymentName);
        const destDeploymentName = _.trim(params.destDeploymentName);
        const uid = req.users.id;

        logger.info('try promote package', {
            uid,
            appName,
            sourceDeploymentName,
            destDeploymentName,
            body: JSON.stringify(body),
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                const appId = col.appid;
                return Promise.all([
                    deploymentsManager.findDeloymentByName(sourceDeploymentName, appId, logger),
                    deploymentsManager.findDeloymentByName(destDeploymentName, appId, logger),
                ])
                    .then(([sourceDeploymentInfo, destDeploymentInfo]) => {
                        if (!sourceDeploymentInfo) {
                            throw new AppError(`${sourceDeploymentName}  does not exist.`);
                        }
                        if (!destDeploymentInfo) {
                            throw new AppError(`${destDeploymentName}  does not exist.`);
                        }
                        return [sourceDeploymentInfo, destDeploymentInfo];
                    })
                    .then(([sourceDeploymentInfo, destDeploymentInfo]) => {
                        const promoteParams = _.get(body, 'packageInfo', {});
                        // TODO: define packageInfo interface in packageManager
                        _.set(promoteParams as any, 'promoteUid', uid);
                        return packageManager
                            .promotePackage(
                                sourceDeploymentInfo,
                                destDeploymentInfo,
                                promoteParams,
                                logger,
                            )
                            .then((packages) => {
                                return [packages, destDeploymentInfo] as const;
                            });
                    })
                    .then(([packages, destDeploymentInfo]) => {
                        if (packages) {
                            delay(1000).then(() => {
                                packageManager
                                    .createDiffPackagesByLastNums(
                                        destDeploymentInfo.appid,
                                        packages,
                                        config.common.diffNums,
                                        logger,
                                    )
                                    .catch((e) => {
                                        logger.error(e);
                                    });
                            });
                        }
                        // clear cache if exists.
                        if (config.common.updateCheckCache) {
                            delay(2500).then(() => {
                                clientManager.clearUpdateCheckCache(
                                    destDeploymentInfo.deployment_key,
                                    '*',
                                    '*',
                                    '*',
                                    logger,
                                );
                            });
                        }
                        return packages;
                    });
            })
            .then((packages) => {
                logger.info('promote package success', {
                    uid,
                    appName,
                    sourceDeploymentName,
                    destDeploymentName,
                });

                res.send({ package: packages });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('promote package failed', {
                        uid,
                        appName,
                        sourceDeploymentName,
                        destDeploymentName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

function rollbackCb(
    req: Req<{ appName: string; deploymentName: string; label?: string }>,
    res,
    next,
) {
    const { logger, params } = req;
    const appName = _.trim(params.appName);
    const deploymentName = _.trim(params.deploymentName);
    const targetLabel = _.trim(params.label);
    const uid = req.users.id;
    logger.info('try to rollback', {
        uid,
        appName,
        deploymentName,
        targetLabel,
    });
    accountManager
        .collaboratorCan(uid, appName, logger)
        .then((col) => {
            return deploymentsManager.findDeloymentByName(deploymentName, col.appid, logger);
        })
        .then((dep) => {
            return packageManager
                .rollbackPackage(dep.last_deployment_version_id, targetLabel, uid, logger)
                .then((packageInfo: PackagesInterface) => {
                    if (packageInfo) {
                        delay(1000).then(() => {
                            packageManager
                                .createDiffPackagesByLastNums(dep.appid, packageInfo, 1, logger)
                                .catch((e) => {
                                    logger.error(e);
                                });
                        });
                    }
                    // clear cache if exists.
                    if (config.common.updateCheckCache) {
                        delay(2500).then(() => {
                            logger.info('try clear update check cache');
                            clientManager.clearUpdateCheckCache(
                                dep.deployment_key,
                                '*',
                                '*',
                                '*',
                                logger,
                            );
                        });
                    }
                    return packageInfo;
                });
        })
        .then(() => {
            logger.info('rollback success', {
                uid,
                appName,
                deploymentName,
                targetLabel,
            });

            res.send('ok');
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('rollback failed', {
                    uid,
                    appName,
                    deploymentName,
                    targetLabel,
                    error: e.message,
                });

                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
}

appsRouter.post('/:appName/deployments/:deploymentName/rollback', checkToken, rollbackCb);

appsRouter.post('/:appName/deployments/:deploymentName/rollback/:label', checkToken, rollbackCb);

appsRouter.get(
    '/:appName/collaborators',
    checkToken,
    (req: Req<{ appName: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const uid = req.users.id;
        logger.info('try list collaborators', {
            uid,
            appName,
        });
        accountManager
            .collaboratorCan(uid, appName, logger)
            .then((col) => {
                return collaboratorsManager.listCollaborators(col.appid);
            })
            .then((data) => {
                const rs = _.reduce(
                    data,
                    (result, value, key) => {
                        let isCurrentAccount = false;
                        if (_.eq(key, req.users.email)) {
                            isCurrentAccount = true;
                        }

                        result[key] = {
                            ...value,
                            isCurrentAccount,
                        };
                        return result;
                    },
                    {} as Record<string, { permission: string; isCurrentAccount: boolean }>,
                );

                logger.info('list collaborators success', {
                    uid,
                    appName,
                });

                res.send({ collaborators: rs });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('list collaborators failed', {
                        uid,
                        appName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.post(
    '/:appName/collaborators/:email',
    checkToken,
    (req: Req<{ appName: string; email: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const email = _.trim(params.email);
        const uid = req.users.id;
        logger.info('try to add collaborator', {
            uid,
            appName,
            email,
        });
        if (!validator.isEmail(email)) {
            res.status(406).send('Invalid Email!');
            return;
        }
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return accountManager.findUserByEmail(email).then((data) => {
                    return collaboratorsManager.addCollaborator(col.appid, data.id);
                });
            })
            .then((data) => {
                logger.info('add collaborator success', {
                    uid,
                    appName,
                    email,
                });

                res.send(data);
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('add collaborator failed', {
                        uid,
                        appName,
                        email,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.delete(
    '/:appName/collaborators/:email',
    checkToken,
    (req: Req<{ appName: string; email: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const email = _.trim(params.email);
        const uid = req.users.id;
        logger.info('try remove app collaborator', {
            uid,
            appName,
            email,
        });

        if (!validator.isEmail(email)) {
            res.status(406).send('Invalid Email!');
            return;
        }
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return accountManager.findUserByEmail(email).then((data) => {
                    if (_.eq(data.id, uid)) {
                        throw new AppError("can't delete yourself!");
                    } else {
                        return collaboratorsManager.deleteCollaborator(col.appid, data.id);
                    }
                });
            })
            .then(() => {
                logger.info('remove app collaborator success', {
                    uid,
                    appName,
                    email,
                });

                res.send('');
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('remove app collaborator failed', {
                        uid,
                        appName,
                        email,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.delete('/:appName', checkToken, (req: Req<{ appName: string }>, res, next) => {
    const { logger, params } = req;
    const appName = _.trim(params.appName);
    const uid = req.users.id;
    logger.info('try remove app', {
        uid,
        appName,
    });

    accountManager
        .ownerCan(uid, appName, logger)
        .then((col) => {
            return appManager.deleteApp(col.appid);
        })
        .then((data) => {
            logger.info('remove app success', {
                uid,
                appName,
            });

            res.send(data);
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('remove app failed', {
                    uid,
                    appName,
                    error: e.message,
                });

                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
});

appsRouter.patch('/:appName', checkToken, (req: Req<{ appName }, { name }>, res, next) => {
    const { logger, params, body } = req;
    const appName = _.trim(params.appName);
    const newAppName = _.trim(body.name);
    const uid = req.users.id;
    logger.info('try rename app', {
        uid,
        appName,
        newAppName,
    });
    if (_.isEmpty(newAppName)) {
        res.status(406).send('Please input name!');
        return;
    }
    accountManager
        .ownerCan(uid, appName, logger)
        .then((col) => {
            return appManager.findAppByName(uid, newAppName).then((appInfo) => {
                if (!_.isEmpty(appInfo)) {
                    throw new AppError(`${newAppName} Exist!`);
                }
                return appManager.modifyApp(col.appid, { name: newAppName });
            });
        })
        .then(() => {
            logger.info('rename app success', {
                uid,
                appName,
                newAppName,
            });

            res.send('');
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('rename app failed', {
                    uid,
                    appName,
                    newAppName,
                    error: e.message,
                });

                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
});

appsRouter.post(
    '/:appName/transfer/:email',
    checkToken,
    (req: Req<{ appName: string; email: string }>, res, next) => {
        const { logger, params } = req;
        const appName = _.trim(params.appName);
        const email = _.trim(params.email);
        const uid = req.users.id;
        logger.info('try transfer app', {
            uid,
            appName,
            to: email,
        });
        if (!validator.isEmail(email)) {
            res.status(406).send('Invalid Email!');
            return;
        }
        accountManager
            .ownerCan(uid, appName, logger)
            .then((col) => {
                return accountManager.findUserByEmail(email).then((data) => {
                    if (_.eq(data.id, uid)) {
                        throw new AppError("You can't transfer to yourself!");
                    }
                    return appManager.transferApp(col.appid, uid, data.id);
                });
            })
            .then((data) => {
                logger.info('transfer app success', {
                    uid,
                    appName,
                    to: email,
                });

                res.send(data);
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('transfer app failed', {
                        uid,
                        appName,
                        to: email,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

appsRouter.post(
    '/',
    checkToken,
    // eslint-disable-next-line max-lines-per-function
    (
        req: Req<
            Record<string, string>,
            {
                name: string;
                os: string;
                platform: string;
                manuallyProvisionDeployments: unknown;
            }
        >,
        res,
        next,
    ) => {
        const { logger, body } = req;
        const uid = req.users.id;
        logger.info('try add app', {
            uid,
            body: JSON.stringify(body),
        });
        const appName = body.name;
        if (_.isEmpty(appName)) {
            res.status(406).send('Please input name!');
            return;
        }
        const osName = _.toLower(body.os);
        let os;
        if (osName === _.toLower(IOS_NAME)) {
            os = IOS;
        } else if (osName === _.toLower(ANDROID_NAME)) {
            os = ANDROID;
        } else if (osName === _.toLower(WINDOWS_NAME)) {
            os = WINDOWS;
        } else {
            res.status(406).send('Please input os [iOS|Android|Windows]!');
            return;
        }
        const platformName = _.toLower(body.platform);
        let platform;
        if (platformName === _.toLower(REACT_NATIVE_NAME)) {
            platform = REACT_NATIVE;
        } else if (platformName === _.toLower(CORDOVA_NAME)) {
            platform = CORDOVA;
        } else {
            res.status(406).send('Please input platform [React-Native|Cordova]!');
            return;
        }
        logger.info('try add app params check pass', {
            uid,
            appName,
        });

        appManager
            .findAppByName(uid, appName)
            .then((appInfo) => {
                if (!_.isEmpty(appInfo)) {
                    throw new AppError(`${appName} Exist!`);
                }
                return appManager.addApp(uid, appName, os, platform, req.users.identical);
            })
            .then(() => {
                logger.info('add app success', {
                    uid,
                    name: appName,
                });

                const data = {
                    name: appName,
                    collaborators: { [req.users.email]: { permission: 'Owner' } },
                };
                res.send({ app: data });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('add app failed', {
                        uid,
                        name: appName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);
