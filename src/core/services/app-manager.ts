import _ from 'lodash';
import { Op } from 'sequelize';
import { Apps, AppsInterface } from '../../models/apps';
import { Collaborators } from '../../models/collaborators';
import { Deployments } from '../../models/deployments';
import { Users } from '../../models/users';
import { AppError } from '../app-error';
import {
    IOS,
    IOS_NAME,
    ANDROID,
    ANDROID_NAME,
    WINDOWS,
    WINDOWS_NAME,
    CORDOVA,
    CORDOVA_NAME,
    REACT_NATIVE,
    REACT_NATIVE_NAME,
    STAGING,
    PRODUCTION,
} from '../const';
import { sequelize } from '../utils/connections';
import { randToken } from '../utils/security';

class AppManager {
    findAppByName(uid: number, appName: string) {
        return Apps.findOne({ where: { name: appName, uid } });
    }

    addApp(uid: number, appName: string, os, platform, identical: string) {
        return sequelize.transaction((t) => {
            return Apps.create(
                {
                    name: appName,
                    uid,
                    os,
                    platform,
                },
                {
                    transaction: t,
                },
            ).then((apps) => {
                const appId = apps.id;
                const deployments = [];
                let deploymentKey = randToken(28) + identical;
                deployments.push({
                    appid: appId,
                    name: PRODUCTION,
                    last_deployment_version_id: 0,
                    label_id: 0,
                    deployment_key: deploymentKey,
                });
                deploymentKey = randToken(28) + identical;
                deployments.push({
                    appid: appId,
                    name: STAGING,
                    last_deployment_version_id: 0,
                    label_id: 0,
                    deployment_key: deploymentKey,
                });
                return Promise.all([
                    Collaborators.create({ appid: appId, uid, roles: 'Owner' }, { transaction: t }),
                    Deployments.bulkCreate(deployments, { transaction: t }),
                ]);
            });
        });
    }

    deleteApp(appId) {
        return sequelize.transaction((t) => {
            return Promise.all([
                Apps.destroy({ where: { id: appId }, transaction: t }),
                Collaborators.destroy({ where: { appid: appId }, transaction: t }),
                Deployments.destroy({ where: { appid: appId }, transaction: t }),
            ]);
        });
    }

    modifyApp(appId, params) {
        return Apps.update(params, { where: { id: appId } }).then(([affectedCount]) => {
            if (!_.gt(affectedCount, 0)) {
                throw new AppError('modify errors');
            }
            return affectedCount;
        });
    }

    transferApp(appId: number, fromUid: number, toUid: number) {
        return sequelize.transaction((t) => {
            return Promise.all([
                Apps.update({ uid: toUid }, { where: { id: appId }, transaction: t }),
                Collaborators.destroy({ where: { appid: appId, uid: fromUid }, transaction: t }),
                Collaborators.destroy({ where: { appid: appId, uid: toUid }, transaction: t }),
                Collaborators.create(
                    { appid: appId, uid: toUid, roles: 'Owner' },
                    { transaction: t },
                ),
            ]);
        });
    }

    listApps(uid: number) {
        return Collaborators.findAll({ where: { uid } })
            .then((data) => {
                if (_.isEmpty(data)) {
                    return [] as AppsInterface[];
                }
                const appIds = _.map(data, (v) => {
                    return v.appid;
                });
                return Apps.findAll({ where: { id: { [Op.in]: appIds } } });
            })
            .then((appInfos) => {
                const rs = Promise.all(
                    _.values(appInfos).map((v) => {
                        return this.getAppDetailInfo(v, uid).then((info) => {
                            let os = '';
                            if (info.os === IOS) {
                                os = IOS_NAME;
                            } else if (info.os === ANDROID) {
                                os = ANDROID_NAME;
                            } else if (info.os === WINDOWS) {
                                os = WINDOWS_NAME;
                            }

                            let platform = '';
                            if (info.platform === REACT_NATIVE) {
                                platform = REACT_NATIVE_NAME;
                            } else if (info.platform === CORDOVA) {
                                platform = CORDOVA_NAME;
                            }
                            return {
                                ...info,
                                os,
                                platform,
                            };
                        });
                    }),
                );
                return rs;
            });
    }

    private getAppDetailInfo(appInfo: AppsInterface, currentUid: number) {
        const appId = appInfo.get('id');
        return Promise.all([
            Deployments.findAll({ where: { appid: appId } }),
            Collaborators.findAll({ where: { appid: appId } }).then((collaboratorInfos) => {
                return collaboratorInfos.reduce((prev, collaborator) => {
                    return prev.then((allCol) => {
                        return Users.findOne({ where: { id: collaborator.get('uid') } }).then(
                            (u) => {
                                let isCurrentAccount = false;
                                if (_.eq(u.get('id'), currentUid)) {
                                    isCurrentAccount = true;
                                }
                                allCol[u.get('email')] = {
                                    permission: collaborator.get('roles'),
                                    isCurrentAccount,
                                };
                                return allCol;
                            },
                        );
                    });
                }, Promise.resolve({}));
            }),
        ]).then(([deploymentInfos, collaborators]) => {
            return {
                collaborators,
                deployments: _.map(deploymentInfos, (item) => {
                    return _.get(item, 'name');
                }),
                os: appInfo.get('os'),
                platform: appInfo.get('platform'),
                name: appInfo.get('name'),
                id: appInfo.get('id'),
            };
        });
    }
}

export const appManager = new AppManager();
