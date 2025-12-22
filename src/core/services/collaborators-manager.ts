import _ from 'lodash';
import { Op } from 'sequelize';
import { Collaborators, CollaboratorsInterface } from '../../models/collaborators';
import { Users } from '../../models/users';
import { AppError } from '../app-error';

class CollaboratorsManager {
    listCollaborators(appId: number) {
        return Collaborators.findAll({ where: { appid: appId } })
            .then(
                (
                    data,
                ): {
                    uids: number[];
                    colByUid: Record<number, CollaboratorsInterface>;
                } => {
                    return _.reduce(
                        data,
                        (result, value) => {
                            result.uids.push(value.uid);
                            result.colByUid[value.uid] = value;
                            return result;
                        },
                        {
                            uids: [],
                            colByUid: {},
                        },
                    );
                },
            )
            .then((coInfo) => {
                return Users.findAll({ where: { id: { [Op.in]: coInfo.uids } } }).then((data2) => {
                    return _.reduce(
                        data2,
                        (result, value) => {
                            let permission = '';
                            if (!_.isEmpty(coInfo.colByUid[value.id])) {
                                permission = coInfo.colByUid[value.id].roles;
                            }
                            result[value.email] = { permission };
                            return result;
                        },
                        {} as Record<string, { permission: string }>,
                    );
                });
            });
    }

    addCollaborator(appId: number, uid: number) {
        return Collaborators.findOne({ where: { appid: appId, uid } }).then((data) => {
            if (_.isEmpty(data)) {
                return Collaborators.create({
                    appid: appId,
                    uid,
                    roles: 'Collaborator',
                });
            }
            throw new AppError('user already is Collaborator.');
        });
    }

    deleteCollaborator(appId: number, uid: number) {
        return Collaborators.findOne({ where: { appid: appId, uid } }).then((data) => {
            if (_.isEmpty(data)) {
                throw new AppError('user is not a Collaborator');
            } else {
                return Collaborators.destroy({ where: { id: data.id } });
            }
        });
    }
}

export const collaboratorsManager = new CollaboratorsManager();
