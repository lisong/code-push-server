import express from 'express';
import _ from 'lodash';
import moment from 'moment';
import { AppError } from '../core/app-error';
import { checkToken, Req } from '../core/middleware';
import { accountManager } from '../core/services/account-manager';
import { randToken } from '../core/utils/security';
import { UserTokens } from '../models/user_tokens';

export const accessKeysRouter = express.Router();

accessKeysRouter.get('/', checkToken, (req: Req, res, next) => {
    const { logger } = req;
    const uid = req.users.id;
    logger.info('try get accessKeys', { uid });
    accountManager
        .getAllAccessKeyByUid(uid)
        .then((accessKeys) => {
            logger.info('get accessKeys success', { uid });
            res.send({ accessKeys });
        })
        .catch((e) => {
            next(e);
        });
});

accessKeysRouter.post(
    '/',
    checkToken,
    (
        req: Req<
            Record<string, string>,
            {
                createdBy: string;
                friendlyName: string;
                ttl: string;
                description: string;
            }
        >,
        res,
        next,
    ) => {
        const { logger, body } = req;
        const uid = req.users.id;
        const createdBy = _.trim(body.createdBy);
        const friendlyName = _.trim(body.friendlyName);
        // TODO: 엑세스토큰 만료일을 수정하고 싶은 경우 tokens.pug 에서 전송되는 ttl을 수정
        const ttl = parseInt(body.ttl, 10); // 현재 호출부 기본값 30일 = 60*60*24*30*1000
        const description = _.trim(body.description);
        logger.info('try to generate access key', {
            uid,
            name: friendlyName,
            body: JSON.stringify(body),
        });
        return accountManager
            .isExistAccessKeyName(uid, friendlyName)
            .then((data) => {
                if (!_.isEmpty(data)) {
                    throw new AppError(`The access key "${friendlyName}"  already exists.`);
                }
            })
            .then(() => {
                const { identical } = req.users;
                const newAccessKey = randToken(28).concat(identical);
                return accountManager.createAccessKey(
                    uid,
                    newAccessKey,
                    ttl,
                    friendlyName,
                    createdBy,
                    description,
                );
            })
            .then((newToken) => {
                const info = {
                    name: newToken.tokens,
                    createdTime: moment(newToken.created_at).valueOf(),
                    createdBy: newToken.created_by,
                    expires: moment(newToken.expires_at).valueOf(),
                    description: newToken.description,
                    friendlyName: newToken.name,
                };
                logger.info('create access key success', {
                    uid,
                    name: newToken.name,
                });
                res.send({ accessKey: info });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('create access key failed', {
                        uid,
                        name: friendlyName,
                        error: e.message,
                    });

                    res.status(406).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

accessKeysRouter.delete('/:name', checkToken, (req: Req<{ name: string }>, res, next) => {
    const { logger, params } = req;
    const name = _.trim(params.name);
    const uid = req.users.id;
    logger.info('try to delete access key', { uid, name });
    return UserTokens.destroy({ where: { name, uid } })
        .then(() => {
            logger.info('delete acceesKey success', { uid, name });
            res.send({ friendlyName: name });
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('delete acceesKey failed', { uid, name });
                res.status(406).send(e.message);
            } else {
                next(e);
            }
        });
});
