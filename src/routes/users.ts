import express from 'express';
import _ from 'lodash';
import { AppError, AppErrorI18n } from '../core/app-error';
import { checkToken, Req } from '../core/middleware';
import { accountManager } from '../core/services/account-manager';
import { Users } from '../models/users';

export const usersRouter = express.Router();

usersRouter.get('/', checkToken, (req: Req, res) => {
    res.send({ title: 'CodePushServer' });
});

usersRouter.post(
    '/',
    (
        req: Req<
            void,
            {
                email: string;
                token: string;
                password: string;
            },
            void
        >,
        res,
        next,
    ) => {
        const { logger, body } = req;
        const email = _.trim(body.email);
        const token = _.trim(body.token);
        const password = _.trim(body.password);
        logger.info('try register account', { email, token });
        return accountManager
            .checkRegisterCode(email, token)
            .then(() => {
                if (_.isString(password) && password.length < 6) {
                    throw new AppErrorI18n('error.password_length_register');
                }
                return accountManager.register(email, password);
            })
            .then(() => {
                logger.info('register account success', { email });
                res.send({ status: 'OK' });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('register account failed', { email, error: e.message });
                    res.send({ status: 'ERROR', message: e.message });
                } else {
                    next(e);
                }
            });
    },
);

usersRouter.get('/exists', (req: Req<void, void, { email: string }>, res, next) => {
    const email = _.trim(req.query.email);
    const emailRequiredMessage = req.t('error.input_email_required');
    if (!email) {
        res.send({ status: 'ERROR', message: emailRequiredMessage });
        return;
    }
    Users.findOne({ where: { email } })
        .then((u) => {
            res.send({ status: 'OK', exists: !!u });
        })
        .catch((e) => {
            if (e instanceof AppError) {
                res.send({ status: 'ERROR', message: e.message });
            } else {
                next(e);
            }
        });
});

usersRouter.post('/registerCode', (req: Req<void, { email: string }, void>, res, next) => {
    const { logger, body } = req;
    const { email } = body;
    logger.info('try send register code', { email });
    return accountManager
        .sendRegisterCode(email)
        .then(() => {
            logger.info('send register code success', { email });
            res.send({ status: 'OK' });
        })
        .catch((e) => {
            if (e instanceof AppError) {
                logger.info('send register code error', { email, error: e.message });
                res.send({ status: 'ERROR', message: e.message });
            } else {
                next(e);
            }
        });
});

usersRouter.get(
    '/registerCode/exists',
    (req: Req<void, void, { email: string; token: string }>, res, next) => {
        const { query } = req;
        const email = _.trim(query.email);
        const token = _.trim(query.token);
        return accountManager
            .checkRegisterCode(email, token)
            .then(() => {
                res.send({ status: 'OK' });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    res.send({ status: 'ERROR', message: e.message });
                } else {
                    next(e);
                }
            });
    },
);

// Change Password
usersRouter.patch(
    '/password',
    checkToken,
    (
        req: Req<
            Record<string, string>,
            {
                oldPassword: string;
                newPassword: string;
            }
        >,
        res,
        next,
    ) => {
        const { logger, body } = req;
        const oldPassword = _.trim(body.oldPassword);
        const newPassword = _.trim(body.newPassword);
        const uid = req.users.id;
        logger.info('try change password', { uid });
        return accountManager
            .changePassword(uid, oldPassword, newPassword)
            .then(() => {
                logger.info('change password success', { uid });
                res.send({ status: 'OK' });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('change password failed', { uid });
                    res.send({ status: 'ERROR', message: e.message });
                } else {
                    next(e);
                }
            });
    },
);
