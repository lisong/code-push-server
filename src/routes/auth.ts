import express from 'express';
import jwt from 'jsonwebtoken';
import _ from 'lodash';
import { AppErrorI18n } from '../core/app-error';
import { config } from '../core/config';
import { Req } from '../core/middleware';
import { accountManager } from '../core/services/account-manager';
import { md5 } from '../core/utils/security';

// route for auth web pages
export const authRouter = express.Router();

authRouter.get('/password', (req: Req, res) => {
    res.render('auth/password', { title: 'CodePushServer' });
});

authRouter.get('/login', (req: Req<void, void, { email: string }>, res) => {
    res.render('auth/login', {
        title: 'CodePushServer',
        email: req.query.email || '',
        showRegister: config.common.allowRegistration,
    });
});

authRouter.get('/link', (req: Req, res) => {
    res.redirect(`/auth/login`);
});

authRouter.get('/register', (req: Req<void, void, { email: string }>, res) => {
    if (config.common.allowRegistration) {
        res.render('auth/register', { title: 'CodePushServer', email: req.query.email || '' });
    } else {
        res.redirect(`/auth/login`);
    }
});

authRouter.get('/confirm', (req: Req<void, void, { email: string }>, res) => {
    res.render('auth/confirm', { title: 'CodePushServer', email: req.query.email || '' });
});

authRouter.post('/logout', (req: Req, res) => {
    res.send('ok');
});

authRouter.post(
    '/login',
    (req: Req<void, { account: string; password: string }, void>, res, next) => {
        const { logger, body } = req;
        const account = _.trim(body.account);
        const password = _.trim(body.password);
        logger.info('try login', {
            account,
        });
        accountManager
            .login(account, password)
            .then((users) => {
                logger.info('login success', {
                    account,
                    uid: users.id,
                });
                return jwt.sign(
                    { uid: users.id, hash: md5(users.ack_code), expiredIn: 7200 },
                    config.jwt.tokenSecret,
                );
            })
            .then((token) => {
                logger.info('jwt token signed', {
                    account,
                });
                res.send({ status: 'OK', results: { tokens: token } });
            })
            .catch((e) => {
                if (e instanceof AppErrorI18n) {
                    const { messageKey, messageVars } = e;
                    const message =
                        typeof req.t === 'function' ? req.t(messageKey, messageVars) : messageKey; // fallback

                    logger.info('login failed', {
                        account,
                        error: message,
                        messageKey,
                    });
                    res.send({
                        status: 'ERROR',
                        message,
                        code: messageKey,
                    });
                } else {
                    next(e);
                }
            });
    },
);
