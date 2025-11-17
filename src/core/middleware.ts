import { randomUUID } from 'crypto';
import type { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { logger, Logger } from 'kv-logger';
import _ from 'lodash';
import moment from 'moment';
import { Op } from 'sequelize';
import { UserTokens } from '../models/user_tokens';
import { Users, UsersInterface } from '../models/users';
import { AppError, Unauthorized } from './app-error';
import { config } from './config';
import { t } from './i18n';
import { shouldHideWebUI } from './utils/common';
import { parseToken, md5 } from './utils/security';

export type LocaleI18n = 'en' | 'ko' | 'zh';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface Req<P = Record<string, string>, B = any, Q = Record<string, string | string[]>>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    extends Request<P, any, B, Partial<Q>> {
    users: UsersInterface;
    logger: Logger;
    lang?: LocaleI18n;
    t?: (key: string, vars?: Record<string, any>) => string;
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface, @typescript-eslint/no-explicit-any
export interface Res<B = any> extends Response<B> {}

/**
 * bind logger to request
 */
export function withLogger(req: Req, res: Res, next: NextFunction) {
    const { method, path, headers } = req;
    const requestId = headers['x-request-id'] || randomUUID();
    req.logger = logger.bindContext({
        path,
        method,
        requestId,
    });
    res.header('X-Request-Id', requestId);
    next();
}

async function checkAuthToken(authToken: string) {
    const objToken = parseToken(authToken);
    const users = await Users.findOne({
        where: { identical: objToken.identical },
    });
    if (_.isEmpty(users)) {
        throw new Unauthorized();
    }

    const tokenInfo = await UserTokens.findOne({
        where: {
            tokens: authToken,
            uid: users.id,
            expires_at: {
                [Op.gt]: moment().format('YYYY-MM-DD HH:mm:ss'),
            },
        },
    });
    if (_.isEmpty(tokenInfo)) {
        throw new Unauthorized();
    }

    return users;
}

async function checkAccessToken(accessToken: string) {
    if (_.isEmpty(accessToken)) {
        throw new Unauthorized();
    }

    let authData: { uid: number; hash: string };
    try {
        authData = jwt.verify(accessToken, config.jwt.tokenSecret) as {
            uid: number;
            hash: string;
        };
    } catch (e) {
        throw new Unauthorized();
    }

    const { uid, hash } = authData;
    if (uid <= 0) {
        throw new Unauthorized();
    }

    const users = await Users.findOne({
        where: { id: uid },
    });
    if (_.isEmpty(users)) {
        throw new Unauthorized();
    }

    if (hash !== md5(users.get('ack_code'))) {
        throw new Unauthorized();
    }
    return users;
}

/**
 * check user token and bind user to request
 */
export function checkToken(req: Req, res: Res, next: NextFunction) {
    // get token and type
    let authType: 1 | 2 = 1;
    let authToken = '';
    const authArr = _.split(req.get('Authorization'), ' ');
    if (authArr[0] === 'Bearer') {
        [, authToken] = authArr; // Bearer
        if (authToken && authToken.length > 64) {
            authType = 2;
        } else {
            authType = 1;
        }
    } else if (authArr[0] === 'Basic') {
        authType = 2;
        const b = Buffer.from(authArr[1], 'base64');
        const user = _.split(b.toString(), ':');
        [, authToken] = user;
    }

    // do check token
    let checkTokenResult: Promise<UsersInterface>;
    if (authToken && authType === 1) {
        checkTokenResult = checkAuthToken(authToken);
    } else if (authToken && authType === 2) {
        checkTokenResult = checkAccessToken(authToken);
    } else {
        res.send(new Unauthorized(`Auth type not supported.`));
        return;
    }

    checkTokenResult
        .then((users) => {
            req.users = users;
            next();
        })
        .catch((e) => {
            if (e instanceof AppError) {
                res.status(e.status || 404).send(e.message);
            } else {
                next(e);
            }
        });
}

function getLocaleFromReq(req: Req): LocaleI18n {
    const langHeader = (req.headers['x-lang'] || req.headers['accept-language'] || '').toString();

    if (langHeader.startsWith('ko')) {
        return 'ko';
    }
    return 'en';
}

export function i18nMiddleware(req: Req, res: Res, next: NextFunction) {
    const locale = getLocaleFromReq(req);

    req.lang = locale;
    req.t = (key: string, vars?: Record<string, any>) => t(locale, key, vars);

    next();
}

export function webUiGuard(req: Req, res: Response, next: NextFunction) {
    if (!shouldHideWebUI()) {
        return next();
    }

    req.logger?.info?.('blocked web UI access', {
        path: req.path,
        method: req.method,
    });

    return res.status(405).send('Method Not Allowed');
}

export function ipWhitelistOnly(req: Req, res: Response, next: NextFunction) {
    const whitelist = config.common.webUIWhitelist || [];

    if (!Array.isArray(whitelist) || whitelist.length === 0) {
        return next();
    }

    const defaultIp = req.ip;
    const forwardedFor = req.headers['x-forwarded-for'];

    const realIp = Array.isArray(forwardedFor)
        ? forwardedFor[0]
        : forwardedFor?.split(',')[0]?.trim() || defaultIp;

    const isAllowed = whitelist.includes(realIp);

    if (!isAllowed) {
        req.logger?.info?.('IP whitelist blocked', {
            requestIp: realIp,
            path: req.path,
        });

        return res.status(403).send('Forbidden');
    }

    req.logger?.info?.('IP whitelist allowed', {
        requestIp: realIp,
        path: req.path,
    });

    return next();
}
