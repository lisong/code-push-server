import fs from 'fs';
import path from 'path';
import bodyParser from 'body-parser';
import cookieParser from 'cookie-parser';
import express, { NextFunction } from 'express';
import helmet from 'helmet';
import { logger } from 'kv-logger';
import { AppError, NotFound } from './core/app-error';
import { config } from './core/config';
import { i18n } from './core/i18n';
import { i18nMiddleware, Req, Res, withLogger } from './core/middleware';
import { accessKeysRouter } from './routes/accessKeys';
import { accountRouter } from './routes/account';
import { appsRouter } from './routes/apps';
import { authRouter } from './routes/auth';
import { indexRouter } from './routes/index';
import { indexV1Router } from './routes/indexV1';
import { usersRouter } from './routes/users';

export const app = express();

app.use(
    helmet({
        contentSecurityPolicy: false,
    }),
);

// view engine setup
app.set('views', path.join(__dirname, '../views'));
app.set('view engine', 'pug');

// translations
app.use(i18n.init);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, '../public')));
app.use(withLogger);

app.all('*', (req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header(
        'Access-Control-Allow-Headers',
        'Origin, X-Requested-With, Content-Type, Accept, Authorization, X-CodePush-Plugin-Version, X-CodePush-Plugin-Name, X-CodePush-SDK-Version, X-Request-Id',
    );
    res.header('Access-Control-Allow-Methods', 'PUT,POST,GET,PATCH,DELETE,OPTIONS');
    next();
});

// i18n 미들웨어 (req.lang, req.t 세팅)
app.use(i18nMiddleware);

logger.debug(`config common.storageType value: ${config.common.storageType}`);

// config local storage
if (config.common.storageType === 'local') {
    const localStorageDir = config.local.storageDir;
    if (localStorageDir) {
        logger.debug(`config common.storageDir value: ${localStorageDir}`);

        if (!fs.existsSync(localStorageDir)) {
            const e = new Error(`Please create dir ${localStorageDir}`);
            logger.error(e);
            throw e;
        }
        try {
            logger.debug('checking storageDir fs.W_OK | fs.R_OK');
            // eslint-disable-next-line no-bitwise
            fs.accessSync(localStorageDir, fs.constants.W_OK | fs.constants.R_OK);
            logger.debug('storageDir fs.W_OK | fs.R_OK is ok');
        } catch (e) {
            logger.error(e);
            throw e;
        }
        logger.debug(`static download uri value: ${config.local.public}`);
        app.use(config.local.public, express.static(localStorageDir));
    } else {
        logger.error('please config local storageDir');
    }
}

// config routes
// code-push-client routes
app.use('/', indexRouter);
app.use('/v0.1/public/codepush', indexV1Router);
// code-push-cli routes
app.use('/accessKeys', accessKeysRouter);
app.use('/apps', appsRouter);
app.use('/account', accountRouter);
// code-push-server routes
app.use('/auth', authRouter);
app.use('/users', usersRouter);

// 404 handler
// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((req, res, next) => {
    throw new NotFound(`${req.method} ${req.url} not found`);
});

// error handler
// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((err: Error, req: Req, res: Res, next: NextFunction) => {
    const thisLogger = req.logger || logger;
    if (err instanceof AppError) {
        res.status(err.status).send(err.message);
        thisLogger.debug(err);
    } else {
        res.status(500).send(err.message);
        thisLogger.error(err);
    }
});
