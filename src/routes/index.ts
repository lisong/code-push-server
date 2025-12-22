import express from 'express';
import { AppError } from '../core/app-error';
import { i18n } from '../core/i18n';
import { checkToken, ipWhitelistOnly, Req, webUiGuard } from '../core/middleware';
import { clientManager } from '../core/services/client-manager';

export const indexRouter = express.Router();

/**
 * 프로덕션 레벨 미들웨어 적용
 * - ipWhitelistOnly: IP주소 검사
 * - webUiGuard: WEB_UI_ALLOW='true'
 */
indexRouter.get('/', [ipWhitelistOnly, webUiGuard], (req, res) => {
    res.render('index', { title: 'CodePushServer' });
});

indexRouter.get('/healthcheck', (req: Req, res) => {
    const message = req.t('hot update server');
    const timestamp = new Date().toISOString();

    res.status(200).json({
        success: true,
        lang: req.lang,
        message,
        timestamp,
    });
});

indexRouter.get('/tokens', (req, res) => {
    // eslint-disable-next-line no-underscore-dangle
    res.render('tokens', { title: `${i18n.__('Obtain')} token` });
});

indexRouter.get(
    '/updateCheck',
    (
        req: Req<
            void,
            void,
            {
                deploymentKey: string;
                appVersion: string;
                label: string;
                packageHash: string;
                clientUniqueId: string;
            }
        >,
        res,
        next,
    ) => {
        const { logger, query } = req;
        logger.info('updateCheck', {
            query: JSON.stringify(query),
        });
        const { deploymentKey, appVersion, label, packageHash, clientUniqueId } = query;
        clientManager
            .updateCheckFromCache(
                deploymentKey,
                appVersion,
                label,
                packageHash,
                clientUniqueId,
                logger,
            )
            .then((rs) => {
                // 그레이 릴리즈(Gray Release, 灰度检测, 점진적 배포대상) 체크 === 현재 유저가 이 업데이트를 받을 대상인지 판별하는 과정
                return clientManager
                    .chosenMan(rs.packageId, rs.rollout, clientUniqueId)
                    .then((data) => {
                        if (!data) {
                            rs.isAvailable = false;
                            return rs;
                        }
                        return rs;
                    });
            })
            .then((rs) => {
                logger.info('updateCheck success');

                delete rs.packageId;
                delete rs.rollout;
                res.send({ updateInfo: rs });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('updateCheck failed', {
                        error: e.message,
                    });
                    res.status(404).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

indexRouter.post(
    '/reportStatus/download',
    (
        req: Req<
            void,
            {
                clientUniqueId: string;
                label: string;
                deploymentKey: string;
            },
            void
        >,
        res,
    ) => {
        const { logger, body } = req;
        logger.info('reportStatus/download', {
            body: JSON.stringify(body),
        });
        const { clientUniqueId, label, deploymentKey } = body;
        clientManager.reportStatusDownload(deploymentKey, label, clientUniqueId).catch((err) => {
            if (err instanceof AppError) {
                logger.info('reportStatus/deploy failed', {
                    error: err.message,
                });
            } else {
                logger.error(err);
            }
        });
        res.send('OK');
    },
);

indexRouter.post(
    '/reportStatus/deploy',
    (
        req: Req<
            void,
            {
                clientUniqueId: string;
                label: string;
                deploymentKey: string;
            },
            void
        >,
        res,
    ) => {
        const { logger, body } = req;
        logger.info('reportStatus/deploy', {
            body: JSON.stringify(body),
        });
        const { clientUniqueId, label, deploymentKey } = body;
        clientManager
            .reportStatusDeploy(deploymentKey, label, clientUniqueId, req.body)
            .catch((err) => {
                if (err instanceof AppError) {
                    logger.info('reportStatus/deploy failed', {
                        error: err.message,
                    });
                } else {
                    logger.error(err);
                }
            });
        res.send('OK');
    },
);

indexRouter.get('/authenticated', checkToken, (req, res) => {
    return res.send({ authenticated: true });
});
