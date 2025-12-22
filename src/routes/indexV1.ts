import express from 'express';
import { AppError } from '../core/app-error';
import { Req } from '../core/middleware';
import { clientManager } from '../core/services/client-manager';

// routes for latest code push client
export const indexV1Router = express.Router();

indexV1Router.get(
    '/update_check',
    (
        req: Req<
            void,
            void,
            {
                deployment_key: string;
                app_version: string;
                label: string;
                package_hash: string;
                is_companion: unknown;
                client_unique_id: string;
            }
        >,
        res,
        next,
    ) => {
        const { logger, query } = req;
        logger.info('try update_check', {
            query: JSON.stringify(query),
        });
        const {
            deployment_key: deploymentKey,
            app_version: appVersion,
            label,
            package_hash: packageHash,
            client_unique_id: clientUniqueId,
        } = query;
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
                logger.info('update_check success');

                res.send({
                    update_info: {
                        download_url: rs.downloadUrl,
                        description: rs.description,
                        is_available: rs.isAvailable,
                        is_disabled: rs.isDisabled,
                        // Note: need to use appVersion here to get it compatible with client side change...
                        // https://github.com/microsoft/code-push/commit/7d2ffff395cc54db98aefba7c67889f509e8c249#diff-a937c637a47cbd31cbb52c89bef7d197R138
                        target_binary_range: rs.appVersion,
                        label: rs.label,
                        package_hash: rs.packageHash,
                        package_size: rs.packageSize,
                        should_run_binary_version: rs.shouldRunBinaryVersion,
                        update_app_version: rs.updateAppVersion,
                        is_mandatory: rs.isMandatory,
                    },
                });
            })
            .catch((e) => {
                if (e instanceof AppError) {
                    logger.info('update check failed', {
                        error: e.message,
                    });
                    res.status(404).send(e.message);
                } else {
                    next(e);
                }
            });
    },
);

indexV1Router.post(
    '/report_status/download',
    (
        req: Req<
            void,
            {
                client_unique_id: string;
                label: string;
                deployment_key: string;
            },
            void
        >,
        res,
    ) => {
        const { logger, body } = req;
        logger.info('report_status/download', { body: JSON.stringify(body) });
        const { client_unique_id: clientUniqueId, label, deployment_key: deploymentKey } = body;
        clientManager.reportStatusDownload(deploymentKey, label, clientUniqueId).catch((err) => {
            if (err instanceof AppError) {
                logger.info('report_status/download failed', {
                    error: err.message,
                });
            } else {
                logger.error(err);
            }
        });
        res.send('OK');
    },
);

indexV1Router.post(
    '/report_status/deploy',
    (
        req: Req<
            void,
            {
                client_unique_id: string;
                label: string;
                deployment_key: string;
            },
            void
        >,
        res,
    ) => {
        const { logger, body } = req;
        logger.info('report_status/deploy', { body: JSON.stringify(body) });
        const { client_unique_id: clientUniqueId, label, deployment_key: deploymentKey } = body;
        clientManager
            .reportStatusDeploy(deploymentKey, label, clientUniqueId, req.body)
            .catch((err) => {
                if (err instanceof AppError) {
                    logger.info('report_status/deploy failed', {
                        error: err.message,
                    });
                } else {
                    logger.error(err);
                }
            });
        res.send('OK');
    },
);
