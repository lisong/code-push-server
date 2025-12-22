#!/usr/bin/env node
import 'dotenv/config';
import http from 'http';
import { logger } from 'kv-logger';
import _ from 'lodash';
import validator from 'validator';
import { app } from './app';
import { CURRENT_DB_VERSION } from './core/const';
import { Versions } from './models/versions';

/**
 * Normalize a port into a number, string, or false.
 */
function normalizePort(val): number | string | false {
    const port = parseInt(val, 10);

    if (Number.isNaN(port)) {
        // named pipe
        return val;
    }

    if (port >= 0) {
        // port number
        return port;
    }

    return false;
}

// check if the db is initialized
Versions.findOne({ where: { type: 1 } })
    .then((v) => {
        if (!v || v.version !== CURRENT_DB_VERSION) {
            throw new Error(
                'Please upgrade your database. use `npm run upgrade` or `code-push-server-db upgrade`',
            );
        }
        // create server and listen
        const server = http.createServer(app);

        const port = normalizePort(process.env.PORT || '3000');

        let host = null;
        if (process.env.HOST) {
            logger.debug(`process.env.HOST ${process.env.HOST}`);
            if (validator.isIP(process.env.HOST)) {
                logger.debug(`${process.env.HOST} valid`);
                host = process.env.HOST;
            } else {
                logger.warn(`process.env.HOST ${process.env.HOST} is invalid, use 0.0.0.0 instead`);
            }
        }

        server.listen(port, host);

        server.on('error', (error: Error & { syscall: string; code: string }) => {
            if (error.syscall === 'listen') {
                const bind = typeof port === 'string' ? `Pipe ${port}` : `Port ${port}`;

                // handle specific listen errors with friendly messages
                switch (error.code) {
                    case 'EACCES':
                        logger.error(`${bind} requires elevated privileges`);
                        process.exit(1);
                        break;
                    case 'EADDRINUSE':
                        logger.error(`${bind} is already in use`);
                        process.exit(1);
                        break;
                    default:
                        break;
                }
            }
            logger.error(error);
            throw error;
        });

        server.on('listening', () => {
            const addr = server.address();
            logger.info(`server is listening on ${JSON.stringify(addr)}`);
        });
    })
    .catch((e) => {
        if (_.startsWith(e.message, 'ER_NO_SUCH_TABLE')) {
            logger.error(
                new Error(
                    'Please upgrade your database. use `npm run upgrade` or `code-push-server-db upgrade`',
                ),
            );
        } else {
            logger.error(e);
        }
        process.exit(1);
    });
