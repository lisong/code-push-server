#!/usr/bin/env node
/* eslint-disable no-console */

/**
 * Module dependencies.
 */
import fs from 'fs';
import path from 'path';
import _ from 'lodash';
import mysql from 'mysql2';
import yargs from 'yargs';
import { CURRENT_DB_VERSION } from './core/const';

const argv = yargs
    .usage('Usage: $0 <command> [options]')
    .command('init', '데이터베이스 초기화', {
        dbpassword: {
            alias: 'dbpassword',
            type: 'string',
        },
    })
    .command('upgrade', '데이터베이스 업그레이드', {
        dbpassword: {
            alias: 'dbpassword',
            type: 'string',
        },
    })
    .example(
        '$0 init --dbname codepush --dbhost localhost --dbuser root --dbpassword 123456 --dbport 3306 --force',
        'code-push-server 데이터베이스 초기화',
    )
    .example(
        '$0 upgrade --dbname codepush --dbhost localhost --dbuser root --dbpassword 123456 --dbport 3306',
        'code-push-server 데이터베이스 업그레이드',
    )
    .default({
        dbname: 'codepush',
        dbhost: 'localhost',
        dbuser: 'codepush',
        dbpassword: 'codepush',
    })
    .help('h')
    .alias('h', 'help')
    .parseSync();

const command = argv._[0];
const dbname = argv.dbname ? argv.dbname : 'codepush';
const dbhost = argv.dbhost ? argv.dbhost : 'localhost';
const dbuser = argv.dbuser ? argv.dbuser : 'codepush';
const dbport = argv.dbport ? argv.dbport : 3306;
const { dbpassword } = argv;

if (command === 'init') {
    let connection2;
    const connection = mysql
        .createConnection({
            host: dbhost,
            user: dbuser,
            password: dbpassword,
            port: dbport,
        })
        .promise();
    const createDatabaseSql = argv.force
        ? `CREATE DATABASE IF NOT EXISTS ${dbname}`
        : `CREATE DATABASE ${dbname}`;
    connection.connect();
    connection
        .query(createDatabaseSql)
        .then(() => {
            connection2 = mysql
                .createConnection({
                    host: dbhost,
                    user: dbuser,
                    password: dbpassword,
                    database: dbname,
                    multipleStatements: true,
                    port: dbport,
                })
                .promise();
            connection2.connect();
            return connection2;
        })
        .then(() => {
            const sql = fs.readFileSync(
                path.resolve(__dirname, '../sql/codepush-all.sql'),
                'utf-8',
            );
            return connection2.query(sql);
        })
        .then(() => {
            console.log('success.');
        })
        .catch((e) => {
            console.log(e);
        })
        .finally(() => {
            if (connection) connection.end();
            if (connection2) connection2.end();
        });
} else if (command === 'upgrade') {
    let connection;
    try {
        connection = mysql
            .createConnection({
                host: dbhost,
                user: dbuser,
                password: dbpassword,
                database: dbname,
                multipleStatements: true,
                port: dbport,
            })
            .promise();
        connection.connect();
    } catch (e) {
        console.error('MySQL 연결 오류, 입력한 설정을 확인하세요.', e);
        process.exit(1);
    }

    let versionNo = '0.0.1';
    connection
        .query('select `version` from `versions` where `type`=1 limit 1')
        .then((rs) => {
            versionNo = _.get(rs, '0.version', '0.0.1');
            if (versionNo === CURRENT_DB_VERSION) {
                console.log('모든 항목이 최신 버전입니다.');
                process.exit(0);
            }
            const allSqlFile = [
                {
                    version: '0.2.14',
                    path: path.resolve(__dirname, '../sql/codepush-v0.2.14-patch.sql'),
                },
                {
                    version: '0.2.15',
                    path: path.resolve(__dirname, '../sql/codepush-v0.2.15-patch.sql'),
                },
                {
                    version: '0.3.0',
                    path: path.resolve(__dirname, '../sql/codepush-v0.3.0-patch.sql'),
                },
                {
                    version: '0.4.0',
                    path: path.resolve(__dirname, '../sql/codepush-v0.4.0-patch.sql'),
                },
                {
                    version: '0.5.0',
                    path: path.resolve(__dirname, '../sql/codepush-v0.5.0-patch.sql'),
                },
            ];
            return allSqlFile.reduce((prev, sqlFile) => {
                if (!_.gt(sqlFile.version, versionNo)) {
                    return prev;
                }
                const sql = fs.readFileSync(sqlFile.path, 'utf-8');
                console.log(`SQL 파일 실행 중: ${sqlFile.path}`);
                return connection.query(sql).then(() => {
                    console.log(`SQL 파일 실행 성공: ${sqlFile.path}`);
                });
            }, Promise.resolve());
        })
        .then(() => {
            console.log('업그레이드 성공.');
        })
        .catch((e) => {
            console.error(e);
        })
        .finally(() => {
            if (connection) connection.end();
        });
} else {
    yargs.showHelp();
}
