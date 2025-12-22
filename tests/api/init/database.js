const mysql = require('mysql2');
const should = require('should');
const fs = require('fs');
const path = require('path');

const { config } = require('../../../bin/core/config');
const { redisClient } = require('../../../bin/core/utils/connections');

describe('api/init/database.js', function () {
    describe('create database', function (done) {
        it('should create database successful', function (done) {
            var connection = mysql.createConnection({
                host: config.db.host,
                user: config.db.username,
                password: config.db.password,
                multipleStatements: true,
            });
            connection.connect();
            connection.query(
                `DROP DATABASE IF EXISTS ${config.db.database};CREATE DATABASE IF NOT EXISTS ${config.db.database}`,
                function (err, rows, fields) {
                    should.not.exist(err);
                    done();
                },
            );
            connection.end();
        });
    });

    describe('flushall redis', function (done) {
        it('should flushall redis successful', function (done) {
            redisClient
                .flushAll()
                .then(function (reply) {
                    reply.toLowerCase().should.equal('ok');
                    done();
                })
                .catch(function (err) {
                    should.not.exist(err);
                });
        });
    });

    describe('import data from sql files', function (done) {
        var connection;
        before(function () {
            connection = mysql.createConnection({
                host: config.db.host,
                user: config.db.username,
                password: config.db.password,
                database: config.db.database,
                multipleStatements: true,
            });
            connection.connect();
        });

        after(function () {
            connection.end();
        });

        it('should import data codepush-all.sql successful', function (done) {
            var sql = fs.readFileSync(
                path.resolve(__dirname, '../../../sql/codepush-all.sql'),
                'utf-8',
            );
            connection.query(sql, function (err, results) {
                should.not.exist(err);
                done();
            });
        });
    });
});
