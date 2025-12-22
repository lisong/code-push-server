const { app } = require('../../../bin/app');
const request = require('supertest')(app);
const should = require('should');
const _ = require('lodash');

const { config } = require('../../../bin/core/config');

describe('api/auth/test.js', function () {
    var account = '522539441@qq.com';
    var password = '123456';

    describe('sign in view', function (done) {
        it('should show sign in view successful', function (done) {
            request
                .get('/auth/login')
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('sign up view', function (done) {
        it('should show sign in redirect view if sign up not enabled', function (done) {
            _.set(config, 'common.allowRegistration', false);
            request
                .get('/auth/register')
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(302);
                    done();
                });
        });

        it('should show sign up view successful', function (done) {
            _.set(config, 'common.allowRegistration', true);
            request
                .get('/auth/register')
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('sign in', function (done) {
        it('should not sign in successful when account is empty', function (done) {
            request
                .post('/auth/login')
                .send({
                    account: '',
                    password: password,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: '请您输入邮箱地址',
                    });
                    done();
                });
        });
        it('should not sign in successful when account is not exist', function (done) {
            request
                .post('/auth/login')
                .send({
                    account: account + '1',
                    password: password,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: '您输入的邮箱或密码有误',
                    });
                    done();
                });
        });
        it('should not sign in successful when password is wrong', function (done) {
            request
                .post('/auth/login')
                .send({
                    account: account,
                    password: password + '1',
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: '您输入的邮箱或密码有误',
                    });
                    done();
                });
        });
        it('should sign in successful', function (done) {
            request
                .post('/auth/login')
                .send({
                    account: account,
                    password: password,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({ status: 'OK' });
                    done();
                });
        });
    });

    describe('logout', function (done) {
        it('should logout successful', function (done) {
            request.post('/auth/logout').end(function (err, res) {
                should.not.exist(err);
                res.text.should.equal('ok');
                done();
            });
        });
    });

    describe('link', function (done) {
        it('should link successful', function (done) {
            request.get('/auth/link').end(function (err, res) {
                should.not.exist(err);
                res.headers.location.should.equal('/auth/login');
                done();
            });
        });
    });
});
