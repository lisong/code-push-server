const { app } = require('../../../bin/app');
const request = require('supertest')(app);
const should = require('should');
const _ = require('lodash');

const { md5 } = require('../../../bin/core/utils/security');
const { redisClient } = require('../../../bin/core/utils/connections');

describe('api/users/users.test.js', function () {
    var accountExist = 'lisong2010@gmail.com';
    var account = '522539441@qq.com';
    var registerKey = `REGISTER_CODE_${md5(account)}`;
    var password = '654321';
    var newPassword = '123456';

    describe('check email does exists', function (done) {
        it('should not check email successful when not input email', function (done) {
            request
                .get(`/users/exists`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: '请您输入邮箱地址',
                    });
                    done();
                });
        });

        it('should not exists account when sign up', function (done) {
            request
                .get(`/users/exists?email=${account}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'OK',
                        exists: false,
                    });
                    done();
                });
        });
    });

    describe('send register code to email', function (done) {
        it('should not send register code successful when not input email', function (done) {
            request
                .post(`/users/registerCode`)
                .send({})
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: '请您输入邮箱地址',
                    });
                    done();
                });
        });

        it('should not send register code successful when email already exists', function (done) {
            request
                .post(`/users/registerCode`)
                .send({ email: accountExist })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `"${accountExist}" 已经注册过，请更换邮箱注册`,
                    });
                    done();
                });
        });

        it('should send register successful', function (done) {
            request
                .post(`/users/registerCode`)
                .send({ email: account })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({ status: 'OK' });
                    done();
                });
        });
    });

    describe('check register code', function (done) {
        var token = 'invalid token';
        var account2 = '522539441@qq.com2';
        var storageToken;
        before(function (done) {
            redisClient.get(registerKey).then(function (t) {
                storageToken = t;
                done();
            });
        });

        it('should not check register code successful when email already exists', function (done) {
            request
                .get(`/users/registerCode/exists?email=${accountExist}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `"${accountExist}" 已经注册过，请更换邮箱注册`,
                    });
                    done();
                });
        });

        it('should not check register code successful when token expired', function (done) {
            request
                .get(`/users/registerCode/exists?email=${account2}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `验证码已经失效，请您重新获取`,
                    });
                    done();
                });
        });

        it('should not check register code successful when token is invalid', function (done) {
            request
                .get(`/users/registerCode/exists?email=${account}&token=${token}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `您输入的验证码不正确，请重新输入`,
                    });
                    done();
                });
        });

        it('should check register code successful', function (done) {
            request
                .get(`/users/registerCode/exists?email=${account}&token=${storageToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({ status: 'OK' });
                    done();
                });
        });
    });

    describe('sign up', function (done) {
        var storageToken;
        before(function (done) {
            redisClient.get(registerKey).then(function (t) {
                storageToken = t;
                done();
            });
        });

        it('should not sign up successful when password length invalid', function (done) {
            request
                .post(`/users`)
                .send({ email: account, password: '1234', token: storageToken })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `请您输入6～20位长度的密码`,
                    });
                    done();
                });
        });

        it('should sign up successful', function (done) {
            request
                .post(`/users`)
                .send({ email: account, password: password, token: storageToken })
                .end(function (err, res) {
                    should.not.exist(err);
                    JSON.parse(res.text).should.containEql({ status: 'OK' });
                    done();
                });
        });
    });

    describe('change password', function (done) {
        var authToken;
        before(function (done) {
            request
                .post('/auth/login')
                .send({
                    account: account,
                    password: password,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    var rs = JSON.parse(res.text);
                    rs.should.containEql({ status: 'OK' });
                    authToken = Buffer.from(`auth:${_.get(rs, 'results.tokens')}`).toString(
                        'base64',
                    );
                    done();
                });
        });

        it('should not change password successful when authToken invalid', function (done) {
            request
                .patch(`/users/password`)
                .set('Authorization', `Basic 11345`)
                .send({ oldPassword: password, newPassword: newPassword })
                .end(function (err, res) {
                    should.not.exist(err);
                    var rs = JSON.parse(res.text);
                    res.status.should.equal(200);
                    rs.should.containEql({ status: 401 });
                    done();
                });
        });

        it('should not change password successful where password invalid', function (done) {
            request
                .patch(`/users/password`)
                .set('Authorization', `Basic ${authToken}`)
                .send({ oldPassword: '123321', newPassword: newPassword })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `您输入的旧密码不正确，请重新输入`,
                    });
                    done();
                });
        });

        it('should not change password successful where new password invalid', function (done) {
            request
                .patch(`/users/password`)
                .set('Authorization', `Basic ${authToken}`)
                .send({ oldPassword: password, newPassword: '1234' })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    JSON.parse(res.text).should.containEql({
                        status: 'ERROR',
                        message: `请您输入6～20位长度的新密码`,
                    });
                    done();
                });
        });

        it('should change password successful', function (done) {
            request
                .patch(`/users/password`)
                .set('Authorization', `Basic ${authToken}`)
                .send({ oldPassword: password, newPassword: newPassword })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    JSON.parse(res.text).should.containEql({ status: 'OK' });
                    done();
                });
        });
    });

    describe('user modules', function (done) {
        var authToken;
        before(function (done) {
            request
                .post('/auth/login')
                .send({
                    account: account,
                    password: newPassword,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    var rs = JSON.parse(res.text);
                    rs.should.containEql({ status: 'OK' });
                    authToken = Buffer.from(`auth:${_.get(rs, 'results.tokens')}`).toString(
                        'base64',
                    );
                    done();
                });
        });

        it('should get userinfo successful', function (done) {
            request
                .get(`/users`)
                .set('Authorization', `Basic ${authToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });
});
