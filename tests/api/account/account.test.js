const { app } = require('../../../bin/app');
const request = require('supertest')(app);
const should = require('should');
const _ = require('lodash');

describe('api/account/account.test.js', function () {
    var account = '522539441@qq.com';
    var password = '123456';

    describe('user modules', function (done) {
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

        it('should get account info successful', function (done) {
            request
                .get(`/account`)
                .set('Authorization', `Basic ${authToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('account');
                    rs.account.should.have.properties(['email', 'linkedProviders', 'name']);
                    done();
                });
        });
    });
});
