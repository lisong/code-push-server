const { app } = require('../../../bin/app');
const request = require('supertest')(app);
const should = require('should');
const _ = require('lodash');

describe('api/index/index.test.js', function () {
    var account = '522539441@qq.com';
    var password = '123456';
    var authToken;
    var appName = 'Demo-ios';
    var deploymentKey;
    var packageHash;
    var label;
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
                authToken = Buffer.from(`auth:${_.get(rs, 'results.tokens')}`).toString('base64');
                done();
            });
    });

    describe('list apps all deployments', function (done) {
        it('should list apps all deployments successful', function (done) {
            request
                .get(`/apps/${appName}/deployments`)
                .set('Authorization', `Basic ${authToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('deployments');
                    rs.deployments.should.be.an.instanceOf(Array);
                    rs.deployments.should.matchEach(function (it) {
                        if (_.get(it, 'name') == 'Production') {
                            deploymentKey = _.get(it, 'key');
                            packageHash = _.get(it, 'package.packageHash');
                            label = _.get(it, 'package.label');
                        }
                        return it.should.have.properties([
                            'createdTime',
                            'id',
                            'key',
                            'name',
                            'package',
                        ]);
                    });
                    done();
                });
        });
    });

    describe('render index views', function (done) {
        it('should render index views successful', function (done) {
            request
                .get(`/`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('render tokens views', function (done) {
        it('should render tokens views successful', function (done) {
            request
                .get(`/tokens`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('authenticated', function (done) {
        it('should authenticated successful', function (done) {
            request
                .get(`/authenticated`)
                .set('Authorization', `Basic ${authToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('updateCheck', function (done) {
        it('should not updateCheck successful where deploymentKey is empty', function (done) {
            request
                .get(`/updateCheck?deploymentKey=&appVersion=1.0.0&label=&packageHash=`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(404);
                    res.text.should.equal(`please input deploymentKey and appVersion`);
                    done();
                });
        });

        it('should not updateCheck successful where deploymentKey does not exist', function (done) {
            request
                .get(`/updateCheck?deploymentKey=123&appVersion=1.0.0&label=&packageHash=`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(404);
                    res.text.should.equal(`Not found deployment, check deployment key is right.`);
                    done();
                });
        });

        it('should updateCheck successful', function (done) {
            request
                .get(
                    `/updateCheck?deploymentKey=${deploymentKey}&appVersion=1.0.0&label=&packageHash=`,
                )
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('updateInfo');
                    rs.updateInfo.should.have.properties([
                        'downloadURL',
                        'description',
                        'isAvailable',
                        'isMandatory',
                        'appVersion',
                        'packageHash',
                        'label',
                        'packageSize',
                        'updateAppVersion',
                        'shouldRunBinaryVersion',
                    ]);
                    rs.updateInfo.isAvailable.should.be.true;
                    done();
                });
        });

        it('should updateCheck successful when packageHash is newer', function (done) {
            request
                .get(
                    `/updateCheck?deploymentKey=${deploymentKey}&appVersion=1.0.0&label=&packageHash=${packageHash}`,
                )
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('updateInfo');
                    rs.updateInfo.should.have.properties([
                        'downloadURL',
                        'description',
                        'isAvailable',
                        'isMandatory',
                        'appVersion',
                        'packageHash',
                        'label',
                        'packageSize',
                        'updateAppVersion',
                        'shouldRunBinaryVersion',
                    ]);
                    rs.updateInfo.isAvailable.should.be.false;
                    done();
                });
        });
    });

    describe('reportStatus download', function (done) {
        it('should reportStatus download successful', function (done) {
            request
                .post(`/reportStatus/download`)
                .send({
                    clientUniqueId: Math.random(),
                    label: label,
                    deploymentKey: deploymentKey,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`OK`);
                    setTimeout(function () {
                        done();
                    }, 1000);
                });
        });
    });

    describe('reportStatus deploy', function (done) {
        it('should reportStatus deploy successful', function (done) {
            request
                .post(`/reportStatus/deploy`)
                .send({
                    clientUniqueId: Math.random(),
                    label: label,
                    deploymentKey: deploymentKey,
                    status: 'DeploymentSucceeded',
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`OK`);
                    setTimeout(function () {
                        done();
                    }, 1000);
                });
        });

        it('should reportStatus deploy successful', function (done) {
            request
                .post(`/reportStatus/deploy`)
                .send({
                    clientUniqueId: Math.random(),
                    label: label,
                    deploymentKey: deploymentKey,
                    status: 'DeploymentFailed',
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`OK`);
                    setTimeout(function () {
                        done();
                    }, 1000);
                });
        });
    });
});
