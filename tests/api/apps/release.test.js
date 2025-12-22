const { app } = require('../../../bin/app');
const request = require('supertest')(app);
const should = require('should');
const path = require('path');
const _ = require('lodash');

const SLEEP_TIME = 5000;

describe('api/apps/release.test.js', function () {
    var account = '522539441@qq.com';
    var password = '123456';
    var authToken;
    var machineName = `Login-${Math.random()}`;
    var friendlyName = `Login-${Math.random()}`;
    var appName = 'Demo-ios';
    var bearerToken;

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

    describe('create accessKeys', function (done) {
        it('should create accessKeys successful', function (done) {
            request
                .post(`/accessKeys`)
                .set('Authorization', `Basic ${authToken}`)
                .send({
                    createdBy: machineName,
                    friendlyName: friendlyName,
                    ttl: 30 * 24 * 60 * 60,
                })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('accessKey');
                    rs.accessKey.should.have.properties([
                        'name',
                        'createdTime',
                        'createdBy',
                        'expires',
                        'description',
                        'friendlyName',
                    ]);
                    bearerToken = _.get(rs, 'accessKey.name');
                    done();
                });
        });
    });

    describe('add apps', function (done) {
        it('should add apps successful', function (done) {
            request
                .post(`/apps`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send({ name: appName, os: 'iOS', platform: 'React-Native' })
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('app');
                    rs.app.should.have.properties(['name', 'collaborators']);
                    done();
                });
        });
    });

    describe('release apps', function (done) {
        it('should release apps successful', function (done) {
            request
                .post(`/apps/${appName}/deployments/Staging/release`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .attach('package', path.resolve(__dirname, './bundle.zip'))
                .field(
                    'packageInfo',
                    `{"appVersion": "1.0.0", "description": "test", "isMandatory": false}`,
                )
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });

        it('should release apps v2 successful', function (done) {
            request
                .post(`/apps/${appName}/deployments/Staging/release`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .attach('package', path.resolve(__dirname, './bundle_v2.zip'))
                .field(
                    'packageInfo',
                    `{"appVersion": "1.0.0", "description": "test", "isMandatory": false}`,
                )
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    setTimeout(function () {
                        done();
                    }, SLEEP_TIME);
                });
        });
    });

    describe('promote apps', function (done) {
        it('should promote apps successful', function (done) {
            request
                .post(`/apps/${appName}/deployments/Staging/promote/Production`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    res.status.should.equal(200);
                    done();
                });
        });
    });

    describe('rollback deployments', function (done) {
        it('should rollback deployments successful when point labels', function (done) {
            request
                .post(`/apps/${appName}/deployments/Staging/rollback/v1`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`ok`);
                    setTimeout(function () {
                        done();
                    }, SLEEP_TIME);
                });
        });

        it('should rollback deployments successful', function (done) {
            request
                .post(`/apps/${appName}/deployments/Staging/rollback`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`ok`);
                    setTimeout(function () {
                        done();
                    }, SLEEP_TIME);
                });
        });
    });

    describe('show deployments history', function (done) {
        it('should not show deployments history successful where deployments does not exist', function (done) {
            request
                .get(`/apps/${appName}/deployments/Test/history`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(406);
                    res.text.should.equal(`does not find the deployment`);
                    done();
                });
        });

        it('should show deployments history successful', function (done) {
            request
                .get(`/apps/${appName}/deployments/Staging/history`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('history');
                    rs.history.should.be.an.instanceOf(Array);
                    rs.history.should.matchEach(function (it) {
                        return it.should.have.properties([
                            'description',
                            'isDisabled',
                            'isMandatory',
                            'rollout',
                            'appVersion',
                            'packageHash',
                            'blobUrl',
                            'size',
                            'manifestBlobUrl',
                            'diffPackageMap',
                            'releaseMethod',
                            'uploadTime',
                            'originalLabel',
                            'originalDeployment',
                            'label',
                            'releasedBy',
                        ]);
                    });
                    done();
                });
        });
    });

    describe('delete deployments history', function (done) {
        it('should not delete deployments history successful where deployments does not exist', function (done) {
            request
                .delete(`/apps/${appName}/deployments/Test/history`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(406);
                    res.text.should.equal(`does not find the deployment`);
                    done();
                });
        });

        it('should delete deployments history successful', function (done) {
            request
                .delete(`/apps/${appName}/deployments/Staging/history`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    res.text.should.equal(`ok`);
                    done();
                });
        });
    });

    describe('show deployments metrics', function (done) {
        it('should not show deployments metrics successful where deployments does not exist', function (done) {
            request
                .get(`/apps/${appName}/deployments/Test/metrics`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    done();
                });
        });

        it('should show deployments metrics successful', function (done) {
            request
                .get(`/apps/${appName}/deployments/Staging/metrics`)
                .set('Authorization', `Bearer ${bearerToken}`)
                .send()
                .end(function (err, res) {
                    should.not.exist(err);
                    res.status.should.equal(200);
                    var rs = JSON.parse(res.text);
                    rs.should.have.properties('metrics');
                    rs.metrics.should.be.an.instanceOf(Object);
                    rs.metrics.should.matchEach(function (it) {
                        return it.should.have.properties([
                            'active',
                            'downloaded',
                            'failed',
                            'installed',
                        ]);
                    });
                    done();
                });
        });
    });
});
