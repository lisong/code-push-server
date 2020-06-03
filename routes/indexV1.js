var express = require('express');
var router = express.Router();
var Promise = require('bluebird');
var AppError = require('../core/app-error');
var middleware = require('../core/middleware');
var ClientManager = require('../core/services/client-manager');
var _ = require('lodash');
var log4js = require('log4js');
var log = log4js.getLogger("cps:indexV1");

router.get('/update_check', (req, res, next) => {
  var deploymentKey = _.get(req, "query.deployment_key");
  var appVersion = _.get(req, "query.app_version");
  var label = _.get(req, "query.label");
  var packageHash = _.get(req, "query.package_hash")
  var isCompanion = _.get(req, "query.is_companion")
  var clientUniqueId = _.get(req, "query.client_unique_id")
  var clientManager = new ClientManager();
  log.debug('req.query', req.query);
  clientManager.updateCheckFromCache(deploymentKey, appVersion, label, packageHash, clientUniqueId)
  .then((rs) => {
    //灰度检测
    return clientManager.chosenMan(rs.packageId, rs.rollout, clientUniqueId)
    .then((data)=>{
      if (!data) {
        rs.isAvailable = false;
        return rs;
      }
      return rs;
    });
  })
  .then((rs) => {
    delete rs.packageId;
    delete rs.rollout;
    var update_info = {
        download_url : rs.downloadUrl,
        description : rs.description,
        is_available : rs.isAvailable,
        is_disabled : rs.isDisabled,
        // Note: need to use appVersion here to get it compatible with client side change...
        // https://github.com/microsoft/code-push/commit/7d2ffff395cc54db98aefba7c67889f509e8c249#diff-a937c637a47cbd31cbb52c89bef7d197R138
        target_binary_range: rs.appVersion,
        label: rs.label,
        package_hash: rs.packageHash,
        package_size: rs.packageSize,
        should_run_binary_version: rs.shouldRunBinaryVersion,
        update_app_version: rs.updateAppVersion,
        is_mandatory: rs.isMandatory,
    };
    res.send({"update_info": update_info});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(404).send(e.message);
    } else {
      next(e);
    }
  });
});

router.post('/report_status/download', (req, res) => {
  log.debug('req.body', req.body);
  var clientUniqueId = _.get(req, "body.client_unique_id");
  var label = _.get(req, "body.label");
  var deploymentKey = _.get(req, "body.deployment_key");
  var clientManager = new ClientManager();
  clientManager.reportStatusDownload(deploymentKey, label, clientUniqueId)
  .catch((err) => {
    if (!err instanceof AppError.AppError) {
      console.error(err.stack)
    }
  });
  res.send('OK');
});

router.post('/report_status/deploy', (req, res) => {
  log.debug('req.body', req.body);
  var clientUniqueId = _.get(req, "body.client_unique_id");
  var label = _.get(req, "body.label");
  var deploymentKey = _.get(req, "body.deployment_key");
  var clientManager = new ClientManager();
  clientManager.reportStatusDeploy(deploymentKey, label, clientUniqueId, req.body)
  .catch((err) => {
    if (!err instanceof AppError.AppError) {
      console.error(err.stack)
    }
  });
  res.send('OK');
});

module.exports = router;
