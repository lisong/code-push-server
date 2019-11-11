'use strict';
var Promise = require('bluebird');
var models = require('../../models');
var _ = require('lodash');
var common = require('../utils/common');
var factory = require('../utils/factory');
var AppError = require('../app-error');
var config    = require('../config');
var log4js = require('log4js');
var log = log4js.getLogger("cps:ClientManager");
// log.level = 'debug';
var Sequelize = require('sequelize');

var proto = module.exports = function (){
  function ClientManager() {

  }
  ClientManager.__proto__ = proto;
  return ClientManager;
};

const UPDATE_CHECK = "UPDATE_CHECK";
const CHOSEN_MAN = "CHOSEN_MAN";
const EXPIRED = 600;

proto.getUpdateCheckCacheKey = function(deploymentKey, appVersion, label, packageHash) {
  return [UPDATE_CHECK, deploymentKey, appVersion, label, packageHash].join(':');
}

proto.clearUpdateCheckCache = function(deploymentKey, appVersion, label, packageHash) {
  log.debug('clear cache Deployments key:', deploymentKey);
  let redisCacheKey = this.getUpdateCheckCacheKey(deploymentKey, appVersion, label, packageHash);
  var client = factory.getRedisClient("default");
  return client.keysAsync(redisCacheKey)
  .then((data) => {
    if (_.isArray(data)) {
      return Promise.map(data, (key) => {
        return client.delAsync(key);
      });
    }
    return null;
  })
  .finally(() => client.quit());
}

proto.updateCheckFromCache = function(deploymentKey, appVersion, label, packageHash, clientUniqueId) {
  const self = this;
  var updateCheckCache = _.get(config, 'common.updateCheckCache', false);
  if (updateCheckCache === false) {
    return self.updateCheck(deploymentKey, appVersion, label, packageHash);
  }
  let redisCacheKey = self.getUpdateCheckCacheKey(deploymentKey, appVersion, label, packageHash);
  var client = factory.getRedisClient("default");
  return client.getAsync(redisCacheKey)
  .then((data) => {
    if (data) {
      try {
        log.debug('updateCheckFromCache read from catch');
        var obj = JSON.parse(data);
        return obj;
      } catch (e) {
      }
    }
    return self.updateCheck(deploymentKey, appVersion, label, packageHash, clientUniqueId)
    .then((rs) => {
      try {
        log.debug('updateCheckFromCache read from db');
        var strRs = JSON.stringify(rs);
        client.setexAsync(redisCacheKey, EXPIRED, strRs);
      } catch (e) {
      }
      return rs;
    });
  })
  .finally(() => client.quit());
}

proto.getChosenManCacheKey = function(packageId, rollout, clientUniqueId) {
  return [CHOSEN_MAN, packageId, rollout, clientUniqueId].join(':');
}

proto.random = function(rollout) {
  var r = Math.ceil(Math.random()*10000);
  if (r < rollout * 100) {
    return Promise.resolve(true);
  } else {
    return Promise.resolve(false);
  }
}

proto.chosenMan = function (packageId, rollout, clientUniqueId) {
  var self = this;
  if (rollout >= 100) {
    return Promise.resolve(true);
  }
  var rolloutClientUniqueIdCache = _.get(config, 'common.rolloutClientUniqueIdCache', false);
  if (rolloutClientUniqueIdCache === false) {
    return self.random(rollout);
  } else {
    var client = factory.getRedisClient("default");
    var redisCacheKey = self.getChosenManCacheKey(packageId, rollout, clientUniqueId);
    return client.getAsync(redisCacheKey)
    .then((data) => {
      if (data == 1) {
        return true;
      } else if (data == 2) {
        return false;
      } else {
        return self.random(rollout)
        .then((r)=>{
           return client.setexAsync(redisCacheKey, 60*60*24*7, r ? 1:2)
            .then(()=>{
              return r;
            });
        });
      }
    })
    .finally(() => client.quit());
  }
}

proto.updateCheck = function(deploymentKey, appVersion, label, packageHash, clientUniqueId) {
  var rs = {
    packageId: 0,
    downloadURL: "",
    downloadUrl: "",
    description: "",
    isAvailable: false,
    isMandatory: false,
    appVersion: appVersion,
    targetBinaryRange: "",
    packageHash: "",
    label: "",
    packageSize: 0,
    updateAppVersion: false,
    shouldRunBinaryVersion: false,
    rollout: 100
  };
  var self = this;
  if (_.isEmpty(deploymentKey) || _.isEmpty(appVersion)) {
    return Promise.reject(new AppError.AppError("please input deploymentKey and appVersion"))
  }
  return models.Deployments.findOne({where: {deployment_key: deploymentKey}})
  .then((dep) => {
    if (_.isEmpty(dep)) {
      throw new AppError.AppError('Not found deployment, check deployment key is right.');
    }
    var version = common.parseVersion(appVersion);
    return models.DeploymentsVersions.findAll({where: {
      deployment_id: dep.id,
      min_version: { [Sequelize.Op.lte]: version },
      max_version: { [Sequelize.Op.gt]: version }
    }})
    .then((deploymentsVersionsMore) => {
      var distance = 0;
      var item = null;
      _.map(deploymentsVersionsMore, function(value, index) {
        if (index == 0) {
          item = value;
          distance = value.max_version - value.min_version;
        } else {
          if (distance > (value.max_version - value.min_version)) {
            distance = value.max_version - value.min_version;
            item = value;
          }
        }
      });
      log.debug(item);
      return item;
    });
  })
  .then((deploymentsVersions) => {
    if(!deploymentsVersions){return;}
    var currentDeploymentVersionId = deploymentsVersions.id;
    return models.Packages.findAll({
      where:{
        // 考虑到用户当前已经更新的版本可能已经是disabled 状态了，所以不能在查找数据的时候就过滤掉，只能后续的逻辑上处理
        // is_disabled: 0, //只找没有被禁用的版本，毕竟返回给前端的应该是完全OK的版本才对
        deployment_version_id: currentDeploymentVersionId
      },
      order: [['id','asc']],
    }).then((packages) => {
      if(!(packages && packages.length)){
        return;
      }
      // 非disabled状态的数组
      var nonDisabledPackages = _.filter(packages, function (value) {
        return !value.is_disabled
      });
      if(!(nonDisabledPackages && nonDisabledPackages.length)){
        return;
      }
      // 这里基于id一定是逐渐递增的，所以最后一个非disabled的就是需要更新的包
      var latestPackage = nonDisabledPackages[nonDisabledPackages.length -1];
      if (_.eq(latestPackage.deployment_id, deploymentsVersions.deployment_id)
        && !_.eq(latestPackage.package_hash, packageHash)) {
        rs.packageId = latestPackage.id;
        rs.targetBinaryRange = deploymentsVersions.app_version;
        rs.downloadUrl = rs.downloadURL = common.getBlobDownloadUrl(_.get(latestPackage, 'blob_url'));
        rs.description = _.get(latestPackage, 'description', '');
        rs.isAvailable = _.eq(latestPackage.is_disabled, 1) ? false : true;
        rs.isMandatory = _.eq(latestPackage.is_mandatory, 1) ? true : false;
        rs.appVersion = appVersion;
        rs.packageHash = _.get(latestPackage, 'package_hash', '');
        rs.label = _.get(latestPackage, 'label', '');
        rs.packageSize = _.get(latestPackage, 'size', 0);
        rs.rollout = _.get(latestPackage, 'rollout', 100);
      }
      // 强制更新字段需要重新处理，对于多版本的情况下，需要判断当前客户端版本到 最新版本之前是否有强制更新，有的话需要将强制更新赋值成1
      // 非强制更新时才做这个处理，强制的不需要额外处理
      if(!latestPackage.is_mandatory){
        log.debug('non mandatory, further check',currentDeploymentVersionId);
        // 先找到当前客户端的版本
        var currentClientPackage = _.find(packages, function (value) {
          return value.package_hash === packageHash
        });
        var currentClientPackageId = currentClientPackage ? currentClientPackage.id : 0;

        // 这里还是基于Package中id是增长的
        var mandatory = _.findIndex(_.filter(nonDisabledPackages,function (value) {
          return value.id > currentClientPackageId;
        }), function (value) {
          return value.is_mandatory
        }) !== -1;
        log.debug('mandatory', mandatory);
        rs.isMandatory = mandatory;
      }
      log.debug(rs);
      return latestPackage;
    })
    .then((packages) => {
      //增量更新
      if (!_.isEmpty(packages) && !_.eq(_.get(packages, 'package_hash', ""), packageHash)) {
        return models.PackagesDiff.findOne({where: {package_id:packages.id, diff_against_package_hash: packageHash}})
        .then((diffPackage) => {
          if (!_.isEmpty(diffPackage)) {
            rs.downloadURL = common.getBlobDownloadUrl(_.get(diffPackage, 'diff_blob_url'));
            rs.downloadUrl = common.getBlobDownloadUrl(_.get(diffPackage, 'diff_blob_url'));
            rs.packageSize = _.get(diffPackage, 'diff_size', 0);
          }
          return;
        });
      } else {
        return;
      }
    });
  })
  .then(() => {
    return rs;
  });
};

proto.getPackagesInfo = function (deploymentKey, label) {
  if (_.isEmpty(deploymentKey) || _.isEmpty(label)) {
    return Promise.reject(new AppError.AppError("please input deploymentKey and label"))
  }
  return models.Deployments.findOne({where: {deployment_key: deploymentKey}})
  .then((dep) => {
    if (_.isEmpty(dep)) {
      throw new AppError.AppError('does not found deployment');
    }
    return models.Packages.findOne({where: {deployment_id: dep.id, label: label}});
  })
  .then((packages) => {
    if (_.isEmpty(packages)) {
      throw new AppError.AppError('does not found packages');
    }
    return packages;
  });
};

proto.reportStatusDownload = function(deploymentKey, label, clientUniqueId) {
  return this.getPackagesInfo(deploymentKey, label)
  .then((packages) => {
    return Promise.all([
      models.PackagesMetrics.findOne({where: {package_id: packages.id}})
      .then((metrics)=>{
        if (metrics) {
          return metrics.increment('downloaded');
        }
        return;
      }),
      models.LogReportDownload.create({
        package_id: packages.id,
        client_unique_id: clientUniqueId
      })
    ]);
  });
};

proto.reportStatusDeploy = function (deploymentKey, label, clientUniqueId, others) {
  return this.getPackagesInfo(deploymentKey, label)
  .then((packages) => {
    var constConfig = require('../const');
    var statusText =  _.get(others, "status");
    var status = 0;
    if (_.eq(statusText, "DeploymentSucceeded")) {
      status = constConfig.DEPLOYMENT_SUCCEEDED;
    } else if (_.eq(statusText, "DeploymentFailed")) {
      status = constConfig.DEPLOYMENT_FAILED;
    }
    var packageId = packages.id;
    var previous_deployment_key = _.get(others, 'previousDeploymentKey');
    var previous_label = _.get(others, 'previousLabelOrAppVersion');
    if (status > 0) {
      return Promise.all([
        models.LogReportDeploy.create({
          package_id: packageId,
          client_unique_id: clientUniqueId,
          previous_label: previous_label,
          previous_deployment_key: previous_deployment_key,
          status: status
        }),
        models.PackagesMetrics.findOne({where: {package_id: packageId}})
        .then((metrics)=>{
          if (_.isEmpty(metrics)) {
            return;
          }
          if (_.eq(status, constConfig.DEPLOYMENT_SUCCEEDED)) {
            return metrics.increment(['installed', 'active'],{by: 1});
          } else {
            return metrics.increment(['installed', 'failed'],{by: 1});
          }
        })
      ])
      .then(()=>{
        if (previous_deployment_key && previous_label) {
          return models.Deployments.findOne({where: {deployment_key: previous_deployment_key}})
          .then((dep)=>{
            if (_.isEmpty(dep)) {
              return;
            }
            return models.Packages.findOne({where: {deployment_id: dep.id, label: previous_label}})
            .then((p)=>{
              if (_.isEmpty(p)) {
                return;
              }
              return models.PackagesMetrics.findOne({where:{package_id: p.id}});
            });
          })
          .then((metrics)=>{
            if (metrics) {
              return metrics.decrement('active');
            }
            return;
          });
        }
        return;
      });
    } else {
      return;
    }
  });
};
