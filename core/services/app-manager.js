'use strict';
var Promise = require('bluebird');
var models = require('../../models');
var _ = require('lodash');
var security = require('../../core/utils/security');
var AppError = require('../app-error');

var proto = module.exports = function (){
  function AppManager() {

  }
  AppManager.__proto__ = proto;
  return AppManager;
};

proto.findAppByName = function (uid, appName) {
  return models.Apps.findOne({where: {name: appName, uid: uid}});
};

proto.addApp = function (uid, appName, os, platform, identical) {
  return models.sequelize.transaction((t) => {
    return models.Apps.create({
      name: appName,
      uid: uid,
      os: os,
      platform: platform
    },{
      transaction: t
    })
    .then((apps) => {
      var constName = require('../const');
      var appId = apps.id;
      var deployments = [];
      var deployments_names = process.env.DEFAULT_DEPLOYMENTS ? process.env.DEFAULT_DEPLOYMENTS.split(":") : [constName.PRODUCTION, constName.STAGING];
      for(var i in deployments_names) {
        var deploymentKey = security.randToken(28) + identical;
        deployments.push({
          appid: appId,
          name: deployments_names[i],
          last_deployment_version_id: 0,
          label_id: 0,
          deployment_key: deploymentKey
        });
      }
      return Promise.all([
        models.Collaborators.create({appid: appId, uid: uid, roles: "Owner"}, {transaction: t}),
        models.Deployments.bulkCreate(deployments, {transaction: t})
      ]);
    });
  });
};

proto.deleteApp = function (appId) {
  return models.sequelize.transaction((t) => {
    return Promise.all([
      models.Apps.destroy({where: {id: appId}, transaction: t}),
      models.Collaborators.destroy({where: {appid: appId}, transaction: t}),
      models.Deployments.destroy({where: {appid: appId}, transaction: t})
    ]);
  });
};

proto.modifyApp = function (appId, params) {
  return models.Apps.update(params, {where: {id:appId}})
  .spread((affectedCount, affectedRows) => {
    if (!_.gt(affectedCount, 0)) {
      throw AppError.AppError('modify errors');
    }
    return affectedCount;
  });
};

proto.transferApp = function (appId, fromUid, toUid) {
  return models.sequelize.transaction((t) => {
    return Promise.all([
      models.Apps.update({uid: toUid}, {where: {id: appId}, transaction: t}),
      models.Collaborators.destroy({where: {appid: appId, uid: fromUid}, transaction: t}),
      models.Collaborators.destroy({where: {appid: appId, uid: toUid}, transaction: t}),
      models.Collaborators.create({appid: appId, uid: toUid, roles: "Owner"}, {transaction: t})
    ]);
  });
};

proto.listApps = function (uid) {
  const self = this;
  return models.Collaborators.findAll({where : {uid: uid}})
  .then((data) => {
    if (_.isEmpty(data)){
      return [];
    } else {
      var appIds = _.map(data, (v) => { return v.appid });
      var Sequelize = require('sequelize');
      return models.Apps.findAll({where: {id: {[Sequelize.Op.in]: appIds}}});
    }
  })
  .then((appInfos) => {
    var rs = Promise.map(_.values(appInfos), (v) => {
      return self.getAppDetailInfo(v, uid)
      .then((info) => {
        var constName = require('../const');
        if (info.os == constName.IOS) {
          info.os = constName.IOS_NAME;
        } else if (info.os == constName.ANDROID) {
          info.os = constName.ANDROID_NAME;
        } else if (info.os == constName.WINDOWS) {
          info.os = constName.WINDOWS_NAME;
        }
        if (info.platform == constName.REACT_NATIVE) {
          info.platform = constName.REACT_NATIVE_NAME;
        } else if (info.platform == constName.CORDOVA) {
          info.platform = constName.CORDOVA_NAME;
        }
        return info;
      });
    });
    return rs;
  });
};

proto.getAppDetailInfo  = function (appInfo, currentUid) {
  var appId = appInfo.get('id');
  return Promise.all([
    models.Deployments.findAll({where: {appid: appId}}),
    models.Collaborators.findAll({where: {appid: appId}}),
  ])
  .spread((deploymentInfos, collaboratorInfos) => {
    return Promise.props({
      collaborators: Promise.reduce(collaboratorInfos, (allCol, collaborator) => {
        return models.Users.findOne({where: {id: collaborator.get('uid')}})
        .then((u) => {
          var isCurrentAccount = false;
          if (_.eq(u.get('id'), currentUid)) {
            isCurrentAccount = true;
          }
          allCol[u.get('email')] = {permission: collaborator.get('roles'), isCurrentAccount: isCurrentAccount};
          return allCol;
        });
      }, {}),

      deployments: _.map(deploymentInfos, (item) => {
        return _.get(item, 'name');
      }),
      os: appInfo.get('os'),
      platform: appInfo.get('platform'),
      name: appInfo.get('name'),
      id: appInfo.get('id')
    });
  });
};
