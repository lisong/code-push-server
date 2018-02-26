var express = require('express');
var Promise = require('bluebird');
var router = express.Router();
var _ = require('lodash');
var middleware = require('../core/middleware');
var validator = require('validator');
var accountManager = require('../core/services/account-manager')();
var Deployments = require('../core/services/deployments');
var Collaborators = require('../core/services/collaborators');
var AppManager = require('../core/services/app-manager');
var PackageManager = require('../core/services/package-manager');
var AppError = require('../core/app-error');
var common = require('../core/utils/common');
var config    = require('../core/config');
const REGEX = /^(\w+)(-android|-ios)$/;
const REGEX_ANDROID = /^(\w+)(-android)$/;
const REGEX_IOS = /^(\w+)(-ios)$/;
const OLD_REGEX_ANDROID = /^(android_)/;
const OLD_REGEX_IOS = /^(ios_)/;
var log4js = require('log4js');
var log = log4js.getLogger("cps:apps");

router.get('/',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appManager = new AppManager();
  appManager.listApps(uid)
  .then((data) => {
    res.send({apps: data});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.get('/:appName/deployments',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var deployments = new Deployments();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return deployments.listDeloyments(col.appid);
  })
  .then((data) => {
    res.send({deployments: data});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.get('/:appName/deployments/:deploymentName',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var deployments = new Deployments();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return deployments.findDeloymentByName(deploymentName, col.appid)
  })
  .then((deploymentInfo) => {
    if (_.isEmpty(deploymentInfo)) {
      throw new AppError.AppError("does not find the deployment");
    }
    res.send({deployment: deployments.listDeloyment(deploymentInfo)});
    return true;
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.post('/:appName/deployments',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var name = req.body.name;
  var deployments = new Deployments();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return deployments.addDeloyment(name, col.appid, uid);
  })
  .then((data) => {
    res.send({deployment: {name: data.name, key: data.deployment_key}});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.get('/:appName/deployments/:deploymentName/metrics',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var deployments = new Deployments();
  var packageManager = new PackageManager();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return deployments.findDeloymentByName(deploymentName, col.appid)
    .then((deploymentInfo) => {
      if (_.isEmpty(deploymentInfo)) {
        throw new AppError.AppError("does not find the deployment");
      }
      return deploymentInfo;
    })
  })
  .then((deploymentInfo) => {
    return deployments.getAllPackageIdsByDeploymentsId(deploymentInfo.id);
  })
  .then((packagesInfos) => {
    return Promise.reduce(packagesInfos, (result, v) => {
      return packageManager.getMetricsbyPackageId(v.get('id'))
      .then((metrics) => {
        if (metrics) {
          result[v.get('label')] = {
            active: metrics.get('active'),
            downloaded: metrics.get('downloaded'),
            failed: metrics.get('failed'),
            installed: metrics.get('installed'),
          };
        }
        return result;
      });
    }, {});
  })
  .then((rs) => {
    res.send({"metrics": rs});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.send({"metrics": null});
    } else {
      next(e);
    }
  });
});

router.get('/:appName/deployments/:deploymentName/history',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var deployments = new Deployments();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return deployments.findDeloymentByName(deploymentName, col.appid)
    .then((deploymentInfo) => {
      if (_.isEmpty(deploymentInfo)) {
        throw new AppError.AppError("does not find the deployment");
      }
      return deploymentInfo;
    });
  })
  .then((deploymentInfo) => {
    return deployments.getDeploymentHistory(deploymentInfo.id);
  })
  .then((rs) => {
    res.send({history: rs});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.delete('/:appName/deployments/:deploymentName/history',
  middleware.checkToken, (req, res, next) => {
  var uid = req.users.id;
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var deployments = new Deployments();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return deployments.findDeloymentByName(deploymentName, col.appid)
    .then((deploymentInfo) => {
      if (_.isEmpty(deploymentInfo)) {
        throw new AppError.AppError("does not find the deployment");
      }
      return deploymentInfo;
    });
  })
  .then((deploymentInfo) => {
    return deployments.deleteDeploymentHistory(deploymentInfo.id);
  })
  .then((rs) => {
    res.send("ok");
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.patch('/:appName/deployments/:deploymentName',
  middleware.checkToken, (req, res, next) => {
  var name = req.body.name;
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var uid = req.users.id;
  var deployments = new Deployments();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return deployments.renameDeloymentByName(deploymentName, col.appid, name);
  })
  .then((data) => {
    res.send({deployment: data});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.delete('/:appName/deployments/:deploymentName',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var uid = req.users.id;
  var deployments = new Deployments();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return deployments.deleteDeloymentByName(deploymentName, col.appid);
  })
  .then((data) => {
    res.send({deployment: data});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.post('/:appName/deployments/:deploymentName/release',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var uid = req.users.id;
  var deployments = new Deployments();
  var packageManager = new PackageManager();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    var pubType = '';
    log.debug(`check publish type`);
    if (REGEX_ANDROID.test(appName)) {
      pubType = 'android';
    } else if (REGEX_IOS.test(appName)) {
      pubType = 'ios';
    } else {
      log.debug(`you have to rename app name, eg. Demo-android Demo-ios`);
      throw new AppError.AppError(`you have to rename app name, eg. Demo-android Demo-ios`);
    }
    log.debug(`publish type is ${pubType}`);
    return deployments.findDeloymentByName(deploymentName, col.appid)
    .then((deploymentInfo) => {
      if (_.isEmpty(deploymentInfo)) {
        log.debug(`does not find the deployment`);
        throw new AppError.AppError("does not find the deployment");
      }
      return packageManager.parseReqFile(req)
      .then((data) => {
        return packageManager.releasePackage(deploymentInfo.id, data.packageInfo, data.package.type, data.package.path, uid, pubType)
        .finally(() => {
          common.deleteFolderSync(data.package.path);
        });
      })
      .then((packages) => {
        if (packages) {
          Promise.delay(2000)
          .then(() => {
            packageManager.createDiffPackagesByLastNums(packages.id, _.get(config, 'common.diffNums', 1))
            .catch((e) => {
              console.error(e);
            });
          });
        }
        //clear cache if exists.
        if (_.get(config, 'common.updateCheckCache', false) !== false) {
          Promise.delay(2500)
          .then(() => {
            var ClientManager = require('../core/services/client-manager');
            var clientManager = new ClientManager();
            clientManager.clearUpdateCheckCache(deploymentInfo.deployment_key, '*', '*', '*');
          });
        }
        return null;
      });
    });
  })
  .then((data) => {
    res.send('{"msg": "succeed"}');
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.patch('/:appName/deployments/:deploymentName/release',
  middleware.checkToken, (req, res, next) => {
    return res.status(406).send('Not supported currently');
    var appName = _.trim(req.params.appName);
    var deploymentName = _.trim(req.params.deploymentName);
    var uid = req.users.id;
    var deployments = new Deployments();
    var packageManager = new PackageManager();
    accountManager.collaboratorCan(uid, appName)
    .then((col) => {
      return deployments.findDeloymentByName(deploymentName, col.appid)
      .then((deploymentInfo) => {
        if (_.isEmpty(deploymentInfo)) {
          throw new AppError.AppError("does not find the deployment");
        }
        var label = deploymentInfo.label;
        var deploymentVersionId = deploymentInfo.last_deployment_version_id;
        return packageManager.modifyReleasePackage(deploymentInfo.id, deploymentVersionId, _.get(req, 'body.packageInfo'));
      });
    }).then((data) => {
      res.send("");
    })
    .catch((e) => {
      if (e instanceof AppError.AppError) {
        res.status(406).send(e.message);
      } else {
        next(e);
      }
    });
});

router.post('/:appName/deployments/:sourceDeploymentName/promote/:destDeploymentName',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var sourceDeploymentName = _.trim(req.params.sourceDeploymentName);
  var destDeploymentName = _.trim(req.params.destDeploymentName);
  var uid = req.users.id;
  var packageManager = new PackageManager();
  var deployments = new Deployments();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    var appId = col.appid;
    return Promise.all([
      deployments.findDeloymentByName(sourceDeploymentName, appId),
      deployments.findDeloymentByName(destDeploymentName, appId)
    ])
    .spread((sourceDeploymentInfo, destDeploymentInfo) => {
      if (!sourceDeploymentInfo) {
        throw new AppError.AppError(`${sourceDeploymentName}  does not exist.`);
      }
      if (!destDeploymentInfo) {
        throw new AppError.AppError(`${destDeploymentName}  does not exist.`);
      }
      //clear cache if exists.
      if (_.get(config, 'common.updateCheckCache', false) !== false) {
        Promise.delay(2500)
        .then(() => {
          var ClientManager = require('../core/services/client-manager');
          var clientManager = new ClientManager();
          clientManager.clearUpdateCheckCache(destDeploymentInfo.deployment_key, '*', '*', '*');
        });
      }
      return [sourceDeploymentInfo.id, destDeploymentInfo.id];
    })
    .spread((sourceDeploymentId, destDeploymentId) => {
      return packageManager.promotePackage(sourceDeploymentId, destDeploymentId, uid);
    });
  })
  .then((packages) => {
    if (packages) {
      Promise.delay(2000)
      .then(() => {
        packageManager.createDiffPackagesByLastNums(packages.id, _.get(config, 'common.diffNums', 1))
        .catch((e) => {
          console.log(e);
        });
      });
    }
    return null;
  })
  .then(() => {
     res.send('ok');
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

var rollbackCb = function (req, res, next) {
  var appName = _.trim(req.params.appName);
  var deploymentName = _.trim(req.params.deploymentName);
  var uid = req.users.id;
  var targetLabel = _.trim(_.get(req, 'params.label'));
  var deployments = new Deployments();
  var packageManager = new PackageManager();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return deployments.findDeloymentByName(deploymentName, col.appid);
  })
  .then((dep) => {
    //clear cache if exists.
    if (_.get(config, 'common.updateCheckCache', false) !== false) {
      Promise.delay(2500)
      .then(() => {
        var ClientManager = require('../core/services/client-manager');
        var clientManager = new ClientManager();
        clientManager.clearUpdateCheckCache(dep.deployment_key, '*', '*', '*');
      });
    }
    return packageManager.rollbackPackage(dep.last_deployment_version_id, targetLabel, uid);
  })
  .then(() => {
     res.send('ok');
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
};

router.post('/:appName/deployments/:deploymentName/rollback',
  middleware.checkToken, rollbackCb);

router.post('/:appName/deployments/:deploymentName/rollback/:label',
  middleware.checkToken, rollbackCb);

router.get('/:appName/collaborators',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var uid = req.users.id;
  var collaborators = new Collaborators();
  accountManager.collaboratorCan(uid, appName)
  .then((col) => {
    return collaborators.listCollaborators(col.appid);
  })
  .then((data) => {
    rs = _.reduce(data, (result, value, key) => {
      if (_.eq(key, req.users.email)) {
        value.isCurrentAccount = true;
      }else {
        value.isCurrentAccount = false;
      }
      result[key] = value;
      return result;
    },{});
    res.send({collaborators: rs});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.post('/:appName/collaborators/:email',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var email = _.trim(req.params.email);
  var uid = req.users.id;
  if (!validator.isEmail(email)){
    return res.status(406).send("Invalid Email!");
  }
  var collaborators = new Collaborators();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return accountManager.findUserByEmail(email)
    .then((data) => {
      return collaborators.addCollaborator(col.appid, data.id);
    });
  })
  .then((data) => {
    res.send(data);
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.delete('/:appName/collaborators/:email',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var email = _.trim(decodeURI(req.params.email));
  var uid = req.users.id;
  if (!validator.isEmail(email)){
    return res.status(406).send("Invalid Email!");
  }
  var collaborators = new Collaborators();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return accountManager.findUserByEmail(email)
    .then((data) => {
      if (_.eq(data.id, uid)) {
        throw new AppError.AppError("can't delete yourself!");
      } else {
        return collaborators.deleteCollaborator(col.appid, data.id);
      }
    });
  })
  .then(() => {
    res.send("");
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.delete('/:appName',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var uid = req.users.id;
  var appManager = new AppManager();
  accountManager.ownerCan(uid, appName)
  .then((col) => {
    return appManager.deleteApp(col.appid);
  })
  .then((data) => {
    res.send(data);
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.patch('/:appName',
  middleware.checkToken, (req, res, next) => {
  var newAppName = _.trim(req.body.name);
  var appName = _.trim(req.params.appName);
  var uid = req.users.id;
  if (_.isEmpty(newAppName)) {
    return res.status(406).send("Please input name!");
  } else {
    var appManager = new AppManager();
    return accountManager.ownerCan(uid, appName)
    .then((col) => {
      if (REGEX_ANDROID.test(appName) || OLD_REGEX_ANDROID.test(appName)) {
        if (!REGEX_ANDROID.test(newAppName)) {
          throw new AppError.AppError(`new appName have to point -android suffix! eg. Demo-android`);
        }
      } else if (REGEX_IOS.test(appName) || OLD_REGEX_IOS.test(appName)) {
        if (!REGEX_IOS.test(newAppName)) {
          throw new AppError.AppError(`new appName have to point -ios suffix! eg. Demo-ios`);
        }
      } else {
        throw new AppError.AppError(`appName have to point -android or -ios suffix! eg. ${appName}-android ${appName}-ios`);
      }
      return appManager.findAppByName(uid, newAppName)
      .then((appInfo) => {
        if (!_.isEmpty(appInfo)){
          throw new AppError.AppError(newAppName + " Exist!");
        }
        return appManager.modifyApp(col.appid, {name: newAppName});
      });
    })
    .then(() => {
      res.send("");
    })
    .catch((e) => {
      if (e instanceof AppError.AppError) {
        res.status(406).send(e.message);
      } else {
        next(e);
      }
    });
  }
});

router.post('/:appName/transfer/:email',
  middleware.checkToken, (req, res, next) => {
  var appName = _.trim(req.params.appName);
  var email = _.trim(req.params.email);
  var uid = req.users.id;
  if (!validator.isEmail(email)){
    return res.status(406).send("Invalid Email!");
  }
  return accountManager.ownerCan(uid, appName)
  .then((col) => {
    return accountManager.findUserByEmail(email)
    .then((data) => {
      if (_.eq(data.id, uid)) {
        throw new AppError.AppError("You can't transfer to yourself!");
      }
      var appManager = new AppManager();
      return appManager.transferApp(col.appid, uid, data.id);
    });
  })
  .then((data) => {
    res.send(data);
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

router.post('/', middleware.checkToken, (req, res, next) => {
  var appName = req.body.name;
  var uid = req.users.id;
  var appManager = new AppManager();
  if (_.isEmpty(appName)) {
    return res.status(406).send("Please input name!");
  }
  appManager.findAppByName(uid, appName)
  .then((appInfo) => {
    if (!_.isEmpty(appInfo)){
      throw new AppError.AppError(appName + " Exist!");
    }
    if (!REGEX.test(appName)) {
      throw new AppError.AppError(`appName have to point -android or -ios suffix! eg. ${appName}-android ${appName}-ios`);
    }
    return appManager.addApp(uid, appName, req.users.identical)
    .then(() => {
      return {name: appName, collaborators: {[req.users.email]: {permission: "Owner"}}};
    });
  })
  .then((data) => {
    res.send({app: data});
  })
  .catch((e) => {
    if (e instanceof AppError.AppError) {
      res.status(406).send(e.message);
    } else {
      next(e);
    }
  });
});

module.exports = router;
