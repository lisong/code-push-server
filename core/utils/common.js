'use strict';
var Promise = require('bluebird');
var fs = require("fs");
var fsextra = require("fs-extra");
var unzip = require('node-unzip-2');
var config    = require('../config');
var _ = require('lodash');
var qiniu = require("qiniu");
var common = {};
module.exports = common;

common.createFileFromRequest = function (url, filePath) {
  return new Promise(function (resolve, reject) {
    fs.exists(filePath, function (exists) {
      if (!exists) {
        var request = require('request');
        request(url).on('error', function (error) {
          reject(error);
        })
        .on('response', function (response) {
          if (response.statusCode == 200) {
            let stream = fs.createWriteStream(filePath);
            response.pipe(stream);
            stream.on('close',function(){
              resolve(null);
            });
            stream.on('error', function (error) {
              reject(error)
            })
          } else {
            reject({message:'request fail'})
          }
        });
      }else {
        resolve(null);
      }
    });
  });
}

common.move = function (sourceDst, targertDst) {
  return new Promise(function (resolve, reject) {
    fsextra.move(sourceDst, targertDst, {clobber: true, limit: 16}, function (err) {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
};

common.deleteFolder = function (folderPath) {
  return new Promise(function (resolve, reject) {
    fsextra.remove(folderPath, function (err) {
      if (err) {
        reject(err);
      }else {
        resolve(null);
      }
    });
  });
};

common.deleteFolderSync = function (folderPath) {
  return fsextra.removeSync(folderPath);
};

common.createEmptyFolder = function (folderPath) {
  return new Promise(function (resolve, reject) {
    common.deleteFolder(folderPath).then(function (data) {
      fsextra.mkdirs(folderPath, function (err) {
        if (err) {
          reject(new Error("create error"));
        } else {
          resolve(folderPath);
        }
      });
    });
  });
};

common.createEmptyFolderSync = function (folderPath) {
  common.deleteFolderSync(folderPath);
  return fsextra.mkdirsSync(folderPath);
};

common.unzipFile = function (zipFile, outputPath) {
  return new Promise(function (resolve, reject) {
    try {
      fs.exists(zipFile, function(exists){
        if (!exists) {
          reject(new Error("zipfile not found!"))
        }
        var readStream = fs.createReadStream(zipFile);
        var extract = unzip.Extract({ path: outputPath });
        readStream.pipe(extract);
        extract.on("close", function () {
          resolve(outputPath);
        });
      })
    } catch (e) {
      reject(e)
    }
  });
};

common.uptoken = function (bucket, key) {
  var putPolicy = new qiniu.rs.PutPolicy(bucket+":"+key);
  return putPolicy.token();
};

common.uploadFileToStorage = function (key, filePath) {
  if (_.get(config, 'common.storageType') === 'local') {
    return common.uploadFileToLocal(key, filePath);
  } else if (_.get(config, 'common.storageType') === 's3') {
    return common.uploadFileToS3(key, filePath);
  } else if (_.get(config, 'common.storageType') === 'oss') {
    return common.uploadFileToOSS(key, filePath);
  }
  return common.uploadFileToQiniu(key, filePath);
};

common.uploadFileToLocal = function (key, filePath) {
  return new Promise(function (resolve, reject) {
    var storageDir = _.get(config, 'local.storageDir');
    if (!storageDir) {
      throw new Error('please set config local storageDir');
    }
    if (!fs.existsSync(storageDir)) {
      throw new Error(`please create dir ${storageDir}`);
    }
    fs.accessSync(storageDir, fs.W_OK);
    var stats = fs.statSync(storageDir);
    if (!stats.isDirectory()) {
      throw new Error(`${storageDir} must be directory`);
    }
    fs.accessSync(filePath, fs.R_OK);
    stats = fs.statSync(filePath);
    if (!stats.isFile()) {
      throw new Error(`${filePath} must be file`);
    }
    fsextra.copy(filePath, `${storageDir}/${key}`, {clobber: true, limit: 16}, function (err) {
      if (err) {
        return reject(err);
      }
      resolve(key);
    });
  });
};

common.getDownloadUrl = function () {
  if (_.get(config, 'common.storageType') === 'local') {
    return _.get(config, 'local.downloadUrl');
  } else if (_.get(config, 'common.storageType') === 's3') {
    return _.get(config, 's3.downloadUrl');
  } else if (_.get(config, 'common.storageType') === 'oss') {
    return _.get(config, 'oss.downloadUrl');
  }
  return _.get(config, 'qiniu.downloadUrl');
}

common.getBlobDownloadUrl = function (blobUrl) {
  return `${common.getDownloadUrl()}/${blobUrl}`
};

common.uploadFileToQiniu = function (key, filePath) {
  return new Promise(function (resolve, reject) {
    qiniu.conf.ACCESS_KEY = _.get(config, "qiniu.accessKey");
    qiniu.conf.SECRET_KEY = _.get(config, "qiniu.secretKey");
    var bucket = _.get(config, "qiniu.bucketName", "jukang");
    var client = new qiniu.rs.Client();
    client.stat(bucket, key, function(err, ret) {
      if (!err) {
        resolve(ret.hash);
      } else {
        try {
          var uptoken = common.uptoken(bucket, key);
        } catch (e) {
          reject(e);
        }
        var extra = new qiniu.io.PutExtra();
        qiniu.io.putFile(uptoken, key, filePath, extra, function(err, ret) {
          if(!err) {
            // 上传成功， 处理返回值
            resolve(ret.hash);
          } else {
            // 上传失败， 处理返回代码
            reject(new Error(JSON.stringify(err)));
          }
        });
      }
    });
  });
};

common.uploadFileToS3 = function (key, filePath) {
  var AWS = require('aws-sdk');
  return (
    new Promise(function(resolve, reject) {
      AWS.config.update({
        region: _.get(config, 's3.region')
      });
      var s3 = new AWS.S3({
        params: {Bucket: _.get(config, 's3.bucketName')}
      });
      fs.readFile(filePath, function(err, data) {
        s3.upload({
          Key: key,
          Body: data,
          ACL:'public-read',
        }, function(err, response) {
          if(err) {
            reject(new Error(JSON.stringify(err)));
          } else {
            resolve(response.ETag)
          }
        })
      });
    })
  );
};

common.uploadFileToOSS = function (key, filePath) {
  var ALY = require('aliyun-sdk');
  var ossStream = require('aliyun-oss-upload-stream')(new ALY.OSS({
    accessKeyId:  _.get(config, 'oss.accessKeyId'),
    secretAccessKey: _.get(config, 'oss.secretAccessKey'),
    endpoint: _.get(config, 'oss.endpoint'),
    apiVersion: '2013-10-15',
  }));
  var upload = ossStream.upload({
    Bucket: _.get(config, 'oss.bucketName'),
    Key: `${_.get(config, 'oss.prefix')}/${key}`,
  });

  return new Promise(function (resolve, reject) {
    upload.on('error', function (error) {
      reject(error);
    });

    upload.on('uploaded', function (details) {
      resolve(details.ETag);
    });
    fs.createReadStream(filePath).pipe(upload);
  });
};

common.diffCollectionsSync = function (collection1, collection2) {
  var diffFiles = [];
  var collection1Only = [];
  var newCollection2 = Object.assign({}, collection2);
  if (collection1 instanceof Object) {
    for(var key of Object.keys(collection1)) {
      if (_.isEmpty(newCollection2[key])) {
        collection1Only.push(key);
      } else {
        if (!_.eq(collection1[key], newCollection2[key])) {
          diffFiles.push(key);
        }
        delete newCollection2[key];
      }
    }
  }
  return {diff:diffFiles, collection1Only: collection1Only, collection2Only: Object.keys(newCollection2)}
};
