var env = process.env.NODE_ENV || 'development';
var _    = require('lodash');
var path = require('path');
var config = {};
var log4js = require('log4js');
var log = log4js.getLogger("cps:config");
var CONFIG_PATH = path.join(__dirname, '../config/config.js');
if (process.env.CONFIG_FILE) {
  CONFIG_PATH = path.join(__dirname, path.relative(__dirname, process.env.CONFIG_FILE));
  log.info(`process.env.CONFIG_FILE value: ` +process.env.CONFIG_FILE)
}
log.info(`use config file ${CONFIG_PATH}`)
log.info(`use env ${env}`)
config = _.get(require(CONFIG_PATH), env);
log.info(`load config ${config}`)
module.exports = config;
