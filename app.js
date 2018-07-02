var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var helmet = require('helmet');
var config = require('./core/config');
var _ = require('lodash');
var fs = require('fs');
var routes = require('./routes/index');
var auth = require('./routes/auth');
var accessKeys = require('./routes/accessKeys');
var sessions = require('./routes/sessions');
var account = require('./routes/account');
var users = require('./routes/users');
var apps = require('./routes/apps');
var AppError = require('./core/app-error');
var log4js = require('log4js');
var log = log4js.getLogger("cps:app");
var app = express();
app.use(helmet());
app.disable('x-powered-by');
// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

app.use(log4js.connectLogger(log4js.getLogger("http"), {level: log4js.levels.INFO, nolog:'\\.gif|\\.jpg|\\.js|\\.css$' }));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

//use nginx in production
if (app.get('env') === 'development') {
  log.debug("set Access-Control Header");
  app.all('*', function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization");
    res.header("Access-Control-Allow-Methods","PUT,POST,GET,PATCH,DELETE,OPTIONS");
    log.debug("use set Access-Control Header");
    next();
  });
}

log.debug("config common.storageType value: " + _.get(config, 'common.storageType'));

if (_.get(config, 'common.storageType') === 'local') {
  var localStorageDir = _.get(config, 'local.storageDir');
  if (localStorageDir) {

    log.debug("config common.storageDir value: " + localStorageDir);

    if (!fs.existsSync(localStorageDir)) {
      var e = new Error(`Please create dir ${localStorageDir}`);
      log.error(e);
      throw e;
    }
    try {
      log.debug('checking storageDir fs.W_OK | fs.R_OK');
      fs.accessSync(localStorageDir, fs.W_OK | fs.R_OK);
      log.debug('storageDir fs.W_OK | fs.R_OK is ok');
    } catch (e) {
      log.error(e);
      throw e;
    }
    log.debug("static download uri value: " + _.get(config, 'local.public', '/download'));
    app.use(_.get(config, 'local.public', '/download'), express.static(localStorageDir));
  } else {
    log.error('please config local storageDir');
  }
}

app.use('/', routes);
app.use('/auth', auth);
app.use('/accessKeys', accessKeys);
app.use('/sessions', sessions);
app.use('/account', account);
app.use('/users', users);
app.use('/apps', apps);

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(req, res, next) {
    var err = new AppError.NotFound();
    res.status(err.status || 404);
    res.render('error', {
      message: err.message,
      error: err
    });
    log.error(err);
  });
  app.use(function(err, req, res, next) {
    if (err instanceof AppError.AppError) {
      res.send(err);
      log.debug(err);
    } else {
      res.status(err.status || 500);
      res.render('error', {
        message: err.message,
        error: err
      });
      log.error(err);
    }
  });
} else {
  app.use(function(req, res, next) {
    var e = new AppError.NotFound();
    res.status(404).send(e);
    log.debug(e);
  });
  // production error handler
  // no stacktraces leaked to user
  app.use(function(err, req, res, next) {
    if (err instanceof AppError.AppError) {
      res.send(err);
    } else {
      var status = err.status || 500;
      var error = new AppError.AppError(`服务器繁忙，请稍后再试!`);
      error.status = status;
      res.status(status).send(error);
      log.error(err);
    }
  });
}

module.exports = app;
