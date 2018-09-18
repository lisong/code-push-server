
## INSTALL NODE AND NPM

[see](https://nodejs.org/en/download/)

> (chosen latest LTS version)

## INSTALL PM2

```bash
$ sudo npm i -g pm2
```

## INSTALL MYSQL

- [Linux](https://dev.mysql.com/doc/refman/8.0/en/linux-installation.html)
- [macOS](https://dev.mysql.com/doc/refman/8.0/en/osx-installation.html)
- [Microsoft Windows](https://dev.mysql.com/doc/refman/8.0/en/windows-installation.html)
- [Others](https://dev.mysql.com/doc/refman/8.0/en/installing.html)


## GET code-push-server FROM NPM

```shell
$ npm install code-push-server@latest -g
```


## GET code-push-server FROM SOURCE CODE

```shell
$ git clone https://github.com/lisong/code-push-server.git
$ cd code-push-server
$ npm install
```

## INIT DATABASE

```shell
$ code-push-server-db init --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password"
```

or from source code

```shell
$ ./bin/db init --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password"
```

> output: success

## CONFIGURE for code-push-server

save the file [config.js](https://github.com/lisong/code-push-server/blob/master/config/config.js)

some config have to change:

- `local`.`storageDir` change to your directory,make sure have read/write permissions.
- `local`.`downloadUrl` replace `127.0.0.1` to your machine ip.
- `common`.`dataDir` change to your directory,make sure have read/write permissions.
- `jwt`.`tokenSecret` get the random string from `https://www.grc.com/passwords.htm`, and replace the value `INSERT_RANDOM_TOKEN_KEY`.
- `db` config: `username`,`password`,`host`,`port` change your own's

## CONFIGURE for pm2

save the file [process.json](https://github.com/lisong/code-push-server/blob/master/docs/process.json)

some config have to change:

- `script` if you install code-push-server from npm use `code-push-server`,or use `"your source code dir"/bin/www`
- `CONFIG_FILE` above config.js file path,use absolute path.

## START SERVICE

```shell
$ pm2 start process.json
```

## RESTART SERVICE

```shell
$ pm2 restart process.json
```

## STOP SERVICE

```shell
$ pm2 stop process.json
```

## CHECK SERVICE IS OK 

```shell
$ curl -I http://YOUR_CODE_PUSH_SERVER_IP:3000/
```

> return httpCode `200 OK`

```http
HTTP/1.1 200 OK
X-DNS-Prefetch-Control: off
X-Frame-Options: SAMEORIGIN
Strict-Transport-Security: max-age=15552000; includeSubDomains
X-Download-Options: noopen
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
Content-Type: text/html; charset=utf-8
Content-Length: 592
ETag: W/"250-IiCMcM1ZUFSswSYCU0KeFYFEMO8"
Date: Sat, 25 Aug 2018 15:45:46 GMT
Connection: keep-alive
```


## Use redis impove concurrent and security

> config redis in config.js

- `updateCheckCache`
- `rolloutClientUniqueIdCache`
- `tryLoginTimes`


## UPGRADE

*from npm package*

```shell
$ npm install -g code-push-server@latest
$ code-push-server-db upgrade --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password" # upgrade codepush database
$ pm2 restart code-push-server # restart service
```

*from source code*

```shell
$ cd /path/to/code-push-server
$ git pull --rebase origin master
$ ./bin/db upgrade --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password"
# upgrade codepush database
$ pm2 restart code-push-server # restart service
```


## view pm2 logs

```shell
$ pm2 ls
$ pm2 show code-push-server
$ tail -f "output file path"
```


## Support Storage mode 

- local (default)
- qiniu (qiniu)
- s3 (aws)
- oss (aliyun)
- tencentcloud

## Default listen Host/Port  0.0.0.0/3000 

> you can change it in process.json, env: PORT,HOST


## [code-push-cli](https://github.com/Microsoft/code-push)

> Use code-push-cli manager CodePushServer

```shell
$ npm install code-push-cli@latest -g
$ code-push login http://YOU_SERVICE_IP:3000 #login in browser account:admin password:123456
```

> change admin password eg.

```shell
$ curl -X PATCH -H "Authorization: Bearer mytoken" -H "Accept: application/json" -H "Content-Type:application/json" -d '{"oldPassword":"123456","newPassword":"654321"}' http://YOU_SERVICE_IP:3000/users/password
```


## config react-native project

> Follow the react-native-code-push docs, addition iOS add a new entry named CodePushServerURL, whose value is the key of ourself CodePushServer URL. Android use the new CodePush constructor in MainApplication point CodePushServerUrl

iOS eg. in file Info.plist

```xml
...
<key>CodePushDeploymentKey</key>
<string>YourCodePushKey</string>
<key>CodePushServerURL</key>
<string>YourCodePushServerUrl</string>
...
```

Android eg. in file MainApplication.java

```java
@Override
protected List<ReactPackage> getPackages() {
  return Arrays.<ReactPackage>asList(
      new MainReactPackage(),
      new CodePush(
         "YourKey",
         MainApplication.this,
         BuildConfig.DEBUG,
         "YourCodePushServerUrl" 
      )
  );
}
```


## [cordova-plugin-code-push](https://github.com/Microsoft/cordova-plugin-code-push) for cordova

```shell
$ cd /path/to/project
$ cordova plugin add cordova-plugin-code-push@latest --save
```

## config cordova project

edit config.xml. add code below.

```xml
<platform name="android">
    <preference name="CodePushDeploymentKey" value="nVHPr6asLSusnWoLBNCSktk9FWbiqLF160UDg" />
    <preference name="CodePushServerUrl" value="http://api.code-push.com/" />
</platform>
<platform name="ios">
    <preference name="CodePushDeploymentKey" value="Iw5DMZSIrCOS7hbLsY5tHAHNITFQqLF160UDg" />
    <preference name="CodePushServerUrl" value="http://api.code-push.com/" />
</platform>
```

## Use [CodePush Web](https://github.com/lisong/code-push-web) manage apps

> add codePushWebUrl config in config.js

eg.

```javascript
"common": {
  "codePushWebUrl": "Your CodePush Web address",
}
```
