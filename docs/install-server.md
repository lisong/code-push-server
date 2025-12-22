## Install Node and NPM

[see](https://nodejs.org/en/download/)

> (chosen latest LTS version)

## Install PM2

```bash
$ sudo npm i -g pm2
```

## Install MySQL

-   [Linux](https://dev.mysql.com/doc/refman/8.0/en/linux-installation.html)
-   [macOS](https://dev.mysql.com/doc/refman/8.0/en/osx-installation.html)
-   [Microsoft Windows](https://dev.mysql.com/doc/refman/8.0/en/windows-installation.html)
-   [Others](https://dev.mysql.com/doc/refman/8.0/en/installing.html)

> notice. mysql8.x default auth caching_sha2_pasword not support in node-mysql2 see [issue](https://github.com/mysqljs/mysql/pull/1962)

## Install Redis

-   [Redis Quick Start](https://redis.io/topics/quickstart)

## Get code-push-server from NPM

```shell
$ npm install @shm-open/code-push-server@latest -g
```

## Init Database

```shell
$ code-push-server-db init --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password"
```

> output: success

## Configure code-push-server

check out the supported config items in [config.ts](../src/core/config.ts)

save the file [process.json](../process.json) for PM2, and add your config items to `"env"`

some config items have to be changed:

-   `local`.`storageDir` change to your directory, make sure have read/write permissions.
-   `local`.`downloadUrl` replace `127.0.0.1` to your machine ip.
-   `common`.`dataDir` change to your directory,make sure have read/write permissions.
-   `jwt`.`tokenSecret` get the random string from `https://www.grc.com/passwords.htm`, and replace the value `INSERT_RANDOM_TOKEN_KEY`.
-   `db` config: `username`,`password`,`host`,`port` change to your own

## Start Service

```shell
$ pm2 start process.json
```

## Restart Service

```shell
$ pm2 restart process.json
```

## Stop Service

```shell
$ pm2 stop process.json
```

## Check Service is OK

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

-   `updateCheckCache`
-   `rolloutClientUniqueIdCache`
-   `tryLoginTimes`

## Upgrade from old version

```shell
$ npm install -g @shm-open/code-push-server@latest
$ code-push-server-db upgrade --dbhost "your mysql host" --dbport "your mysql port"  --dbuser "your mysql user" --dbpassword "your mysql password" # upgrade codepush database
$ pm2 restart code-push-server # restart service
```

## view pm2 logs

```shell
$ pm2 ls
$ pm2 show code-push-server
$ tail -f "output file path"
```

## Default listen Host/Port 0.0.0.0/3000

> you can change it in process.json, env: PORT,HOST

## [code-push-cli](https://github.com/shm-open/code-push-cli)

> Use code-push-cli manage CodePush Server

```shell
$ npm install @shm-open/code-push-cli@latest -g
$ code-push login http://YOU_SERVICE_IP:3000 #login in browser account:admin password:123456
```

## Change Admin Password

```shell
$ curl -X PATCH -H "Authorization: Bearer mytoken" -H "Accept: application/json" -H "Content-Type:application/json" -d '{"oldPassword":"123456","newPassword":"654321"}' http://YOU_SERVICE_IP:3000/users/password
```
