# docker deploy code-push-server

[[Chinese version 中文版]](./install-server-by-docker.cn.md)

> This document is used to describe docker deployment code-push-server, the example consists of three parts

- code-push-server section
    - Update packages are stored in `local` by default (i.e. stored on the local machine). Using the docker volume storage method, container destruction will not cause data loss unless the volume is manually deleted.
    - The pm2 cluster mode is used to manage processes internally. The default number of open processes is the number of cpus. You can set the deploy parameter in the docker-compose.yml file according to your own machine configuration.
    - docker-compose.yml only provides some parameter settings of the application. If you need to set other configurations, you can modify the file config.js.
- mysql section
    - Data is stored using docker volume, and container destruction will not cause data loss unless the volume is manually deleted.
    - Do not use the root user for the application. For security, you can create permissions with relatively small permissions for use by code-push-server. You only need to give `select, update, insert` permissions. To initialize the database, you need to use root or a user with table building privileges
- redis part
    - `tryLoginTimes` login error limit
    - `updateCheckCache` improves application performance
    - `rolloutClientUniqueIdCache` grayscale release

## Install Docker

Refer to the official Docker installation tutorial

- [>>mac click here](https://docs.docker.com/docker-for-mac/install/)
- [>>windows click here](https://docs.docker.com/docker-for-windows/install/)
- [>>linux click here](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

`$ docker info` can successfully output relevant information, the installation is successful, and the following steps can be continued

## get code

```shell
$ git clone https://github.com/shm-open/code-push-server.git
$ cd code-push-server
````

## Modify the configuration file

```shell
$ vim docker-compose.yml
````

_Replace `YOUR_MACHINE_IP` in `DOWNLOAD_URL` with your own external network ip or domain name_

### jwt.tokenSecret modification

> code-push-server verifies the json web token encryption method used by the login authentication method. The symmetric encryption algorithm is public, so it is very important to modify the tokenSecret value in config.js.

_Very important! Very important! Very important! _

> You can open the connection `https://www.grc.com/passwords.htm` to obtain a randomly generated number of type `63 random alpha-numeric characters` as the key

_Replace `YOUR_JWT_TOKEN_SECRET` in `TOKEN_SECRET` with the key_

## deploy

```shell
$ docker-compose up -d
````

> If the internet speed is not good, a long and patient wait is required. . . Let's chat with the girl for a while ^\_^

## View progress

```shell
$ docker-compose ps
````

## Access interface simple verification

`$ curl -I http://YOUR_CODE_PUSH_SERVER_IP:3000/`

returns `200 OK`

````http
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
````

## Browser login

> Default username: admin Password: 123456 Remember to change the default password
> If you log in and enter the wrong password for more than a certain number of times, you will no longer be able to log in. You need to clear the redis cache

```shell
$ docker exec -it code-push-server_redis_1 redis-cli # Enter redis
> flushall
> quit
````

## View service log

```shell
$ docker-compose logs server
````

## View storage `docker volume ls`

| DRIVER | VOLUME NAME | DESCRIPTION |
| ------ | ----------------------------- | ------------ ------------------ |
| local | code-push-server_data-mysql | database storage data directory |
| local | code-push-server_data-storage | Storage package file directory |
| local | code-push-server_data-tmp | Temporary directory for calculating update package difference files |
| local | code-push-server_data-redis | redis landing data |

## destroy exit application

```shell
$ docker-compose down
```
