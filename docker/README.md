# Deploy code-push-server with Docker

>This document describes how to deploy code-push-server using Docker, the instance includes three parts

- code-push-server part
  - Update packages use `local` storage by default (stored on the local machine). Using docker volume storage method, container destruction won't cause data loss unless the volume is manually deleted.
  - Uses pm2 cluster mode internally to manage processes, default process count is equal to CPU count, you can configure the deploy parameters in docker-compose.yml file according to your machine configuration.
  - docker-compose.yml only provides some application parameter settings, if you need to set other configurations, you can modify the config.js file.
- mysql part
  - Data uses docker volume storage method, container destruction won't cause data loss unless the volume is manually deleted.
  - Applications should not use root user, for security you can create a user with relatively small permissions for code-push-server to use, only `select,update,insert` permissions are needed. Initializing the database requires root or a user with table creation permissions
- redis part
  - `tryLoginTimes` login error count limit
  - `updateCheckCache` improves application performance 
  - `rolloutClientUniqueIdCache` gray release 

## Install Docker

Refer to Docker's official installation guide

- [>>Click here for Mac](https://docs.docker.com/docker-for-mac/install/)
- [>>Click here for Windows](https://docs.docker.com/docker-for-windows/install/)
- [>>Click here for Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)


`$ docker info` should output relevant information successfully, then the installation is successful and you can proceed with the following steps

## Start Swarm

```shell
$ sudo docker swarm init
```


## Get Code

```shell
$ git clone https://github.com/lisong/code-push-server.git
$ cd code-push-server/docker
```

## Modify Configuration File

```shell
$ vim docker-compose.yml
```

*Replace `YOU_MACHINE_IP` in `DOWNLOAD_URL` with your machine's external IP or domain*

*Replace `YOU_MACHINE_IP` in `MYSQL_HOST` with your machine's internal IP*

*Replace `YOU_MACHINE_IP` in `REDIS_HOST` with your machine's internal IP*

## Modify jwt.tokenSecret

> code-push-server uses JSON web token encryption method for login verification, this symmetric encryption algorithm is public, so modifying the tokenSecret value in config.js is very important.

*Very important! Very important! Very important!*

> You can open the link `https://www.grc.com/passwords.htm` to get a random number of type `63 random alpha-numeric characters` as the key

## Deploy

```shell
$ sudo docker stack deploy -c docker-compose.yml code-push-server
```

> If the network speed is poor, you'll need to wait patiently... go chat with your friends meanwhile ^_^


## Check Progress

```shell
$ sudo docker service ls
$ sudo docker service ps code-push-server_db
$ sudo docker service ps code-push-server_redis
$ sudo docker service ps code-push-server_server
```

> Confirm that `CURRENT STATE` is `Running about ...`, then deployment is complete

## Simple API Access Verification

`$ curl -I http://YOUR_CODE_PUSH_SERVER_IP:3000/`

Returns `200 OK`

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

## Browser Login

> Default username: admin password: 123456 Remember to change the default password
> If you enter the wrong password too many times in a row, you will be locked out. You need to clear the redis cache

```shell
$ redis-cli -p6388  # enter redis
> flushall
> quit
```


## View Service Logs

```shell
$ sudo docker service logs code-push-server_server
$ sudo docker service logs code-push-server_db
$ sudo docker service logs code-push-server_redis
```

## View Storage `docker volume ls`

DRIVER | VOLUME NAME | Description    
------ | ----- | -------
local  | code-push-server_data-mysql | Database storage data directory
local  | code-push-server_data-storage | Storage package files directory
local  | code-push-server_data-tmp | Temporary directory for calculating update package differences
local  | code-push-server_data-redis | Redis persistent data

## Remove and Exit Application

```bash
$ sudo docker stack rm code-push-server
$ sudo docker swarm leave --force
```
