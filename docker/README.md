# docker 部署 code-push-server

>该文档用于描述docker部署code-push-server，实例包含三个部分

- code-push-server部分
  - 更新包默认采用`local`存储(即存储在本地机器上)。使用docker volume存储方式，容器销毁不会导致数据丢失，除非人为删除volume。
  - 内部使用pm2 cluster模式管理进程，默认开启进程数为cpu数，可以根据自己机器配置设置docker-compose.yml文件中deploy参数。
  - docker-compose.yml只提供了应用的一部分参数设置，如需要设置其他配置，可以修改文件config.js。
- mysql部分
  - 数据使用docker volume存储方式，容器销毁不会导致数据丢失，除非人为删除volume。
  - 应用请勿使用root用户，为了安全可以创建权限相对较小的权限供code-push-server使用，只需要给予`select,update,insert`权限即可。初始化数据库需要使用root或有建表权限用户
- redis部分
  - `tryLoginTimes` 登录错误次数限制
  - `updateCheckCache` 提升应用性能 
  - `rolloutClientUniqueIdCache` 灰度发布 

## 安装docker

参考docker官方安装教程

- [>>mac点这里](https://docs.docker.com/docker-for-mac/install/)
- [>>windows点这里](https://docs.docker.com/docker-for-windows/install/)
- [>>linux点这里](https://docs.docker.com/install/linux/docker-ce/ubuntu/)


`$ docker info` 能成功输出相关信息，则安装成功，才能继续下面步骤

## 启动swarm

```shell
$ sudo docker swarm init
```


## 获取代码

```shell
$ git clone https://github.com/lisong/code-push-server.git
$ cd code-push-server/docker
```

## 修改配置文件

```shell
$ vim docker-compose.yml
```

*将`DOWNLOAD_URL`中`YOU_MACHINE_IP`替换成本机外网ip或者域名*

*将`MYSQL_HOST`中`YOU_MACHINE_IP`替换成本机内网ip*

*将`REDIS_HOST`中`YOU_MACHINE_IP`替换成本机内网ip*

## jwt.tokenSecret修改

> code-push-server 验证登录验证方式使用的json web token加密方式,该对称加密算法是公开的，所以修改config.js中tokenSecret值很重要。

*非常重要！非常重要！ 非常重要！*

> 可以打开连接`https://www.grc.com/passwords.htm`获取 `63 random alpha-numeric characters`类型的随机生成数作为密钥

## 部署

```shell
$ sudo docker-compose build
$ sudo docker stack deploy -c docker-compose.yml code-push-server
```

> 如果网速不佳，需要漫长而耐心的等待。。。去和妹子聊会天吧^_^


## 查看进展

```shell
$ sudo docker service ls
$ sudo docker service ps code-push-server_db
$ sudo docker service ps code-push-server_redis
$ sudo docker service ps code-push-server_server
```

> 确认`CURRENT STATE` 为 `Running about ...`, 则已经部署完成

## 访问接口简单验证

`$ curl -I http://YOUR_CODE_PUSH_SERVER_IP:3000/`

返回`200 OK`

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

## 浏览器登录

> 默认用户名:admin 密码:123456 记得要修改默认密码哦
> 如果登录连续输错密码超过一定次数，会限定无法再登录. 需要清空redis缓存

```shell
$ redis-cli -p6388  # 进入redis
> flushall
> quit
```


## 查看服务日志

```shell
$ sudo docker service logs code-push-server_server
$ sudo docker service logs code-push-server_db
$ sudo docker service logs code-push-server_redis
```

## 查看存储 `docker volume ls`

DRIVER | VOLUME NAME |  描述    
------ | ----- | -------
local  | code-push-server_data-mysql | 数据库存储数据目录
local  | code-push-server_data-storage | 存储打包文件目录
local  | code-push-server_data-tmp | 用于计算更新包差异文件临时目录
local  | code-push-server_data-redis | redis落地数据

## 销毁退出应用

```bash
$ sudo docker stack rm code-push-server
$ sudo docker swarm leave --force
```
