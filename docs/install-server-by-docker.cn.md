# docker 部署 code-push-server

> 该文档用于描述 docker 部署 code-push-server，实例包含三个部分

-   code-push-server 部分
    -   更新包默认采用`local`存储(即存储在本地机器上)。使用 docker volume 存储方式，容器销毁不会导致数据丢失，除非人为删除 volume。
    -   内部使用 pm2 cluster 模式管理进程，默认开启进程数为 cpu 数，可以根据自己机器配置设置 docker-compose.yml 文件中 deploy 参数。
    -   docker-compose.yml 只提供了应用的一部分参数设置，如需要设置其他配置，可以修改文件 config.js。
-   mysql 部分
    -   数据使用 docker volume 存储方式，容器销毁不会导致数据丢失，除非人为删除 volume。
    -   应用请勿使用 root 用户，为了安全可以创建权限相对较小的权限供 code-push-server 使用，只需要给予`select,update,insert`权限即可。初始化数据库需要使用 root 或有建表权限用户
-   redis 部分
    -   `tryLoginTimes` 登录错误次数限制
    -   `updateCheckCache` 提升应用性能
    -   `rolloutClientUniqueIdCache` 灰度发布

## 安装 Docker

参考 Docker 官方安装教程

-   [>>mac 点这里](https://docs.docker.com/docker-for-mac/install/)
-   [>>windows 点这里](https://docs.docker.com/docker-for-windows/install/)
-   [>>linux 点这里](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

`$ docker info` 能成功输出相关信息，则安装成功，才能继续下面步骤

## 获取代码

```shell
$ git clone https://github.com/shm-open/code-push-server.git
$ cd code-push-server
```

## 修改配置文件

```shell
$ vim docker-compose.yml
```

_将`DOWNLOAD_URL`中`YOUR_MACHINE_IP`替换成本机外网 ip 或者域名_

### jwt.tokenSecret 修改

> code-push-server 验证登录验证方式使用的 json web token 加密方式,该对称加密算法是公开的，所以修改 config.js 中 tokenSecret 值很重要。

_非常重要！非常重要！ 非常重要！_

> 可以打开连接`https://www.grc.com/passwords.htm`获取 `63 random alpha-numeric characters`类型的随机生成数作为密钥

_将`TOKEN_SECRET`中`YOUR_JWT_TOKEN_SECRET`替换成密钥_

## 部署

```shell
$ docker-compose up -d
```

> 如果网速不佳，需要漫长而耐心的等待。。。去和妹子聊会天吧^\_^

## 查看进展

```shell
$ docker-compose ps
```

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
> 如果登录连续输错密码超过一定次数，会限定无法再登录. 需要清空 redis 缓存

```shell
$ docker exec -it code-push-server_redis_1 redis-cli  # 进入redis
> flushall
> quit
```

## 查看服务日志

```shell
$ docker-compose logs server
```

## 查看存储 `docker volume ls`

| DRIVER | VOLUME NAME                   | 描述                           |
| ------ | ----------------------------- | ------------------------------ |
| local  | code-push-server_data-mysql   | 数据库存储数据目录             |
| local  | code-push-server_data-storage | 存储打包文件目录               |
| local  | code-push-server_data-tmp     | 用于计算更新包差异文件临时目录 |
| local  | code-push-server_data-redis   | redis 落地数据                 |

## 销毁退出应用

```shell
$ docker-compose down
```
