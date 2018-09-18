# `react-native` 如何使用 `code-push` 热更新

## 使用前须知

 - Q: “苹果应用商店和android应用商店允不允许使用热更新？”    
   A: “都允许。”

   > 苹果允许使用热更新[Apple's developer agreement](https://developer.apple.com/programs/ios/information/iOS_Program_Information_4_3_15.pdf), 但是规定不能弹框提示用户更新，影响用户体验。 
   > Google Play也允许热更新，但必须弹框告知用户更新。在中国的android市场发布时，都必须关闭更新弹框，否则会在审核应用时以“请上传最新版本的二进制应用包”驳回应用。 
       
 - Q: “react-native 开发环境更新模式是否可以直接用在生产环境下？”    
   A: “不能。”

 - Q: “code-push使用复杂么？”    
   A: “不复杂。很多网上的文章说复杂，是因为作者没有仔细理解官方文档，而且认为踩坑了。”

 - Q: “为什么推荐code-push？”    
   A: ”非常好。除了满足基本更新功能外，还有统计，hash计算容错和补丁更新功能。微软的项目，大公司技术有保障，而且开源。近几年微软在拥抱开源方面，让大家也是刮目相看。“

## 安装依赖包

#### 1. [react-native-cli](https://github.com/facebook/react-native) react-native命令行工具，安装后可以在终端使用`react-native`命令
 
```shell
$ npm install react-native-cli@latest -g
```
 
#### 2. [code-push-cli](https://github.com/Microsoft/code-push) 连接微软云端，管理发布更新版本命令行工具，安装后可以在终端使用`code-push`命令
   
```shell
$ npm install code-push-cli@latest -g 
```

#### 3. [react-native-code-push](https://github.com/Microsoft/react-native-code-push) 集成到react-native项目，按照以下步骤安装并修改配置既可集成

```shell
$ react-native init CodePushDemo #初始化一个react-native项目
$ cd CodePushDemo
$ npm install --save react-native-code-push@latest  #安装react-native-code-push
$ react-native link react-native-code-push  #连接到项目中，提示输入配置可以先行忽略
```

#### 4. [code-push-server](https://github.com/lisong/code-push-server) 微软云服务在中国太慢，可以用它搭建自己的服务端。

- [docker](https://github.com/lisong/code-push-server/blob/master/docker/README.md) (recommend)
- [manual operation](https://github.com/lisong/code-push-server/blob/master/docs/README.md)

## 创建服务端应用

基于code-push-server服务

```shell
$ code-push login http://YOUR_CODE_PUSH_SERVER_IP:3000  #浏览器中登录获取token，用户名:admin, 密码:123456
$ code-push app add CodePushDemoiOS ios react-native #创建iOS版, 获取Production DeploymentKey
$ code-push app add CodePushDemoAndroid android react-native #创建android版，获取获取Production DeploymentKey
```

## 配置CodePushDemo react-native项目

#### iOS 配置

编辑`Info.plist`文件，添加`CodePushDeploymentKey`和`CodePushServerURL`

1. `CodePushDeploymentKey`值设置为CodePushDemo-ios的Production DeploymentKey值。

2. `CodePushServerURL`值设置为code-push-server服务地址 http://YOUR_CODE_PUSH_SERVER_IP:3000/ 不在同一台机器的时候，请将YOUR_CODE_PUSH_SERVER_IP改成外网ip或者域名地址。

3. 将默认版本号1.0改成三位1.0.0

```xml
...
<key>CodePushDeploymentKey</key>
<string>YourCodePushKey</string>
<key>CodePushServerURL</key>
<string>YourCodePushServerUrl</string>
...
```

#### android 配置

编辑`MainApplication.java`

1. `YourKey`替换成CodePushDemo-android的Production DeploymentKey值

2. `YourCodePushServerUrl`值设置为code-push-server服务地址 http://YOUR_CODE_PUSH_SERVER_IP:3000/ 不在同一台机器的时候，请将YOUR_CODE_PUSH_SERVER_IP改成外网ip或者域名地址。

3. 将默认版本号1.0改成三位1.0.0

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

## 添加更新检查

可以参考[code-push-demo-app](https://github.com/lisong/code-push-demo-app/)
可以在入口componentDidMount添加

```javascript
CodePush.sync({
    installMode: CodePush.InstallMode.IMMEDIATE,
    updateDialog: true
});
```

不要忘记头部引入

```javascript
import CodePush from "react-native-code-push" 
```

## 运行CodePushDemo react-native项目

#### iOS

```shell
$ cd /path/to/CodePushDemo
$ open ios/CodePushDemo.xcodeproj 
```
在Xcode中打开菜单 Product > Scheme > Edit Scheme... > Run 选项中Build Configuration修改成Release, 然后运行编译

### android

```shell
$ cd /path/to/CodePushDemo
$ cd android
$ ./gradlew assembleRelease
$ cd app/build/outputs/apk  #将打好的包app-release.apk安装到您的手机上
```

## 发布更新到服务上

iOS和android要分开发布，所以创建了`CodePushDemo-ios`和`CodePushDemo-android`应用

```shell
$ cd /path/to/CodePushDemo
$ code-push release-react CodePushDemo-ios ios -d Production #iOS版
$ code-push release-react CodePushDemo-android android -d Production #android版
```

## 例子

[code-push-demo-app](https://github.com/lisong/code-push-demo-app)


### 更多信息参考[code-push-server](https://github.com/lisong/code-push-server)


