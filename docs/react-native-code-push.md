# How to Use `code-push` Hot Updates with `react-native`

## Prerequisites

 - Q: "Do Apple App Store and Android App Store allow hot updates?"    
   A: "Yes, both allow it."

   > Apple allows hot updates according to [Apple's developer agreement](https://developer.apple.com/programs/ios/information/iOS_Program_Information_4_3_15.pdf), but it requires silent updates without dialog prompts to maintain user experience. 
   > Google Play also allows hot updates but requires a dialog to inform users about updates. When publishing to Chinese Android markets, update dialogs must be disabled, otherwise the app will be rejected with the reason "Please upload the latest version of the binary application package".
       
 - Q: "Can React Native development environment update mode be used directly in production?"    
   A: "No."

 - Q: "Is code-push complicated to use?"    
   A: "No, it's not complicated. Many online articles say it's complex because the authors haven't carefully read the official documentation and think they've encountered issues."

 - Q: "Why is code-push recommended?"    
   A: "It's excellent. Besides meeting basic update functionality, it also includes statistics, hash calculation error tolerance, and patch update features. It's a Microsoft project, so the technology is reliable, and it's open source. In recent years, Microsoft's embrace of open source has been impressive."

## Install Dependencies

#### 1. [react-native-cli](https://github.com/facebook/react-native) React Native command line tool, after installation you can use the `react-native` command in terminal
 
```shell
$ npm install react-native-cli@latest -g
```
 
#### 2. [code-push-cli](https://github.com/Microsoft/code-push) Command line tool to connect to Microsoft cloud and manage release updates, after installation you can use the `code-push` command in terminal
   
```shell
$ npm install code-push-cli@latest -g 
```

#### 3. [react-native-code-push](https://github.com/Microsoft/react-native-code-push) Integrate into React Native project, follow these steps to install and modify configuration

```shell
$ react-native init CodePushDemo #Initialize a React Native project
$ cd CodePushDemo
$ npm install --save react-native-code-push@latest  #Install react-native-code-push
$ react-native link react-native-code-push  #Link to project, you can ignore configuration prompts for now
```

#### 4. [code-push-server](https://github.com/lisong/code-push-server) Microsoft cloud service is slow in China, you can use this to build your own server.

- [docker](https://github.com/lisong/code-push-server/blob/master/docker/README.md) (recommend)
- [manual operation](https://github.com/lisong/code-push-server/blob/master/docs/README.md)

## Create Server Applications

Based on code-push-server

```shell
$ code-push login http://YOUR_CODE_PUSH_SERVER_IP:3000  #Login in browser to get token, username:admin, password:123456
$ code-push app add CodePushDemoiOS ios react-native #Create iOS version, get Production DeploymentKey
$ code-push app add CodePushDemoAndroid android react-native #Create Android version, get Production DeploymentKey
```

## Configure CodePushDemo React Native Project

#### iOS Configuration

Edit the `Info.plist` file, add `CodePushDeploymentKey` and `CodePushServerURL`

1. Set `CodePushDeploymentKey` value to the Production DeploymentKey value of CodePushDemo-ios.

2. Set `CodePushServerURL` value to the code-push-server service address http://YOUR_CODE_PUSH_SERVER_IP:3000/ When not on the same machine, please change YOUR_CODE_PUSH_SERVER_IP to the external IP or domain address.

3. Change the default version number from 1.0 to three digits 1.0.0

```xml
...
<key>CodePushDeploymentKey</key>
<string>YourCodePushKey</string>
<key>CodePushServerURL</key>
<string>YourCodePushServerUrl</string>
...
```

#### Android Configuration

Edit `MainApplication.java`

1. Replace `YourKey` with the Production DeploymentKey value of CodePushDemo-android

2. Set `YourCodePushServerUrl` value to the code-push-server service address http://YOUR_CODE_PUSH_SERVER_IP:3000/ When not on the same machine, please change YOUR_CODE_PUSH_SERVER_IP to the external IP or domain address.

3. Change the default version number from 1.0 to three digits 1.0.0

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

## Add Update Check

You can refer to [code-push-demo-app](https://github.com/lisong/code-push-demo-app/)
Add this in the entry componentDidMount:

```javascript
CodePush.sync({
    installMode: CodePush.InstallMode.IMMEDIATE,
    updateDialog: true
});
```

Don't forget to import at the top:

```javascript
import CodePush from "react-native-code-push" 
```

## Run CodePushDemo React Native Project

#### iOS

```shell
$ cd /path/to/CodePushDemo
$ open ios/CodePushDemo.xcodeproj 
```
In Xcode, open menu Product > Scheme > Edit Scheme... > Run option, change Build Configuration to Release, then run and compile

### Android

```shell
$ cd /path/to/CodePushDemo
$ cd android
$ ./gradlew assembleRelease
$ cd app/build/outputs/apk  #Install the generated app-release.apk on your phone
```

## Publish Updates to Server

iOS and Android need to be published separately, so we created `CodePushDemo-ios` and `CodePushDemo-android` applications

```shell
$ cd /path/to/CodePushDemo
$ code-push release-react CodePushDemo-ios ios -d Production #iOS version
$ code-push release-react CodePushDemo-android android -d Production #Android version
```

## Examples

[code-push-demo-app](https://github.com/lisong/code-push-demo-app)


### For more information, refer to [code-push-server](https://github.com/lisong/code-push-server)


