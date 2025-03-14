# CodePush Server [source](https://github.com/lisong/code-push-server) 

[![NPM](https://nodei.co/npm/code-push-server.svg?downloads=true&downloadRank=true&stars=true)](https://nodei.co/npm/code-push-server/)

[![NPM Version](https://img.shields.io/npm/v/code-push-server.svg)](https://npmjs.org/package/code-push-server)
[![Node.js Version](https://img.shields.io/node/v/code-push-server.svg)](https://nodejs.org/en/download/)
[![Linux Status](https://img.shields.io/travis/lisong/code-push-server/master.svg?label=linux)](https://travis-ci.org/lisong/code-push-server)
[![Windows Status](https://img.shields.io/appveyor/ci/lisong/code-push-server/master.svg?label=windows)](https://ci.appveyor.com/project/lisong/code-push-server)
[![Coverage Status](https://img.shields.io/coveralls/lisong/code-push-server/master.svg)](https://coveralls.io/github/lisong/code-push-server)
[![Dependency Status](https://img.shields.io/david/lisong/code-push-server.svg)](https://david-dm.org/lisong/code-push-server)
[![Known Vulnerabilities](https://snyk.io/test/npm/code-push-server/badge.svg)](https://snyk.io/test/npm/code-push-server)
[![Licenses](https://img.shields.io/npm/l/code-push-server.svg)](https://spdx.org/licenses/MIT)

CodePush Server is a CodePush progam server! microsoft CodePush cloud is slow in China, we can use this to build our's. I use [qiniu](http://www.qiniu.com/) to store the files, because it's simple and quick!  Or you can use [local/s3/oss/tencentcloud] storage, just modify config.js file, it's simple configure.


## Support Storage mode 

- local *storage bundle file in local machine*
- qiniu *storage bundle file in [qiniu](http://www.qiniu.com/)*
- s3 *storage bundle file in [aws](https://aws.amazon.com/)*
- oss *storage bundle file in [aliyun](https://www.aliyun.com/product/oss)*
- tencentcloud *storage bundle file in [tencentcloud](https://cloud.tencent.com/product/cos)*

## Proper Usage of Code-Push Hot Updates

- Apple Apps allow hot updates according to [Apple's developer agreement](https://developer.apple.com/programs/ios/information/iOS_Program_Information_4_3_15.pdf). To maintain a good user experience, silent updates are required. Google Play does not allow silent updates and requires a dialog to inform users about App updates. Chinese Android markets require silent updates (if a dialog prompt is shown, the App will be rejected with the reason "Please upload the latest version of the binary application package").
- React Native bundle packages are different for different platforms, so you must create different applications to distinguish them when using code-push-server (e.g., CodePushDemo-ios and CodePushDemo-android).
- React-native-code-push only updates resource files and does not update Java and Objective C. Therefore, when upgrading npm dependency package versions, if the dependency package uses native implementations, you must change the application version number (modify CFBundleShortVersionString in Info.plist for iOS, modify versionName in build.gradle for Android), then recompile the app and publish it to the app store.
- It is recommended to use the code-push release-react command to publish applications, as this command combines packaging and publishing commands (e.g., code-push release-react CodePushDemo-ios ios -d Production).
- Every time you submit a new version to the App Store, you should also publish an initial version to code-push-server based on that submission version. (This is because code-push-server will compare each subsequent version with the initial version to generate patch versions.)


### shell login

```shell
$ code-push login http://api.code-push.com #login
```

### [web](http://www.code-push.com) 

Visit: http://www.code-push.com

### client eg.

[ReactNative CodePushDemo](https://github.com/lisong/code-push-demo-app)

[Cordova CodePushDemo](https://github.com/lisong/code-push-cordova-demo-app)

## HOW TO INSTALL code-push-server

- [docker](https://github.com/lisong/code-push-server/blob/master/docker/README.md) (recommend)
- [manual operation](https://github.com/lisong/code-push-server/blob/master/docs/README.md)

## DEFAULT ACCOUNT AND PASSWORD

- account: `admin`
- password: `123456`

## HOW TO USE

- [normal](https://github.com/lisong/code-push-server/blob/master/docs/react-native-code-push.md)
- [react-native-code-push](https://github.com/Microsoft/react-native-code-push)
- [code-push](https://github.com/Microsoft/code-push)


## ISSUES

[code-push-server normal solution](https://github.com/lisong/code-push-server/issues/135)

[An unknown error occurred](https://github.com/lisong/code-push-server/issues?utf8=%E2%9C%93&q=unknown)

[modify password](https://github.com/lisong/code-push-server/issues/43)


# UPDATE TIME LINE

- targetBinaryVersion support
  - `*` 
  - `1.2.3`
  - `1.2`/`1.2.*`
  - `1.2.3 - 1.2.7`
  - `>=1.2.3 <1.2.7`
  - `~1.2.3`
  - `^1.2.3`


## Advance Feature

> use google diff-match-patch calculate text file diff patch

- support iOS and Android
- use `"react-native-code-push": "git+https://git@github.com/lisong/react-native-code-push.git"` instead `"react-native-code-push": "x.x.x"` in `package.json`
- change `apps`.`is_use_diff_text` to `1` in mysql codepush database

## License
MIT License [read](https://github.com/lisong/code-push-server/blob/master/LICENSE)


