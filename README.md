# CodePush Server ![Node.js CI](https://github.com/shm-open/code-push-server/workflows/Node.js%20CI/badge.svg)

[[Korean version 한국어]](./README.ko.md) [[Chinese version 中文版]](./README.cn.md)

CodePush Server is a CodePush program server. The official Microsoft CodePush service is slow in China, therefore we use this to host our own server.

## About this fork

Since the original [code-push-server](https://github.com/lisong/code-push-server) project is not actively maintained, we created this fork to:

-   keep dependencies up-to-date
-   fix any compatibility issue with latest official code-push clients
-   we only stick to official react-native-code-push client, therefore the customized feature like [is_use_diff_text](https://github.com/lisong/code-push-server#advance-feature) won't be supported.
-   we only use react-native-code-push client in production, most of the feature should be no difference for the rest CodePush clients, but if you found any, issues and PRs are always welcome.

## Support Storage mode

-   local: store bundle files in local machine
-   qiniu: store bundle files in [qiniu](http://www.qiniu.com/)
-   s3: store bundle files in [aws](https://aws.amazon.com/)
-   oss: store bundle files in [aliyun](https://www.aliyun.com/product/oss)
-   tencentcloud: store bundle files in [tencentcloud](https://cloud.tencent.com/product/cos)

## Correct use of code-push hot update

-   Apple App allows the use of hot updates [Apple's developer agreement](https://developer.apple.com/programs/ios/information/iOS_Program_Information_4_3_15.pdf), in order not to affect the user experience, it is stipulated that silent updates must be used. Google Play cannot use silent updates, and a pop-up box must inform users that there is an update to the app. China's android market must use silent updates (if the pop-up box prompts, the app will be rejected by the reason of "please upload the latest version of the binary application package").
-   The bundles of react-native are different for different platforms. When using code-push-server, you must create different applications to distinguish them (eg. CodePushDemo-ios and CodePushDemo-android)
-   react-native-code-push only updates resource files, not java and Objective C, so when npm upgrades the version of the dependent package, if the localized implementation used by the dependent package, the application version number must be changed at this time (ios modify Info CFBundleShortVersionString in .plist, android modify versionName in build.gradle), then recompile the app and publish it to the app store.
-   It is recommended to use the code-push release-react command to release the application, which combines the packaging and release commands (eg. code-push release-react CodePushDemo-ios ios -d Production)
-   Every time a new version is submitted to the App Store, an initial version should also be released to code-push-server based on the submitted version. (Because every time a version is released to code-push-server, code-puse-server will compare it with the initial version to generate a patch version)

### CodePush Cli

check out the [code-push-cli](https://github.com/shm-open/code-push-cli) which works with server for manage apps and publish releases

### Clients

-   [React Native](https://github.com/Microsoft/react-native-code-push)
-   [Cordova](https://github.com/microsoft/cordova-plugin-code-push)
-   [Capacitor](https://github.com/mapiacompany/capacitor-codepush)

## How To Install code-push-server

-   [docker](./docs/install-server-by-docker.md) (recommended)
-   [manual operation](./docs/install-server.md)

## Default Account and Password

-   account: `admin`
-   password: `123456`

## FAQ

-   [modify password](https://github.com/lisong/code-push-server/issues/43)
-   [code-push-server normal solution (CN)](https://github.com/lisong/code-push-server/issues/135)
-   targetBinaryVersion support
    -   `*`
    -   `1.2.3`
    -   `1.2`/`1.2.*`
    -   `1.2.3 - 1.2.7`
    -   `>=1.2.3 <1.2.7`
    -   `~1.2.3`
    -   `^1.2.3`
