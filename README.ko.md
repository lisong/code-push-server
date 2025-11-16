# CodePush Server ![Node.js CI](https://github.com/shm-open/code-push-server/workflows/Node.js%20CI/badge.svg)

[[영어 버전 English]](./README.md) [[중국어 버전 中文版]](./README.cn.md)

CodePush Server는 CodePush 프로그램 서버입니다.
Microsoft 공식 CodePush 서비스는 아시아 지역에서 속도가 느린 경우가 많기 때문에, 우리는 자체 서버를 구축하여 사용합니다.

-   (해당 Repo의 오너가 중국인이므로 중국어 위주의 안내를 단순 한국어로 번역 했음을 미리 안내합니다.)

## Requirement

-   Node.js v24.6.0
-   npm install -g install @shm-open/code-push-cli
-   data, storage 이름으로 디렉토리 생성후 서버 구동
-   앱테스터 semver 포맷 지원을 위해 parseVersion 유틸함수 일부 수정 &rarr; cleanVersion 선행
    > e.g., '2.10.2-stg-01' &rarr; '2.10.2'

## 이 포크(Fork)에 대하여

원본 프로젝트인 [code-push-server](https://github.com/lisong/code-push-server)가 현재 활발히 유지·보수되고 있지 않기 때문에, 우리는 다음 목적을 위해 이 포크를 만들었습니다:

-   의존성을 최신 상태로 유지
-   최신 공식 CodePush 클라이언트와의 호환성 문제 해결
-   공식 `react-native-code-push` 클라이언트만 사용하기 때문에,  
    [is_use_diff_text](https://github.com/lisong/code-push-server#advance-feature) 같은 커스텀 기능은 지원하지 않습니다.
-   프로덕션에서는 `react-native-code-push`만 사용합니다.  
    다른 CodePush 클라이언트도 대부분 동일하게 동작할 것이지만, 문제가 있다면 Issue 또는 PR 환영합니다.

## 지원되는 저장소(Storage) 모드

-   local: 로컬 머신(서버 디스크)에 번들 파일 저장
-   qiniu: [qiniu](http://www.qiniu.com/)에 저장
-   s3: [AWS S3](https://aws.amazon.com/)에 저장
-   oss: [Alibaba Cloud OSS](https://www.aliyun.com/product/oss)에 저장
-   tencentcloud: [Tencent Cloud COS](https://cloud.tencent.com/product/cos)에 저장

## CodePush 핫 업데이트 올바르게 사용하기

-   Apple App은 핫 업데이트 사용을 허용하지만,  
    [Apple 개발자 약관](https://developer.apple.com/programs/ios/information/iOS_Program_Information_4_3_15.pdf)에 따라 사용자 경험을 해치지 않도록 **반드시 Silent Update(무알림 업데이트)** 로만 사용해야 합니다.

    Google Play는 silent update를 허용하지 않으며, **업데이트 안내 팝업을 반드시 표시해야 합니다.**

    중국 Android 마켓은 **Silent Update만 허용**합니다.  
    팝업을 띄우면 “최신 버전의 바이너리 앱을 제출하세요”라는 이유로 반려될 수 있습니다.

-   React Native는 플랫폼별로 bundle 파일이 다르기 때문에,  
    CodePush Server 사용 시 **iOS/Android 앱을 각각 따로 생성**해야 합니다.  
    예: `CodePushDemo-ios`, `CodePushDemo-android`

-   `react-native-code-push`는 리소스 파일만 업데이트하고,  
    **Java / Objective-C 네이티브 코드는 업데이트하지 않습니다.**

    따라서 npm 패키지 버전을 올렸는데 해당 패키지가 네이티브 코드를 변경했다면,  
    반드시 **앱 버전(ios: Info.plist의 CFBundleShortVersionString / android: build.gradle의 versionName)** 을 증가시키고  
    **새로운 앱을 스토어에 제출해야 합니다.**

-   `code-push release-react` 명령을 사용하여 배포할 것을 추천합니다.  
    (예: `code-push release-react CodePushDemo-ios ios -d Production`)  
    이 명령은 번들 생성과 배포를 한 번에 수행합니다.

-   App Store에 새 버전을 제출할 때는 반드시 **해당 버전에 대한 초기 CodePush 릴리즈도 함께 업로드**해야 합니다.  
    이후 모든 CodePush 릴리즈는 이 초기 버전을 기준으로 diff 패치를 생성하기 때문입니다.

### CodePush CLI

-   앱 관리 및 CodePush 릴리즈 배포는 다음 [code-push-cli](https://github.com/shm-open/code-push-cli)를 사용하세요.

### 클라이언트

-   [React Native](https://github.com/Microsoft/react-native-code-push)
-   [Cordova](https://github.com/microsoft/cordova-plugin-code-push)
-   [Capacitor](https://github.com/mapiacompany/capacitor-codepush)

## CodePush Server 설치 방법

-   [docker](./docs/install-server-by-docker.md) (추천)
-   [수동 설치](./docs/install-server.md)

## 기본 계정 정보

-   계정: `admin`
-   비밀번호: `123456`

## FAQ

-   [비밀번호 변경](https://github.com/lisong/code-push-server/issues/43)
-   [code-push-server 일반적인 문제 해결 (중국어)](https://github.com/lisong/code-push-server/issues/135)
-   지원되는 targetBinaryVersion 형식
    -   `*`
    -   `1.2.3`
    -   `1.2` / `1.2.*`
    -   `1.2.3 - 1.2.7`
    -   `>=1.2.3 <1.2.7`
    -   `~1.2.3`
    -   `^1.2.3`
