# Changelog for code-push-server

## 0.5.x

## New Features
- Optimized for text incremental updates using Google's `diff-match-patch` algorithm
   - react-native-code-push Android client adaptation, requires merging https://github.com/Microsoft/react-native-code-push/pull/1393 to use text incremental update feature
   - react-native-code-push iOS client adaptation (requires merging https://github.com/Microsoft/react-native-code-push/pull/1399)
   - react-native-code-push Windows client adaptation (in progress)

## Bug Fixes

- Fixed activation count in statistics
- Fixed gray release bug
- Added calculation and last incremental update version after rollback

## How to Upgrade to This Version

### Upgrade Database

`$ npm run upgrade`

or

`$ code-push-server-db upgrade`


## 0.4.x

### New Features

- targetBinaryVersion supports regex matching, `deployments_versions` table adds `min_version`, `max_version` fields
  - `*` matches all versions
  - `1.2.3` matches specific version `1.2.3`
  - `1.2`/`1.2.*` matches all 1.2 patch versions 
  - `>=1.2.3<1.3.7`
  - `~1.2.3` matches `>=1.2.3<1.3.0`
  - `^1.2.3` matches `>=1.2.3<2.0.0`
- Added docker orchestration service deployment, updated documentation
- Support Tencent cloud cos storageType  

## How to Upgrade to This Version

- Upgrade Database
`$ ./bin/db upgrade`
or
`$ mysql codepush < ./sql/codepush-v0.4.0-patch.sql`

- Process Existing Data
``` shell
   $ git clone https://github.com/lisong/tools
   $ cd tools
   $ npm i
   $ vim ./bin/fixMinMaxVersion //modify data configuration
   $ node  ./bin/fixMinMaxVersion //success prompt will appear
```

## 0.3.x

- Support for gray release
- Adapted to `code-push app add` command, applications are now distinguished by type rather than name
  - Added `os`, `platform` fields to apps database table
- Improved `code-push release/release-react/release-cordova` commands
  - Added `is_disabled`, `rollout` fields to packages database table
- Adapted to `code-push patch` command
- Added `log_report_download`, `log_report_deploy` log tables
- Upgraded npm dependencies
