CREATE DATABASE IF NOT EXISTS `codepush`;

CREATE USER IF NOT EXISTS 'codepush'@'%' IDENTIFIED BY '123456';
GRANT SELECT,UPDATE,INSERT ON `codepush`.* TO 'codepush'@'%';
flush privileges;

use `codepush`;
CREATE TABLE IF NOT EXISTS `apps` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL DEFAULT '',
  `uid` bigint(20) unsigned NOT NULL DEFAULT '0',
  `os` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `platform` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `is_use_diff_text` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`(12))
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `collaborators` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `appid` int(10) unsigned NOT NULL DEFAULT '0',
  `uid` bigint(20) unsigned NOT NULL DEFAULT '0',
  `roles` varchar(20) NOT NULL DEFAULT '',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_appid` (`appid`),
  KEY `idx_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `deployments` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `appid` int(10) unsigned NOT NULL DEFAULT '0',
  `name` varchar(20) NOT NULL DEFAULT '',
  `description` varchar(500) NOT NULL DEFAULT '',
  `deployment_key` varchar(64) NOT NULL,
  `last_deployment_version_id` int(10) unsigned NOT NULL DEFAULT '0',
  `label_id` int(11) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_appid` (`appid`),
  KEY `idx_deploymentkey` (`deployment_key`(40))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `deployments_history` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `deployment_id` int(11) unsigned NOT NULL DEFAULT '0',
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_deployment_id` (`deployment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `deployments_versions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `deployment_id` int(11) unsigned NOT NULL DEFAULT '0',
  `app_version` varchar(100) NOT NULL DEFAULT '',
  `current_package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `min_version` bigint(20) unsigned NOT NULL DEFAULT '0',
  `max_version` bigint(20) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `idx_did_minversion` (`deployment_id`,`min_version`),
  KEY `idx_did_maxversion` (`deployment_id`,`max_version`),
  KEY `idx_did_appversion` (`deployment_id`,`app_version`(30))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `packages` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `deployment_version_id` int(10) unsigned NOT NULL DEFAULT '0',
  `deployment_id` int(10) unsigned NOT NULL DEFAULT '0',
  `description` varchar(500) NOT NULL DEFAULT '',
  `package_hash` varchar(64) NOT NULL DEFAULT '',
  `blob_url` varchar(255) NOT NULL DEFAULT '',
  `size` int(11) unsigned NOT NULL DEFAULT '0',
  `manifest_blob_url` varchar(255) NOT NULL DEFAULT '',
  `release_method` varchar(20) NOT NULL DEFAULT '',
  `label` varchar(20) NOT NULL DEFAULT '',
  `original_label` varchar(20) NOT NULL DEFAULT '',
  `original_deployment` varchar(20) NOT NULL DEFAULT '',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `released_by` bigint(20) unsigned NOT NULL DEFAULT '0',
  `is_mandatory` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `is_disabled` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `rollout` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_deploymentid_label` (`deployment_id`,`label`(8)),
  KEY `idx_versions_id` (`deployment_version_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `packages_diff` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(11) unsigned NOT NULL DEFAULT '0',
  `diff_against_package_hash` varchar(64) NOT NULL DEFAULT '',
  `diff_blob_url` varchar(255) NOT NULL DEFAULT '',
  `diff_size` int(11) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_packageid_hash` (`package_id`,`diff_against_package_hash`(40))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `packages_metrics` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `active` int(10) unsigned NOT NULL DEFAULT '0',
  `downloaded` int(10) unsigned NOT NULL DEFAULT '0',
  `failed` int(10) unsigned NOT NULL DEFAULT '0',
  `installed` int(10) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_packageid` (`package_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `user_tokens` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `uid` bigint(20) unsigned NOT NULL DEFAULT '0',
  `name` varchar(50) NOT NULL DEFAULT '',
  `tokens` varchar(64) NOT NULL DEFAULT '',
  `created_by` varchar(64) NOT NULL DEFAULT '',
  `description` varchar(500) NOT NULL DEFAULT '',
  `is_session` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `expires_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_uid` (`uid`),
  KEY `idx_tokens` (`tokens`) KEY_BLOCK_SIZE=16
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `users` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL DEFAULT '',
  `password` varchar(255) NOT NULL DEFAULT '',
  `email` varchar(100) NOT NULL DEFAULT '',
  `identical` varchar(10) NOT NULL DEFAULT '',
  `ack_code` varchar(10) NOT NULL DEFAULT '',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx_identical` (`identical`),
  KEY `udx_username` (`username`),
  KEY `idx_email` (`email`) KEY_BLOCK_SIZE=20
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `users` (`id`, `username`, `password`, `email`, `identical`, `ack_code`, `updated_at`, `created_at`)
VALUES
	(1,'admin','$2a$12$mvUY9kTqW4kSoGuZFDW0sOSgKmNY8SPHVyVrSckBTLtXKf6vKX3W.','lisong2010@gmail.com','4ksvOXqog','oZmGE','2016-11-14 10:46:55','2016-02-29 21:24:49');


CREATE TABLE IF NOT EXISTS `versions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `type` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '1.DBversion',
  `version` varchar(10) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx_type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `versions` WRITE;
INSERT INTO `versions` (`id`, `type`, `version`)
VALUES
	(1,1,'0.5.0');
UNLOCK TABLES;

CREATE TABLE IF NOT EXISTS `log_report_deploy` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `status` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `client_unique_id` varchar(100) NOT NULL DEFAULT '',
  `previous_label` varchar(20) NOT NULL DEFAULT '',
  `previous_deployment_key` varchar(64) NOT NULL DEFAULT '',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `log_report_download` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `client_unique_id` varchar(100) NOT NULL DEFAULT '',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
