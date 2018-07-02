DROP TABLE IF EXISTS `apps`;
CREATE TABLE `apps` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL DEFAULT '',
  `uid` bigint(20) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`(12))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `collaborators`;
CREATE TABLE `collaborators` (
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


DROP TABLE IF EXISTS `deployments`;
CREATE TABLE `deployments` (
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

DROP TABLE IF EXISTS `deployments_history`;
CREATE TABLE `deployments_history` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `deployment_id` int(11) unsigned NOT NULL DEFAULT '0',
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_deployment_id` (`deployment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `deployments_versions`;
CREATE TABLE `deployments_versions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `deployment_id` int(11) unsigned NOT NULL DEFAULT '0',
  `app_version` varchar(14) NOT NULL DEFAULT '',
  `current_package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_did_appversion` (`deployment_id`,`app_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `packages`;
CREATE TABLE `packages` (
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
  `released_by` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_deploymentid_label` (`deployment_id`,`label`(8)),
  KEY `idx_versions_id` (`deployment_version_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `packages_diff`;
CREATE TABLE `packages_diff` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(11) unsigned NOT NULL DEFAULT '0',
  `diff_against_package_hash` varchar(64) NOT NULL DEFAULT '',
  `diff_blob_url` varchar(255) NOT NULL DEFAULT '',
  `diff_size` int(11) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_packageid_hash` (`package_id`,`diff_against_package_hash`(40))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `packages_metrics`;
CREATE TABLE `packages_metrics` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(10) unsigned NOT NULL DEFAULT '0',
  `active` int(10) unsigned NOT NULL DEFAULT '0',
  `downloaded` int(10) unsigned NOT NULL DEFAULT '0',
  `failed` int(10) unsigned NOT NULL DEFAULT '0',
  `installed` int(10) unsigned NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx_packageid` (`package_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `user_tokens`;
CREATE TABLE `user_tokens` (
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

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
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
    (1,'tgg','$2a$12$mvUY9kTqW4kSoGuZFDW0sOSgKmNY8SPHVyVrSckBTLtXKf6vKX3W.','tgg_dev@gmail.com','4ksvOXqog','oZmGE','2018-07-02 10:46:55','2018-05-23 09:00:00');
