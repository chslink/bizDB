CREATE TABLE IF NOT EXISTS `users` (
                         `id` int NOT NULL AUTO_INCREMENT COMMENT '用户Id',
                         `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '名字',
                         `email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '邮箱',
                         `age` int NOT NULL COMMENT '年龄',
                         `create_at` datetime NOT NULL COMMENT '创建时间',
                         PRIMARY KEY (`id`),
                         UNIQUE KEY `users_email_index` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表'

