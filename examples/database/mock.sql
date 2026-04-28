-- Org table
CREATE TABLE IF NOT EXISTS `org` (
    `id` INTEGER PRIMARY KEY,
    `created_at` TIMESTAMP,
    `name` TEXT NOT NULL
);

-- User table
CREATE TABLE IF NOT EXISTS `user` (
    `id` INTEGER PRIMARY KEY,
    `created_at` TIMESTAMP,
    `org_id` INTEGER NOT NULL,
    `name` TEXT NOT NULL,
    `email` TEXT NOT NULL,
    UNIQUE (`name`)
);

-- Category table
CREATE TABLE IF NOT EXISTS `category` (
    `id` INTEGER PRIMARY KEY,
    `created_at` TIMESTAMP,
    `type` INTEGER NOT NULL,
    `name` TEXT NOT NULL,
    `description` TEXT,
    UNIQUE (`name`)
);

-- Item table
CREATE TABLE IF NOT EXISTS `item` (
    `id` INTEGER PRIMARY KEY,
    `created_at` TIMESTAMP,
    `category_id` INTEGER NOT NULL,
    `name` TEXT NOT NULL,
    `price` INTEGER NOT NULL
);

-- Order table
CREATE TABLE IF NOT EXISTS `order` (
    `uid` INTEGER PRIMARY KEY,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP,
    `deleted_at` INTEGER DEFAULT 0,
    `version` INTEGER DEFAULT 1,
    `user_id` INTEGER NOT NULL,
    `item_id` INTEGER NOT NULL,
    `amount` INTEGER NOT NULL,
    `price` INTEGER NOT NULL,
    `status` INTEGER NOT NULL DEFAULT 0
);

-- Org
INSERT INTO `org` (id, name) VALUES (1, '组织A');
INSERT INTO `org` (id, name) VALUES (2, '组织B');

-- User
INSERT INTO `user` (id, org_id, name, email) VALUES (1, 1, '张三', 'zhangsan@example.com');
INSERT INTO `user` (id, org_id, name, email) VALUES (2, 2, '李四', 'lisi@example.com');

-- Category
INSERT INTO `category` (id, type, name, description) VALUES (1, 0, '图书', '图书类商品');
INSERT INTO `category` (id, type, name, description) VALUES (2, 1, '视频', '视频类商品');

-- Item
INSERT INTO `item` (id, category_id, name, price) VALUES (1, 1, 'Go语言实战', 5000);
INSERT INTO `item` (id, category_id, name, price) VALUES (2, 2, 'Python入门视频', 8000);
INSERT INTO `item` (id, category_id, name, price) VALUES (3, 1, 'Java核心技术', 6000);
INSERT INTO `item` (id, category_id, name, price) VALUES (4, 2, '前端进阶视频', 90);
INSERT INTO `item` (id, category_id, name, price) VALUES (5, 1, '算法导论', 7000);
INSERT INTO `item` (id, category_id, name, price) VALUES (6, 1, '数据结构', 5);
INSERT INTO `item` (id, category_id, name, price) VALUES (7, 2, '机器学习视频', 9500);
INSERT INTO `item` (id, category_id, name, price) VALUES (8, 1, '网络安全', 6500);
INSERT INTO `item` (id, category_id, name, price) VALUES (9, 2, '云计算视频', 85);
INSERT INTO `item` (id, category_id, name, price) VALUES (10, 1, '数据库系统', 7500);

-- Order
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (1, 1, 1, 2, 10000, 1, '2024-06-01 10:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (2, 2, 2, 1, 8000, 0, '2024-06-02 11:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (3, 1, 3, 1, 6000, 2, '2024-06-03 12:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (4, 2, 4, 2, 18000, 3, '2024-06-04 13:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (5, 1, 5, 1, 7000, 4, '2024-06-05 14:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (6, 2, 6, 1, 5000, 1, '2024-06-06 15:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (7, 1, 7, 3, 24000, 1, '2024-06-07 16:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (8, 2, 8, 2, 12000, 0, '2024-06-08 17:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (9, 1, 9, 1, 9000, 2, '2024-06-09 18:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (10, 2, 10, 2, 14000, 3, '2024-06-10 19:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (11, 1, 6, 1, 5500, 1, '2024-06-11 20:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (12, 2, 7, 2, 19000, 0, '2024-06-12 21:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (13, 1, 8, 1, 6500, 2, '2024-06-13 22:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (14, 2, 9, 2, 17000, 3, '2024-06-14 23:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (15, 1, 10, 1, 7500, 4, '2024-06-15 09:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (16, 2, 10, 1, 5000, 1, '2024-06-16 10:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (17, 1, 9, 2, 16000, 0, '2024-06-17 11:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (18, 2, 8, 1, 6000, 2, '2024-06-18 12:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (19, 1, 7, 3, 27000, 3, '2024-06-19 13:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (20, 2, 6, 1, 7000, 4, '2024-06-20 14:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (21, 1, 5, 2, 11000, 1, '2024-06-21 15:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (22, 2, 4, 1, 9500, 0, '2024-06-22 16:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (23, 1, 3, 2, 13000, 2, '2024-06-23 17:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (24, 2, 2, 1, 8500, 3, '2024-06-24 18:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (25, 1, 10, 2, 15000, 4, '2024-06-25 19:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (26, 2, 1, 3, 15000, 1, '2024-06-26 20:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (27, 1, 2, 1, 8000, 0, '2024-06-27 21:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (28, 2, 3, 2, 12000, 2, '2024-06-28 22:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (29, 1, 4, 1, 9000, 3, '2024-06-29 23:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (30, 2, 5, 3, 21000, 4, '2024-06-30 09:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (31, 1, 6, 1, 5500, 1, '2024-07-01 10:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (32, 2, 7, 2, 19000, 0, '2024-07-02 11:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (33, 1, 8, 1, 6500, 2, '2024-07-03 12:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (34, 2, 9, 3, 25500, 3, '2024-07-04 13:00:00');
INSERT INTO `order` (uid, user_id, item_id, amount, price, status, created_at) VALUES (35, 1, 10, 1, 7500, 4, '2024-07-05 14:00:00');
