-- Active: 1756015859884@@127.0.0.1@33061@test
USE test;

DROP TABLE test

CREATE TABLE test (name VARCHAR(100));

INSERT INTO
    test (name)
VALUES ('张三'),
    ('李四'),
    ('王五'),
    ('小明'),
    ('小红');

DROP TABLE test_backup

-- 创建备份表结构（可包含数据，也可不包含）
CREATE TABLE test_backup LIKE test;