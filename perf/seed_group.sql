-- seed_group.sql
-- 群聊压测前置数据初始化脚本
-- 执行：docker exec -i im-mysql mysql -u root -p123456 IMSystem < perf/seed_group.sql
--
-- 说明：
--   本脚本创建 1 个测试群（group_id=9001），10 名成员（user_id=2001~2010）
--   群成员账号若不存在则自动插入（密码 bcrypt(123456)）
--   已存在的数据用 INSERT IGNORE 跳过，幂等可重复执行

-- ── 1. 插入测试账号 2001~2010 ──────────────────────────────────────────────
-- 密码 123456 对应的 bcrypt 哈希（cost=10，你项目里的默认值）
-- 如果你的盐不同，用 go run gen_bcrypt.go 生成后替换
INSERT IGNORE INTO users (user_id, username, password, nickname, create_time, update_time) VALUES
  (2001, 'test_user_2001', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员01', NOW(), NOW()),
  (2002, 'test_user_2002', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员02', NOW(), NOW()),
  (2003, 'test_user_2003', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员03', NOW(), NOW()),
  (2004, 'test_user_2004', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员04', NOW(), NOW()),
  (2005, 'test_user_2005', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员05', NOW(), NOW()),
  (2006, 'test_user_2006', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员06', NOW(), NOW()),
  (2007, 'test_user_2007', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员07', NOW(), NOW()),
  (2008, 'test_user_2008', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员08', NOW(), NOW()),
  (2009, 'test_user_2009', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员09', NOW(), NOW()),
  (2010, 'test_user_2010', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', '压测成员10', NOW(), NOW());

-- ── 2. 创建测试群 ──────────────────────────────────────────────────────────
INSERT IGNORE INTO groups (group_id, group_name, owner_id, create_time, update_time) VALUES
  (9001, 'perf_group_10人', 2001, NOW(), NOW());

-- ── 3. 插入群成员（10 人） ────────────────────────────────────────────────
INSERT IGNORE INTO group_members (group_id, user_id) VALUES
  (9001, 2001),
  (9001, 2002),
  (9001, 2003),
  (9001, 2004),
  (9001, 2005),
  (9001, 2006),
  (9001, 2007),
  (9001, 2008),
  (9001, 2009),
  (9001, 2010);

-- ── 验证 ──────────────────────────────────────────────────────────────────
SELECT g.group_id, g.group_name, COUNT(m.user_id) AS member_count
FROM groups g
LEFT JOIN group_members m ON g.group_id = m.group_id
WHERE g.group_id = 9001
GROUP BY g.group_id, g.group_name;
-- 期望输出：group_id=9001, group_name=perf_group_10人, member_count=10
