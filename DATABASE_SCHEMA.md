# Yahoo Fantasy Sports 数据库架构文档

## 概述

此数据库架构专为存储和管理 Yahoo Fantasy Sports 的完整数据而设计，支持多游戏、多联盟的数据结构，包含游戏、联盟、团队、球员、统计数据、名单和交易等信息。

## 数据库表结构

### 1. Games 表（游戏基本信息）

存储 Yahoo Fantasy Sports 中的不同游戏类型信息。

```sql
CREATE TABLE games (
    game_key VARCHAR(20) PRIMARY KEY,           -- 游戏唯一标识
    game_id VARCHAR(20) NOT NULL,               -- 游戏ID
    name VARCHAR(100) NOT NULL,                 -- 游戏名称 (如 "Basketball")
    code VARCHAR(20) NOT NULL,                  -- 游戏代码 (如 "nba", "yahoops")
    type VARCHAR(50),                           -- 游戏类型 (如 "full", "pickem-team-list")
    url VARCHAR(500),                           -- 游戏链接
    season VARCHAR(10) NOT NULL,                -- 赛季 (如 "2024")
    is_registration_over BOOLEAN DEFAULT FALSE, -- 注册是否结束
    is_game_over BOOLEAN DEFAULT FALSE,         -- 游戏是否结束
    is_offseason BOOLEAN DEFAULT FALSE,         -- 是否淡季
    editorial_season VARCHAR(10),               -- 编辑赛季
    picks_status VARCHAR(50),                   -- 选择状态
    contest_group_id VARCHAR(20),               -- 比赛组ID
    scenario_generator BOOLEAN DEFAULT FALSE,   -- 是否有场景生成器
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Leagues 表（联盟信息）

存储联盟的基本信息。

```sql
CREATE TABLE leagues (
    league_key VARCHAR(50) PRIMARY KEY,         -- 联盟唯一标识 (如 "454.l.53472")
    league_id VARCHAR(20) NOT NULL,             -- 联盟ID
    game_key VARCHAR(20) NOT NULL,              -- 关联的游戏
    name VARCHAR(200) NOT NULL,                 -- 联盟名称
    url VARCHAR(500),                           -- 联盟链接
    logo_url VARCHAR(500),                      -- 联盟Logo
    password VARCHAR(100),                      -- 联盟密码
    draft_status VARCHAR(50),                   -- 选秀状态 (predraft, postdraft)
    num_teams INTEGER NOT NULL,                 -- 团队数量
    edit_key VARCHAR(50),                       -- 编辑密钥
    weekly_deadline VARCHAR(50),                -- 每周截止时间设置
    league_update_timestamp VARCHAR(50),        -- 联盟更新时间戳
    scoring_type VARCHAR(50),                   -- 计分类型 (head等)
    league_type VARCHAR(50),                    -- 联盟类型 (private, public)
    renew VARCHAR(50),                          -- 上季联盟key
    renewed VARCHAR(50),                        -- 下季联盟key
    felo_tier VARCHAR(50),                      -- Felo评级 (gold, silver, platinum)
    iris_group_chat_id VARCHAR(100),            -- 群聊ID
    short_invitation_url VARCHAR(500),          -- 邀请链接
    allow_add_to_dl_extra_pos BOOLEAN DEFAULT FALSE, -- 是否允许额外DL位置
    is_pro_league BOOLEAN DEFAULT FALSE,        -- 是否专业联盟
    is_cash_league BOOLEAN DEFAULT FALSE,       -- 是否现金联盟
    current_week VARCHAR(10),                   -- 当前周
    start_week VARCHAR(10),                     -- 开始周
    start_date VARCHAR(20),                     -- 开始日期
    end_week VARCHAR(10),                       -- 结束周
    end_date VARCHAR(20),                       -- 结束日期
    is_finished BOOLEAN DEFAULT FALSE,          -- 是否已结束
    is_plus_league BOOLEAN DEFAULT FALSE,       -- 是否Plus联盟
    game_code VARCHAR(20),                      -- 游戏代码
    season VARCHAR(10) NOT NULL,                -- 赛季
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_key) REFERENCES games(game_key)
);
```

### 3. League_Settings 表（联盟设置）

存储联盟的详细设置信息。

```sql
CREATE TABLE league_settings (
    id SERIAL PRIMARY KEY,
    league_key VARCHAR(50) UNIQUE NOT NULL,     -- 关联的联盟
    draft_type VARCHAR(50),                     -- 选秀类型 (live等)
    is_auction_draft BOOLEAN DEFAULT FALSE,     -- 是否拍卖选秀
    persistent_url VARCHAR(500),                -- 持久URL
    uses_playoff BOOLEAN DEFAULT TRUE,          -- 是否使用季后赛
    has_playoff_consolation_games BOOLEAN DEFAULT FALSE, -- 是否有安慰赛
    playoff_start_week VARCHAR(10),             -- 季后赛开始周
    uses_playoff_reseeding BOOLEAN DEFAULT FALSE, -- 是否重新排种子
    uses_lock_eliminated_teams BOOLEAN DEFAULT FALSE, -- 是否锁定被淘汰队伍
    num_playoff_teams INTEGER,                  -- 季后赛队伍数
    num_playoff_consolation_teams INTEGER DEFAULT 0, -- 安慰赛队伍数
    has_multiweek_championship BOOLEAN DEFAULT FALSE, -- 是否多周冠军赛
    waiver_type VARCHAR(20),                    -- 豁免类型 (FR等)
    waiver_rule VARCHAR(50),                    -- 豁免规则 (all等)
    uses_faab BOOLEAN DEFAULT FALSE,            -- 是否使用FAAB
    draft_time VARCHAR(50),                     -- 选秀时间
    draft_pick_time VARCHAR(10),                -- 选秀时间限制
    post_draft_players VARCHAR(10),             -- 选秀后球员状态 (W等)
    max_teams INTEGER,                          -- 最大队伍数
    waiver_time VARCHAR(10),                    -- 豁免时间
    trade_end_date VARCHAR(20),                 -- 交易截止日期
    trade_ratify_type VARCHAR(50),              -- 交易确认类型 (commish等)
    trade_reject_time VARCHAR(10),              -- 交易拒绝时间
    player_pool VARCHAR(20),                    -- 球员池 (ALL等)
    cant_cut_list VARCHAR(50),                  -- 不可裁员列表 (none等)
    draft_together BOOLEAN DEFAULT FALSE,       -- 是否一起选秀
    is_publicly_viewable BOOLEAN DEFAULT TRUE,  -- 是否公开可见
    can_trade_draft_picks BOOLEAN DEFAULT FALSE, -- 是否可交易选秀权
    sendbird_channel_url VARCHAR(200),          -- 聊天频道URL
    roster_positions JSON,                      -- 阵容位置配置
    stat_categories JSON,                       -- 统计类别配置
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (league_key) REFERENCES leagues(league_key)
);
```

### 4. Teams 表（团队信息）

存储联盟中各团队的信息。

```sql
CREATE TABLE teams (
    team_key VARCHAR(50) PRIMARY KEY,           -- 团队唯一标识 (如 "454.l.53472.t.1")
    team_id VARCHAR(20) NOT NULL,               -- 团队ID
    league_key VARCHAR(50) NOT NULL,            -- 所属联盟
    name VARCHAR(200) NOT NULL,                 -- 团队名称
    url VARCHAR(500),                           -- 团队链接
    team_logo_url VARCHAR(500),                 -- 团队Logo
    division_id VARCHAR(10),                    -- 分区ID
    waiver_priority INTEGER,                    -- 豁免优先级
    faab_balance VARCHAR(20),                   -- FAAB余额
    number_of_moves INTEGER DEFAULT 0,          -- 移动次数
    number_of_trades INTEGER DEFAULT 0,         -- 交易次数
    roster_adds_week VARCHAR(10),               -- 名单添加周
    roster_adds_value VARCHAR(10),              -- 名单添加值
    clinched_playoffs BOOLEAN DEFAULT FALSE,    -- 是否已锁定季后赛
    has_draft_grade BOOLEAN DEFAULT FALSE,      -- 是否有选秀评级
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (league_key) REFERENCES leagues(league_key)
);
```

### 5. Managers 表（团队管理员）

存储团队管理员信息。

```sql
CREATE TABLE managers (
    id SERIAL PRIMARY KEY,
    manager_id VARCHAR(20) NOT NULL,            -- 管理员ID
    team_key VARCHAR(50) NOT NULL,              -- 所属团队
    nickname VARCHAR(100) NOT NULL,             -- 昵称
    guid VARCHAR(100) NOT NULL,                 -- 全局唯一标识
    is_commissioner BOOLEAN DEFAULT FALSE,      -- 是否是联盟专员
    email VARCHAR(200),                         -- 邮箱
    image_url VARCHAR(500),                     -- 头像URL
    felo_score VARCHAR(20),                     -- Felo分数
    felo_tier VARCHAR(50),                      -- Felo等级
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_key) REFERENCES teams(team_key)
);
```

### 6. Players 表（球员信息）

存储球员的静态和动态信息。

```sql
CREATE TABLE players (
    player_key VARCHAR(50) PRIMARY KEY,         -- 球员唯一标识 (如 "454.p.3704")
    player_id VARCHAR(20) NOT NULL,             -- 球员ID
    editorial_player_key VARCHAR(50) NOT NULL,  -- 编辑球员key (如 "nba.p.3704")
    league_key VARCHAR(50) NOT NULL,            -- 所属联盟
    
    -- 静态信息
    full_name VARCHAR(200) NOT NULL,            -- 全名
    first_name VARCHAR(100),                    -- 名
    last_name VARCHAR(100),                     -- 姓
    
    -- 动态信息
    current_team_key VARCHAR(20),               -- 当前NBA球队key
    current_team_name VARCHAR(100),             -- 当前NBA球队名
    current_team_abbr VARCHAR(10),              -- 当前NBA球队缩写
    display_position VARCHAR(50),               -- 显示位置 (如 "SF,PF")
    primary_position VARCHAR(10),               -- 主要位置
    position_type VARCHAR(10),                  -- 位置类型
    uniform_number VARCHAR(10),                 -- 球衣号码
    status VARCHAR(20),                         -- 状态 (如 "INJ")
    image_url VARCHAR(500),                     -- 图片URL
    headshot_url VARCHAR(500),                  -- 头像URL
    is_undroppable BOOLEAN DEFAULT FALSE,       -- 是否不可裁员
    eligible_positions JSON,                    -- 合适位置列表
    season VARCHAR(10) NOT NULL,                -- 赛季
    last_updated TIMESTAMP,                     -- 最后更新时间
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (league_key) REFERENCES leagues(league_key)
);
```

### 7. Player_Stats 表（球员统计）

存储球员的统计数据。

```sql
CREATE TABLE player_stats (
    id SERIAL PRIMARY KEY,
    player_key VARCHAR(50) NOT NULL,            -- 关联球员
    stat_id VARCHAR(20) NOT NULL,               -- 统计项ID (如 "9004003", "5")
    value VARCHAR(100) NOT NULL,                -- 统计值
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_key) REFERENCES players(player_key),
    UNIQUE(player_key, stat_id)
);
```

### 8. Rosters 表（团队名单）

存储各团队的球员名单信息。

```sql
CREATE TABLE rosters (
    id SERIAL PRIMARY KEY,
    team_key VARCHAR(50) NOT NULL,              -- 关联团队
    player_key VARCHAR(50) NOT NULL,            -- 关联球员
    coverage_date VARCHAR(20),                  -- 名单日期
    is_prescoring BOOLEAN DEFAULT FALSE,        -- 是否预计分
    is_editable BOOLEAN DEFAULT FALSE,          -- 是否可编辑
    
    -- 球员当前状态
    status VARCHAR(20),                         -- 状态 (如 "INJ")
    status_full VARCHAR(100),                   -- 完整状态描述
    injury_note VARCHAR(200),                   -- 伤病说明
    is_keeper BOOLEAN DEFAULT FALSE,            -- 是否是保留球员
    keeper_cost VARCHAR(20),                    -- 保留成本
    kept BOOLEAN DEFAULT FALSE,                 -- 是否已保留
    
    -- 位置信息
    selected_position VARCHAR(20),              -- 当前选择位置
    eligible_positions_to_add JSON,             -- 可添加的位置
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_key) REFERENCES teams(team_key),
    FOREIGN KEY (player_key) REFERENCES players(player_key)
);
```

### 9. Transactions 表（交易记录）

存储联盟的交易记录。

```sql
CREATE TABLE transactions (
    transaction_key VARCHAR(50) PRIMARY KEY,    -- 交易唯一标识
    transaction_id VARCHAR(20) NOT NULL,        -- 交易ID
    league_key VARCHAR(50) NOT NULL,            -- 所属联盟
    type VARCHAR(50) NOT NULL,                  -- 交易类型 (如 "add/drop", "trade")
    status VARCHAR(50) NOT NULL,                -- 交易状态 (如 "successful")
    timestamp VARCHAR(50) NOT NULL,             -- 交易时间戳
    players_data JSON,                          -- 完整球员交易数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (league_key) REFERENCES leagues(league_key)
);
```

### 10. Transaction_Players 表（交易球员详情）

存储交易中涉及的球员详细信息。

```sql
CREATE TABLE transaction_players (
    id SERIAL PRIMARY KEY,
    transaction_key VARCHAR(50) NOT NULL,       -- 关联交易
    player_key VARCHAR(50) NOT NULL,            -- 球员key
    player_id VARCHAR(20) NOT NULL,             -- 球员ID
    player_name VARCHAR(200) NOT NULL,          -- 球员姓名
    editorial_team_abbr VARCHAR(10),            -- NBA球队缩写
    display_position VARCHAR(50),               -- 显示位置
    position_type VARCHAR(10),                  -- 位置类型
    
    -- 交易数据
    transaction_type VARCHAR(20) NOT NULL,      -- 交易类型 (add, drop, trade)
    source_type VARCHAR(50),                    -- 来源类型 (freeagents, team, waivers)
    source_team_key VARCHAR(50),                -- 来源团队key
    source_team_name VARCHAR(200),              -- 来源团队名
    destination_type VARCHAR(50),               -- 目标类型 (team, waivers)
    destination_team_key VARCHAR(50),           -- 目标团队key
    destination_team_name VARCHAR(200),         -- 目标团队名
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_key) REFERENCES transactions(transaction_key)
);
```

## 表关系图

```
Games (1) ──→ (N) Leagues
    │
    └──→ (N) League_Settings (1:1)
    │
    └──→ (N) Teams
    │        │
    │        └──→ (N) Managers
    │        │
    │        └──→ (N) Rosters ←── (N) Players
    │                              │
    │                              └──→ (N) Player_Stats
    │
    └──→ (N) Transactions
             │
             └──→ (N) Transaction_Players
```

## 索引建议

为了优化查询性能，建议创建以下索引：

```sql
-- 外键索引
CREATE INDEX idx_leagues_game_key ON leagues(game_key);
CREATE INDEX idx_league_settings_league_key ON league_settings(league_key);
CREATE INDEX idx_teams_league_key ON teams(league_key);
CREATE INDEX idx_managers_team_key ON managers(team_key);
CREATE INDEX idx_players_league_key ON players(league_key);
CREATE INDEX idx_player_stats_player_key ON player_stats(player_key);
CREATE INDEX idx_rosters_team_key ON rosters(team_key);
CREATE INDEX idx_rosters_player_key ON rosters(player_key);
CREATE INDEX idx_transactions_league_key ON transactions(league_key);
CREATE INDEX idx_transaction_players_transaction_key ON transaction_players(transaction_key);

-- 查询优化索引
CREATE INDEX idx_players_editorial_key ON players(editorial_player_key);
CREATE INDEX idx_players_current_team ON players(current_team_abbr);
CREATE INDEX idx_transactions_type ON transactions(type);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX idx_rosters_coverage_date ON rosters(coverage_date);
```

## 使用示例

### 1. 查询联盟中的所有团队及其管理员

```sql
SELECT t.name as team_name, m.nickname as manager_nickname, m.felo_tier
FROM teams t
JOIN managers m ON t.team_key = m.team_key
WHERE t.league_key = '454.l.53472'
ORDER BY t.team_id;
```

### 2. 查询球员统计数据

```sql
SELECT p.full_name, p.current_team_abbr, ps.stat_id, ps.value
FROM players p
JOIN player_stats ps ON p.player_key = ps.player_key
WHERE p.league_key = '454.l.53472'
AND ps.stat_id IN ('5', '8', '12')  -- FG%, FT%, Points
ORDER BY p.full_name;
```

### 3. 查询团队当前名单

```sql
SELECT t.name as team_name, p.full_name, p.display_position, r.selected_position
FROM teams t
JOIN rosters r ON t.team_key = r.team_key
JOIN players p ON r.player_key = p.player_key
WHERE t.league_key = '454.l.53472'
AND r.coverage_date = '2025-06-01'
ORDER BY t.team_id, r.selected_position;
```

### 4. 查询最近的交易记录

```sql
SELECT tr.timestamp, tr.type, tp.player_name, tp.transaction_type, 
       tp.source_team_name, tp.destination_team_name
FROM transactions tr
JOIN transaction_players tp ON tr.transaction_key = tp.transaction_key
WHERE tr.league_key = '454.l.53472'
ORDER BY tr.timestamp DESC
LIMIT 20;
```

## 数据导入

使用 `data_importer.py` 脚本可以将从 Yahoo Fantasy API 获取的 JSON 数据导入到数据库：

```bash
python3 data_importer.py
```

该脚本会自动：
1. 创建数据库表（如果不存在）
2. 导入游戏和联盟基本信息
3. 导入选定联盟的详细数据（团队、球员、统计、名单、交易等）
4. 处理数据重复和错误情况

## 环境配置

在 `.env` 文件中配置数据库连接：

```env
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db
``` 