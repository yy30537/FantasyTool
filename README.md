# Yahoo Fantasy Sports 数据获取和管理工具

一个完整的Yahoo Fantasy Sports数据获取、处理和数据库管理系统，支持获取游戏、联盟、团队、球员等各类数据，并提供优化的存储和查询功能。

## 🚀 快速开始

### 1. 环境设置

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境 (Linux/macOS)
source venv/bin/activate

# 安装Python依赖
pip install -r requirements.txt

# 确保安装cryptography库（用于HTTPS支持）
pip install cryptography
```

### 2. Node.js和Prisma设置（可选，用于数据库查询）

```bash
# 初始化Node.js项目
npm init -y
npm install prisma typescript ts-node @prisma/client
npm install -D @types/node

# 初始化Prisma
npx prisma init

# 安装依赖
npm install

# 生成Prisma客户端
npx prisma generate

# 运行查询脚本查看数据
npm run query
```

### 3. 配置文件

- 创建 `.env` 文件存储数据库连接信息
- 配置Yahoo API认证信息（通过 `app.py` 完成OAuth授权）

## 📊 数据获取流程

### 基础数据获取

```bash
# 🎯 推荐：获取完整的数据库就绪数据（样本）
python3 fetch_fantasy_data.py --sample-database-ready

# 获取完整的数据库就绪数据（所有联盟）
python3 fetch_fantasy_data.py --database-ready

# 获取优化存储的完整数据（推荐用于生产）
python3 fetch_fantasy_data.py --sample-optimized-data
```

### 分步骤数据获取

```bash
# 1. 获取游戏数据
python3 fetch_fantasy_data.py --games

# 2. 获取联盟基本数据
python3 fetch_fantasy_data.py --leagues

# 3. 获取联盟详细数据（设置、排名、记分板）
python3 fetch_fantasy_data.py --league-details

# 4. 获取团队数据
python3 fetch_fantasy_data.py --teams

# 5. 获取团队球员名单数据
python3 fetch_fantasy_data.py --team-rosters          # 所有团队
python3 fetch_fantasy_data.py --sample-rosters        # 样本数据

# 6. 获取球员数据（基本信息 + 统计数据）
python3 fetch_fantasy_data.py --players-data          # 完整数据
python3 fetch_fantasy_data.py --sample-players-data   # 样本数据
```

### 高级数据管理

```bash
# 创建统一球员数据库（去重复）
python3 fetch_fantasy_data.py --create-unified-db
python3 fetch_fantasy_data.py --sample-unified-db

# 为每个联盟创建独立数据库
python3 fetch_fantasy_data.py --league-unified-dbs
python3 fetch_fantasy_data.py --sample-league-unified-dbs

# 数据分析和概览
python3 fetch_fantasy_data.py --overview
python3 fetch_fantasy_data.py --extract-keys
```

### 单独获取特定数据

```bash
# 获取特定游戏的联盟数据
python3 fetch_fantasy_data.py --game-key 364

# 获取特定联盟的详细数据
python3 fetch_fantasy_data.py --league-key 364.l.15712

# 获取特定联盟的团队数据
python3 fetch_fantasy_data.py --league-teams 364.l.15712

# 获取特定团队的球员名单
python3 fetch_fantasy_data.py --team-key 364.l.15712.t.1

# 获取特定联盟的球员数据
python3 fetch_fantasy_data.py --league-players-key 364.l.15712 --status A

# 获取特定联盟的球员统计数据
python3 fetch_fantasy_data.py --league-stats 364.l.15712
```

### 球员数据参数

```bash
# 按位置过滤球员
python3 fetch_fantasy_data.py --league-players-key 364.l.15712 --position QB

# 按状态过滤球员
python3 fetch_fantasy_data.py --league-players-key 364.l.15712 --status FA  # 自由球员

# NFL游戏按周次获取数据
python3 fetch_fantasy_data.py --team-key 364.l.15712.t.1 --week 1

# MLB/NBA/NHL按日期获取数据
python3 fetch_fantasy_data.py --team-key 364.l.15712.t.1 --date 2023-10-15
```

## 🗂️ 数据存储结构

### 优化存储结构（推荐）

```
data/
├── games/                      # 游戏数据
│   └── games_data.json
├── leagues/                    # 联盟基本数据
│   ├── league_data_*.json
│   └── all_leagues_data.json
├── league_settings/            # 联盟设置
├── league_standings/           # 联盟排名
├── league_scoreboards/         # 联盟记分板
├── teams/                      # 团队数据
├── team_rosters/              # 团队球员名单（按联盟分类）
│   └── {league_key}/
│       └── team_roster_*.json
├── players/                   # 球员基本信息（按联盟分类）
│   └── {league_key}/
│       ├── players_basic_info.json
│       └── unified_database.json
└── player_stats/             # 球员统计数据（按联盟分类）
    └── {league_key}/
        ├── batch_001_season.json
        ├── batch_002_season.json
        └── batches_index.json
```

### 数据优化特性

✨ **优化功能**：
- **数据分离**：球员基本信息和统计数据分别存储，避免重复
- **按联盟分类**：自动为每个联盟创建子目录管理
- **批次管理**：统计数据按批次存储，便于处理大量数据
- **数据压缩**：去除冗余信息，实现约70-80%的数据压缩
- **索引文件**：提供批次索引和元数据信息

## 🗄️ 数据库管理

### 初始化和重建数据库

```bash
# 初始化数据库
python3 load_db.py --init-db

# 重新创建数据库表并加载数据
python3 load_db.py --recreate-db
```

### 加载数据到数据库

```bash
# 加载所有数据
python3 load_db.py

# 分类加载数据
python3 load_db.py --games                # 只加载游戏数据
python3 load_db.py --leagues              # 只加载联盟数据
python3 load_db.py --teams                # 只加载团队数据
python3 load_db.py --league-settings      # 只加载联盟设置数据
python3 load_db.py --league-standings     # 只加载联盟排名数据
python3 load_db.py --league-scoreboards   # 只加载联盟记分板数据
python3 load_db.py --update-user-games    # 更新用户游戏关联
```

### 数据验证

```bash
# 验证数据库数据完整性
python3 verify_db.py
```

## 🌐 Web界面

启动Web服务器查看数据：

```bash
# 启动Web服务器
python3 app.py
```

访问 http://localhost:3000 查看：
- 主页：显示游戏和用户信息
- 游戏详情页：展示游戏和关联联盟
- API端点：
  - `/api/games` - 游戏数据
  - `/api/leagues` - 联盟数据  
  - `/api/teams` - 团队数据

## 🔧 工具功能

### 交互式模式

```bash
# 启动交互式菜单
python3 fetch_fantasy_data.py
```

提供20个选项的交互式菜单，包括：
1. 获取完整数据
2. 获取游戏数据
3. 获取联盟基本数据
4. 获取联盟详细数据
5. 获取团队数据
6. 获取所有团队球员名单数据
7. 获取团队球员名单数据（样本）
8. 获取联盟球员数据（样本）
9. 获取完整的球员数据
10. 获取球员数据样本
11. 从roster数据中提取球员键
12. 为球员统计数据添加元数据信息
13. 创建统一的球员数据库（去重复）
14. 创建统一的球员数据库样本
15. 获取完整的数据库就绪数据
16. 获取数据库就绪数据样本
17. 显示数据概览和统计信息
18. 为每个联盟创建独立的统一数据库
19. 为样本联盟创建独立的统一数据库
20. 获取优化存储的完整数据

### 数据分析工具

```bash
# 显示数据概览和统计信息
python3 fetch_fantasy_data.py --overview

# 提取并显示所有游戏键
python3 fetch_fantasy_data.py --extract-keys

# 整合联盟数据到汇总文件
python3 fetch_fantasy_data.py --consolidate

# 从roster数据中提取球员键
python3 fetch_fantasy_data.py --extract-player-keys

# 为球员统计数据添加元数据信息
python3 fetch_fantasy_data.py --enrich-player-stats
```

### 请求控制

```bash
# 设置请求间隔时间（避免API限制）
python3 fetch_fantasy_data.py --delay 3 --complete
```

## 📋 球员状态和位置代码

### 球员状态代码
- `A` - 所有球员
- `FA` - 自由球员 (Free Agents)
- `W` - 豁免球员 (Waivers)
- `T` - 已被选中 (Taken)
- `K` - 保留球员 (Keepers)

### 常见位置代码
- **NFL**: QB, RB, WR, TE, K, DEF
- **NBA**: PG, SG, SF, PF, C
- **MLB**: C, 1B, 2B, 3B, SS, OF, SP, RP
- **NHL**: C, LW, RW, D, G

## 🔍 数据类型说明

### 游戏类型过滤
系统自动过滤只获取 `type` 为 `"full"` 的游戏数据，跳过其他类型的游戏。

### 统计数据类型
- `season` - 赛季统计
- `week` - 周统计（NFL）
- `date` - 日期统计（MLB/NBA/NHL）

## 🛠️ 技术栈

- **后端**: Python, SQLAlchemy
- **数据库**: PostgreSQL
- **ORM**: Prisma (TypeScript)
- **Web框架**: Express.js
- **前端**: EJS模板
- **API**: Yahoo Fantasy Sports API

## 📝 注意事项

1. **API限制**: 建议设置适当的请求间隔（默认2秒）避免触发API限制
2. **数据量**: 完整数据获取可能需要较长时间，建议先使用样本模式测试
3. **存储空间**: 完整数据可能占用较大存储空间，优化模式可减少70-80%
4. **认证**: 首次使用需要通过 `app.py` 完成Yahoo OAuth授权

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## �� 许可证

本项目采用MIT许可证。