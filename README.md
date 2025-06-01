# Yahoo Fantasy Sports 数据获取工具

一个完整的Yahoo Fantasy Sports数据获取系统，支持获取联盟、团队、球员和交易等数据，并将数据存储到结构化的数据库中。

## 🚀 快速开始

### 1. 环境设置

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境 (Linux/macOS)
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 数据库配置

创建 `.env` 文件：
```env
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db
```

### 3. Yahoo API授权

```bash
# 启动OAuth授权流程
python3 app.py
```

访问 `http://localhost:5000`，完成Yahoo账号授权，获取API访问令牌。

## 📊 数据获取

### 单联盟完整数据获取

```bash
# 获取完整的联盟数据（推荐方式）
python3 single_league_fetcher.py --complete

# 自定义请求间隔
python3 single_league_fetcher.py --complete --delay 3
```

## 🗄️ 数据库模型

本系统使用PostgreSQL数据库存储Yahoo Fantasy数据。数据库架构包含以下核心表：

### 核心表结构
- **Games** - 游戏基本信息（NBA、NHL等）
- **Leagues** - 联盟信息
- **League_Settings** - 联盟详细设置
- **Teams** - 团队信息
- **Managers** - 团队管理员
- **Players** - 球员信息（静态+动态）
- **Player_Stats** - 球员统计数据
- **Rosters** - 团队名单
- **Transactions** - 交易记录
- **Transaction_Players** - 交易球员详情

详细的数据库架构说明请参考 [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)

### 创建数据库表

```bash
# 使用model.py创建所有数据库表
python3 model.py
```

### 数据导入

```bash
# 将JSON数据导入到数据库
python3 data_importer.py
```

该脚本会自动：
1. 创建数据库表（如果不存在）
2. 导入游戏和联盟基本信息
3. 导入选定联盟的详细数据（团队、球员、统计、名单、交易等）
4. 处理数据重复和错误情况

## 🗂️ 数据文件结构

成功获取数据后，文件将按以下结构存储：

```
data/
├── games_data.json                    # 游戏基本信息
├── all_leagues_data.json             # 所有联盟数据
└── selected_league_{league_key}/     # 选定联盟的完整数据
    ├── league_info.json              # 联盟详细信息（包含设置、排名、记分板）
    ├── teams.json                    # 团队基本信息
    ├── rosters/                      # 团队球员名单
    │   ├── team_roster_454.l.53472.t.1.json
    │   ├── team_roster_454.l.53472.t.2.json
    │   └── ...                       # 每个团队一个文件
    ├── players/                      # 球员数据
    │   ├── static_players.json      # 球员静态信息（ID、姓名）
    │   ├── dynamic_players.json     # 球员动态信息（团队、位置、状态）
    │   └── player_stats.json        # 球员统计数据
    └── transactions/                 # 交易数据
        └── all_transactions.json    # 所有交易记录
```

## 📈 数据查询示例

### 查询联盟团队及管理员

```sql
SELECT t.name as team_name, m.nickname as manager_nickname, m.felo_tier
FROM teams t
JOIN managers m ON t.team_key = m.team_key
WHERE t.league_key = '454.l.53472'
ORDER BY t.team_id;
```

### 查询球员统计数据

```sql
SELECT p.full_name, p.current_team_abbr, ps.stat_id, ps.value
FROM players p
JOIN player_stats ps ON p.player_key = ps.player_key
WHERE p.league_key = '454.l.53472'
AND ps.stat_id IN ('5', '8', '12')  -- FG%, FT%, Points
ORDER BY p.full_name;
```

### 查询团队当前名单

```sql
SELECT t.name as team_name, p.full_name, p.display_position, r.selected_position
FROM teams t
JOIN rosters r ON t.team_key = r.team_key
JOIN players p ON r.player_key = p.player_key
WHERE t.league_key = '454.l.53472'
AND r.coverage_date = '2025-06-01'
ORDER BY t.team_id, r.selected_position;
```

### 查询最近交易记录

```sql
SELECT tr.timestamp, tr.type, tp.player_name, tp.transaction_type, 
       tp.source_team_name, tp.destination_team_name
FROM transactions tr
JOIN transaction_players tp ON tr.transaction_key = tp.transaction_key
WHERE tr.league_key = '454.l.53472'
ORDER BY tr.timestamp DESC
LIMIT 20;
```

## 🔧 主要功能

### 数据获取功能
- ✅ Yahoo API OAuth 2.0 授权
- ✅ 游戏信息获取
- ✅ 联盟信息获取
- ✅ 团队详细信息获取
- ✅ 球员静态信息获取（姓名、ID）
- ✅ 球员动态信息获取（当前队伍、位置、状态）
- ✅ 球员统计数据获取
- ✅ 团队名单获取
- ✅ 交易记录获取（完整历史）
- ✅ 联盟设置和规则获取

### 数据管理功能
- ✅ 结构化数据库存储
- ✅ 数据去重和错误处理
- ✅ 增量数据更新支持
- ✅ 完整的数据关系映射
- ✅ 查询优化和索引

## 📁 项目结构

```
FantasyTool/
├── app.py                    # OAuth授权服务器
├── single_league_fetcher.py  # 主要数据获取脚本
├── model.py                  # 数据库模型定义
├── data_importer.py          # JSON数据导入脚本
├── requirements.txt          # Python依赖
├── README.md                 # 项目说明
├── DATABASE_SCHEMA.md        # 数据库架构文档
├── data/                     # 数据存储目录
├── tokens/                   # OAuth令牌存储
└── yahoo-fantasy-sports-API-docs/  # API文档
```

## 🛠️ 技术栈

- **Python 3.12+** - 主要编程语言
- **Flask** - OAuth授权服务器
- **SQLAlchemy** - ORM和数据库操作
- **PostgreSQL** - 主数据库
- **Requests** - HTTP客户端
- **Yahoo Fantasy Sports API** - 数据源

## 🔗 相关文档

- [Yahoo OAuth Guide](yahoo-fantasy-sports-API-docs/OAuth/Yahoo-OAuth-Guide.md) - Yahoo API授权详细说明
- [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) - 完整数据库架构文档

## 📝 注意事项

1. **API限制**: Yahoo Fantasy API有请求频率限制，建议在请求间添加适当延迟
2. **数据量**: 完整数据获取可能需要较长时间，特别是大联盟的交易数据
3. **数据库**: 确保PostgreSQL服务正在运行并正确配置
4. **令牌管理**: OAuth令牌有有效期，需要定期刷新

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目！

## �� 许可证

本项目采用MIT许可证。
