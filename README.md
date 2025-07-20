# Fantasy Tool 2.0 - 重构版本

**Yahoo Fantasy Sports分析工具** - 标准化架构版本

## 🎯 项目概述

Fantasy Tool 2.0 是对原有scripts的完全重构，采用现代化的模块化架构，为后续的分析功能开发奠定基础。

### 🔄 架构变化

**从旧架构 (scripts/)：**
```
scripts/
├── app.py              # OAuth认证
├── model.py            # 数据库模型  
├── yahoo_api_utils.py  # API工具
├── database_writer.py  # 数据写入
└── yahoo_api_data.py   # 主程序
```

**到新架构 (fantasy_etl/)：**
```
fantasy_etl/
├── __main__.py     # 主入口点 🆕
├── auth/           # 认证模块
├── data/           # 数据层
│   ├── extract/    # 数据提取
│   ├── transform/  # 数据转换
│   └── load/       # 数据加载
├── analytics/      # 分析引擎 🆕
│   ├── team/       # 球队分析
│   ├── trading/    # 交易建议
│   └── stats/      # 统计分析
├── core/           # 核心功能
│   ├── config/     # 配置管理 🆕
│   ├── database/   # 数据库管理
│   └── utils/      # 工具函数 🆕
└── api/            # API接口层
main.py             # 简化启动器 🆕
```

## ✨ 新功能特性

### 🏀 现有功能 (完全保留)
- ✅ Yahoo Fantasy Sports OAuth认证
- ✅ 联盟数据获取和选择
- ✅ 球员统计数据提取（日统计&赛季统计）
- ✅ 阵容历史数据追踪
- ✅ 团队每周对战数据
- ✅ 团队赛季排名数据
- ✅ 交易记录追踪
- ✅ 数据库存储和管理
- ✅ 交互式命令行界面

### 🆕 新增功能框架
- 🔧 **统一配置管理** - 环境变量和配置文件
- 📊 **球队分析引擎** - 球队表现分析和比较
- 💹 **交易建议系统** - 球员价值评估和交易建议
- 📈 **统计计算器** - 高级统计指标计算
- 🛠️ **工具函数库** - 重试机制、数据验证等
- 🔌 **模块化架构** - 易于扩展和维护

### 🚀 为未来准备
- 📊 **PySpark集成支持** - 大数据处理能力
- 📱 **可视化面板基础** - 图表和仪表板
- 🤖 **机器学习准备** - 预测模型基础

## 🚀 快速开始

### 1. 环境配置

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\\Scripts\\activate  # Windows

# 安装依赖
pip install -r requirements/development.txt
```

### 2. 配置设置

```bash
# 复制配置模板
cp environments/development.env .env

# 编辑配置文件，填入Yahoo API密钥
vim .env
```

### 3. 运行应用

```bash
# 启动主程序
python -m fantasy_etl
# 或者
python main.py

# 仅启动认证服务器
python -m fantasy_etl auth-server
```

## 📂 目录结构详解

### `/fantasy_etl/auth/` - 认证模块
- `oauth_manager.py` - OAuth管理器
- `web_auth_server.py` - Web认证服务器

### `/fantasy_etl/data/` - 数据层
- `extract/yahoo_api_client.py` - Yahoo API客户端（支持批量球员统计、团队数据）
- `load/database_loader.py` - 数据库加载器（批量加载、统计解析）
- `transform/` - 数据转换 (预留)

### `/fantasy_etl/analytics/` - 分析引擎 🆕
- `team/analyzer.py` - 球队分析器
- `trading/engine.py` - 交易建议引擎  
- `stats/calculator.py` - 统计计算器

### `/fantasy_etl/core/` - 核心功能
- `config/settings.py` - 统一配置管理
- `database/models.py` - 数据库模型
- `utils/helpers.py` - 工具函数

### `/fantasy_etl/api/` - API接口层
- `fantasy_data_service.py` - 数据服务
- `cli_interface.py` - 命令行界面

## 🔧 配置说明

### 环境变量

| 变量名 | 说明 | 默认值 |
|-------|------|--------|
| `YAHOO_CLIENT_ID` | Yahoo API客户端ID | 必填 |
| `YAHOO_CLIENT_SECRET` | Yahoo API客户端密钥 | 必填 |
| `YAHOO_REDIRECT_URI` | OAuth重定向URI | `http://localhost:8000/auth/callback` |
| `DB_HOST` | 数据库主机 | `localhost` |
| `DB_PORT` | 数据库端口 | `5432` |
| `DB_NAME` | 数据库名称 | `fantasy_db` |
| `DB_USER` | 数据库用户 | `fantasy_user` |
| `DB_PASSWORD` | 数据库密码 | `fantasyPassword` |

## 📋 使用指南

### 1. 认证流程
```python
from src.auth import OAuthManager

oauth = OAuthManager()
if not oauth.is_authenticated():
    # 启动Web认证服务器
    pass
```

### 2. 数据获取
```python
from src.api import FantasyDataService

service = FantasyDataService()
service.fetch_and_select_league()
service.fetch_league_complete_data()
```

### 3. 球队分析 🆕
```python
from src.analytics import TeamAnalyzer

analyzer = TeamAnalyzer()
analysis = analyzer.analyze_team(team_key, league_key, season)
print(f"球队优势: {analysis.strengths}")
print(f"改进建议: {analysis.recommendations}")
```

### 4. 交易建议 🆕
```python
from src.analytics import TradingEngine

engine = TradingEngine()
recommendations = engine.generate_trade_recommendations(team_key, league_key, season)
for rec in recommendations:
    print(f"建议: {rec.reasoning}")
```

## 🧪 测试

```bash
# 运行所有测试
pytest

# 运行特定模块测试
pytest fantasy_etl/auth/tests/
pytest fantasy_etl/analytics/tests/

# 生成覆盖率报告
pytest --cov=fantasy_etl tests/
```

## 📊 数据库架构

保持与原版本完全兼容的18个表结构，包括：
- 核心表: `games`, `leagues`, `teams`, `players`
- 统计表: `player_season_stats`, `player_daily_stats`, `team_stats_weekly`
- 分析表: `league_standings`, `team_matchups`, `transactions`

## 🤝 开发指南

### 添加新分析功能

1. 在 `fantasy_etl/analytics/` 下创建新模块
2. 继承基础分析类
3. 实现分析逻辑
4. 在 `__init__.py` 中导出

### 扩展数据提取

1. 在 `fantasy_etl/data/extract/` 下添加新的提取器
2. 使用统一的 `APIResponse` 格式
3. 集成到 `FantasyDataService`

## 📈 性能优化

- ✅ 数据库连接池
- ✅ 批量数据处理
- ✅ API请求重试机制
- ✅ 配置化延迟设置
- 🔄 Redis缓存 (计划中)
- 🔄 异步处理 (计划中)

## 🚀 部署

### 开发环境
```bash
pip install -r requirements/development.txt
python -m fantasy_etl
```

### 生产环境
```bash
pip install -r requirements/production.txt
gunicorn --config gunicorn.conf.py app:application
```

## 📝 迁移指南

### 从scripts迁移到新架构

1. **认证部分**：
   - `scripts/app.py` → `fantasy_etl/auth/web_auth_server.py`
   - `scripts/yahoo_api_utils.py` → `fantasy_etl/auth/oauth_manager.py`

2. **数据库部分**：
   - `scripts/model.py` → `fantasy_etl/core/database/models.py`
   - `scripts/database_writer.py` → `fantasy_etl/data/load/database_loader.py`

3. **API部分**：
   - `scripts/yahoo_api_utils.py` → `fantasy_etl/data/extract/yahoo_api_client.py`
   - `scripts/yahoo_api_data.py` → `fantasy_etl/api/fantasy_data_service.py`

4. **启动方式**：
   - `scripts/run_app.py` → `fantasy_etl/__main__.py` + `main.py`
   - `scripts/auth_server.py` → `fantasy_etl/__main__.py auth-server`

### 配置迁移

原来的硬编码配置现在通过 `fantasy_etl/core/config/settings.py` 统一管理。

## 🔮 未来规划

### Phase 1: 基础分析 (当前)
- ✅ 球队表现分析
- ✅ 球员价值评估
- ✅ 交易建议引擎

### Phase 2: 高级分析
- 🔄 机器学习预测模型
- 🔄 实时数据更新
- 🔄 自动化交易建议

### Phase 3: 可视化面板
- 🔄 Web仪表板
- 🔄 实时图表
- 🔄 移动端支持

### Phase 4: 大数据处理
- 🔄 PySpark集成
- 🔄 历史数据分析
- 🔄 多联盟比较

## 📞 支持

如有问题，请：
1. 检查 `.env` 配置
2. 查看应用日志
3. 参考 `archive/` 目录中的原始脚本

---

## 📋 Migration Changelog (2025-01-20)

### ✅ 已完成的代码迁移工作

**从 `archive/yahoo_api_data.py` 和 `archive/database_writer.py` 迁移的功能：**

#### 🔄 数据提取层 (`fantasy_etl/data/extract/yahoo_api_client.py`)
- ✅ `get_players_stats_batch()` - 批量获取球员日统计
- ✅ `get_players_season_stats_batch()` - 批量获取球员赛季统计  
- ✅ `get_player_stats_for_date()` - 指定日期球员统计
- ✅ `get_team_roster()` - 团队阵容数据
- ✅ `get_team_matchups()` - 团队对战数据
- ✅ `get_league_standings()` - 联盟排名数据

#### 💾 数据加载层 (`fantasy_etl/data/load/database_loader.py`) 
- ✅ `load_roster_daily_data()` - 每日阵容数据批量加载
- ✅ `load_players_daily_stats_batch()` - 球员日统计批量加载
- ✅ `load_players_season_stats_batch()` - 球员赛季统计批量加载
- ✅ `load_team_matchups_batch()` - 团队对战数据批量加载
- ✅ `load_team_weekly_stats_batch()` - 团队每周统计批量加载
- ✅ `load_league_standings_batch()` - 联盟排名数据批量加载
- ✅ 完整的11项篮球统计解析（投篮、篮板、助攻等）

#### 🎯 服务层 (`fantasy_etl/api/fantasy_data_service.py`)
- ✅ `fetch_roster_history_data()` - 阵容历史数据获取
- ✅ `fetch_player_daily_stats_data()` - 球员日统计数据获取
- ✅ `fetch_player_season_stats_data()` - 球员赛季统计数据获取
- ✅ `fetch_team_weekly_data()` - 团队每周数据获取
- ✅ `fetch_team_season_data()` - 团队赛季数据获取
- ✅ 时间范围计算和赛季日期信息

#### 🖥️ CLI界面 (`fantasy_etl/api/cli_interface.py`)
- ✅ 选项3: 获取阵容历史数据 - 支持时间范围选择
- ✅ 选项4: 获取球员日统计数据 - 支持日期范围和大数据量警告
- ✅ 选项5: 获取球员赛季统计数据 - 一键获取所有球员
- ✅ 选项8: 获取团队每周数据 - 对战和统计数据
- ✅ 选项9: 获取团队赛季数据 - 排名和赛季统计

#### 🧪 测试验证
- ✅ 所有模块初始化测试通过
- ✅ 新增方法存在性验证通过
- ✅ 主程序启动测试成功
- ✅ 认证和配置系统正常工作
- ✅ CLI菜单显示完整

### 🔧 技术改进
- **批量处理**: 每批25个球员，避免API限制
- **错误处理**: 统一的重试机制和错误报告
- **数据解析**: 标准化的统计数据解析器
- **状态管理**: upsert操作支持，避免重复数据
- **时间管理**: 灵活的日期范围选择（最近7天、30天、自定义、完整赛季）

**注意**: 原 `scripts/` 目录已迁移至 `archive/` 作为参考，新架构完全兼容原有功能并提供更好的扩展性。