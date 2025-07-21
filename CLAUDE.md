# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Yahoo Fantasy Sports ETL (Extract, Transform, Load) pipeline that extracts data from Yahoo Fantasy API and stores it in a PostgreSQL database. The project consists of several large modules that need refactoring for better maintainability.

## Current ETL Architecture

### 🎯 NEW: Modular ETL System
The project has been refactored into a modular ETL system:

```
fantasy_etl/
├── core/
│   ├── auth.py           # OAuth authentication (extracted from yahoo_api_utils.py)
│   └── config.py         # Configuration management
├── extractors/
│   ├── base.py           # Base extractor with common API logic
│   ├── leagues.py        # League data extraction
│   ├── teams.py          # Team data extraction  
│   ├── players.py        # Player data extraction
│   └── transactions.py   # Transaction data extraction
├── loaders/
│   ├── base.py           # Base loader with common DB logic
│   ├── league_loader.py  # League data loading
│   ├── team_loader.py    # Team data loading
│   ├── player_loader.py  # Player data loading
│   └── transaction_loader.py # Transaction data loading
├── etl_coordinator.py    # Main ETL orchestrator
├── cli.py               # Command-line interface
└── compat.py            # Compatibility layer for legacy code
```

### Legacy Files (Still Available)
- **yahoo_api_utils.py**: OAuth认证、API请求管理 → Replaced by `fantasy_etl/core/auth.py`
- **yahoo_api_data.py**: 数据提取逻辑 (2797 lines) → Replaced by `fantasy_etl/extractors/` + `etl_coordinator.py`
- **database_writer.py**: 批量数据写入 (2135 lines) → Replaced by `fantasy_etl/loaders/`
- **model.py**: SQLAlchemy数据库模式定义 (unchanged)

### Authentication & Web Interface
- **app.py**: Flask web应用，Yahoo OAuth认证和令牌管理
- **fetch_sample_data.py**: 样本数据获取器，用于测试和调试

## Database Schema

PostgreSQL数据库包含18个核心表：
- **基础实体**: `games`, `leagues`, `teams`, `players`, `managers`
- **球员统计**: `player_daily_stats`, `player_season_stats` (11个标准化篮球统计项)
- **团队数据**: `team_stats_weekly`, `team_matchups`, `league_standings`
- **名单管理**: `roster_daily`, `player_eligible_positions`
- **交易数据**: `transactions`, `transaction_players`
- **配置数据**: `league_settings`, `stat_categories`, `league_roster_positions`
- **维度表**: `date_dimension`

统计数据标准化为Yahoo的11个核心篮球类别：FGM/A, FG%, FTM/A, FT%, 3PTM, PTS, REB, AST, ST, BLK, TO。

详细数据库设计见 `doc/database.md`。

## Development Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables in .env file
YAHOO_CLIENT_ID="your_client_id"
YAHOO_CLIENT_SECRET="your_client_secret"
DB_USER="fantasy_user"
DB_PASSWORD="fantasyPassword"
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="fantasy_db"
```

### Complete ETL Workflow

#### 🎯 NEW: Using Modular System (Recommended)
```bash
# 1. Start OAuth authentication
python app.py
# Visit https://localhost:8000, complete Yahoo OAuth flow

# 2. Initialize database schema
python model.py

# 3. Run modular ETL system
python main_etl.py                           # Demo script with guided workflow

# OR use the CLI interface:
python -m fantasy_etl.cli list-leagues       # List all available leagues
python -m fantasy_etl.cli league <key>       # Process single league
python -m fantasy_etl.cli all                # Process all leagues
python -m fantasy_etl.cli user-info          # Extract user info only

# 4. Migration from legacy system:
python migrate_to_modular.py                 # Guided migration wizard
```

#### Legacy System (Still Available)
```bash
# 3. Run legacy ETL pipeline
python yahoo_api_data.py
# Interactive menu will guide through:
# - League selection
# - Complete league data extraction
# - Player statistics (season & daily)
# - Roster history
# - Team statistics and matchups
```

### Individual Operations

#### 🎯 NEW: Modular Components (Recommended)
```bash
# Using the new modular system programmatically:
python -c "
from fantasy_etl import ETLCoordinator, LeagueExtractor
coordinator = ETLCoordinator()
results = coordinator.process_league_data('nba.l.123456')
print(coordinator.get_processing_summary())
"

# Component testing:
python -c "
from fantasy_etl.extractors.leagues import LeagueExtractor
extractor = LeagueExtractor()
leagues = extractor.extract_user_leagues()
print(leagues)
"
```

#### Legacy Operations
```bash
# Sample data for testing
python fetch_sample_data.py

# Database operations
python database_writer.py  # Direct database interface
```

## Key Technical Details

### OAuth Token Management
- Tokens stored in `tokens/yahoo_token.token` (pickle format)
- Automatic refresh in `yahoo_api_utils.py`
- Hardcoded credentials in `yahoo_api_utils.py` (移到环境变量)

### API Rate Limiting
- Exponential backoff retry mechanism
- Configurable delays between requests
- Comprehensive 401/403 error handling

### Data Storage Strategy
- Direct database writes with batch processing
- Automatic duplicate handling via unique constraints
- Mixed storage: structured columns for core stats + JSON for complex data
- Incremental updates based on timestamps

### ✅ Refactoring Completed

The large monolithic files have been successfully refactored into a modular system:

1. **yahoo_api_data.py** (2797 lines) → **Modularized**:
   - ✅ `fantasy_etl/extractors/leagues.py` - 联盟数据提取
   - ✅ `fantasy_etl/extractors/teams.py` - 团队数据提取
   - ✅ `fantasy_etl/extractors/players.py` - 球员数据提取
   - ✅ `fantasy_etl/extractors/transactions.py` - 交易数据提取
   - ✅ `fantasy_etl/etl_coordinator.py` - 统一协调器

2. **database_writer.py** (2135 lines) → **Modularized**:
   - ✅ `fantasy_etl/loaders/league_loader.py` - 联盟数据加载
   - ✅ `fantasy_etl/loaders/team_loader.py` - 团队数据加载
   - ✅ `fantasy_etl/loaders/player_loader.py` - 球员数据加载
   - ✅ `fantasy_etl/loaders/transaction_loader.py` - 交易数据加载

3. **yahoo_api_utils.py** → **Modularized**:
   - ✅ `fantasy_etl/core/auth.py` - OAuth认证管理
   - ✅ `fantasy_etl/core/config.py` - 配置管理

### Migration Support
- ✅ `fantasy_etl/compat.py` - 兼容性层，支持渐进式迁移
- ✅ `migrate_to_modular.py` - 迁移向导和验证工具

## Code Conventions

- 项目广泛使用中文注释和输出信息
- 全面的错误处理和日志记录
- 类型提示和详细文档字符串
- 模块化设计，但需要进一步refactor大文件

## Common Development Workflows

### 🎯 NEW: Using Modular System

#### Adding New Data Types
1. 在 `model.py` 中定义新表
2. 在相应的 `fantasy_etl/extractors/` 模块中添加提取逻辑
3. 在相应的 `fantasy_etl/loaders/` 模块中实现写入器
4. 在 `fantasy_etl/etl_coordinator.py` 中集成新功能
5. 使用 `main_etl.py` 或 CLI 测试

#### Adding New Extractors
1. 继承 `fantasy_etl/extractors/base.py::BaseExtractor`
2. 实现 `extract()` 方法
3. 在 `fantasy_etl/etl_coordinator.py` 中集成

#### Adding New Loaders  
1. 继承 `fantasy_etl/loaders/base.py::BaseLoader`
2. 实现 `load()` 方法
3. 在 `fantasy_etl/etl_coordinator.py` 中集成

### ✅ Completed: Large Files Refactoring
The monolithic files have been successfully modularized:
- ✅ `yahoo_api_data.py` → `fantasy_etl/extractors/` + `etl_coordinator.py`
- ✅ `database_writer.py` → `fantasy_etl/loaders/`
- ✅ API接口兼容性通过 `fantasy_etl/compat.py` 保持

### Database Schema Changes
1. 修改 `model.py` 中的模型
2. 模块化loaders自动检测结构问题并重建表
3. 使用 `main_etl.py` 或 CLI 测试

## Important Notes

### ✅ Migration Status
- **Refactoring Completed**: Monolithic files successfully modularized into `fantasy_etl/` package
- **Backward Compatibility**: Legacy files still available, compatibility layer provides smooth transition
- **Database Schema**: Unchanged, all existing data remains accessible
- **New Features**: CLI interface, ETL coordinator, modular extractors/loaders

### System Notes
- OAuth凭据通过 `fantasy_etl/core/auth.py` 从环境变量读取
- 模块化loaders包含自动表结构验证和重建功能
- 样本数据存储在 `sample_data/` 目录
- API文档位于 `doc/` 目录
- **✅ Architecture Optimized**: 从adhoc解决方案升级到模块化、可维护的系统

### Migration Support
- Use `migrate_to_modular.py` for guided migration
- Use `main_etl.py` to test the new modular system
- Use `python -m fantasy_etl.cli --help` for CLI options
- Legacy code compatibility maintained via `fantasy_etl/compat.py`