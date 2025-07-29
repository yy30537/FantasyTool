# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Yahoo Fantasy Sports ETL (Extract, Transform, Load) pipeline that extracts data from Yahoo Fantasy API and stores it in a PostgreSQL database. The project consists of several large modules that need refactoring for better maintainability.

## Current ETL Architecture

### 🎯 NEW: Modular ETL System
The project has been refactored into a modular ETL system:

```
fantasy_etl/
├── config/
│   ├── settings.py       # Configuration management
│   └── constants.py      # API URLs and constants
├── auth/
│   └── oauth_manager.py  # OAuth authentication (extracted from yahoo_api_utils.py)
├── api/
│   ├── base_client.py    # Base API client
│   └── yahoo_client.py   # Yahoo API client
├── models/
│   ├── base.py           # Base database models
│   ├── league.py         # League models
│   ├── team.py           # Team models
│   ├── player.py         # Player models
│   ├── stats.py          # Statistics models
│   ├── transaction.py    # Transaction models
│   └── dimension.py      # Dimension tables
├── extractors/
│   ├── base.py           # Base extractor with common API logic
│   ├── league.py         # League data extraction
│   ├── team.py           # Team data extraction  
│   ├── player.py         # Player data extraction
│   ├── stats.py          # Statistics data extraction
│   └── transaction.py    # Transaction data extraction
├── transformers/
│   ├── base.py           # Base transformer
│   ├── league.py         # League data transformation
│   ├── team.py           # Team data transformation
│   ├── player.py         # Player data transformation
│   ├── stats.py          # Statistics data transformation
│   ├── transaction.py    # Transaction data transformation
│   └── dimension.py      # Dimension data transformation
├── loaders/
│   ├── base.py           # Base loader with common DB logic
│   ├── postgres_loader.py # PostgreSQL data loading
│   ├── batch_loader.py   # Batch loading utilities
│   └── utility_loader.py # Utility loading functions
├── orchestrator/
│   ├── etl_orchestrator.py    # Main ETL orchestrator
│   ├── workflow_manager.py    # Workflow management
│   └── league_orchestrator.py # League-specific orchestration
├── cli/
│   ├── commands.py       # Command-line interface
│   └── __main__.py       # CLI entry point
├── utils/
│   ├── logger.py         # Logging and progress tracking
│   ├── error_handler.py  # Error handling and rollback
│   ├── date_utils.py     # Date processing utilities
│   └── retry.py          # Retry mechanisms
└── dto/                  # Data transfer objects
```

### Legacy Files (Still Available)
- **yahoo_api_utils.py**: OAuth认证、API请求管理 → Replaced by `fantasy_etl/auth/oauth_manager.py`
- **yahoo_api_data.py**: 数据提取逻辑 (2797 lines) → Replaced by `fantasy_etl/extractors/` + `fantasy_etl/orchestrator/`
- **database_writer.py**: 批量数据写入 (2135 lines) → Replaced by `fantasy_etl/loaders/`
- **model.py**: SQLAlchemy数据库模式定义 → Enhanced with `fantasy_etl/models/`

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

# 4. Additional CLI commands:
python -m fantasy_etl.cli pipeline nba.l.123456 nba.l.789012  # Pipeline processing
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
from fantasy_etl.orchestrator.etl_orchestrator import ETLOrchestrator
orchestrator = ETLOrchestrator()
result = orchestrator.process_complete_league_data('nba.l.123456')
print(orchestrator.get_execution_summary())
"

# Component testing:
python -c "
from fantasy_etl.extractors.league import LeagueExtractor
from fantasy_etl.api.yahoo_client import YahooFantasyAPIClient
client = YahooFantasyAPIClient()
extractor = LeagueExtractor('', client)
leagues = extractor.extract()
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
- Automatic refresh in `fantasy_etl/auth/oauth_manager.py`
- Credentials loaded from environment variables via `fantasy_etl/config/settings.py`

### API Rate Limiting
- Exponential backoff retry mechanism in `fantasy_etl/utils/retry.py`
- Configurable delays between requests
- Comprehensive 401/403 error handling in `fantasy_etl/api/yahoo_client.py`

### Data Storage Strategy
- Direct database writes with batch processing
- Automatic duplicate handling via unique constraints
- Mixed storage: structured columns for core stats + JSON for complex data
- Incremental updates based on timestamps

### ✅ Refactoring Completed

The large monolithic files have been successfully refactored into a modular system:

1. **yahoo_api_data.py** (2797 lines) → **Modularized**:
   - ✅ `fantasy_etl/extractors/league.py` - 联盟数据提取
   - ✅ `fantasy_etl/extractors/team.py` - 团队数据提取
   - ✅ `fantasy_etl/extractors/player.py` - 球员数据提取
   - ✅ `fantasy_etl/extractors/stats.py` - 统计数据提取
   - ✅ `fantasy_etl/extractors/transaction.py` - 交易数据提取
   - ✅ `fantasy_etl/orchestrator/etl_orchestrator.py` - 统一协调器

2. **database_writer.py** (2135 lines) → **Modularized**:
   - ✅ `fantasy_etl/loaders/postgres_loader.py` - PostgreSQL数据加载
   - ✅ `fantasy_etl/loaders/batch_loader.py` - 批量加载器
   - ✅ `fantasy_etl/loaders/utility_loader.py` - 工具加载器
   - ✅ `fantasy_etl/transformers/` - 数据转换层

3. **yahoo_api_utils.py** → **Modularized**:
   - ✅ `fantasy_etl/auth/oauth_manager.py` - OAuth认证管理
   - ✅ `fantasy_etl/config/settings.py` - 配置管理
   - ✅ `fantasy_etl/api/yahoo_client.py` - API客户端

### New Features Added
- ✅ `fantasy_etl/utils/logger.py` - 结构化日志和进度跟踪
- ✅ `fantasy_etl/utils/error_handler.py` - 错误处理和事务回滚
- ✅ `fantasy_etl/orchestrator/workflow_manager.py` - 工作流依赖管理
- ✅ `fantasy_etl/cli/` - 专业命令行界面

## Code Conventions

- 项目广泛使用中文注释和输出信息
- 全面的错误处理和日志记录
- 类型提示和详细文档字符串
- 模块化设计，但需要进一步refactor大文件

## Common Development Workflows

### 🎯 NEW: Using Modular System

#### Adding New Data Types
1. 在 `model.py` 或 `fantasy_etl/models/` 中定义新表
2. 在相应的 `fantasy_etl/extractors/` 模块中添加提取逻辑
3. 在相应的 `fantasy_etl/transformers/` 模块中添加转换逻辑
4. 在相应的 `fantasy_etl/loaders/` 模块中实现加载器
5. 在 `fantasy_etl/orchestrator/etl_orchestrator.py` 中集成新功能
6. 使用 `main_etl.py` 或 CLI 测试

#### Adding New Extractors
1. 继承 `fantasy_etl/extractors/base.py::BaseExtractor`
2. 实现 `extract()` 方法
3. 在 `fantasy_etl/orchestrator/etl_orchestrator.py` 中注册

#### Adding New Transformers
1. 继承 `fantasy_etl/transformers/base.py::BaseTransformer`
2. 实现 `transform()` 方法
3. 在 `fantasy_etl/orchestrator/etl_orchestrator.py` 中注册

#### Adding New Loaders  
1. 继承 `fantasy_etl/loaders/base.py::BaseLoader`
2. 实现 `load()` 方法
3. 在 `fantasy_etl/orchestrator/etl_orchestrator.py` 中注册

### ✅ Completed: Large Files Refactoring
The monolithic files have been successfully modularized:
- ✅ `yahoo_api_data.py` → `fantasy_etl/extractors/` + `fantasy_etl/orchestrator/`
- ✅ `database_writer.py` → `fantasy_etl/loaders/` + `fantasy_etl/transformers/`
- ✅ `yahoo_api_utils.py` → `fantasy_etl/auth/` + `fantasy_etl/api/`

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
- OAuth凭据通过 `fantasy_etl/auth/oauth_manager.py` 从环境变量读取
- 模块化系统包含完整的错误处理和事务支持
- 结构化日志记录和进度跟踪功能
- 样本数据存储在 `sample_data/` 目录
- API文档位于 `doc/` 目录
- **✅ Architecture Optimized**: 从monolithic解决方案升级到模块化、企业级系统

### Usage Instructions
- Use `main_etl.py` for interactive demo and testing
- Use `python -m fantasy_etl.cli --help` for CLI options
- Use `python -m fantasy_etl.cli list-leagues` to start
- All legacy files remain available for backward compatibility

### Enterprise Features
- **Error Handling**: Comprehensive error classification and recovery
- **Transaction Support**: Automatic rollback on failures  
- **Progress Tracking**: Real-time progress with ETAs
- **Workflow Management**: Dependency-aware execution
- **CLI Tools**: Professional command-line interface
- **Logging**: Structured logging with context tracking