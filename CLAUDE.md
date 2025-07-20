# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---
alwaysApply: true
---

## 🤖 交互规则

以下是与用户交互时必须遵守的核心规则：

- **文档同步要求**：当对项目做出任何改变时，必须更新 @README.md。在生成的回答结尾附加一个changelog。
- **语言使用规范**：Always respond in 中文与用户交互，但代码和文档里使用英文。
- **开发流程**：
  - 首先提供方案概述，说明你打算如何实现（简单问答可忽略，涉及代码执行或文档修改时适用）
  - 在用户确认前不要生成任何代码
  - 只编写项目实际需要的代码，不编写假设性代码。Be essential, not bulky
- **沟通原则**：
  - 当发现信息不足时，主动请求澄清
  - 提供多种实现方案时，基于项目文档给出分析和推荐
  - 代码生成时标明文件位置和上下文


## 📋 Project Overview

This is a Yahoo Fantasy Sports ETL (Extract, Transform, Load) tool that processes fantasy sports data from Yahoo's API into a PostgreSQL database. 

### 🔄 Migration Status
The project has been **完全重构** from the legacy `scripts/` directory to the new `fantasy_etl/` package architecture:

**Legacy Scripts (已完全重构):**
- `scripts/app.py` → `fantasy_etl/auth/web_auth_server.py`
- `scripts/model.py` → `fantasy_etl/core/database/models.py`
- `scripts/yahoo_api_data.py` → `fantasy_etl/api/fantasy_data_service.py`
- `scripts/yahoo_api_utils.py` → `fantasy_etl/data/extract/yahoo_api_client.py`
- `scripts/database_writer.py` → `fantasy_etl/data/load/database_loader.py`

**Current Architecture:**
- `fantasy_etl/auth/` → OAuth authentication management
- `fantasy_etl/data/extract/` → Yahoo API data extraction
- `fantasy_etl/data/transform/` → Data parsing and validation (planned)
- `fantasy_etl/data/load/` → Database loading and management
- `fantasy_etl/analytics/` → Analysis engines (NEW)
- `fantasy_etl/core/config/` → Configuration management (NEW)
- `fantasy_etl/api/` → Service layer and CLI interface

**启动方式**: 统一的包入口点，通过 `python -m fantasy_etl` 或 `python main.py` 启动。

## Key Development Commands

### Environment Setup
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment (required for all operations)
source venv/bin/activate

# Install dependencies
pip install -r requirements/development.txt
```

### Running the Application
```bash
# Main application with interactive menu (recommended)
python -m fantasy_etl
# or
python main.py

# OAuth authentication server only
python -m fantasy_etl auth-server
```

### Configuration Setup
```bash
# Copy configuration template
cp environments/development.env .env

# Edit configuration with your API keys
vim .env
```

## Architecture Overview

The project follows a strict modular architecture:

### Authentication Layer (`fantasy_etl/auth/`)
- **OAuthManager**: Handles Yahoo Fantasy Sports OAuth flow
- **WebAuthServer**: Flask-based authentication server
- Token management and automatic refresh

### Data Layer (`fantasy_etl/data/`)
- **Extract** (`fantasy_etl/data/extract/`): Yahoo API client with rate limiting
- **Transform** (`fantasy_etl/data/transform/`): Data parsing and validation (planned)
- **Load** (`fantasy_etl/data/load/`): Database loading with upsert capabilities

### Analytics Layer (`fantasy_etl/analytics/`) 🆕
- **Team Analysis** (`fantasy_etl/analytics/team/`): Team performance analysis
- **Trading Engine** (`fantasy_etl/analytics/trading/`): Player value assessment and trade recommendations
- **Stats Calculator** (`fantasy_etl/analytics/stats/`): Advanced statistical calculations

### Core Layer (`fantasy_etl/core/`)
- **Configuration** (`fantasy_etl/core/config/`): Unified settings management
- **Database** (`fantasy_etl/core/database/`): SQLAlchemy models and connection management
- **Utils** (`fantasy_etl/core/utils/`): Helper functions and decorators

### API Layer (`fantasy_etl/api/`)
- **FantasyDataService**: High-level data operations
- **CLIInterface**: Interactive command-line interface

## Configuration Management

### Environment Files
- **Template storage**: `environments/development.env`
- **Actual config**: `.env` (project root, not committed)
- **Management code**: `fantasy_etl/core/config/settings.py`

### Required Variables
```env
# Yahoo API (required)
YAHOO_CLIENT_ID=your_client_id
YAHOO_CLIENT_SECRET=your_client_secret
YAHOO_REDIRECT_URI=http://localhost:8000/auth/callback

# Database
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db
```

## Database Schema

The system manages 18 interconnected tables:
- **Core tables**: games, leagues, teams, players, managers
- **Settings tables**: league_settings, stat_categories, league_roster_positions
- **Operational tables**: transactions, roster_daily, player_stats (daily/season)
- **Analytics tables**: team_matchups, team_stats_weekly, league_standings

All tables use composite keys and maintain referential integrity.

## 🎯 Interactive Menu System

The main program provides a comprehensive CLI:

```
a. 登录认证          - OAuth authentication management
1. 选择联盟          - League selection from user's Yahoo account  
2. 获取联盟数据      - Complete league data extraction
3. 获取阵容历史数据  - Historical roster data with date ranges
4. 获取球员日统计数据 - Player daily statistics  
5. 获取球员赛季统计数据 - Player season statistics
6. 数据库摘要        - Database summary and statistics
7. 清空数据库        - Database cleanup operations
8. 获取团队每周数据  - Team weekly matchup data
9. 获取团队赛季数据  - Team season data
0. 退出             - Exit program
```

## Code Patterns and Conventions

### Error Handling
All operations return result objects with consistent patterns:
```python
@dataclass
class APIResponse:
    success: bool
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    status_code: Optional[int] = None

@dataclass
class LoadResult:
    success: bool
    inserted_count: int = 0
    updated_count: int = 0
    error_message: Optional[str] = None
```

### Configuration Access
```python
from fantasy_etl.core.config import settings

# Database URL
db_url = settings.database.url

# Yahoo API validation
if settings.yahoo_api.is_valid:
    # proceed with API calls
```

### Database Operations
```python
from fantasy_etl.core.database.connection_manager import db_manager

with db_manager.session_scope() as session:
    # database operations with automatic commit/rollback
```

## File Organization Rules

- **Authentication logic**: Only in `fantasy_etl/auth/`
- **Data extraction**: Only in `fantasy_etl/data/extract/`
- **Data loading**: Only in `fantasy_etl/data/load/`
- **Analysis logic**: Only in `fantasy_etl/analytics/`
- **Configuration**: Only in `fantasy_etl/core/config/`
- **Main entry point**: `fantasy_etl/__main__.py`

## Performance Considerations

- Rate limiting is enforced (Yahoo API limits)
- Batch processing with configurable batch sizes
- Database connection pooling
- Retry mechanisms with exponential backoff
- Configuration-driven delays and timeouts

## 📝 Development Notes

### 🔄 Project Maintenance
- **重要**: 当对项目做出任何改变时，必须同步更新 `project-structure.md` 文档
- Always work within the virtual environment (`source venv/bin/activate`)
- All new code should follow the modular architecture pattern

### 💻 Code Standards  
- Use English for code and comments in new development
- Database operations should go through the connection manager
- API calls should use the centralized YahooAPIClient
- Follow the established error handling patterns with result objects

### 🏗️ Architecture Rules
- **Extract logic**: Only in `fantasy_etl/data/extract/`
- **Transform logic**: Only in `fantasy_etl/data/transform/`  
- **Load logic**: Only in `fantasy_etl/data/load/`
- **Analysis logic**: Only in `fantasy_etl/analytics/`
- **Configuration**: Only in `fantasy_etl/core/config/`
- **Authentication**: Only in `fantasy_etl/auth/`

## Troubleshooting

Common issues:
- **Database connection errors**: Check PostgreSQL is running and `.env` configuration
- **API rate limits**: The system handles this automatically with exponential backoff
- **Token expiration**: Re-run authentication (option 'a' in main menu)
- **Import errors**: Ensure running from project root directory
- **Configuration errors**: Verify `.env` file exists and contains required variables

## 🚀 Future Development

The architecture supports easy extension:
- **Analytics**: Add new analyzers in `fantasy_etl/analytics/`
- **Data sources**: Add new extractors in `fantasy_etl/data/extract/`
- **Visualization**: Build on top of `fantasy_etl/api/` services
- **ML/AI**: Integrate with `fantasy_etl/analytics/` framework