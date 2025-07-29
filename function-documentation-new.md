# Fantasy ETL Function Documentation (New Modular Structure)

## Overview

This document provides comprehensive documentation for all classes and methods in the modularized Fantasy ETL system. The system has been reorganized into distinct modules for better maintainability and separation of concerns.

## Module Structure

```
fantasy_etl/
├── api/              # API data fetching (YahooFantasyFetcher class)
├── database/         # Database operations (DatabaseQueries class)
├── transformers/     # Data transformation (various Transformer classes)
├── loaders/         # Data loading (various Loader classes)
├── validators/      # Data validation (CoreValidators class)
└── utils/           # Utility functions
```

---

## 1. API Module (`fantasy_etl/api/`)

### 1.1 Client Module (`client.py`)

#### Class: `YahooFantasyAPIClient`
**Purpose**: Main client for interacting with Yahoo Fantasy API
**Location**: `fantasy_etl/api/client.py`

**Methods**:
- `__init__(self, delay: int = 2)`
  - Initializes the Yahoo Fantasy client with OAuth credentials
  - Sets up authentication and base URL

- `get_api_data(self, url: str, max_retries: int = 3) -> Optional[Dict]`
  - Makes authenticated HTTP requests to Yahoo API
  - Handles rate limiting and retries
  - Returns parsed JSON response

- `refresh_token_if_needed(self, token) -> Optional[Dict]`
  - Refreshes expired access tokens
  
- `save_token(self, token) -> bool`
  - Saves OAuth tokens to file
  
- `load_token(self) -> Optional[Dict]`
  - Loads OAuth tokens from file

### 1.2 OAuth Authenticator (`oauth_authenticator.py`)

#### Class: `YahooOAuthAuthenticator`
**Purpose**: Handles OAuth 2.0 authentication for Yahoo Fantasy API
**Location**: `fantasy_etl/api/oauth_authenticator.py`

**Methods**:
- `__init__(self, consumer_key: str, consumer_secret: str, token_file: str = "oauth2.json")`
  - Initializes OAuth authenticator with credentials
  
- `authenticate(self) -> bool`
  - Performs OAuth authentication flow
  - Stores tokens for future use
  
- `refresh_token(self) -> bool`
  - Refreshes expired access tokens
  
- `get_auth_header(self) -> Dict[str, str]`
  - Returns authorization headers for API requests

### 1.3 Fetchers Module (`fetchers.py`)

#### Class: `YahooFantasyFetcher`
**Purpose**: Contains all data fetching methods
**Location**: `fantasy_etl/api/fetchers.py`

**Key Methods**:

##### League Data Methods
- `fetch_leagues_data(self, game_key: str) -> Optional[Dict]`
- `fetch_league_settings(self, league_key: str) -> Optional[Dict]`
- `fetch_and_process_league_standings(self, league_key: str, season: str) -> Optional[Dict]`
- `fetch_all_league_transactions(self, league_key: str, max_count: Optional[int] = None) -> List[Dict]`

##### Team Data Methods
- `fetch_teams_data(self, league_key: str) -> Optional[Dict]`
- `fetch_team_roster(self, team_key: str, week: Optional[int] = None) -> Optional[Dict]`
- `fetch_team_matchups(self, team_key: str) -> Optional[Dict]`

##### Player Data Methods
- `fetch_all_league_players(self, league_key: str) -> List[Dict]`
- `fetch_player_season_stats(self, players: List, league_key: str, season: str) -> Optional[Dict]`
- `fetch_player_daily_stats_for_range(self, players: List, league_key: str, season: str, start_date: date, end_date: date) -> int`

---

## 2. Database Module (`fantasy_etl/database/`)

### 2.1 Connection Module (`connection.py`)

#### Class: `DatabaseConnection`
**Purpose**: Manages database connections and sessions
**Location**: `fantasy_etl/database/connection.py`

**Methods**:
- `__init__(self, connection_string: Optional[str] = None)`
  - Initializes database connection
  
- `get_session(self) -> Session`
  - Returns SQLAlchemy session
  
- `close(self)`
  - Closes database connection

### 2.2 Models Module (`model.py`)

Contains SQLAlchemy ORM models for all database tables:

#### Core Models
- `League`: League information and settings
- `Team`: Team information and metadata
- `Player`: Player information and attributes
- `Manager`: Team manager information

#### Relationship Models
- `RosterDaily`: Daily roster records
- `PlayerDailyStats`: Player statistics by date
- `PlayerSeasonStats`: Player season statistics
- `TeamStatsWeekly`: Team weekly statistics
- `LeagueStandings`: League standings
- `TeamMatchups`: Team matchup records

#### Transaction Models
- `Transaction`: All transaction records
- `TransactionPlayer`: Players involved in transactions

#### Settings Models
- `LeagueSettings`: League configuration
- `StatCategory`: Statistical categories
- `LeagueRosterPosition`: Roster position requirements

### 2.3 Queries Module (`queries.py`)

#### Class: `DatabaseQueries`
**Purpose**: Contains all database query methods
**Location**: `fantasy_etl/database/queries.py`

**Key Methods**:
- `get_leagues_from_database(self) -> Optional[Dict]`
- `get_season_date_info(self, league_key: str) -> Dict`
- `get_teams_data_from_db(self, league_key: str) -> Optional[Dict]`
- `get_stat_category_info(self, league_key: str, stat_id: int) -> Optional[Dict]`
- `get_database_summary(self) -> Dict[str, int]`

---

## 3. Transformers Module (`fantasy_etl/transformers/`)

### 3.1 Core Transformers (`core.py`)

#### Class: `CoreTransformers`
**Purpose**: Core data transformation methods
**Location**: `fantasy_etl/transformers/core.py`

**Key Methods**:
- `transform_position_string(self, position_data) -> Optional[str]`
- `transform_game_keys(self, games_data: Dict) -> List[str]`
- `transform_leagues_from_data(self, data: Dict, game_key: str) -> List[Dict]`
- `transform_league_data(self, league_data: Dict) -> Dict`
- `transform_league_settings(self, settings_data: Dict) -> Dict`
- `transform_team_standings_info(self, team_data) -> Dict`
- `transform_transaction_data(self, transaction_data: Dict) -> Dict`

### 3.2 Team Transformers (`team.py`)

#### Class: `TeamTransformers`
**Purpose**: Team-related data transformations
**Location**: `fantasy_etl/transformers/team.py`

**Key Methods**:
- `transform_team_data_from_api(self, team_data: List) -> Dict`
- `transform_team_keys_from_data(self, teams_data: Dict) -> List[str]`
- `transform_team_matchups(self, matchups_data: Dict, team_key: str) -> List[Dict]`
- `transform_matchup_info(self, matchup_info, team_key: str) -> Dict`
- `transform_team_matchup_details(self, teams_data, target_team_key: str) -> Dict`

### 3.3 Player Transformers (`player.py`)

#### Class: `PlayerTransformers`
**Purpose**: Player-related data transformations
**Location**: `fantasy_etl/transformers/player.py`

**Key Methods**:
- `transform_players_from_league_data(self, players_data: Dict) -> List[Dict]`
- `transform_player_info(self, player_info: Dict) -> Dict`

### 3.4 Roster Transformers (`roster.py`)

#### Class: `RosterTransformers`
**Purpose**: Roster-related data transformations
**Location**: `fantasy_etl/transformers/roster.py`

**Key Methods**:
- `transform_roster_data(self, roster_data: Dict, team_key: str) -> Optional[List[Dict]]`

### 3.5 Stats Transformers (`stats.py`)

#### Class: `StatsTransformers`
**Purpose**: Statistics-related data transformations
**Location**: `fantasy_etl/transformers/stats.py`

**Key Methods**:
- `transform_core_player_season_stats(self, stats_data: Dict) -> Dict`
- `transform_core_daily_stats(self, stats_data: Dict) -> Dict`
- `transform_core_team_weekly_stats(self, categories_won: int, win: Optional[bool] = None) -> Dict`
- `transform_team_season_stats(self, stats_data: Dict) -> Dict`
- `transform_team_weekly_stats_from_stats_data(self, stats_data: Dict) -> Dict`

---

## 4. Loaders Module (`fantasy_etl/loaders/`)

### 4.1 Core Loaders (`core.py`)

#### Class: `CoreLoaders`
**Purpose**: Core data loading methods
**Location**: `fantasy_etl/loaders/core.py`

**Key Methods**:
- `load_roster_data(self, roster_list: List[Dict], selected_league: Dict) -> bool`
- `load_teams_to_db(self, teams_data: Dict, league_key: str) -> bool`
- `load_transactions_to_db(self, transactions: List[Dict], league_key: str) -> bool`
- `load_league_standings_to_db(self, team_info: Dict, league_key: str, season: str) -> bool`
- `load_team_matchups(self, matchup_list: List[Dict], selected_league: Dict) -> bool`

### 4.2 Batch Loaders (`batch.py`)

#### Class: `BatchLoaders`
**Purpose**: Batch data loading operations
**Location**: `fantasy_etl/loaders/batch.py`

**Key Methods**:
- `write_teams_batch(self, teams_data: List[Dict], league_key: str) -> bool`
- `write_players_batch(self, players_data: List[Dict], league_key: str) -> bool`
- `write_transactions_batch(self, transactions_data: List[Dict], league_key: str) -> bool`

### 4.3 Stats Loaders (`stats.py`)

#### Class: `StatsLoaders`
**Purpose**: Statistics data loading
**Location**: `fantasy_etl/loaders/stats.py`

**Key Methods**:
- `write_player_season_stat_values(self, player_key: str, editorial_player_key: str, league_key: str, season: str, stats_data: Dict) -> bool`
- `write_player_daily_stat_values(self, player_key: str, editorial_player_key: str, league_key: str, season: str, date_obj: date, stats_data: Dict, week: Optional[int] = None) -> bool`
- `write_team_stat_values(self, team_key: str, league_key: str, season: str, coverage_type: str, stats_data: Dict, ...) -> bool`

---

## 5. Validators Module (`fantasy_etl/validators/`)

### 5.1 Core Validators (`core.py`)

#### Class: `CoreValidators`
**Purpose**: Data validation methods
**Location**: `fantasy_etl/validators/core.py`

**Key Methods**:
- `verify_league_exists_in_db(self) -> bool`
- `verify_league_selected(self) -> bool`
- `verify_league_data(self, league_data: Dict) -> bool`
- `verify_team_data(self, team_data: Dict) -> bool`
- `verify_player_data(self, player_data: Dict) -> bool`
- `verify_transaction_data(self, transaction_data: Dict) -> bool`
- `verify_stats_data(self, stats_data: Dict) -> bool`

---

## 6. Utils Module (`fantasy_etl/utils/`)

### 6.1 Date Utilities (`date_utils.py`)

Functions:
- `get_current_week(league_start_date: datetime) -> int`
- `get_week_dates(week: int, league_start_date: datetime) -> Tuple[datetime, datetime]`
- `parse_yahoo_date(date_string: str) -> datetime`
- `format_date_for_db(date: datetime) -> str`

### 6.2 Helper Functions (`helpers.py`)

Functions:
- `clean_player_name(name: str) -> str`
- `calculate_win_percentage(wins: int, losses: int, ties: int = 0) -> float`
- `parse_position_string(position: str) -> List[str]`
- `generate_unique_key(prefix: str, *args) -> str`
- `batch_list(items: List, batch_size: int) -> List[List]`

---

## Function Call Flow

### Typical ETL Pipeline Flow:

1. **Initialize Components**
   ```python
   from fantasy_etl.api import YahooFantasyAPIClient, YahooFantasyFetcher
   from fantasy_etl.database import DatabaseConnection, DatabaseQueries
   from fantasy_etl.transformers import CoreTransformers
   from fantasy_etl.validators import CoreValidators
   from fantasy_etl.loaders import CoreLoaders
   
   # Create instances
   api_client = YahooFantasyAPIClient()
   fetcher = YahooFantasyFetcher()
   fetcher.api_client = api_client
   
   db_conn = DatabaseConnection()
   session = db_conn.get_session()
   queries = DatabaseQueries()
   
   transformer = CoreTransformers()
   validator = CoreValidators(session=session)
   loader = CoreLoaders(db_writer)
   ```

2. **Fetch Data**
   ```python
   raw_data = fetcher.fetch_leagues_data(game_key)
   ```

3. **Transform Data**
   ```python
   leagues = transformer.transform_leagues_from_data(raw_data, game_key)
   clean_data = transformer.transform_league_data(leagues[0])
   ```

4. **Validate Data**
   ```python
   if validator.verify_league_data(clean_data):
       # Proceed to load
   ```

5. **Load Data**
   ```python
   success = loader.load_teams_to_db(teams_data, league_key)
   ```

6. **Query Data**
   ```python
   league_info = queries.get_season_date_info(league_key)
   ```

---

## Error Handling

All modules implement consistent error handling:

- **APIError**: For API-related errors
- **ValidationError**: For data validation failures
- **DatabaseError**: For database operation failures
- **TransformationError**: For data transformation issues

Each class includes appropriate error handling and logging for debugging and monitoring.

---

## Module Dependencies

```
api -> utils
transformers -> utils
validators -> database models
database -> SQLAlchemy models
loaders -> database, validators
utils -> None (standalone)
```

This modular structure ensures clean separation of concerns and makes the codebase more maintainable and testable.