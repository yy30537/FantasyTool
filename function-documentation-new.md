# Fantasy ETL Function Documentation (New Modular Structure)

## Overview

This document provides comprehensive documentation for all functions in the modularized Fantasy ETL system. The system has been reorganized into distinct modules for better maintainability and separation of concerns.

## Module Structure

```
fantasy_etl/
├── api/              # API data fetching (fetch_* functions)
├── database/         # Database operations (get_* functions)
├── transformers/     # Data transformation (transform_* functions)
├── loaders/         # Data loading (load_* functions)
├── validators/      # Data validation (verify_* functions)
└── utils/           # Utility functions
```

---

## 1. API Module (`fantasy_etl/api/`)

### 1.1 Client Module (`client.py`)

#### Class: `YahooFantasyClient`
**Purpose**: Main client for interacting with Yahoo Fantasy API
**Location**: `fantasy_etl/api/client.py`

**Methods**:
- `__init__(self, consumer_key: str, consumer_secret: str, token_file: str = "oauth2.json")`
  - Initializes the Yahoo Fantasy client with OAuth credentials
  - Sets up authentication and base URL

- `make_request(self, url: str, params: Optional[Dict] = None) -> Dict`
  - Makes authenticated HTTP requests to Yahoo API
  - Handles rate limiting and retries
  - Returns parsed JSON response

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

#### League Data Fetchers

1. **`fetch_leagues(client: YahooFantasyClient, game_key: str) -> List[Dict]`**
   - Fetches all leagues for a given game
   - Returns list of league metadata

2. **`fetch_league_settings(client: YahooFantasyClient, league_key: str) -> Dict`**
   - Fetches detailed league settings
   - Includes scoring settings, roster positions, etc.

3. **`fetch_league_standings(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
   - Fetches current league standings
   - Returns team rankings and records

4. **`fetch_league_transactions(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
   - Fetches all league transactions
   - Includes trades, adds, drops

5. **`fetch_league_scoreboard(client: YahooFantasyClient, league_key: str, week: Optional[int] = None) -> Dict`**
   - Fetches matchup scores for a given week
   - Returns all matchup results

#### Team Data Fetchers

6. **`fetch_teams(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
   - Fetches all teams in a league
   - Returns team metadata

7. **`fetch_team_info(client: YahooFantasyClient, team_key: str) -> Dict`**
   - Fetches detailed team information
   - Includes manager info, team settings

8. **`fetch_team_roster(client: YahooFantasyClient, team_key: str, week: Optional[int] = None) -> List[Dict]`**
   - Fetches team roster for a specific week
   - Returns all players on roster

9. **`fetch_team_stats(client: YahooFantasyClient, team_key: str, week: Optional[int] = None) -> Dict`**
   - Fetches team statistics
   - Returns aggregated team stats

10. **`fetch_team_matchups(client: YahooFantasyClient, team_key: str) -> List[Dict]`**
    - Fetches all matchups for a team
    - Returns season-long matchup data

#### Player Data Fetchers

11. **`fetch_players(client: YahooFantasyClient, league_key: str, start: int = 0, count: int = 25) -> List[Dict]`**
    - Fetches players with pagination
    - Returns player metadata

12. **`fetch_player_info(client: YahooFantasyClient, player_key: str) -> Dict`**
    - Fetches detailed player information
    - Includes bio, team, position

13. **`fetch_player_stats(client: YahooFantasyClient, player_key: str, week: Optional[int] = None) -> Dict`**
    - Fetches player statistics
    - Returns stats for specified period

14. **`fetch_player_ownership(client: YahooFantasyClient, league_key: str, player_keys: List[str]) -> Dict`**
    - Fetches ownership data for players
    - Returns ownership percentages

15. **`fetch_player_draft_analysis(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches draft analysis data
    - Returns draft picks and values

#### Matchup Data Fetchers

16. **`fetch_matchups(client: YahooFantasyClient, league_key: str, week: int) -> List[Dict]`**
    - Fetches all matchups for a week
    - Returns matchup pairings and scores

17. **`fetch_matchup_grades(client: YahooFantasyClient, team_key: str, week: int) -> Dict`**
    - Fetches matchup grades
    - Returns performance grades

#### Transaction Data Fetchers

18. **`fetch_transactions(client: YahooFantasyClient, league_key: str, types: Optional[List[str]] = None) -> List[Dict]`**
    - Fetches filtered transactions
    - Returns transaction history

19. **`fetch_waiver_claims(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches pending waiver claims
    - Returns claim details

20. **`fetch_trades(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches trade transactions
    - Returns trade history

#### Draft Data Fetchers

21. **`fetch_draft_results(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches complete draft results
    - Returns all draft picks

22. **`fetch_predraft_rankings(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches pre-draft player rankings
    - Returns ranked player list

#### Metadata Fetchers

23. **`fetch_game_weeks(client: YahooFantasyClient, game_key: str) -> List[Dict]`**
    - Fetches game week information
    - Returns week schedule data

24. **`fetch_stat_categories(client: YahooFantasyClient, game_key: str) -> List[Dict]`**
    - Fetches available stat categories
    - Returns stat definitions

25. **`fetch_roster_positions(client: YahooFantasyClient, league_key: str) -> List[Dict]`**
    - Fetches roster position settings
    - Returns position requirements

26. **`fetch_transaction_types(client: YahooFantasyClient, league_key: str) -> List[str]`**
    - Fetches available transaction types
    - Returns transaction type list

#### User Data Fetchers

27. **`fetch_user_games(client: YahooFantasyClient) -> List[Dict]`**
    - Fetches all games for authenticated user
    - Returns user's game history

28. **`fetch_user_leagues(client: YahooFantasyClient, game_key: str) -> List[Dict]`**
    - Fetches user's leagues for a game
    - Returns league participation

29. **`fetch_user_teams(client: YahooFantasyClient) -> List[Dict]`**
    - Fetches all user's teams
    - Returns team ownership data

---

## 2. Database Module (`fantasy_etl/database/`)

### 2.1 Connection Module (`connection.py`)

#### Class: `DatabaseConnection`
**Purpose**: Manages database connections and sessions
**Location**: `fantasy_etl/database/connection.py`

**Methods**:
- `__init__(self, connection_string: str)`
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
- `TeamRoster`: Team-player relationships with dates
- `PlayerStats`: Player statistics by period
- `TeamStats`: Team statistics by period
- `Matchup`: Weekly matchup information
- `MatchupRoster`: Roster for each matchup

#### Transaction Models
- `Transaction`: All transaction records
- `TransactionPlayer`: Players involved in transactions
- `WaiverClaim`: Waiver claim records
- `Trade`: Trade transaction details

#### Draft Models
- `DraftPick`: Draft pick records
- `PreDraftRanking`: Pre-draft player rankings

#### Settings Models
- `ScoringSettings`: League scoring configuration
- `RosterPosition`: Roster position requirements
- `StatCategory`: Statistical categories

### 2.3 Queries Module (`queries.py`)

1. **`get_league_by_key(session: Session, league_key: str) -> Optional[League]`**
   - Retrieves league by Yahoo key
   - Returns League object or None

2. **`get_team_by_key(session: Session, team_key: str) -> Optional[Team]`**
   - Retrieves team by Yahoo key
   - Returns Team object or None

3. **`get_player_by_key(session: Session, player_key: str) -> Optional[Player]`**
   - Retrieves player by Yahoo key
   - Returns Player object or None

4. **`get_team_roster(session: Session, team_id: int, week: Optional[int] = None) -> List[Player]`**
   - Retrieves team roster for a week
   - Returns list of Player objects

5. **`get_player_stats(session: Session, player_id: int, week: Optional[int] = None) -> Optional[PlayerStats]`**
   - Retrieves player statistics
   - Returns PlayerStats object or None

6. **`get_league_standings(session: Session, league_id: int) -> List[Team]`**
   - Retrieves league standings
   - Returns ordered list of teams

7. **`get_matchups_by_week(session: Session, league_id: int, week: int) -> List[Matchup]`**
   - Retrieves matchups for a week
   - Returns list of Matchup objects

---

## 3. Transformers Module (`fantasy_etl/transformers/`)

### 3.1 Core Transformers (`core.py`)

1. **`transform_league_data(raw_data: Dict) -> Dict`**
   - Transforms raw league data from API
   - Standardizes league information

2. **`transform_game_data(raw_data: Dict) -> Dict`**
   - Transforms game metadata
   - Extracts game configuration

3. **`transform_settings_data(raw_data: Dict) -> Dict`**
   - Transforms league settings
   - Parses scoring and roster settings

4. **`transform_standings_data(raw_data: List[Dict]) -> List[Dict]`**
   - Transforms standings data
   - Calculates rankings and records

5. **`transform_draft_data(raw_data: List[Dict]) -> List[Dict]`**
   - Transforms draft results
   - Standardizes pick information

6. **`transform_transaction_data(raw_data: Dict) -> Dict`**
   - Transforms transaction data
   - Parses transaction details

7. **`transform_waiver_data(raw_data: Dict) -> Dict`**
   - Transforms waiver claim data
   - Extracts claim information

### 3.2 Team Transformers (`team.py`)

8. **`transform_team_data(raw_data: Dict) -> Dict`**
   - Transforms team information
   - Standardizes team metadata

9. **`transform_team_stats(raw_data: Dict) -> Dict`**
   - Transforms team statistics
   - Aggregates team performance

10. **`transform_matchup_data(raw_data: Dict) -> Dict`**
    - Transforms matchup information
    - Calculates matchup results

11. **`transform_manager_data(raw_data: Dict) -> Dict`**
    - Transforms manager information
    - Extracts manager details

### 3.3 Player Transformers (`player.py`)

12. **`transform_player_data(raw_data: Dict) -> Dict`**
    - Transforms player information
    - Standardizes player attributes

13. **`transform_player_stats(raw_data: Dict) -> Dict`**
    - Transforms player statistics
    - Calculates statistical values

14. **`transform_player_ownership(raw_data: Dict) -> Dict`**
    - Transforms ownership data
    - Calculates ownership metrics

15. **`transform_draft_analysis(raw_data: Dict) -> Dict`**
    - Transforms draft analysis
    - Calculates draft values

### 3.4 Roster Transformers (`roster.py`)

16. **`transform_roster_data(raw_data: List[Dict]) -> List[Dict]`**
    - Transforms roster information
    - Standardizes roster entries

17. **`transform_roster_positions(raw_data: List[Dict]) -> List[Dict]`**
    - Transforms position data
    - Maps position requirements

18. **`transform_lineup_data(raw_data: Dict) -> Dict`**
    - Transforms lineup settings
    - Extracts active lineup

### 3.5 Stats Transformers (`stats.py`)

19. **`transform_stat_categories(raw_data: List[Dict]) -> List[Dict]`**
    - Transforms stat category definitions
    - Standardizes stat metadata

20. **`transform_scoring_settings(raw_data: Dict) -> Dict`**
    - Transforms scoring configuration
    - Parses point values

21. **`transform_weekly_stats(raw_data: Dict) -> Dict`**
    - Transforms weekly statistics
    - Aggregates weekly performance

22. **`transform_season_stats(raw_data: Dict) -> Dict`**
    - Transforms season statistics
    - Calculates season totals

23. **`transform_projected_stats(raw_data: Dict) -> Dict`**
    - Transforms projected statistics
    - Standardizes projections

24. **`transform_matchup_grades(raw_data: Dict) -> Dict`**
    - Transforms matchup grades
    - Calculates performance grades

### 3.6 Additional Transformers

25. **`transform_trade_data(raw_data: Dict) -> Dict`**
    - Transforms trade information
    - Parses trade details

26. **`transform_schedule_data(raw_data: List[Dict]) -> List[Dict]`**
    - Transforms schedule information
    - Standardizes game schedule

27. **`transform_game_weeks(raw_data: List[Dict]) -> List[Dict]`**
    - Transforms week information
    - Maps week boundaries

28. **`transform_metadata(raw_data: Dict) -> Dict`**
    - Transforms API metadata
    - Extracts meta information

---

## 4. Loaders Module (`fantasy_etl/loaders/`)

### 4.1 Core Loaders (`core.py`)

1. **`load_league(session: Session, league_data: Dict) -> League`**
   - Loads league data to database
   - Creates or updates League record

2. **`load_team(session: Session, team_data: Dict) -> Team`**
   - Loads team data to database
   - Creates or updates Team record

3. **`load_player(session: Session, player_data: Dict) -> Player`**
   - Loads player data to database
   - Creates or updates Player record

4. **`load_manager(session: Session, manager_data: Dict) -> Manager`**
   - Loads manager data to database
   - Creates or updates Manager record

5. **`load_settings(session: Session, settings_data: Dict) -> None`**
   - Loads league settings
   - Updates configuration tables

6. **`load_roster_positions(session: Session, positions_data: List[Dict]) -> None`**
   - Loads roster position settings
   - Updates position requirements

7. **`load_stat_categories(session: Session, categories_data: List[Dict]) -> None`**
   - Loads stat category definitions
   - Updates category metadata

8. **`load_scoring_settings(session: Session, scoring_data: Dict) -> None`**
   - Loads scoring configuration
   - Updates point values

### 4.2 Batch Loaders (`batch.py`)

9. **`load_teams_batch(session: Session, teams_data: List[Dict]) -> List[Team]`**
   - Batch loads multiple teams
   - Optimized for bulk operations

10. **`load_players_batch(session: Session, players_data: List[Dict]) -> List[Player]`**
    - Batch loads multiple players
    - Handles large player sets

11. **`load_roster_batch(session: Session, roster_data: List[Dict]) -> None`**
    - Batch loads roster entries
    - Updates team rosters

12. **`load_transactions_batch(session: Session, transactions_data: List[Dict]) -> None`**
    - Batch loads transactions
    - Processes transaction history

13. **`load_draft_picks_batch(session: Session, picks_data: List[Dict]) -> None`**
    - Batch loads draft picks
    - Updates draft results

14. **`load_matchups_batch(session: Session, matchups_data: List[Dict]) -> None`**
    - Batch loads matchups
    - Updates matchup records

### 4.3 Stats Loaders (`stats.py`)

15. **`load_player_stats(session: Session, stats_data: Dict) -> PlayerStats`**
    - Loads player statistics
    - Creates stats records

16. **`load_team_stats(session: Session, stats_data: Dict) -> TeamStats`**
    - Loads team statistics
    - Creates team stats records

17. **`load_weekly_stats_batch(session: Session, stats_data: List[Dict]) -> None`**
    - Batch loads weekly stats
    - Processes weekly performance

18. **`load_season_stats_batch(session: Session, stats_data: List[Dict]) -> None`**
    - Batch loads season stats
    - Updates season totals

19. **`load_projected_stats(session: Session, projections_data: Dict) -> None`**
    - Loads projected statistics
    - Updates projection data

20. **`load_matchup_results(session: Session, results_data: Dict) -> None`**
    - Loads matchup results
    - Updates scores and outcomes

### 4.4 Additional Loaders

21. **`load_transaction(session: Session, transaction_data: Dict) -> Transaction`**
    - Loads single transaction
    - Creates transaction record

22. **`load_waiver_claim(session: Session, claim_data: Dict) -> WaiverClaim`**
    - Loads waiver claim
    - Creates claim record

23. **`load_trade(session: Session, trade_data: Dict) -> Trade`**
    - Loads trade transaction
    - Creates trade record

24. **`load_standings(session: Session, standings_data: List[Dict]) -> None`**
    - Loads league standings
    - Updates team rankings

25. **`load_schedule(session: Session, schedule_data: List[Dict]) -> None`**
    - Loads game schedule
    - Updates week information

26. **`load_draft_rankings(session: Session, rankings_data: List[Dict]) -> None`**
    - Loads pre-draft rankings
    - Updates ranking data

27. **`load_ownership_data(session: Session, ownership_data: Dict) -> None`**
    - Loads ownership percentages
    - Updates ownership metrics

28. **`load_matchup_grades(session: Session, grades_data: Dict) -> None`**
    - Loads matchup grades
    - Updates performance grades

---

## 5. Validators Module (`fantasy_etl/validators/`)

### 5.1 Core Validators (`core.py`)

1. **`verify_league_data(league_data: Dict) -> bool`**
   - Validates league data structure
   - Checks required fields and formats
   - Returns True if valid, raises ValidationError if not

2. **`verify_team_data(team_data: Dict) -> bool`**
   - Validates team data structure
   - Ensures team integrity
   - Returns validation status

3. **`verify_player_data(player_data: Dict) -> bool`**
   - Validates player data structure
   - Checks player attributes
   - Returns validation status

4. **`verify_transaction_data(transaction_data: Dict) -> bool`**
   - Validates transaction data
   - Ensures transaction consistency
   - Returns validation status

5. **`verify_stats_data(stats_data: Dict) -> bool`**
   - Validates statistics data
   - Checks stat values and types
   - Returns validation status

---

## 6. Utils Module (`fantasy_etl/utils/`)

### 6.1 Date Utilities (`date_utils.py`)

1. **`get_current_week(league_start_date: datetime) -> int`**
   - Calculates current fantasy week
   - Based on league start date

2. **`get_week_dates(week: int, league_start_date: datetime) -> Tuple[datetime, datetime]`**
   - Returns start and end dates for a week
   - Calculates week boundaries

3. **`parse_yahoo_date(date_string: str) -> datetime`**
   - Parses Yahoo date format
   - Returns datetime object

4. **`format_date_for_db(date: datetime) -> str`**
   - Formats date for database storage
   - Returns ISO format string

### 6.2 Helper Functions (`helpers.py`)

5. **`clean_player_name(name: str) -> str`**
   - Cleans player name formatting
   - Removes special characters

6. **`calculate_win_percentage(wins: int, losses: int, ties: int = 0) -> float`**
   - Calculates win percentage
   - Handles ties appropriately

7. **`parse_position_string(position: str) -> List[str]`**
   - Parses position eligibility
   - Returns list of positions

8. **`generate_unique_key(prefix: str, *args) -> str`**
   - Generates unique identifier
   - Combines prefix with arguments

9. **`batch_list(items: List, batch_size: int) -> List[List]`**
   - Splits list into batches
   - For batch processing

---

## Function Call Flow

### Typical ETL Pipeline Flow:

1. **Fetch Data** (API Module)
   ```
   client = YahooFantasyClient(key, secret)
   raw_data = fetch_leagues(client, game_key)
   ```

2. **Transform Data** (Transformers Module)
   ```
   clean_data = transform_league_data(raw_data)
   ```

3. **Validate Data** (Validators Module)
   ```
   if verify_league_data(clean_data):
       proceed_to_load()
   ```

4. **Load Data** (Loaders Module)
   ```
   session = DatabaseConnection(conn_str).get_session()
   load_league(session, clean_data)
   ```

5. **Query Data** (Database Module)
   ```
   league = get_league_by_key(session, league_key)
   ```

---

## Error Handling

All modules implement consistent error handling:

- **APIError**: For API-related errors
- **ValidationError**: For data validation failures
- **DatabaseError**: For database operation failures
- **TransformationError**: For data transformation issues

Each function includes appropriate error handling and logging for debugging and monitoring.

---

## Module Dependencies

```
api -> None (standalone)
transformers -> None (standalone)
validators -> None (standalone)
database -> SQLAlchemy models
loaders -> database, validators
utils -> None (standalone)
```

This modular structure ensures clean separation of concerns and makes the codebase more maintainable and testable.