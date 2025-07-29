"""
数据库模块 - 数据库操作和查询

包含所有 get_* 函数和数据库模型
"""

from .connection import DatabaseConnection
from .queries import (
    DatabaseQueries,
    # 独立函数接口
    get_league_by_key,
    get_team_by_key,
    get_player_by_key,
    get_team_roster,
    get_player_stats,
    get_league_standings,
    get_matchups_by_week
)
# 导入所有模型
from .model import (
    Base,
    Game,
    League,
    LeagueSettings,
    StatCategory,
    Team,
    Manager,
    Player,
    PlayerEligiblePosition,
    RosterDaily,
    PlayerDailyStats,
    PlayerSeasonStats,
    TeamStatsWeekly,
    LeagueStandings,
    TeamMatchups,
    Transaction,
    TransactionPlayer,
    DateDimension,
    LeagueRosterPosition,
    # 数据库管理函数
    get_database_url,
    create_database_engine,
    create_tables,
    recreate_tables,
    get_session
)

__all__ = [
    'DatabaseConnection',
    'DatabaseQueries',
    # 独立函数
    'get_league_by_key',
    'get_team_by_key', 
    'get_player_by_key',
    'get_team_roster',
    'get_player_stats',
    'get_league_standings',
    'get_matchups_by_week',
    # 模型类
    'Base',
    'Game',
    'League',
    'LeagueSettings',
    'StatCategory',
    'Team',
    'Manager',
    'Player',
    'PlayerEligiblePosition',
    'RosterDaily',
    'PlayerDailyStats',
    'PlayerSeasonStats',
    'TeamStatsWeekly',
    'LeagueStandings',
    'TeamMatchups',
    'Transaction',
    'TransactionPlayer',
    'DateDimension',
    'LeagueRosterPosition',
    # 数据库管理函数
    'get_database_url',
    'create_database_engine',
    'create_tables',
    'recreate_tables',
    'get_session'
]