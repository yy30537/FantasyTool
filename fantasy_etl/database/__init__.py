"""
数据库模块 - 数据库查询、连接管理和模型定义

核心功能:
- 数据库连接管理
- SQLAlchemy模型定义  
- 数据查询和检索
- 联盟、球员、统计数据查询
"""

# 导入连接管理和查询功能
from .queries import DatabaseQueries
from .connection import DatabaseConnection

# 导入模型定义和数据库管理函数
from .model import *

__all__ = [
    # 连接和查询类
    'DatabaseQueries', 'DatabaseConnection',
    
    # 模型类
    'Base', 'Game', 'League', 'Team', 'Player', 'Manager',
    'LeagueSettings', 'StatCategory', 'PlayerEligiblePosition',
    'RosterDaily', 'PlayerDailyStats', 'PlayerSeasonStats', 
    'TeamStatsWeekly', 'LeagueStandings', 'TeamMatchups',
    'Transaction', 'TransactionPlayer', 'DateDimension',
    'LeagueRosterPosition',
    
    # 数据库管理函数
    'get_database_url', 'create_database_engine', 'create_tables',
    'recreate_tables', 'get_session',
]