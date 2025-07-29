"""
数据加载模块 - 数据库写入操作

包含所有 load_* 函数
"""

from .core import (
    CoreLoaders,
    # 独立函数
    load_league,
    load_team,
    load_player,
    load_manager,
    load_settings,
    load_roster_positions,
    load_stat_categories,
    load_scoring_settings
)
from .batch import (
    BatchLoaders,
    # 独立函数
    load_teams_batch,
    load_players_batch,
    load_roster_batch,
    load_transactions_batch,
    load_draft_picks_batch,
    load_matchups_batch
)
from .stats import (
    StatsLoaders,
    # 独立函数
    load_player_stats,
    load_team_stats,
    load_weekly_stats_batch,
    load_season_stats_batch,
    load_projected_stats,
    load_matchup_results,
    load_transaction,
    load_waiver_claim,
    load_trade,
    load_standings,
    load_schedule,
    load_draft_rankings,
    load_ownership_data,
    load_matchup_grades
)

__all__ = [
    # 类
    'CoreLoaders',
    'BatchLoaders',
    'StatsLoaders',
    # 独立函数
    'load_league',
    'load_team',
    'load_player',
    'load_manager',
    'load_settings',
    'load_roster_positions',
    'load_stat_categories',
    'load_scoring_settings',
    'load_teams_batch',
    'load_players_batch',
    'load_roster_batch',
    'load_transactions_batch',
    'load_draft_picks_batch',
    'load_matchups_batch',
    'load_player_stats',
    'load_team_stats',
    'load_weekly_stats_batch',
    'load_season_stats_batch',
    'load_projected_stats',
    'load_matchup_results',
    'load_transaction',
    'load_waiver_claim',
    'load_trade',
    'load_standings',
    'load_schedule',
    'load_draft_rankings',
    'load_ownership_data',
    'load_matchup_grades'
]