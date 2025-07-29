"""
数据转换模块

包含所有 transform_* 函数，按业务领域分组
"""

from .core import (
    CoreTransformers,
    # 独立函数
    transform_league_data,
    transform_game_data,
    transform_settings_data,
    transform_standings_data,
    transform_draft_data,
    transform_transaction_data,
    transform_waiver_data
)
from .roster import (
    RosterTransformers,
    # 独立函数
    transform_roster_data,
    transform_roster_positions,
    transform_lineup_data
)
from .team import (
    TeamTransformers,
    # 独立函数
    transform_team_data,
    transform_team_stats,
    transform_matchup_data,
    transform_manager_data
)
from .player import (
    PlayerTransformers,
    # 独立函数
    transform_player_data,
    transform_player_stats,
    transform_player_ownership,
    transform_draft_analysis
)
from .stats import (
    StatsTransformers,
    # 独立函数
    transform_stat_categories,
    transform_scoring_settings,
    transform_weekly_stats,
    transform_season_stats,
    transform_projected_stats,
    transform_matchup_grades,
    transform_trade_data,
    transform_schedule_data,
    transform_game_weeks,
    transform_metadata
)

__all__ = [
    # 类
    'CoreTransformers',
    'RosterTransformers', 
    'TeamTransformers',
    'PlayerTransformers',
    'StatsTransformers',
    # 独立函数
    'transform_league_data',
    'transform_game_data',
    'transform_settings_data',
    'transform_standings_data',
    'transform_draft_data',
    'transform_transaction_data',
    'transform_waiver_data',
    'transform_roster_data',
    'transform_roster_positions',
    'transform_lineup_data',
    'transform_team_data',
    'transform_team_stats',
    'transform_matchup_data',
    'transform_manager_data',
    'transform_player_data',
    'transform_player_stats',
    'transform_player_ownership',
    'transform_draft_analysis',
    'transform_stat_categories',
    'transform_scoring_settings',
    'transform_weekly_stats',
    'transform_season_stats',
    'transform_projected_stats',
    'transform_matchup_grades',
    'transform_trade_data',
    'transform_schedule_data',
    'transform_game_weeks',
    'transform_metadata'
]