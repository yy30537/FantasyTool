"""
API模块 - Yahoo Fantasy API数据获取

包含所有 fetch_* 函数
"""

from .fetchers import (
    YahooFantasyFetcher,
    # 独立函数接口
    fetch_leagues,
    fetch_league_settings,
    fetch_league_standings,
    fetch_league_transactions,
    fetch_league_scoreboard,
    fetch_teams,
    fetch_team_info,
    fetch_team_roster,
    fetch_team_stats,
    fetch_team_matchups,
    fetch_players,
    fetch_player_info,
    fetch_player_stats,
    fetch_player_ownership,
    fetch_player_draft_analysis,
    fetch_matchups,
    fetch_matchup_grades,
    fetch_transactions,
    fetch_waiver_claims,
    fetch_trades,
    fetch_draft_results,
    fetch_predraft_rankings,
    fetch_game_weeks,
    fetch_stat_categories,
    fetch_roster_positions,
    fetch_transaction_types,
    fetch_user_games,
    fetch_user_leagues,
    fetch_user_teams
)
from .client import YahooFantasyAPIClient, YahooFantasyClient

__all__ = [
    'YahooFantasyFetcher', 
    'YahooFantasyAPIClient',
    'YahooFantasyClient',  # 别名
    # 独立函数
    'fetch_leagues',
    'fetch_league_settings',
    'fetch_league_standings',
    'fetch_league_transactions',
    'fetch_league_scoreboard',
    'fetch_teams',
    'fetch_team_info',
    'fetch_team_roster',
    'fetch_team_stats',
    'fetch_team_matchups',
    'fetch_players',
    'fetch_player_info',
    'fetch_player_stats',
    'fetch_player_ownership',
    'fetch_player_draft_analysis',
    'fetch_matchups',
    'fetch_matchup_grades',
    'fetch_transactions',
    'fetch_waiver_claims',
    'fetch_trades',
    'fetch_draft_results',
    'fetch_predraft_rankings',
    'fetch_game_weeks',
    'fetch_stat_categories',
    'fetch_roster_positions',
    'fetch_transaction_types',
    'fetch_user_games',
    'fetch_user_leagues',
    'fetch_user_teams'
]