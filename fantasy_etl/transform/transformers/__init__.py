"""
Transformers Package - 数据转换器

提供各种数据转换器的统一接口
"""

from .base_transformer import BaseTransformer, TransformResult, ValidationError
from .league_transformer import LeagueTransformer
from .player_transformer import PlayerTransformer
from .roster_transformer import RosterTransformer
from .team_transformer import TeamTransformer
from .transaction_transformer import TransactionTransformer

__all__ = [
    'BaseTransformer',
    'TransformResult', 
    'ValidationError',
    'LeagueTransformer',
    'PlayerTransformer',
    'RosterTransformer',
    'TeamTransformer',
    'TransactionTransformer',
    # 工厂函数
    'create_league_transformer',
    'create_player_transformer',
    'create_roster_transformer',
    'create_team_transformer',
    'create_transaction_transformer'
]

# 工厂函数
def create_league_transformer(strict_mode: bool = False) -> LeagueTransformer:
    """创建联盟数据转换器"""
    return LeagueTransformer(strict_mode=strict_mode)

def create_player_transformer(strict_mode: bool = False) -> PlayerTransformer:
    """创建球员数据转换器"""
    return PlayerTransformer(strict_mode=strict_mode)

def create_roster_transformer(strict_mode: bool = False) -> RosterTransformer:
    """创建阵容数据转换器"""
    return RosterTransformer(strict_mode=strict_mode)

def create_team_transformer(strict_mode: bool = False) -> TeamTransformer:
    """创建团队数据转换器"""
    return TeamTransformer(strict_mode=strict_mode)

def create_transaction_transformer(strict_mode: bool = False) -> TransactionTransformer:
    """创建交易数据转换器"""
    return TransactionTransformer(strict_mode=strict_mode) 