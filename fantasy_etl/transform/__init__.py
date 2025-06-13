"""
Transform Layer - 数据转换层

提供统一的数据转换接口，将Yahoo Fantasy API的原始数据转换为标准化格式
"""

from .transformers.base_transformer import BaseTransformer, TransformResult, ValidationError
from .transformers.roster_transformer import RosterTransformer
from .transformers.team_transformer import TeamTransformer
from .transformers.player_transformer import PlayerTransformer
from .transformers.league_transformer import LeagueTransformer
from .transformers.transaction_transformer import TransactionTransformer

__all__ = [
    # 基础类
    'BaseTransformer',
    'TransformResult', 
    'ValidationError',
    
    # 转换器
    'RosterTransformer',
    'TeamTransformer',
    'PlayerTransformer',
    'LeagueTransformer',
    'TransactionTransformer'
]

# 转换器工厂函数
def get_transformer(data_type: str) -> BaseTransformer:
    """
    根据数据类型获取对应的转换器
    
    Args:
        data_type: 数据类型 ('roster', 'team', 'player', 'league', 'transaction')
        
    Returns:
        对应的转换器实例
        
    Raises:
        ValueError: 不支持的数据类型
    """
    transformers = {
        'roster': RosterTransformer,
        'team': TeamTransformer,
        'player': PlayerTransformer,
        'league': LeagueTransformer,
        'transaction': TransactionTransformer
    }
    
    if data_type not in transformers:
        raise ValueError(f"不支持的数据类型: {data_type}. 支持的类型: {list(transformers.keys())}")
    
    return transformers[data_type]()

# 便捷函数
def transform_roster_data(raw_data: dict, strict_mode: bool = False) -> TransformResult:
    """转换阵容数据"""
    transformer = RosterTransformer(strict_mode=strict_mode)
    return transformer.transform(raw_data)

def transform_team_data(raw_data: dict, strict_mode: bool = False) -> TransformResult:
    """转换团队数据"""
    transformer = TeamTransformer(strict_mode=strict_mode)
    return transformer.transform(raw_data)

def transform_player_data(raw_data: dict, strict_mode: bool = False) -> TransformResult:
    """转换球员数据"""
    transformer = PlayerTransformer(strict_mode=strict_mode)
    return transformer.transform(raw_data)

def transform_league_data(raw_data: dict, strict_mode: bool = False) -> TransformResult:
    """转换联盟数据"""
    transformer = LeagueTransformer(strict_mode=strict_mode)
    return transformer.transform(raw_data)

def transform_transaction_data(raw_data: dict, strict_mode: bool = False) -> TransformResult:
    """转换交易数据"""
    transformer = TransactionTransformer(strict_mode=strict_mode)
    return transformer.transform(raw_data) 