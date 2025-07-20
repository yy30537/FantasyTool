"""
交易建议引擎
分析球员价值并推荐交易
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, date, timedelta

from ...core.database.connection_manager import db_manager
from ...core.database.models import (
    Player, PlayerDailyStats, PlayerSeasonStats, Team, League
)

@dataclass
class PlayerValue:
    """球员价值评估"""
    player_key: str
    player_name: str
    current_value: float
    projected_value: float
    trend: str  # "上升", "下降", "稳定"
    position: str
    team: str
    injury_risk: float

@dataclass
class TradeRecommendation:
    """交易建议"""
    trade_type: str  # "获取", "出售", "平衡交易"
    target_players: List[PlayerValue]
    offer_players: List[PlayerValue]
    value_gain: float
    risk_level: str
    reasoning: str
    confidence: float

class TradingEngine:
    """交易建议引擎"""
    
    def __init__(self):
        self.position_weights = {
            'PG': {'assists': 0.3, 'steals': 0.2, 'points': 0.25, 'field_goal_percentage': 0.15, 'turnovers': -0.1},
            'SG': {'points': 0.3, 'three_pointers_made': 0.25, 'field_goal_percentage': 0.2, 'steals': 0.15, 'turnovers': -0.1},
            'SF': {'points': 0.25, 'rebounds': 0.2, 'assists': 0.15, 'steals': 0.15, 'blocks': 0.15, 'turnovers': -0.1},
            'PF': {'rebounds': 0.3, 'points': 0.25, 'blocks': 0.2, 'field_goal_percentage': 0.15, 'turnovers': -0.1},
            'C': {'rebounds': 0.35, 'blocks': 0.25, 'points': 0.2, 'field_goal_percentage': 0.15, 'turnovers': -0.05}
        }
    
    def evaluate_player_value(self, player_key: str, league_key: str, season: str) -> Optional[PlayerValue]:
        """评估球员价值"""
        try:
            with db_manager.session_scope() as session:
                # 获取球员信息
                player = session.query(Player).filter_by(
                    player_key=player_key, league_key=league_key
                ).first()
                
                if not player:
                    return None
                
                # 获取球员统计数据
                season_stats = session.query(PlayerSeasonStats).filter_by(
                    player_key=player_key, season=season
                ).first()
                
                # 获取最近的日统计 (用于趋势分析)
                recent_stats = session.query(PlayerDailyStats).filter_by(
                    player_key=player_key
                ).order_by(PlayerDailyStats.date.desc()).limit(10).all()
                
                # 计算当前价值
                current_value = self._calculate_player_value(player, season_stats)
                
                # 预测未来价值
                projected_value = self._project_future_value(player, season_stats, recent_stats)
                
                # 分析趋势
                trend = self._analyze_trend(recent_stats)
                
                # 评估伤病风险
                injury_risk = self._assess_injury_risk(player, recent_stats)
                
                return PlayerValue(
                    player_key=player_key,
                    player_name=player.full_name,
                    current_value=current_value,
                    projected_value=projected_value,
                    trend=trend,
                    position=player.primary_position or 'UTIL',
                    team=player.current_team_abbr or 'FA',
                    injury_risk=injury_risk
                )
                
        except Exception as e:
            print(f"评估球员 {player_key} 价值时出错: {e}")
            return None
    
    def generate_trade_recommendations(self, team_key: str, league_key: str, season: str,
                                     max_recommendations: int = 5) -> List[TradeRecommendation]:
        """为球队生成交易建议"""
        try:
            with db_manager.session_scope() as session:
                # 获取球队当前阵容 (简化: 获取联盟所有球员)
                all_players = session.query(Player).filter_by(league_key=league_key).all()
                
                recommendations = []
                
                # 分析每个位置的需求
                for position in ['PG', 'SG', 'SF', 'PF', 'C']:
                    position_recs = self._analyze_position_needs(
                        team_key, position, all_players, league_key, season
                    )
                    recommendations.extend(position_recs)
                
                # 按价值增益排序
                recommendations.sort(key=lambda x: x.value_gain, reverse=True)
                
                return recommendations[:max_recommendations]
                
        except Exception as e:
            print(f"生成交易建议时出错: {e}")
            return []
    
    def compare_trade_scenarios(self, scenarios: List[Dict[str, List[str]]]) -> Dict[str, Any]:
        """比较多个交易方案"""
        comparisons = []
        
        for i, scenario in enumerate(scenarios):
            give_players = scenario.get('give', [])
            get_players = scenario.get('get', [])
            
            # 计算交易价值
            give_value = sum(self._get_cached_player_value(p) for p in give_players)
            get_value = sum(self._get_cached_player_value(p) for p in get_players)
            
            net_value = get_value - give_value
            
            comparisons.append({
                'scenario_id': i + 1,
                'give_players': give_players,
                'get_players': get_players,
                'give_value': give_value,
                'get_value': get_value,
                'net_value': net_value,
                'recommendation': 'favorable' if net_value > 0 else 'unfavorable'
            })
        
        return {
            'scenarios': comparisons,
            'best_scenario': max(comparisons, key=lambda x: x['net_value']) if comparisons else None
        }
    
    def _calculate_player_value(self, player: Player, season_stats: Optional[PlayerSeasonStats]) -> float:
        """计算球员当前价值"""
        if not season_stats:
            return 0.0
        
        base_value = 50.0  # 基础价值
        position = player.primary_position or 'UTIL'
        
        # 根据位置权重计算价值
        if position in self.position_weights:
            weights = self.position_weights[position]
            
            for stat, weight in weights.items():
                stat_value = getattr(season_stats, f'total_{stat}', 0) or getattr(season_stats, stat, 0) or 0
                
                # 标准化统计值 (简化处理)
                if stat in ['field_goal_percentage', 'free_throw_percentage']:
                    normalized_value = stat_value * 100 if stat_value else 0
                else:
                    normalized_value = stat_value
                
                base_value += normalized_value * weight
        
        return max(0.0, base_value)
    
    def _project_future_value(self, player: Player, season_stats: Optional[PlayerSeasonStats],
                            recent_stats: List[PlayerDailyStats]) -> float:
        """预测球员未来价值"""
        current_value = self._calculate_player_value(player, season_stats)
        
        # 基于最近表现调整
        if recent_stats:
            recent_avg = sum(s.points or 0 for s in recent_stats) / len(recent_stats)
            season_avg = (season_stats.total_points or 0) / 82 if season_stats and season_stats.total_points else 0
            
            if recent_avg > season_avg * 1.1:  # 最近表现好
                return current_value * 1.05
            elif recent_avg < season_avg * 0.9:  # 最近表现差
                return current_value * 0.95
        
        return current_value
    
    def _analyze_trend(self, recent_stats: List[PlayerDailyStats]) -> str:
        """分析球员表现趋势"""
        if len(recent_stats) < 5:
            return "稳定"
        
        # 简单趋势分析: 比较前半段和后半段平均值
        mid_point = len(recent_stats) // 2
        early_avg = sum(s.points or 0 for s in recent_stats[mid_point:]) / (len(recent_stats) - mid_point)
        recent_avg = sum(s.points or 0 for s in recent_stats[:mid_point]) / mid_point
        
        if recent_avg > early_avg * 1.1:
            return "上升"
        elif recent_avg < early_avg * 0.9:
            return "下降"
        else:
            return "稳定"
    
    def _assess_injury_risk(self, player: Player, recent_stats: List[PlayerDailyStats]) -> float:
        """评估伤病风险"""
        risk = 0.1  # 基础风险
        
        # 基于球员状态
        if player.status == 'INJ':
            risk += 0.5
        elif player.status in ['DTD', 'SUSP']:
            risk += 0.3
        
        # 基于最近出场情况
        if recent_stats:
            games_missed = len([s for s in recent_stats if (s.points or 0) == 0])
            miss_rate = games_missed / len(recent_stats)
            risk += miss_rate * 0.3
        
        return min(1.0, risk)
    
    def _analyze_position_needs(self, team_key: str, position: str, all_players: List[Player],
                              league_key: str, season: str) -> List[TradeRecommendation]:
        """分析某位置的交易需求"""
        # 这里应该实现更复杂的位置需求分析
        # 暂时返回空列表
        return []
    
    def _get_cached_player_value(self, player_key: str) -> float:
        """获取缓存的球员价值 (简化实现)"""
        # 实际应该从缓存或数据库获取
        return 50.0