"""
统计计算器
提供各种fantasy统计计算功能
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, date, timedelta
import statistics

from ...core.database.connection_manager import db_manager
from ...core.database.models import (
    PlayerDailyStats, PlayerSeasonStats, TeamStatsWeekly, League
)

@dataclass
class PlayerStats:
    """球员统计数据"""
    player_key: str
    player_name: str
    games_played: int
    averages: Dict[str, float]
    totals: Dict[str, int]
    percentiles: Dict[str, float]  # 在联盟中的百分位

@dataclass
class LeagueStats:
    """联盟统计数据"""
    league_key: str
    season: str
    averages: Dict[str, float]
    medians: Dict[str, float]
    top_performers: Dict[str, List[str]]  # 各统计类别的前几名

class StatsCalculator:
    """统计计算器"""
    
    def __init__(self):
        self.stat_categories = [
            'field_goals_made', 'field_goals_attempted', 'field_goal_percentage',
            'free_throws_made', 'free_throws_attempted', 'free_throw_percentage',
            'three_pointers_made', 'points', 'rebounds', 'assists', 
            'steals', 'blocks', 'turnovers'
        ]
    
    def calculate_player_averages(self, player_key: str, league_key: str, 
                                season: str, days: int = 30) -> Optional[PlayerStats]:
        """计算球员平均数据"""
        try:
            with db_manager.session_scope() as session:
                # 获取指定时间段的统计数据
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days)
                
                daily_stats = session.query(PlayerDailyStats).filter(
                    PlayerDailyStats.player_key == player_key,
                    PlayerDailyStats.league_key == league_key,
                    PlayerDailyStats.season == season,
                    PlayerDailyStats.date >= start_date,
                    PlayerDailyStats.date <= end_date
                ).all()
                
                if not daily_stats:
                    return None
                
                # 计算平均值和总计
                averages = {}
                totals = {}
                
                for category in self.stat_categories:
                    values = [getattr(stat, category, 0) or 0 for stat in daily_stats]
                    
                    if category in ['field_goal_percentage', 'free_throw_percentage']:
                        # 百分比统计使用有效值计算平均
                        valid_values = [v for v in values if v > 0]
                        averages[category] = statistics.mean(valid_values) if valid_values else 0.0
                        totals[category] = len(valid_values)
                    else:
                        averages[category] = statistics.mean(values)
                        totals[category] = sum(values)
                
                # 计算在联盟中的百分位
                percentiles = self._calculate_percentiles(
                    player_key, league_key, season, averages
                )
                
                return PlayerStats(
                    player_key=player_key,
                    player_name="",  # 需要从Player表获取
                    games_played=len(daily_stats),
                    averages=averages,
                    totals=totals,
                    percentiles=percentiles
                )
                
        except Exception as e:
            print(f"计算球员 {player_key} 平均数据时出错: {e}")
            return None
    
    def calculate_league_benchmarks(self, league_key: str, season: str) -> Optional[LeagueStats]:
        """计算联盟基准数据"""
        try:
            with db_manager.session_scope() as session:
                # 获取联盟所有球员的赛季统计
                season_stats = session.query(PlayerSeasonStats).filter_by(
                    league_key=league_key, season=season
                ).all()
                
                if not season_stats:
                    return None
                
                averages = {}
                medians = {}
                top_performers = {}
                
                for category in self.stat_categories:
                    values = []
                    player_values = []
                    
                    for stat in season_stats:
                        value = getattr(stat, category.replace('total_', ''), 0) or 0
                        if category.startswith('total_'):
                            # 赛季总计统计
                            value = getattr(stat, category, 0) or 0
                        
                        if value > 0:  # 只包含有效数据
                            values.append(value)
                            player_values.append((stat.player_key, value))
                    
                    if values:
                        averages[category] = statistics.mean(values)
                        medians[category] = statistics.median(values)
                        
                        # 找出前5名
                        top_5 = sorted(player_values, key=lambda x: x[1], reverse=True)[:5]
                        top_performers[category] = [player[0] for player in top_5]
                
                return LeagueStats(
                    league_key=league_key,
                    season=season,
                    averages=averages,
                    medians=medians,
                    top_performers=top_performers
                )
                
        except Exception as e:
            print(f"计算联盟 {league_key} 基准数据时出错: {e}")
            return None
    
    def calculate_efficiency_metrics(self, player_key: str, season: str) -> Dict[str, float]:
        """计算效率指标"""
        try:
            with db_manager.session_scope() as session:
                season_stats = session.query(PlayerSeasonStats).filter_by(
                    player_key=player_key, season=season
                ).first()
                
                if not season_stats:
                    return {}
                
                metrics = {}
                
                # 真实投篮命中率 (TS%)
                points = season_stats.total_points or 0
                fga = season_stats.field_goals_attempted or 0
                fta = season_stats.free_throws_attempted or 0
                
                if fga > 0 and fta > 0:
                    ts_attempts = fga + 0.44 * fta
                    metrics['true_shooting_percentage'] = points / (2 * ts_attempts) if ts_attempts > 0 else 0
                
                # 使用率 (简化计算)
                fgm = season_stats.field_goals_made or 0
                ftm = season_stats.free_throws_made or 0
                assists = season_stats.total_assists or 0
                turnovers = season_stats.total_turnovers or 0
                
                if fgm + ftm + assists + turnovers > 0:
                    metrics['usage_rate'] = (fgm + ftm + assists) / (fgm + ftm + assists + turnovers)
                
                # 助攻失误比
                if turnovers > 0:
                    metrics['assist_turnover_ratio'] = assists / turnovers
                else:
                    metrics['assist_turnover_ratio'] = assists
                
                # 每分钟得分
                # 注意: 这里需要上场时间数据，暂时用比赛数估算
                games_estimate = 82  # 简化
                minutes_estimate = games_estimate * 30  # 假设每场30分钟
                metrics['points_per_minute'] = points / minutes_estimate if minutes_estimate > 0 else 0
                
                return metrics
                
        except Exception as e:
            print(f"计算球员 {player_key} 效率指标时出错: {e}")
            return {}
    
    def calculate_consistency_score(self, player_key: str, season: str, days: int = 30) -> float:
        """计算球员一致性得分"""
        try:
            with db_manager.session_scope() as session:
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days)
                
                daily_stats = session.query(PlayerDailyStats).filter(
                    PlayerDailyStats.player_key == player_key,
                    PlayerDailyStats.season == season,
                    PlayerDailyStats.date >= start_date,
                    PlayerDailyStats.date <= end_date
                ).all()
                
                if len(daily_stats) < 5:  # 需要至少5场比赛
                    return 0.0
                
                # 计算各项统计的变异系数
                consistency_scores = []
                
                for category in ['points', 'rebounds', 'assists']:
                    values = [getattr(stat, category, 0) or 0 for stat in daily_stats]
                    
                    if len(values) > 1:
                        mean_val = statistics.mean(values)
                        if mean_val > 0:
                            std_val = statistics.stdev(values)
                            cv = std_val / mean_val  # 变异系数
                            consistency_score = max(0, 1 - cv)  # 越小越一致
                            consistency_scores.append(consistency_score)
                
                return statistics.mean(consistency_scores) if consistency_scores else 0.0
                
        except Exception as e:
            print(f"计算球员 {player_key} 一致性得分时出错: {e}")
            return 0.0
    
    def _calculate_percentiles(self, player_key: str, league_key: str, season: str,
                             player_averages: Dict[str, float]) -> Dict[str, float]:
        """计算球员在联盟中的百分位排名"""
        try:
            with db_manager.session_scope() as session:
                # 获取联盟所有球员的平均数据
                all_season_stats = session.query(PlayerSeasonStats).filter_by(
                    league_key=league_key, season=season
                ).all()
                
                percentiles = {}
                
                for category, player_avg in player_averages.items():
                    if category in ['field_goal_percentage', 'free_throw_percentage']:
                        continue  # 跳过百分比统计
                    
                    # 获取所有球员该项统计的值
                    league_values = []
                    for stat in all_season_stats:
                        value = getattr(stat, category.replace('total_', ''), 0) or 0
                        if category.startswith('total_'):
                            value = getattr(stat, category, 0) or 0
                        
                        if value > 0:
                            league_values.append(value)
                    
                    if league_values:
                        # 计算百分位
                        sorted_values = sorted(league_values)
                        rank = sum(1 for v in sorted_values if v < player_avg)
                        percentile = rank / len(sorted_values) * 100
                        percentiles[category] = percentile
                
                return percentiles
                
        except Exception as e:
            print(f"计算百分位时出错: {e}")
            return {}