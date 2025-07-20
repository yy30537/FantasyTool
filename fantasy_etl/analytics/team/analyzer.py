"""
球队分析器
提供球队表现分析功能
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, date

from ...core.database.connection_manager import db_manager
from ...core.database.models import (
    Team, Player, PlayerDailyStats, PlayerSeasonStats,
    TeamStatsWeekly, LeagueStandings, TeamMatchups
)

@dataclass
class TeamAnalysis:
    """球队分析结果"""
    team_key: str
    team_name: str
    current_rank: int
    total_points: int
    strengths: List[str]
    weaknesses: List[str]
    recommendations: List[str]
    player_analysis: Dict[str, Any]

class TeamAnalyzer:
    """球队分析器"""
    
    def __init__(self):
        self.statistical_categories = [
            'field_goal_percentage', 'free_throw_percentage', 'three_pointers_made',
            'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers'
        ]
    
    def analyze_team(self, team_key: str, league_key: str, season: str) -> Optional[TeamAnalysis]:
        """分析单个球队表现"""
        try:
            with db_manager.session_scope() as session:
                # 获取球队信息
                team = session.query(Team).filter_by(team_key=team_key).first()
                if not team:
                    return None
                
                # 获取排名信息
                standings = session.query(LeagueStandings).filter_by(
                    team_key=team_key, league_key=league_key, season=season
                ).first()
                
                current_rank = standings.rank if standings else 0
                
                # 获取球队统计数据
                team_stats = self._get_team_stats(session, team_key, season)
                
                # 获取球员表现
                player_analysis = self._analyze_team_players(session, team_key, league_key, season)
                
                # 分析优势和劣势
                strengths, weaknesses = self._identify_strengths_weaknesses(team_stats)
                
                # 生成建议
                recommendations = self._generate_recommendations(strengths, weaknesses, player_analysis)
                
                return TeamAnalysis(
                    team_key=team_key,
                    team_name=team.name,
                    current_rank=current_rank,
                    total_points=team_stats.get('total_points', 0),
                    strengths=strengths,
                    weaknesses=weaknesses,
                    recommendations=recommendations,
                    player_analysis=player_analysis
                )
                
        except Exception as e:
            print(f"分析球队 {team_key} 时出错: {e}")
            return None
    
    def compare_teams(self, team_keys: List[str], league_key: str, season: str) -> Dict[str, Any]:
        """比较多个球队"""
        try:
            team_analyses = []
            for team_key in team_keys:
                analysis = self.analyze_team(team_key, league_key, season)
                if analysis:
                    team_analyses.append(analysis)
            
            if not team_analyses:
                return {}
            
            # 生成比较报告
            comparison = {
                'teams': team_analyses,
                'rankings': sorted(team_analyses, key=lambda x: x.current_rank),
                'category_leaders': self._find_category_leaders(team_analyses),
                'trade_opportunities': self._identify_trade_opportunities(team_analyses)
            }
            
            return comparison
            
        except Exception as e:
            print(f"比较球队时出错: {e}")
            return {}
    
    def _get_team_stats(self, session, team_key: str, season: str) -> Dict[str, Any]:
        """获取球队统计数据"""
        stats = {}
        
        # 获取周统计数据汇总
        weekly_stats = session.query(TeamStatsWeekly).filter_by(
            team_key=team_key, season=season
        ).all()
        
        if weekly_stats:
            # 计算赛季累计统计
            for category in self.statistical_categories:
                total = sum(getattr(stat, category, 0) or 0 for stat in weekly_stats)
                stats[category] = total
                stats[f'{category}_avg'] = total / len(weekly_stats) if weekly_stats else 0
            
            stats['total_points'] = stats.get('points', 0)
            stats['games_played'] = len(weekly_stats)
        
        return stats
    
    def _analyze_team_players(self, session, team_key: str, league_key: str, season: str) -> Dict[str, Any]:
        """分析球队球员表现"""
        # 这里应该获取球队当前阵容和球员统计
        # 暂时返回空字典，后续实现
        return {
            'top_performers': [],
            'underperformers': [],
            'injury_concerns': [],
            'breakout_candidates': []
        }
    
    def _identify_strengths_weaknesses(self, team_stats: Dict[str, Any]) -> tuple[List[str], List[str]]:
        """识别球队优势和劣势"""
        strengths = []
        weaknesses = []
        
        # 基于联盟平均水平进行比较 (这里简化处理)
        if team_stats.get('field_goal_percentage_avg', 0) > 0.45:
            strengths.append("投篮命中率高")
        elif team_stats.get('field_goal_percentage_avg', 0) < 0.40:
            weaknesses.append("投篮命中率低")
        
        if team_stats.get('assists_avg', 0) > 25:
            strengths.append("助攻能力强")
        elif team_stats.get('assists_avg', 0) < 20:
            weaknesses.append("助攻偏少")
        
        if team_stats.get('rebounds_avg', 0) > 45:
            strengths.append("篮板控制力强")
        elif team_stats.get('rebounds_avg', 0) < 40:
            weaknesses.append("篮板保护不足")
        
        if team_stats.get('turnovers_avg', 0) < 12:
            strengths.append("失误控制好")
        elif team_stats.get('turnovers_avg', 0) > 16:
            weaknesses.append("失误过多")
        
        return strengths, weaknesses
    
    def _generate_recommendations(self, strengths: List[str], weaknesses: List[str], 
                                player_analysis: Dict[str, Any]) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        if "投篮命中率低" in weaknesses:
            recommendations.append("考虑交易获得高效投手")
        
        if "助攻偏少" in weaknesses:
            recommendations.append("寻找组织能力强的控球后卫")
        
        if "篮板保护不足" in weaknesses:
            recommendations.append("补强内线篮板球员")
        
        if "失误过多" in weaknesses:
            recommendations.append("调整阵容减少高失误球员的上场时间")
        
        # 基于优势的建议
        if len(strengths) > len(weaknesses):
            recommendations.append("保持当前阵容稳定性，专注于细节优化")
        
        return recommendations
    
    def _find_category_leaders(self, team_analyses: List[TeamAnalysis]) -> Dict[str, str]:
        """找出各项统计的领先球队"""
        # 简化实现，实际需要更复杂的统计比较
        leaders = {}
        
        # 按排名找出第一名
        if team_analyses:
            best_team = min(team_analyses, key=lambda x: x.current_rank)
            leaders['overall'] = best_team.team_name
        
        return leaders
    
    def _identify_trade_opportunities(self, team_analyses: List[TeamAnalysis]) -> List[Dict[str, Any]]:
        """识别交易机会"""
        opportunities = []
        
        # 这里可以实现复杂的交易分析逻辑
        # 暂时返回空列表
        
        return opportunities