# 排名数据加载器
#
# 迁移来源: @database_writer.py 中的排名相关写入逻辑
# 主要映射:
#   - write_league_standings() -> StandingsLoader.load_standings()
#   - LeagueStandings模型写入逻辑 -> 排名数据加载功能
#
# 职责:
#   - 联盟排名数据加载：
#     * LeagueStandings模型写入：rank、wins、losses等
#     * 排名信息：rank、playoff_seed字段
#     * 胜负记录：wins、losses、ties、win_percentage
#     * 落后情况：games_back字段（"-"表示第一名）
#   - 战绩数据处理：
#     * 常规赛战绩：总胜负记录和胜率计算
#     * 分区战绩：divisional_wins、divisional_losses等
#     * 胜率标准化：percentage字段的小数处理
#     * 战绩一致性：wins+losses与总场次的验证
#   - 季后赛信息处理：
#     * 季后赛种子：playoff_seed的设置和验证
#     * 季后赛资格：基于排名的季后赛入围判断
#     * 种子排序：playoff_seed的数值排序逻辑
#   - 数据验证和清洗：
#     * 必需字段验证：league_key、team_key、season、rank
#     * 排名合理性：rank值在合理范围内（1到团队数）
#     * 战绩逻辑性：胜负数据的逻辑一致性
#     * 百分比准确性：win_percentage的计算验证
#   - 关联数据处理：
#     * 外键约束：league_key、team_key的引用完整性
#     * 赛季一致性：season与League表的匹配
#     * 团队存在性：team_key在Teams表中的存在验证
#   - 去重和更新策略：
#     * 唯一约束：(league_key, team_key, season)的唯一性
#     * 排名更新：排名变化时的数据更新
#     * 战绩累计：新比赛结果对战绩的影响
#   - 排名计算和验证：
#     * 排名逻辑：基于胜率和其他规则的排名计算
#     * 排名连续性：确保排名序列的完整性（1,2,3...）
#     * 并列排名：相同胜率时的排名处理
#     * 排名更新：实时排名变化的处理
#   - 业务规则验证：
#     * 联盟规则：特定联盟的排名和季后赛规则
#     * 分区规则：如果有分区制的排名逻辑
#     * 季后赛规则：季后赛入围的条件验证
#   - 历史排名管理：
#     * 排名历史：保持排名变化的历史记录
#     * 趋势分析：排名变化趋势的数据支持
#     * 快照保存：特定时间点的排名快照
#   - 数据完整性检查：
#     * 排名覆盖：确保所有团队都有排名记录
#     * 排名重复：检查是否有重复排名
#     * 数据同步：排名数据与其他表数据的同步
#   - 统计和报告：
#     * 处理统计：更新的排名记录数量
#     * 排名分布：各排名位置的团队分布
#     * 季后赛情况：进入季后赛的团队统计
#     * 数据质量：排名数据的完整性和准确性报告
#
# 输入: 标准化的排名数据 (Dict或List[Dict])
# 输出: 排名数据加载结果和统计信息 

"""
Standings Data Loader - Handles league standings information
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from .base_loader import BaseLoader, LoadResult
from ..database.models import LeagueStandings


class LeagueStandingsLoader(BaseLoader):
    """Loader for league standings information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate league standings record"""
        required_fields = ['league_key', 'team_key', 'season', 'rank']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> LeagueStandings:
        """Create LeagueStandings model instance"""
        return LeagueStandings(
            league_key=record['league_key'],
            team_key=record['team_key'],
            season=record['season'],
            rank=record['rank'],
            playoff_seed=record.get('playoff_seed'),
            wins=self._safe_int(record.get('wins', 0)),
            losses=self._safe_int(record.get('losses', 0)),
            ties=self._safe_int(record.get('ties', 0)),
            win_percentage=self._safe_float(record.get('win_percentage')),
            games_back=record.get('games_back', '-'),
            divisional_wins=self._safe_int(record.get('divisional_wins', 0)),
            divisional_losses=self._safe_int(record.get('divisional_losses', 0)),
            divisional_ties=self._safe_int(record.get('divisional_ties', 0))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for league standings record"""
        return f"{record['league_key']}_{record['team_key']}_{record['season']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess league standings record"""
        # Handle integer fields
        int_fields = ['rank', 'wins', 'losses', 'ties', 'divisional_wins', 'divisional_losses', 'divisional_ties']
        for field in int_fields:
            if field in record:
                record[field] = self._safe_int(record[field])
        
        # Handle float fields
        if 'win_percentage' in record:
            record['win_percentage'] = self._safe_float(record['win_percentage'])
        
        # Handle games_back - ensure it's a string
        if 'games_back' in record:
            record['games_back'] = str(record['games_back']) if record['games_back'] is not None else '-'
        
        # Extract standings data from team_standings if present
        if 'team_standings' in record:
            standings_data = record['team_standings']
            extracted_data = self._extract_standings_data(standings_data)
            record.update(extracted_data)
        
        return record
    
    def _extract_standings_data(self, team_standings: Dict[str, Any]) -> Dict[str, Any]:
        """Extract standings data from Yahoo API team_standings format"""
        extracted = {}
        
        try:
            if isinstance(team_standings, dict):
                # Extract basic standings info
                extracted['rank'] = self._safe_int(team_standings.get('rank'))
                extracted['playoff_seed'] = team_standings.get('playoff_seed')
                extracted['games_back'] = team_standings.get('games_back', '-')
                
                # Extract outcome totals (wins/losses/ties)
                outcome_totals = team_standings.get('outcome_totals', {})
                if isinstance(outcome_totals, dict):
                    extracted['wins'] = self._safe_int(outcome_totals.get('wins', 0))
                    extracted['losses'] = self._safe_int(outcome_totals.get('losses', 0))
                    extracted['ties'] = self._safe_int(outcome_totals.get('ties', 0))
                    extracted['win_percentage'] = self._safe_float(outcome_totals.get('percentage'))
                
                # Extract divisional outcome totals
                divisional_totals = team_standings.get('divisional_outcome_totals', {})
                if isinstance(divisional_totals, dict):
                    extracted['divisional_wins'] = self._safe_int(divisional_totals.get('wins', 0))
                    extracted['divisional_losses'] = self._safe_int(divisional_totals.get('losses', 0))
                    extracted['divisional_ties'] = self._safe_int(divisional_totals.get('ties', 0))
        
        except Exception as e:
            print(f"Error extracting standings data: {e}")
        
        return extracted


class CompleteStandingsLoader:
    """Complete standings data loader that handles league standings"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loader
        self.standings_loader = LeagueStandingsLoader(connection_manager, batch_size)
    
    def load_standings_data(self, standings_data: List[Dict[str, Any]]) -> LoadResult:
        """Load league standings data"""
        return self.standings_loader.load(standings_data)
    
    def load_league_standings_from_api(self, league_key: str, season: str, 
                                      standings_api_data: Dict[str, Any]) -> LoadResult:
        """Load league standings from Yahoo API standings response"""
        processed_standings = []
        
        try:
            # Extract teams from the API response
            fantasy_content = standings_api_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # Find standings container
            standings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "standings" in item:
                        standings_container = item["standings"]
                        break
            elif isinstance(league_data, dict) and "standings" in league_data:
                standings_container = league_data["standings"]
            
            if not standings_container:
                print("No standings container found in API data")
                return LoadResult()
            
            # Find teams container within standings
            teams_container = None
            if isinstance(standings_container, list):
                for item in standings_container:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(standings_container, dict) and "teams" in standings_container:
                teams_container = standings_container["teams"]
            
            if not teams_container:
                print("No teams container found in standings data")
                return LoadResult()
            
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # Extract team information and standings
                team_info = self._extract_team_standings_info(team_data)
                if team_info:
                    team_info.update({
                        'league_key': league_key,
                        'season': season
                    })
                    processed_standings.append(team_info)
        
        except Exception as e:
            print(f"Error processing league standings API data: {e}")
        
        return self.standings_loader.load(processed_standings)
    
    def _extract_team_standings_info(self, team_data: Any) -> Optional[Dict[str, Any]]:
        """Extract team standings information from complex nested team data"""
        try:
            team_key = None
            team_standings = None
            
            # Recursively extract team_key and team_standings
            def extract_from_data(data, target_key):
                if isinstance(data, dict):
                    if target_key in data:
                        return data[target_key]
                    for value in data.values():
                        result = extract_from_data(value, target_key)
                        if result is not None:
                            return result
                elif isinstance(data, list):
                    for item in data:
                        result = extract_from_data(item, target_key)
                        if result is not None:
                            return result
                return None
            
            team_key = extract_from_data(team_data, "team_key")
            team_standings = extract_from_data(team_data, "team_standings")
            
            if not team_key or not team_standings:
                return None
            
            standings_info = {
                'team_key': team_key,
                'team_standings': team_standings
            }
            
            return standings_info
            
        except Exception as e:
            print(f"Error extracting team standings info: {e}")
            return None 