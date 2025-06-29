# 对战数据加载器
#
# 迁移来源: @database_writer.py 中的对战相关写入逻辑
# 主要映射:
#   - write_team_matchup() -> MatchupLoader.load_matchup()
#   - write_team_matchup_from_data() -> MatchupLoader.load_matchup_from_data()
#   - _parse_stat_winners() -> MatchupLoader.parse_stat_winners()
#   - _parse_teams_matchup_data() -> MatchupLoader.parse_teams_data()
#   - TeamMatchups模型写入逻辑
#
# 职责:
#   - 对战基础数据加载：
#     * TeamMatchups模型写入：team_key、week、opponent等
#     * 对战时间信息：week_start、week_end、status
#     * 对战结果：is_winner、is_tied、team_points等
#     * 特殊标记：is_playoffs、is_consolation等
#   - 统计类别获胜情况加载：
#     * 9个核心统计类别的获胜标记：wins_field_goal_pct等
#     * 从stat_winners解析：统计类别ID到获胜状态的映射
#     * 布尔字段设置：各统计类别的wins_*字段
#     * 获胜计算：team在各统计类别中的表现
#   - 比赛场次信息加载：
#     * 团队比赛状态：completed_games、remaining_games、live_games
#     * 对手比赛状态：opponent_*系列字段
#     * 比赛进度：对战双方的比赛完成情况
#     * 实时状态：live_games的实时更新
#   - 对战结果处理：
#     * 胜负判断：基于team_points vs opponent_points
#     * 平局处理：积分相等时的is_tied设置
#     * 获胜者记录：winner_team_key的设置和验证
#     * 积分验证：team_points的合理性检查
#   - 数据验证和清洗：
#     * 必需字段验证：team_key、season、week
#     * 对战逻辑验证：opponent_team_key的存在性
#     * 积分逻辑验证：胜负与积分的一致性
#     * 时间逻辑验证：week在合理范围内
#   - 关联数据处理：
#     * 外键约束：league_key、team_key的引用
#     * 对手关系：opponent_team_key的有效性
#     * 赛季一致性：season与League的匹配
#     * 周数验证：week在联盟赛程范围内
#   - 复杂数据解析：
#     * stat_winners解析：从API的stat_winners数组提取
#     * teams数据解析：从复杂嵌套结构提取对战信息
#     * 积分计算：从team_points字典提取总积分
#     * 比赛状态解析：从team_remaining_games提取
#   - 去重和更新策略：
#     * 唯一约束：(team_key, season, week)的唯一性
#     * 对战更新：对战结果的实时更新
#     * 状态变更：从live到completed的状态转换
#   - 业务规则验证：
#     * 对战配对：确保对战双方的数据一致性
#     * 季后赛规则：playoffs对战的特殊处理
#     * 积分规则：团队积分的计算逻辑验证
#     * 统计获胜：各统计类别获胜的合理性
#   - 批量对战处理：
#     * 周度批量：同一周所有对战的批量处理
#     * 对战关联：确保对战双方数据的同步更新
#     * 事务一致性：对战数据的事务完整性
#   - 统计类别映射：
#     * stat_id映射：API统计ID到数据库字段的映射
#     * 核心统计：9个关键统计类别的处理
#     * FG%(5)、FT%(8)、3PTM(10)、PTS(12)、REB(15)、AST(16)、ST(17)、BLK(18)、TO(19)
#   - 性能优化：
#     * 批量插入：多个对战的高效写入
#     * 索引利用：基于team和week的查询优化
#     * 数据缓存：对战状态的缓存策略
#   - 统计和报告：
#     * 处理统计：对战记录的加载数量
#     * 对战分布：各周对战的分布情况
#     * 胜率统计：团队胜率和对战表现
#     * 数据质量：对战数据的完整性报告
#
# 输入: 标准化的对战数据 (Dict)，包含stat_winners和teams信息
# 输出: 对战数据加载结果和统计信息 

"""
Matchup Data Loader - Handles team matchup information
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from .base_loader import BaseLoader, LoadResult
from ..database.models import TeamMatchups


class TeamMatchupsLoader(BaseLoader):
    """Loader for team matchup information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate team matchup record"""
        required_fields = ['league_key', 'team_key', 'season', 'week']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> TeamMatchups:
        """Create TeamMatchups model instance"""
        return TeamMatchups(
            league_key=record['league_key'],
            team_key=record['team_key'],
            season=record['season'],
            week=record['week'],
            week_start=record.get('week_start'),
            week_end=record.get('week_end'),
            status=record.get('status'),
            opponent_team_key=record.get('opponent_team_key'),
            is_winner=self._safe_bool_nullable(record.get('is_winner')),
            is_tied=self._safe_bool(record.get('is_tied', False)),
            team_points=self._safe_int(record.get('team_points', 0)),
            opponent_points=self._safe_int(record.get('opponent_points', 0)),
            winner_team_key=record.get('winner_team_key'),
            is_playoffs=self._safe_bool(record.get('is_playoffs', False)),
            is_consolation=self._safe_bool(record.get('is_consolation', False)),
            is_matchup_of_week=self._safe_bool(record.get('is_matchup_of_week', False)),
            # Stat category wins
            wins_field_goal_pct=self._safe_bool(record.get('wins_field_goal_pct', False)),
            wins_free_throw_pct=self._safe_bool(record.get('wins_free_throw_pct', False)),
            wins_three_pointers=self._safe_bool(record.get('wins_three_pointers', False)),
            wins_points=self._safe_bool(record.get('wins_points', False)),
            wins_rebounds=self._safe_bool(record.get('wins_rebounds', False)),
            wins_assists=self._safe_bool(record.get('wins_assists', False)),
            wins_steals=self._safe_bool(record.get('wins_steals', False)),
            wins_blocks=self._safe_bool(record.get('wins_blocks', False)),
            wins_turnovers=self._safe_bool(record.get('wins_turnovers', False)),
            # Game counts
            completed_games=self._safe_int(record.get('completed_games', 0)),
            remaining_games=self._safe_int(record.get('remaining_games', 0)),
            live_games=self._safe_int(record.get('live_games', 0)),
            opponent_completed_games=self._safe_int(record.get('opponent_completed_games', 0)),
            opponent_remaining_games=self._safe_int(record.get('opponent_remaining_games', 0)),
            opponent_live_games=self._safe_int(record.get('opponent_live_games', 0))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for team matchup record"""
        return f"{record['team_key']}_{record['season']}_{record['week']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess team matchup record"""
        # Handle boolean fields
        bool_fields = ['is_tied', 'is_playoffs', 'is_consolation', 'is_matchup_of_week',
                      'wins_field_goal_pct', 'wins_free_throw_pct', 'wins_three_pointers',
                      'wins_points', 'wins_rebounds', 'wins_assists', 'wins_steals',
                      'wins_blocks', 'wins_turnovers']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        # Handle nullable boolean (is_winner can be None for ties)
        if 'is_winner' in record:
            record['is_winner'] = self._safe_bool_nullable(record['is_winner'])
        
        # Handle integer fields
        int_fields = ['week', 'team_points', 'opponent_points', 'completed_games',
                     'remaining_games', 'live_games', 'opponent_completed_games',
                     'opponent_remaining_games', 'opponent_live_games']
        
        for field in int_fields:
            if field in record:
                record[field] = self._safe_int(record[field])
        
        # Extract matchup data from Yahoo API format if present
        if 'matchup_data' in record:
            matchup_data = record['matchup_data']
            extracted_data = self._extract_matchup_data(matchup_data, record['team_key'])
            record.update(extracted_data)
        
        return record
    
    def _extract_matchup_data(self, matchup_data: Dict[str, Any], team_key: str) -> Dict[str, Any]:
        """Extract matchup data from Yahoo API matchup format"""
        extracted = {}
        
        try:
            # Extract basic matchup info
            extracted['week'] = matchup_data.get('week')
            extracted['week_start'] = matchup_data.get('week_start')
            extracted['week_end'] = matchup_data.get('week_end')
            extracted['status'] = matchup_data.get('status')
            extracted['is_playoffs'] = self._safe_bool(matchup_data.get('is_playoffs', False))
            extracted['is_consolation'] = self._safe_bool(matchup_data.get('is_consolation', False))
            extracted['is_matchup_of_week'] = self._safe_bool(matchup_data.get('is_matchup_of_week', False))
            extracted['is_tied'] = self._safe_bool(matchup_data.get('is_tied', False))
            extracted['winner_team_key'] = matchup_data.get('winner_team_key')
            
            # Parse stat winners
            stat_winners = matchup_data.get('stat_winners', [])
            stat_wins = self._parse_stat_winners(stat_winners, team_key)
            extracted.update(stat_wins)
            
            # Parse teams data for opponent and points
            teams_data = matchup_data.get('0', {}).get('teams', {})
            team_details = self._parse_teams_matchup_data(teams_data, team_key)
            extracted.update(team_details)
            
        except Exception as e:
            print(f"Error extracting matchup data: {e}")
        
        return extracted
    
    def _parse_stat_winners(self, stat_winners: List[Dict[str, Any]], team_key: str) -> Dict[str, bool]:
        """Parse stat winners to determine which categories this team won"""
        wins = {
            'wins_field_goal_pct': False,      # stat_id: 5
            'wins_free_throw_pct': False,      # stat_id: 8
            'wins_three_pointers': False,      # stat_id: 10
            'wins_points': False,              # stat_id: 12
            'wins_rebounds': False,            # stat_id: 15
            'wins_assists': False,             # stat_id: 16
            'wins_steals': False,              # stat_id: 17
            'wins_blocks': False,              # stat_id: 18
            'wins_turnovers': False            # stat_id: 19
        }
        
        # Map stat_id to win field name
        stat_id_map = {
            '5': 'wins_field_goal_pct',
            '8': 'wins_free_throw_pct',
            '10': 'wins_three_pointers',
            '12': 'wins_points',
            '15': 'wins_rebounds',
            '16': 'wins_assists',
            '17': 'wins_steals',
            '18': 'wins_blocks',
            '19': 'wins_turnovers'
        }
        
        try:
            for stat_winner in stat_winners:
                if 'stat_winner' in stat_winner:
                    stat_info = stat_winner['stat_winner']
                    stat_id = str(stat_info.get('stat_id', ''))
                    winner_key = stat_info.get('winner_team_key', '')
                    
                    if stat_id in stat_id_map and winner_key == team_key:
                        wins[stat_id_map[stat_id]] = True
        
        except Exception as e:
            print(f"Error parsing stat winners: {e}")
        
        return wins
    
    def _parse_teams_matchup_data(self, teams_data: Dict[str, Any], target_team_key: str) -> Dict[str, Any]:
        """Parse teams data to extract opponent and scoring details"""
        details = {
            'opponent_team_key': None,
            'is_winner': None,
            'team_points': 0,
            'opponent_points': 0,
            'completed_games': 0,
            'remaining_games': 0,
            'live_games': 0,
            'opponent_completed_games': 0,
            'opponent_remaining_games': 0,
            'opponent_live_games': 0
        }
        
        try:
            teams_count = int(teams_data.get('count', 0))
            
            target_team_data = None
            opponent_team_data = None
            
            # Find target team and opponent team data
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if 'team' not in team_container:
                    continue
                
                team_info = team_container['team']
                
                # Extract team_key from nested structure
                current_team_key = None
                if isinstance(team_info, list) and len(team_info) >= 1:
                    team_basic_info = team_info[0]
                    if isinstance(team_basic_info, list):
                        for info_item in team_basic_info:
                            if isinstance(info_item, dict) and 'team_key' in info_item:
                                current_team_key = info_item['team_key']
                                break
                
                # Categorize teams
                if current_team_key == target_team_key:
                    target_team_data = team_info
                elif current_team_key:
                    details['opponent_team_key'] = current_team_key
                    opponent_team_data = team_info
            
            # Extract target team data
            if target_team_data and len(target_team_data) > 1:
                team_stats_container = target_team_data[1]
                
                # Extract team points
                if 'team_points' in team_stats_container:
                    team_points_data = team_stats_container['team_points']
                    if isinstance(team_points_data, dict) and 'total' in team_points_data:
                        details['team_points'] = self._safe_int(team_points_data['total'])
                
                # Extract game counts
                if 'team_remaining_games' in team_stats_container:
                    remaining_games_data = team_stats_container['team_remaining_games']
                    if isinstance(remaining_games_data, dict) and 'total' in remaining_games_data:
                        total_data = remaining_games_data['total']
                        if isinstance(total_data, dict):
                            details['completed_games'] = self._safe_int(total_data.get('completed_games', 0))
                            details['remaining_games'] = self._safe_int(total_data.get('remaining_games', 0))
                            details['live_games'] = self._safe_int(total_data.get('live_games', 0))
            
            # Extract opponent team data
            if opponent_team_data and len(opponent_team_data) > 1:
                opponent_stats_container = opponent_team_data[1]
                
                # Extract opponent points
                if 'team_points' in opponent_stats_container:
                    opponent_points_data = opponent_stats_container['team_points']
                    if isinstance(opponent_points_data, dict) and 'total' in opponent_points_data:
                        details['opponent_points'] = self._safe_int(opponent_points_data['total'])
                
                # Extract opponent game counts
                if 'team_remaining_games' in opponent_stats_container:
                    opponent_remaining_data = opponent_stats_container['team_remaining_games']
                    if isinstance(opponent_remaining_data, dict) and 'total' in opponent_remaining_data:
                        total_data = opponent_remaining_data['total']
                        if isinstance(total_data, dict):
                            details['opponent_completed_games'] = self._safe_int(total_data.get('completed_games', 0))
                            details['opponent_remaining_games'] = self._safe_int(total_data.get('remaining_games', 0))
                            details['opponent_live_games'] = self._safe_int(total_data.get('live_games', 0))
            
            # Determine win/loss
            if details['team_points'] > details['opponent_points']:
                details['is_winner'] = True
            elif details['team_points'] < details['opponent_points']:
                details['is_winner'] = False
            else:
                details['is_winner'] = None  # Tie
            
        except Exception as e:
            print(f"Error parsing teams matchup data: {e}")
        
        return details
    
    def _safe_bool_nullable(self, value: Any) -> Optional[bool]:
        """Safely convert value to boolean, allowing None"""
        if value is None:
            return None
        return self._safe_bool(value)


class CompleteMatchupLoader:
    """Complete matchup data loader that handles team matchups"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loader
        self.matchup_loader = TeamMatchupsLoader(connection_manager, batch_size)
    
    def load_matchup_data(self, matchup_data: List[Dict[str, Any]]) -> LoadResult:
        """Load team matchup data"""
        return self.matchup_loader.load(matchup_data)
    
    def load_team_matchups_from_api(self, team_key: str, league_key: str, season: str,
                                   matchups_api_data: Dict[str, Any]) -> LoadResult:
        """Load team matchups from Yahoo API matchups response"""
        processed_matchups = []
        
        try:
            fantasy_content = matchups_api_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team", [])
            
            # Find matchups container
            matchups_container = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "matchups" in item:
                        matchups_container = item["matchups"]
                        break
            
            if not matchups_container:
                print("No matchups container found in API data")
                return LoadResult()
            
            matchups_count = int(matchups_container.get("count", 0))
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_container = matchups_container[str_index]
                if "matchup" not in matchup_container:
                    continue
                
                matchup_info = matchup_container["matchup"]
                
                # Process matchup data
                matchup_record = {
                    'team_key': team_key,
                    'league_key': league_key,
                    'season': season,
                    'matchup_data': matchup_info
                }
                
                processed_matchups.append(matchup_record)
        
        except Exception as e:
            print(f"Error processing team matchups API data: {e}")
        
        return self.matchup_loader.load(processed_matchups) 