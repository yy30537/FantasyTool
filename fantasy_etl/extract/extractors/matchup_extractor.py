#!/usr/bin/env python3
"""
团队对战数据提取器
提取团队的每周对战数据和结果
对应旧脚本中的 _fetch_team_matchups(), _process_team_matchups_to_db()
"""

from typing import List, Dict, Any, Optional
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class MatchupExtractor(BaseExtractor):
    """团队对战数据提取器
    
    提取团队对战数据，包括：
    - 每周对战安排
    - 对战结果和得分
    - 统计类别获胜情况
    - 季后赛标记
    - 比赛场次统计
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化对战数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="MatchupExtractor",
            extractor_type=ExtractorType.OPERATIONAL
        )
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，包含：
                - team_keys: 团队键列表（必需）
                - league_key: 联盟键（必需）
                - season: 赛季
                - week: 特定周数（可选）
                
        Returns:
            List[Dict]: 提取的对战数据
        """
        team_keys = params.get('team_keys')
        league_key = params.get('league_key')
        
        if not team_keys or not league_key:
            raise ValueError("MatchupExtractor requires 'team_keys' and 'league_key' parameters")
        
        season = params.get('season', '2024')
        specific_week = params.get('week')
        
        return self._extract_all_matchups(team_keys, league_key, season, specific_week)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["TeamExtractor"]  # 依赖团队数据
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return True  # 对战数据会持续更新
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 12 * 3600  # 12小时更新一次
    
    def _extract_all_matchups(self, team_keys: List[str], league_key: str, season: str, specific_week: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        提取所有团队的对战数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            season: 赛季
            specific_week: 特定周数（如果提供，只提取该周的数据）
            
        Returns:
            List[Dict]: 所有对战数据
        """
        all_matchups = []
        
        for team_key in team_keys:
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    matchups_data = self._parse_matchups_response(response_data, team_key, league_key, season, specific_week)
                    all_matchups.extend(matchups_data)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract matchups for team {team_key}: {e}")
                continue
        
        return all_matchups
    
    def _parse_matchups_response(self, response_data: Dict, team_key: str, league_key: str, season: str, specific_week: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        解析对战API响应
        
        Args:
            response_data: API响应数据
            team_key: 团队键
            league_key: 联盟键
            season: 赛季
            specific_week: 特定周数过滤
            
        Returns:
            List[Dict]: 解析的对战数据
        """
        matchups = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 查找matchups容器
            matchups_container = None
            if isinstance(team_data, list):
                for item in team_data:
                    if isinstance(item, dict) and "matchups" in item:
                        matchups_container = item["matchups"]
                        break
            elif isinstance(team_data, dict) and "matchups" in team_data:
                matchups_container = team_data["matchups"]
            
            if not matchups_container:
                return matchups
            
            matchups_count = int(matchups_container.get("count", 0))
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_container = matchups_container[str_index]
                if "matchup" not in matchup_container:
                    continue
                
                matchup_data = self._parse_single_matchup(
                    matchup_container["matchup"], team_key, league_key, season, specific_week
                )
                if matchup_data:
                    matchups.append(matchup_data)
                    
        except Exception as e:
            self.logger.error(f"Failed to parse matchups response for team {team_key}: {e}")
        
        return matchups
    
    def _parse_single_matchup(self, matchup_info: List, team_key: str, league_key: str, season: str, specific_week: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        解析单个对战记录
        
        Args:
            matchup_info: 对战信息列表
            team_key: 团队键
            league_key: 联盟键
            season: 赛季
            specific_week: 特定周数过滤
            
        Returns:
            Dict: 标准化的对战数据，如果不符合条件则返回None
        """
        try:
            # 提取基本对战信息
            basic_info = None
            teams_info = None
            stat_winners = None
            
            for item in matchup_info:
                if isinstance(item, dict):
                    if "week" in item:
                        basic_info = item
                    elif "teams" in item:
                        teams_info = item["teams"]
                    elif "stat_winners" in item:
                        stat_winners = item["stat_winners"]
            
            if not basic_info:
                return None
            
            week = basic_info.get("week")
            if week is None:
                return None
            
            # 如果指定了特定周数，过滤掉其他周的数据
            if specific_week is not None and int(week) != specific_week:
                return None
            
            # 解析基本信息
            matchup_data = {
                "team_key": team_key,
                "league_key": league_key,
                "season": season,
                "week": int(week),
                "week_start": basic_info.get("week_start"),
                "week_end": basic_info.get("week_end"),
                "status": basic_info.get("status"),
                "is_playoffs": self._safe_bool(basic_info.get("is_playoffs", False)),
                "is_consolation": self._safe_bool(basic_info.get("is_consolation", False)),
                "is_matchup_of_week": self._safe_bool(basic_info.get("is_matchup_of_week", False)),
                "is_tied": self._safe_bool(basic_info.get("is_tied", False)),
                "winner_team_key": basic_info.get("winner_team_key")
            }
            
            # 解析统计类别获胜情况
            stat_wins = self._parse_stat_winners(stat_winners, team_key)
            matchup_data.update(stat_wins)
            
            # 解析团队对战详情
            team_details = self._parse_teams_data(teams_info, team_key)
            matchup_data.update(team_details)
            
            return matchup_data
            
        except Exception as e:
            self.logger.error(f"Failed to parse single matchup for team {team_key}: {e}")
            return None
    
    def _parse_stat_winners(self, stat_winners: List, team_key: str) -> Dict[str, bool]:
        """
        解析统计类别获胜情况
        
        Args:
            stat_winners: 统计获胜者列表
            team_key: 团队键
            
        Returns:
            Dict: 各统计类别获胜状态
        """
        wins = {
            "wins_field_goal_pct": False,      # stat_id: 5 - FG%
            "wins_free_throw_pct": False,      # stat_id: 8 - FT%
            "wins_three_pointers": False,      # stat_id: 10 - 3PTM
            "wins_points": False,              # stat_id: 12 - PTS
            "wins_rebounds": False,            # stat_id: 15 - REB
            "wins_assists": False,             # stat_id: 16 - AST
            "wins_steals": False,              # stat_id: 17 - ST
            "wins_blocks": False,              # stat_id: 18 - BLK
            "wins_turnovers": False            # stat_id: 19 - TO
        }
        
        if not stat_winners:
            return wins
        
        # 统计类别ID到字段名的映射
        stat_mapping = {
            "5": "wins_field_goal_pct",
            "8": "wins_free_throw_pct",
            "10": "wins_three_pointers",
            "12": "wins_points",
            "15": "wins_rebounds",
            "16": "wins_assists",
            "17": "wins_steals",
            "18": "wins_blocks",
            "19": "wins_turnovers"
        }
        
        try:
            for stat_winner in stat_winners:
                if isinstance(stat_winner, dict) and "stat_winner" in stat_winner:
                    stat_info = stat_winner["stat_winner"]
                    stat_id = str(stat_info.get("stat_id", ""))
                    winner_key = stat_info.get("winner_team_key", "")
                    
                    # 如果该团队是这个统计类别的获胜者
                    if stat_id in stat_mapping and winner_key == team_key:
                        wins[stat_mapping[stat_id]] = True
        
        except Exception as e:
            self.logger.error(f"Failed to parse stat winners: {e}")
        
        return wins
    
    def _parse_teams_data(self, teams_info: Dict, target_team_key: str) -> Dict[str, Any]:
        """
        解析团队对战数据
        
        Args:
            teams_info: 团队信息容器
            target_team_key: 目标团队键
            
        Returns:
            Dict: 团队对战详情
        """
        details = {
            "opponent_team_key": None,
            "is_winner": None,
            "team_points": 0,
            "opponent_points": 0,
            "completed_games": 0,
            "remaining_games": 0,
            "live_games": 0,
            "opponent_completed_games": 0,
            "opponent_remaining_games": 0,
            "opponent_live_games": 0
        }
        
        if not teams_info:
            return details
        
        try:
            teams_count = int(teams_info.get("count", 0))
            
            target_team_data = None
            opponent_team_data = None
            
            # 遍历所有团队，找到目标团队和对手团队
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_info:
                    continue
                
                team_container = teams_info[str_index]
                if "team" not in team_container:
                    continue
                
                team_info = team_container["team"]
                
                # 提取team_key
                current_team_key = self._extract_team_key_from_info(team_info)
                
                # 根据team_key分类
                if current_team_key == target_team_key:
                    target_team_data = team_info
                elif current_team_key:
                    details["opponent_team_key"] = current_team_key
                    opponent_team_data = team_info
            
            # 提取目标团队数据
            if target_team_data:
                team_stats = self._extract_team_stats_from_info(target_team_data)
                details.update({
                    "team_points": team_stats.get("points", 0),
                    "completed_games": team_stats.get("completed_games", 0),
                    "remaining_games": team_stats.get("remaining_games", 0),
                    "live_games": team_stats.get("live_games", 0)
                })
            
            # 提取对手团队数据
            if opponent_team_data:
                opponent_stats = self._extract_team_stats_from_info(opponent_team_data)
                details.update({
                    "opponent_points": opponent_stats.get("points", 0),
                    "opponent_completed_games": opponent_stats.get("completed_games", 0),
                    "opponent_remaining_games": opponent_stats.get("remaining_games", 0),
                    "opponent_live_games": opponent_stats.get("live_games", 0)
                })
            
            # 判断胜负
            if details["team_points"] > details["opponent_points"]:
                details["is_winner"] = True
            elif details["team_points"] < details["opponent_points"]:
                details["is_winner"] = False
            else:
                details["is_winner"] = None  # 平局
            
        except Exception as e:
            self.logger.error(f"Failed to parse teams data: {e}")
        
        return details
    
    def _extract_team_key_from_info(self, team_info: List) -> Optional[str]:
        """从团队信息中提取团队键"""
        try:
            if isinstance(team_info, list) and len(team_info) >= 1:
                team_basic_info = team_info[0]
                if isinstance(team_basic_info, list):
                    for item in team_basic_info:
                        if isinstance(item, dict) and "team_key" in item:
                            return item["team_key"]
                elif isinstance(team_basic_info, dict) and "team_key" in team_basic_info:
                    return team_basic_info["team_key"]
        except Exception:
            pass
        return None
    
    def _extract_team_stats_from_info(self, team_info: List) -> Dict[str, int]:
        """从团队信息中提取统计数据"""
        stats = {
            "points": 0,
            "completed_games": 0,
            "remaining_games": 0,
            "live_games": 0
        }
        
        try:
            if isinstance(team_info, list) and len(team_info) > 1:
                team_stats_container = team_info[1]
                
                # 提取team_points
                if "team_points" in team_stats_container:
                    team_points_data = team_stats_container["team_points"]
                    if isinstance(team_points_data, dict) and "total" in team_points_data:
                        stats["points"] = self._safe_int(team_points_data["total"]) or 0
                
                # 提取remaining_games
                if "team_remaining_games" in team_stats_container:
                    remaining_games_data = team_stats_container["team_remaining_games"]
                    if isinstance(remaining_games_data, dict) and "total" in remaining_games_data:
                        total_data = remaining_games_data["total"]
                        if isinstance(total_data, dict):
                            stats["completed_games"] = self._safe_int(total_data.get("completed_games", 0)) or 0
                            stats["remaining_games"] = self._safe_int(total_data.get("remaining_games", 0)) or 0
                            stats["live_games"] = self._safe_int(total_data.get("live_games", 0)) or 0
        
        except Exception as e:
            self.logger.error(f"Failed to extract team stats: {e}")
        
        return stats
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes')
        if isinstance(value, (int, float)):
            return value != 0
        return False
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        try:
            if value is None or value == '':
                return None
            return int(float(value))  # 先转float再转int，处理'1.0'格式
        except (ValueError, TypeError):
            return None
    
    def extract(self, team_keys: List[str], league_key: str, season: str = "2024", week: int = None, **kwargs) -> ExtractionResult:
        """
        提取指定团队的对战数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            season: 赛季
            week: 特定周数（可选）
            **kwargs: 其他参数
            
        Returns:
            ExtractionResult: 包含对战数据的提取结果
        """
        try:
            # 调用基类方法
            matchups_data = self._extract_data(
                team_keys=team_keys,
                league_key=league_key,
                season=season,
                week=week,
                **kwargs
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=matchups_data,
                total_records=len(matchups_data),
                message=f"Successfully extracted {len(matchups_data)} matchups for {len(team_keys)} teams"
            )
            
        except Exception as e:
            self.logger.error(f"MatchupExtractor failed for league {league_key}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_current_week_matchups(self, team_keys: List[str], league_key: str, season: str = "2024") -> ExtractionResult:
        """
        提取当前周的对战数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            season: 赛季
            
        Returns:
            ExtractionResult: 当前周对战数据的提取结果
        """
        # 这里可以扩展为自动检测当前周
        return self.extract(team_keys, league_key, season) 