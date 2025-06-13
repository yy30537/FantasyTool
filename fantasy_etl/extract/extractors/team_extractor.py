"""
Team Extractor - 团队数据提取器

负责提取团队相关的原始数据，包括团队信息、对战数据、排名等
"""
from typing import Dict, List, Optional
import logging

from .base_extractor import BaseExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class TeamExtractor(BaseExtractor):
    """团队数据提取器 - 负责提取团队相关的原始数据"""
    
    def extract_league_teams(self, league_key: str) -> ExtractionResult:
        """提取联盟所有团队数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 包含teams数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_teams")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的团队数据")
            
            endpoint = f"league/{league_key}/teams?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的团队数据",
                    metadata=self._build_metadata(
                        operation="extract_league_teams",
                        league_key=league_key
                    )
                )
            
            # 提取teams数据
            teams_data = self._extract_teams_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的 {len(teams_data)} 个团队")
            
            return ExtractionResult(
                success=True,
                data=teams_data,
                total_records=len(teams_data),
                metadata=self._build_metadata(
                    operation="extract_league_teams",
                    league_key=league_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 团队数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_teams",
                    league_key=league_key
                )
            )
    
    def extract_team_matchups(self, team_key: str) -> ExtractionResult:
        """提取团队对战数据
        
        Args:
            team_key: 团队键
            
        Returns:
            ExtractionResult: 包含matchups数据的提取结果
        """
        try:
            if not team_key:
                return ExtractionResult(
                    success=False,
                    error="team_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_team_matchups")
                )
            
            self.logger.info(f"开始提取团队 {team_key} 的对战数据")
            
            endpoint = f"team/{team_key}/matchups?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取团队 {team_key} 的对战数据",
                    metadata=self._build_metadata(
                        operation="extract_team_matchups",
                        team_key=team_key
                    )
                )
            
            # 提取matchups数据
            matchups_data = self._extract_matchups_from_response(response_data)
            
            self.logger.info(f"成功提取团队 {team_key} 的 {len(matchups_data)} 个对战记录")
            
            return ExtractionResult(
                success=True,
                data=matchups_data,
                total_records=len(matchups_data),
                metadata=self._build_metadata(
                    operation="extract_team_matchups",
                    team_key=team_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取团队 {team_key} 对战数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_team_matchups",
                    team_key=team_key
                )
            )
    
    def extract_league_standings(self, league_key: str) -> ExtractionResult:
        """提取联盟排名数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 包含standings数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_standings")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的排名数据")
            
            endpoint = f"league/{league_key}/standings?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的排名数据",
                    metadata=self._build_metadata(
                        operation="extract_league_standings",
                        league_key=league_key
                    )
                )
            
            # 提取standings数据
            standings_data = self._extract_standings_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的 {len(standings_data)} 个排名记录")
            
            return ExtractionResult(
                success=True,
                data=standings_data,
                total_records=len(standings_data),
                metadata=self._build_metadata(
                    operation="extract_league_standings",
                    league_key=league_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 排名数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_standings",
                    league_key=league_key
                )
            )
    
    def extract(self, operation: str, **kwargs) -> ExtractionResult:
        """通用提取方法"""
        operation_mapping = {
            "league_teams": self.extract_league_teams,
            "team_matchups": self.extract_team_matchups,
            "league_standings": self.extract_league_standings,
        }
        
        if operation not in operation_mapping:
            return ExtractionResult(
                success=False,
                error=f"不支持的操作: {operation}",
                metadata=self._build_metadata(operation=operation)
            )
        
        return operation_mapping[operation](**kwargs)
    
    def _extract_teams_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取团队数据"""
        teams = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找teams容器
            teams_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(league_data, dict) and "teams" in league_data:
                teams_container = league_data["teams"]
            
            if not teams_container:
                self.logger.warning("响应中未找到teams容器")
                return teams
            
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                team_info = self._process_team_data(team_data)
                
                if team_info:
                    teams.append(team_info)
        
        except Exception as e:
            self.logger.error(f"解析团队数据时出错: {str(e)}")
        
        return teams
    
    def _extract_matchups_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取对战数据"""
        matchups = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team", [])
            
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
                self.logger.warning("响应中未找到matchups容器")
                return matchups
            
            matchups_count = int(matchups_container.get("count", 0))
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_container = matchups_container[str_index]
                if "matchup" not in matchup_container:
                    continue
                
                matchup_data = matchup_container["matchup"]
                matchup_info = self._process_matchup_data(matchup_data)
                
                if matchup_info:
                    matchups.append(matchup_info)
        
        except Exception as e:
            self.logger.error(f"解析对战数据时出错: {str(e)}")
        
        return matchups
    
    def _extract_standings_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取排名数据"""
        standings = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找standings容器
            standings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "standings" in item:
                        standings_container = item["standings"]
                        break
            elif isinstance(league_data, dict) and "standings" in league_data:
                standings_container = league_data["standings"]
            
            if not standings_container:
                self.logger.warning("响应中未找到standings容器")
                return standings
            
            # 查找teams容器
            teams_container = None
            if isinstance(standings_container, list):
                for item in standings_container:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(standings_container, dict) and "teams" in standings_container:
                teams_container = standings_container["teams"]
            
            if not teams_container:
                self.logger.warning("standings中未找到teams容器")
                return standings
            
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                standing_info = self._process_standing_data(team_data)
                
                if standing_info:
                    standings.append(standing_info)
        
        except Exception as e:
            self.logger.error(f"解析排名数据时出错: {str(e)}")
        
        return standings
    
    def _process_team_data(self, team_data: List) -> Optional[Dict]:
        """处理团队数据，提取关键信息"""
        try:
            if not isinstance(team_data, list) or len(team_data) == 0:
                return None
            
            team_info_list = team_data[0]
            if not isinstance(team_info_list, list):
                return None
            
            team_dict = {}
            managers_data = []
            
            # 处理team_data的第一层
            for item in team_info_list:
                if isinstance(item, dict):
                    if "managers" in item:
                        managers_data = item["managers"]
                    elif "team_logos" in item and item["team_logos"]:
                        if len(item["team_logos"]) > 0 and "team_logo" in item["team_logos"][0]:
                            team_dict["team_logo_url"] = item["team_logos"][0]["team_logo"].get("url")
                    elif "roster_adds" in item:
                        roster_adds = item["roster_adds"]
                        team_dict["roster_adds_week"] = roster_adds.get("coverage_value")
                        team_dict["roster_adds_value"] = roster_adds.get("value")
                    else:
                        team_dict.update(item)
            
            # 添加managers数据
            team_dict["managers"] = managers_data
            
            # 验证必要字段
            if not team_dict.get("team_key"):
                return None
            
            return team_dict
            
        except Exception as e:
            self.logger.error(f"处理团队数据时出错: {str(e)}")
            return None
    
    def _process_matchup_data(self, matchup_data) -> Optional[Dict]:
        """处理对战数据，提取关键信息"""
        try:
            matchup_dict = {}
            
            # 处理matchup数据结构
            if isinstance(matchup_data, list):
                for item in matchup_data:
                    if isinstance(item, dict):
                        matchup_dict.update(item)
            elif isinstance(matchup_data, dict):
                matchup_dict.update(matchup_data)
            
            return matchup_dict
            
        except Exception as e:
            self.logger.error(f"处理对战数据时出错: {str(e)}")
            return None
    
    def _process_standing_data(self, team_data) -> Optional[Dict]:
        """处理排名数据，提取关键信息"""
        try:
            standing_dict = {}
            
            # 从复杂的team_data结构中提取信息
            def extract_from_nested(data, target_key):
                if isinstance(data, dict):
                    if target_key in data:
                        return data[target_key]
                    for value in data.values():
                        result = extract_from_nested(value, target_key)
                        if result is not None:
                            return result
                elif isinstance(data, list):
                    for item in data:
                        result = extract_from_nested(item, target_key)
                        if result is not None:
                            return result
                return None
            
            # 提取关键字段
            standing_dict["team_key"] = extract_from_nested(team_data, "team_key")
            standing_dict["team_standings"] = extract_from_nested(team_data, "team_standings")
            standing_dict["team_points"] = extract_from_nested(team_data, "team_points")
            
            if not standing_dict["team_key"]:
                return None
            
            return standing_dict
            
        except Exception as e:
            self.logger.error(f"处理排名数据时出错: {str(e)}")
            return None 