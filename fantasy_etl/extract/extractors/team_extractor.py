"""
Team Data Extractor - 团队数据提取器
=================================

提取团队信息和管理员信息，对应数据库teams和managers表
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import TeamData, ExtractionResult
from ..yahoo_client import YahooAPIClient

logger = logging.getLogger(__name__)


class TeamExtractor(BaseExtractor):
    """
    团队数据提取器
    
    功能：
    - 提取指定联盟的所有团队信息
    - 提取团队管理员信息
    - 数据写入teams和managers表
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化团队数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="TeamExtractor",
            extractor_type=ExtractorType.CORE_ENTITY
        )
    
    def extract(self, league_key: str, **kwargs) -> ExtractionResult:
        """
        提取团队数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 提取结果，包含团队数据列表
        """
        try:
            logger.info(f"开始提取联盟 {league_key} 的团队数据...")
            
            # 构建API URL
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                return ExtractionResult(
                    success=False,
                    data=[],
                    error_message="Failed to retrieve teams data from API",
                    total_count=0
                )
            
            # 解析团队数据
            teams_data = self._parse_teams_response(response_data, league_key)
            
            # 验证和标准化数据
            validated_teams = []
            for team_data in teams_data:
                try:
                    team_model = TeamData(**team_data)
                    validated_teams.append(team_model)
                except Exception as e:
                    logger.warning(f"团队数据验证失败: {team_data.get('team_key', 'unknown')}, 错误: {e}")
                    continue
            
            logger.info(f"成功提取 {len(validated_teams)} 个团队数据")
            
            return ExtractionResult(
                success=True,
                data=validated_teams,
                total_count=len(validated_teams),
                extraction_time=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"团队数据提取失败: {e}")
            return ExtractionResult(
                success=False,
                data=[],
                error_message=str(e),
                total_count=0
            )
    
    def _parse_teams_response(self, response_data: Dict, league_key: str) -> List[Dict[str, Any]]:
        """
        解析Yahoo API返回的团队数据
        
        Args:
            response_data: API响应数据
            league_key: 联盟键
            
        Returns:
            List[Dict]: 标准化的团队数据列表
        """
        teams_list = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找teams容器
            teams_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            
            if not teams_container:
                logger.warning("API响应中未找到teams数据")
                return teams_list
            
            teams_count = int(teams_container.get("count", 0))
            logger.info(f"发现 {teams_count} 个团队")
            
            # 提取每个团队的信息
            for i in range(teams_count):
                team_index = str(i)
                if team_index not in teams_container:
                    continue
                
                team_container = teams_container[team_index]
                if "team" not in team_container:
                    continue
                
                team_data_list = team_container["team"]
                if not isinstance(team_data_list, list) or len(team_data_list) == 0:
                    continue
                
                # 解析团队数据
                team_info = self._extract_team_info(team_data_list, league_key)
                if team_info:
                    teams_list.append(team_info)
            
        except Exception as e:
            logger.error(f"解析团队数据失败: {e}")
        
        return teams_list
    
    def _extract_team_info(self, team_data_list: List, league_key: str) -> Optional[Dict[str, Any]]:
        """
        从team数据列表中提取团队信息
        
        Args:
            team_data_list: 团队数据列表
            league_key: 联盟键
            
        Returns:
            Optional[Dict]: 标准化的团队数据，失败返回None
        """
        try:
            # team_data_list[0] 应该包含团队基本信息的列表
            if len(team_data_list) == 0 or not isinstance(team_data_list[0], list):
                return None
            
            team_basic_info_list = team_data_list[0]
            team_info = {"league_key": league_key}
            managers_data = []
            
            # 合并团队基本信息
            for item in team_basic_info_list:
                if isinstance(item, dict):
                    if "managers" in item:
                        managers_data = item["managers"]
                    elif "team_logos" in item and item["team_logos"]:
                        # 处理team logo
                        if len(item["team_logos"]) > 0 and "team_logo" in item["team_logos"][0]:
                            team_info["team_logo_url"] = item["team_logos"][0]["team_logo"].get("url")
                    elif "roster_adds" in item:
                        # 处理roster adds
                        roster_adds = item["roster_adds"]
                        team_info["roster_adds_week"] = roster_adds.get("coverage_value")
                        team_info["roster_adds_value"] = roster_adds.get("value")
                    else:
                        # 直接更新其他字段
                        team_info.update(item)
            
            # 验证必要字段
            if not team_info.get("team_key"):
                return None
            
            # 标准化团队数据
            standardized_team = self._standardize_team_data(team_info, managers_data)
            return standardized_team
            
        except Exception as e:
            logger.warning(f"提取团队信息失败: {e}")
            return None
    
    def _standardize_team_data(self, team_info: Dict, managers_data: List) -> Dict[str, Any]:
        """
        标准化团队数据
        
        Args:
            team_info: 原始团队信息
            managers_data: 管理员数据列表
            
        Returns:
            Dict: 标准化的团队数据
        """
        try:
            # 构建标准化数据
            standardized = {
                "team_key": team_info.get("team_key", ""),
                "team_id": team_info.get("team_id", ""),
                "league_key": team_info.get("league_key", ""),
                "name": team_info.get("name", ""),
                "url": team_info.get("url", ""),
                "team_logo_url": team_info.get("team_logo_url", ""),
                "division_id": team_info.get("division_id", ""),
                "waiver_priority": self._safe_int(team_info.get("waiver_priority")),
                "faab_balance": team_info.get("faab_balance", ""),
                "number_of_moves": self._safe_int(team_info.get("number_of_moves")),
                "number_of_trades": self._safe_int(team_info.get("number_of_trades")),
                "roster_adds_week": team_info.get("roster_adds_week", ""),
                "roster_adds_value": team_info.get("roster_adds_value", ""),
                "clinched_playoffs": self._safe_bool(team_info.get("clinched_playoffs")),
                "has_draft_grade": self._safe_bool(team_info.get("has_draft_grade")),
                "managers": self._parse_managers_data(managers_data)
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"标准化团队数据失败: {e}")
            return {}
    
    def _parse_managers_data(self, managers_data: List) -> List[Dict[str, Any]]:
        """
        解析管理员数据
        
        Args:
            managers_data: 原始管理员数据列表
            
        Returns:
            List[Dict]: 标准化的管理员数据列表
        """
        managers_list = []
        
        try:
            if not isinstance(managers_data, list):
                return managers_list
            
            for manager_item in managers_data:
                if not isinstance(manager_item, dict) or "manager" not in manager_item:
                    continue
                
                manager_info = manager_item["manager"]
                if not isinstance(manager_info, dict):
                    continue
                
                # 标准化管理员数据
                standardized_manager = {
                    "manager_id": manager_info.get("manager_id", ""),
                    "nickname": manager_info.get("nickname", ""),
                    "guid": manager_info.get("guid", ""),
                    "is_commissioner": self._safe_bool(manager_info.get("is_commissioner")),
                    "email": manager_info.get("email", ""),
                    "image_url": manager_info.get("image_url", ""),
                    "felo_score": manager_info.get("felo_score", ""),
                    "felo_tier": manager_info.get("felo_tier", "")
                }
                
                managers_list.append(standardized_manager)
                
        except Exception as e:
            logger.warning(f"解析管理员数据失败: {e}")
        
        return managers_list
    
    def _safe_bool(self, value: Any) -> bool:
        """安全地转换布尔值"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ['true', '1', 'yes']
        if isinstance(value, (int, float)):
            return value != 0
        return False
    
    def _safe_int(self, value: Any) -> int:
        """安全地转换整数值"""
        if value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数
            
        Returns:
            List[Dict]: 提取的团队数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("league_key is required for team extraction")
        
        # 构建API URL
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        
        # 调用API
        response_data = self.client.get(url)
        if not response_data:
            raise ValueError("Failed to retrieve teams data from API")
        
        # 解析团队数据
        return self._parse_teams_response(response_data, league_key)
    

    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["LeagueExtractor"]  # 依赖联盟数据
    
    def supports_incremental_update(self) -> bool:
        """
        是否支持增量更新
        
        Returns:
            bool: 团队数据支持增量更新
        """
        return True