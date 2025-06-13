"""
Roster Extractor - 阵容数据提取器

负责提取团队阵容相关的原始数据，包括当前阵容和历史阵容数据
"""
from typing import Dict, List, Optional
import logging
from datetime import date

from .base_extractor import BaseExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class RosterExtractor(BaseExtractor):
    """阵容数据提取器 - 负责提取阵容相关的原始数据"""
    
    def extract_team_roster(self, team_key: str, date_str: Optional[str] = None) -> ExtractionResult:
        """提取团队阵容数据
        
        Args:
            team_key: 团队键
            date_str: 日期字符串 (YYYY-MM-DD)，None表示当前阵容
            
        Returns:
            ExtractionResult: 包含roster数据的提取结果
        """
        try:
            if not team_key:
                return ExtractionResult(
                    success=False,
                    error="team_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_team_roster")
                )
            
            date_info = f" ({date_str})" if date_str else " (当前)"
            self.logger.info(f"开始提取团队 {team_key} 的阵容数据{date_info}")
            
            # 构建端点
            endpoint = f"team/{team_key}/roster"
            if date_str:
                endpoint += f";date={date_str}"
            endpoint += "?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取团队 {team_key} 的阵容数据",
                    metadata=self._build_metadata(
                        operation="extract_team_roster",
                        team_key=team_key,
                        date_str=date_str
                    )
                )
            
            # 提取roster数据
            roster_data = self._extract_roster_from_response(response_data)
            
            self.logger.info(f"成功提取团队 {team_key} 的 {len(roster_data.get('players', []))} 个球员阵容数据{date_info}")
            
            return ExtractionResult(
                success=True,
                data=roster_data,
                total_records=len(roster_data.get('players', [])),
                metadata=self._build_metadata(
                    operation="extract_team_roster",
                    team_key=team_key,
                    date_str=date_str,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取团队 {team_key} 阵容数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_team_roster",
                    team_key=team_key,
                    date_str=date_str
                )
            )
    
    def extract_multiple_team_rosters(self, team_keys: List[str], date_str: Optional[str] = None) -> ExtractionResult:
        """提取多个团队的阵容数据
        
        Args:
            team_keys: 团队键列表
            date_str: 日期字符串 (YYYY-MM-DD)，None表示当前阵容
            
        Returns:
            ExtractionResult: 包含所有teams roster数据的提取结果
        """
        try:
            if not team_keys:
                return ExtractionResult(
                    success=False,
                    error="team_keys参数不能为空",
                    metadata=self._build_metadata(operation="extract_multiple_team_rosters")
                )
            
            date_info = f" ({date_str})" if date_str else " (当前)"
            self.logger.info(f"开始提取 {len(team_keys)} 个团队的阵容数据{date_info}")
            
            all_rosters = {}
            total_players = 0
            failed_teams = []
            
            for team_key in team_keys:
                try:
                    result = self.extract_team_roster(team_key, date_str)
                    if result.success and result.data:
                        all_rosters[team_key] = result.data
                        total_players += len(result.data.get('players', []))
                    else:
                        failed_teams.append(team_key)
                        self.logger.warning(f"团队 {team_key} 阵容数据提取失败: {result.error}")
                        
                except Exception as e:
                    failed_teams.append(team_key)
                    self.logger.error(f"团队 {team_key} 阵容数据提取异常: {str(e)}")
            
            success = len(all_rosters) > 0
            
            self.logger.info(f"多团队阵容数据提取完成: 成功 {len(all_rosters)}/{len(team_keys)}, 总球员数 {total_players}")
            
            return ExtractionResult(
                success=success,
                data=all_rosters,
                total_records=total_players,
                metadata=self._build_metadata(
                    operation="extract_multiple_team_rosters",
                    team_count=len(team_keys),
                    success_count=len(all_rosters),
                    failed_teams=failed_teams,
                    date_str=date_str
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取多团队阵容数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_multiple_team_rosters",
                    team_count=len(team_keys) if team_keys else 0,
                    date_str=date_str
                )
            )
    
    def extract(self, operation: str, **kwargs) -> ExtractionResult:
        """通用提取方法"""
        operation_mapping = {
            "team_roster": self.extract_team_roster,
            "multiple_team_rosters": self.extract_multiple_team_rosters,
        }
        
        if operation not in operation_mapping:
            return ExtractionResult(
                success=False,
                error=f"不支持的操作: {operation}",
                metadata=self._build_metadata(operation=operation)
            )
        
        return operation_mapping[operation](**kwargs)
    
    def _extract_roster_from_response(self, response_data: Dict) -> Dict:
        """从API响应中提取阵容数据"""
        roster_info = {
            "players": [],
            "roster_metadata": {}
        }
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team", [])
            
            # 查找roster容器
            roster_container = None
            if isinstance(team_data, list):
                for item in team_data:
                    if isinstance(item, dict) and "roster" in item:
                        roster_container = item["roster"]
                        break
            elif isinstance(team_data, dict) and "roster" in team_data:
                roster_container = team_data["roster"]
            
            if not roster_container:
                self.logger.warning("响应中未找到roster容器")
                return roster_info
            
            # 提取阵容元数据
            roster_info["roster_metadata"] = {
                "date": roster_container.get("date"),
                "is_prescoring": roster_container.get("is_prescoring", False),
                "is_editable": roster_container.get("is_editable", False),
            }
            
            # 查找players容器
            players_container = None
            if "0" in roster_container and "players" in roster_container["0"]:
                players_container = roster_container["0"]["players"]
            
            if not players_container:
                self.logger.warning("roster中未找到players容器")
                return roster_info
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                if "player" not in player_container:
                    continue
                
                player_data = player_container["player"]
                player_info = self._process_roster_player_data(player_data)
                
                if player_info:
                    roster_info["players"].append(player_info)
        
        except Exception as e:
            self.logger.error(f"解析阵容数据时出错: {str(e)}")
        
        return roster_info
    
    def _process_roster_player_data(self, player_data: List) -> Optional[Dict]:
        """处理阵容中的球员数据，提取关键信息"""
        try:
            if not isinstance(player_data, list) or len(player_data) == 0:
                return None
            
            # player_data[0] 包含球员基本信息
            player_basic_info = player_data[0]
            # player_data[1] 包含位置和状态信息
            position_data = player_data[1] if len(player_data) > 1 else {}
            
            player_dict = {}
            
            # 处理基本信息
            if isinstance(player_basic_info, list):
                for item in player_basic_info:
                    if isinstance(item, dict):
                        player_dict.update(item)
            elif isinstance(player_basic_info, dict):
                player_dict.update(player_basic_info)
            
            # 处理位置信息
            if isinstance(position_data, dict):
                player_dict.update(position_data)
            
            # 验证必要字段
            if not player_dict.get("player_key"):
                return None
            
            return player_dict
            
        except Exception as e:
            self.logger.error(f"处理阵容球员数据时出错: {str(e)}")
            return None 