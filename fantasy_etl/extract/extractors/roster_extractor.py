#!/usr/bin/env python3
"""
名单数据提取器
提取团队的球员名单和位置分配数据
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class RosterExtractor(BaseExtractor):
    """名单数据提取器
    
    提取团队名单数据，包括：
    - 球员分配到的位置
    - 首发/替补状态
    - 伤病名单状态
    - Keeper状态和成本
    - 球员状态信息
    
    支持单日和日期范围的名单数据提取
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化名单数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="RosterExtractor",
            extractor_type=ExtractorType.OPERATIONAL
        )
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，包含：
                - team_keys: 团队键列表（必需）
                - league_key: 联盟键（必需）
                - date: 单日期（可选）
                - start_date: 开始日期（可选）
                - end_date: 结束日期（可选）
                - season: 赛季
                
        Returns:
            List[Dict]: 提取的名单数据
        """
        team_keys = params.get('team_keys')
        league_key = params.get('league_key')
        
        if not team_keys or not league_key:
            raise ValueError("RosterExtractor requires 'team_keys' and 'league_key' parameters")
        
        date_obj = params.get('date')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        season = params.get('season', '2024')
        
        # 如果指定了单个日期，则提取该日期的名单
        if date_obj:
            return self._extract_roster_for_date(team_keys, league_key, date_obj, season)
        
        # 如果指定了日期范围，则提取范围内的名单
        if start_date and end_date:
            return self._extract_roster_for_date_range(team_keys, league_key, start_date, end_date, season)
        
        # 否则提取当前名单（不指定日期）
        return self._extract_current_roster(team_keys, league_key, season)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["TeamExtractor", "PlayerExtractor"]  # 依赖团队和球员数据
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return True  # 名单数据经常变化
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 24 * 3600  # 每天更新一次
    
    def _extract_current_roster(self, team_keys: List[str], league_key: str, season: str) -> List[Dict[str, Any]]:
        """
        提取当前名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            season: 赛季
            
        Returns:
            List[Dict]: 当前名单数据
        """
        all_roster_data = []
        current_date = date.today()
        
        for team_key in team_keys:
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    roster_data = self._parse_roster_response(response_data, team_key, league_key, current_date, season)
                    all_roster_data.extend(roster_data)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract current roster for team {team_key}: {e}")
                continue
        
        return all_roster_data
    
    def _extract_roster_for_date(self, team_keys: List[str], league_key: str, date_obj: date, season: str) -> List[Dict[str, Any]]:
        """
        提取指定日期的名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            date_obj: 指定日期
            season: 赛季
            
        Returns:
            List[Dict]: 指定日期的名单数据
        """
        all_roster_data = []
        date_str = date_obj.strftime('%Y-%m-%d')
        
        for team_key in team_keys:
            try:
                # 构建API URL，包含日期参数
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    roster_data = self._parse_roster_response(response_data, team_key, league_key, date_obj, season)
                    all_roster_data.extend(roster_data)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract roster for team {team_key} on {date_str}: {e}")
                continue
        
        return all_roster_data
    
    def _extract_roster_for_date_range(self, team_keys: List[str], league_key: str, 
                                     start_date: date, end_date: date, season: str) -> List[Dict[str, Any]]:
        """
        提取日期范围内的名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            start_date: 开始日期
            end_date: 结束日期
            season: 赛季
            
        Returns:
            List[Dict]: 日期范围内的名单数据
        """
        all_roster_data = []
        
        # 生成日期列表
        current_date = start_date
        while current_date <= end_date:
            try:
                daily_roster_data = self._extract_roster_for_date(team_keys, league_key, current_date, season)
                all_roster_data.extend(daily_roster_data)
                
                # 移动到下一天
                from datetime import timedelta
                current_date += timedelta(days=1)
                
            except Exception as e:
                self.logger.error(f"Failed to extract roster for date {current_date}: {e}")
                # 继续处理下一天
                from datetime import timedelta
                current_date += timedelta(days=1)
                continue
        
        return all_roster_data
    
    def _parse_roster_response(self, response_data: Dict, team_key: str, league_key: str, 
                             date_obj: date, season: str) -> List[Dict[str, Any]]:
        """
        解析名单API响应
        
        Args:
            response_data: API响应数据
            team_key: 团队键
            league_key: 联盟键
            date_obj: 日期
            season: 赛季
            
        Returns:
            List[Dict]: 解析后的名单数据
        """
        roster_data = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
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
                self.logger.warning(f"No roster found for team {team_key}")
                return roster_data
            
            # 提取第一个roster对象
            roster_info = None
            if isinstance(roster_container, list) and len(roster_container) > 0:
                roster_info = roster_container[0]
            elif isinstance(roster_container, dict):
                roster_info = roster_container
            
            if not roster_info:
                self.logger.warning(f"No roster info found for team {team_key}")
                return roster_data
            
            # 提取players容器
            players_container = roster_info.get("players")
            if not players_container:
                self.logger.warning(f"No players found in roster for team {team_key}")
                return roster_data
            
            players_count = int(players_container.get("count", 0))
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                if "player" not in player_container:
                    continue
                
                player_data = self._parse_roster_player_data(player_container["player"], team_key, league_key, date_obj, season)
                if player_data:
                    roster_data.append(player_data)
                    
        except Exception as e:
            self.logger.error(f"Failed to parse roster response for team {team_key}: {e}")
        
        return roster_data
    
    def _parse_roster_player_data(self, player_info: List, team_key: str, league_key: str, 
                                date_obj: date, season: str) -> Optional[Dict[str, Any]]:
        """
        解析名单中的球员数据
        
        Args:
            player_info: 球员信息列表
            team_key: 团队键
            league_key: 联盟键
            date_obj: 日期
            season: 赛季
            
        Returns:
            Dict: 球员名单数据，如果解析失败则返回None
        """
        try:
            if not isinstance(player_info, list) or len(player_info) < 2:
                return None
            
            # 提取球员基本信息（第一部分）
            player_basic_info = player_info[0]
            player_key, player_id = self._extract_player_basic_info(player_basic_info)
            
            if not player_key:
                return None
            
            # 提取球员名单信息（第二部分）
            player_roster_info = player_info[1]
            roster_details = self._extract_roster_details(player_roster_info)
            
            # 组装名单数据
            roster_player_data = {
                "team_key": team_key,
                "player_key": player_key,
                "player_id": player_id,
                "league_key": league_key,
                "date": date_obj,
                "season": season,
                **roster_details
            }
            
            return roster_player_data
            
        except Exception as e:
            self.logger.error(f"Failed to parse roster player data: {e}")
            return None
    
    def _extract_player_basic_info(self, player_basic_info) -> tuple:
        """提取球员基本信息"""
        player_key = None
        player_id = None
        
        try:
            if isinstance(player_basic_info, list):
                for item in player_basic_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_key = item["player_key"]
                        elif "player_id" in item:
                            player_id = item["player_id"]
            elif isinstance(player_basic_info, dict):
                player_key = player_basic_info.get("player_key")
                player_id = player_basic_info.get("player_id")
        except Exception:
            pass
        
        return player_key, player_id
    
    def _extract_roster_details(self, player_roster_info: Dict) -> Dict[str, Any]:
        """提取名单详细信息"""
        roster_details = {}
        
        try:
            # 提取selected_position信息
            selected_position_data = player_roster_info.get("selected_position")
            if selected_position_data:
                roster_details["selected_position"] = self._extract_position_string(selected_position_data)
                
                # 判断位置类型
                position = roster_details["selected_position"]
                if position:
                    roster_details["is_starting"] = not position.upper().startswith(('BN', 'BENCH'))
                    roster_details["is_bench"] = position.upper().startswith(('BN', 'BENCH'))
                    roster_details["is_injured_reserve"] = position.upper().startswith(('IL', 'IR'))
                else:
                    roster_details["is_starting"] = False
                    roster_details["is_bench"] = False
                    roster_details["is_injured_reserve"] = False
            
            # 提取球员状态信息
            if "status" in player_roster_info:
                roster_details["player_status"] = player_roster_info["status"]
            
            if "status_full" in player_roster_info:
                roster_details["status_full"] = player_roster_info["status_full"]
            
            if "injury_note" in player_roster_info:
                roster_details["injury_note"] = player_roster_info["injury_note"]
            
            # 提取Fantasy相关信息
            roster_details["is_keeper"] = self._safe_bool(player_roster_info.get("is_keeper", False))
            
            if "keeper_cost" in player_roster_info:
                roster_details["keeper_cost"] = str(player_roster_info["keeper_cost"])
            
            roster_details["is_prescoring"] = self._safe_bool(player_roster_info.get("is_prescoring", False))
            roster_details["is_editable"] = self._safe_bool(player_roster_info.get("is_editable", True))
            
            # 提取周信息（如果有）
            if "week" in player_roster_info:
                roster_details["week"] = self._safe_int(player_roster_info["week"])
                
        except Exception as e:
            self.logger.error(f"Failed to extract roster details: {e}")
        
        return roster_details
    
    def _extract_position_string(self, position_data) -> Optional[str]:
        """从位置数据中提取位置字符串"""
        if not position_data:
            return None
        
        if isinstance(position_data, str):
            return position_data
        
        if isinstance(position_data, dict):
            return position_data.get("position", None)
        
        if isinstance(position_data, list) and len(position_data) > 0:
            if isinstance(position_data[0], str):
                return position_data[0]
            elif isinstance(position_data[0], dict):
                return position_data[0].get("position", None)
        
        return None
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        try:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in ('1', 'true', 'yes')
            if isinstance(value, (int, float)):
                return bool(value)
            return False
        except:
            return False
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        try:
            if value is None or value == '':
                return None
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def extract_current_roster(self, team_keys: List[str], league_key: str, season: str = "2024") -> ExtractionResult:
        """
        提取当前名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            season: 赛季
            
        Returns:
            ExtractionResult: 包含当前名单数据的提取结果
        """
        try:
            roster_data = self._extract_data(
                team_keys=team_keys,
                league_key=league_key,
                season=season
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=roster_data,
                total_records=len(roster_data),
                message=f"Successfully extracted current roster for {len(team_keys)} teams"
            )
            
        except Exception as e:
            self.logger.error(f"RosterExtractor current roster failed: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_roster_for_date(self, team_keys: List[str], league_key: str, date_obj: date, season: str = "2024") -> ExtractionResult:
        """
        提取指定日期的名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            date_obj: 指定日期
            season: 赛季
            
        Returns:
            ExtractionResult: 包含指定日期名单数据的提取结果
        """
        try:
            roster_data = self._extract_data(
                team_keys=team_keys,
                league_key=league_key,
                date=date_obj,
                season=season
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=roster_data,
                total_records=len(roster_data),
                message=f"Successfully extracted roster for {len(team_keys)} teams on {date_obj}"
            )
            
        except Exception as e:
            self.logger.error(f"RosterExtractor failed for date {date_obj}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_roster_for_date_range(self, team_keys: List[str], league_key: str, 
                                    start_date: date, end_date: date, season: str = "2024") -> ExtractionResult:
        """
        提取日期范围内的名单数据
        
        Args:
            team_keys: 团队键列表
            league_key: 联盟键
            start_date: 开始日期
            end_date: 结束日期
            season: 赛季
            
        Returns:
            ExtractionResult: 包含日期范围内名单数据的提取结果
        """
        try:
            roster_data = self._extract_data(
                team_keys=team_keys,
                league_key=league_key,
                start_date=start_date,
                end_date=end_date,
                season=season
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=roster_data,
                total_records=len(roster_data),
                message=f"Successfully extracted roster for {len(team_keys)} teams from {start_date} to {end_date}"
            )
            
        except Exception as e:
            self.logger.error(f"RosterExtractor failed for date range {start_date} to {end_date}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )