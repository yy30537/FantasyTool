"""
Player Data Extractor - 球员数据提取器
===================================

提取球员基本信息和位置资格，对应数据库players和player_eligible_positions表
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from ..base_extractor import BaseExtractor, ExtractorType, PaginatedExtractor
from ..api_models import PlayerData, ExtractionResult
from ..yahoo_client import YahooAPIClient

logger = logging.getLogger(__name__)


class PlayerExtractor(PaginatedExtractor):
    """
    球员数据提取器
    
    功能：
    - 提取指定联盟的所有球员信息
    - 提取球员位置资格信息
    - 支持分页获取数据
    - 数据写入players和player_eligible_positions表
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化球员数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="PlayerExtractor",
            extractor_type=ExtractorType.CORE_ENTITY,
            page_size=25  # Yahoo API默认分页大小
        )
    
    def extract(self, league_key: str, **kwargs) -> ExtractionResult:
        """
        提取球员数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 提取结果，包含球员数据列表
        """
        try:
            logger.info(f"开始提取联盟 {league_key} 的球员数据...")
            
            # 使用分页方式获取所有球员数据
            all_players = []
            start = 0
            max_iterations = 100  # 防止无限循环
            iteration = 0
            
            while iteration < max_iterations:
                iteration += 1
                
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
                if start > 0:
                    url += f";start={start}"
                url += "?format=json"
                
                logger.info(f"获取第 {iteration} 批球员数据，起始位置: {start}")
                
                # 调用API
                response_data = self.client.get(url)
                if not response_data:
                    logger.warning(f"第 {iteration} 批球员数据获取失败")
                    break
                
                # 解析球员数据
                batch_players = self._parse_players_response(response_data, league_key)
                if not batch_players:
                    logger.info("没有更多球员数据")
                    break
                
                all_players.extend(batch_players)
                logger.info(f"第 {iteration} 批获取到 {len(batch_players)} 个球员")
                
                # 如果这批数据少于页面大小，说明已经是最后一页
                if len(batch_players) < self.page_size:
                    logger.info("已到达最后一页")
                    break
                
                start += self.page_size
                
                # 避免API限制，短暂等待
                self.client.rate_limiter.wait()
            
            # 验证和标准化数据
            validated_players = []
            for player_data in all_players:
                try:
                    player_model = PlayerData(**player_data)
                    validated_players.append(player_model)
                except Exception as e:
                    logger.warning(f"球员数据验证失败: {player_data.get('player_key', 'unknown')}, 错误: {e}")
                    continue
            
            logger.info(f"成功提取 {len(validated_players)} 个球员数据")
            
            return ExtractionResult(
                success=True,
                data=validated_players,
                total_count=len(validated_players),
                extraction_time=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"球员数据提取失败: {e}")
            return ExtractionResult(
                success=False,
                data=[],
                error_message=str(e),
                total_count=0
            )
    
    def _parse_players_response(self, response_data: Dict, league_key: str) -> List[Dict[str, Any]]:
        """
        解析Yahoo API返回的球员数据
        
        Args:
            response_data: API响应数据
            league_key: 联盟键
            
        Returns:
            List[Dict]: 标准化的球员数据列表
        """
        players_list = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                logger.warning("API响应中未找到players数据")
                return players_list
            
            players_count = int(players_container.get("count", 0))
            logger.debug(f"本批次发现 {players_count} 个球员")
            
            # 提取每个球员的信息
            for i in range(players_count):
                player_index = str(i)
                if player_index not in players_container:
                    continue
                
                player_container = players_container[player_index]
                if "player" not in player_container:
                    continue
                
                player_data_list = player_container["player"]
                if not isinstance(player_data_list, list) or len(player_data_list) == 0:
                    continue
                
                # 解析球员数据
                player_info = self._extract_player_info(player_data_list, league_key)
                if player_info:
                    players_list.append(player_info)
            
        except Exception as e:
            logger.error(f"解析球员数据失败: {e}")
        
        return players_list
    
    def _extract_player_info(self, player_data_list: List, league_key: str) -> Optional[Dict[str, Any]]:
        """
        从player数据列表中提取球员信息
        
        Args:
            player_data_list: 球员数据列表
            league_key: 联盟键
            
        Returns:
            Optional[Dict]: 标准化的球员数据，失败返回None
        """
        try:
            if len(player_data_list) == 0:
                return None
            
            # player_data_list[0] 包含球员基本信息
            player_basic_info = player_data_list[0]
            player_info = {"league_key": league_key}
            
            # 处理不同格式的球员基本信息
            if isinstance(player_basic_info, list):
                # 如果是列表，合并所有字典
                for item in player_basic_info:
                    if isinstance(item, dict):
                        player_info.update(item)
            elif isinstance(player_basic_info, dict):
                # 如果是字典，直接更新
                player_info.update(player_basic_info)
            
            # 验证必要字段
            if not player_info.get("player_key"):
                return None
            
            # 标准化球员数据
            standardized_player = self._standardize_player_data(player_info)
            return standardized_player
            
        except Exception as e:
            logger.warning(f"提取球员信息失败: {e}")
            return None
    
    def _standardize_player_data(self, player_info: Dict) -> Dict[str, Any]:
        """
        标准化球员数据
        
        Args:
            player_info: 原始球员信息
            
        Returns:
            Dict: 标准化的球员数据
        """
        try:
            # 处理姓名信息
            name_info = player_info.get("name", {})
            if isinstance(name_info, dict):
                full_name = name_info.get("full", "")
                first_name = name_info.get("first", "")
                last_name = name_info.get("last", "")
            else:
                full_name = str(name_info) if name_info else ""
                first_name = ""
                last_name = ""
            
            # 处理头像信息
            headshot_url = ""
            if "headshot" in player_info:
                headshot_info = player_info["headshot"]
                if isinstance(headshot_info, dict) and "url" in headshot_info:
                    headshot_url = headshot_info["url"]
            
            # 提取位置信息
            eligible_positions = self._extract_eligible_positions(player_info)
            
            # 构建标准化数据
            standardized = {
                "player_key": player_info.get("player_key", ""),
                "player_id": player_info.get("player_id", ""),
                "editorial_player_key": player_info.get("editorial_player_key", ""),
                "league_key": player_info.get("league_key", ""),
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "current_team_key": player_info.get("editorial_team_key", ""),
                "current_team_name": player_info.get("editorial_team_full_name", ""),
                "current_team_abbr": player_info.get("editorial_team_abbr", ""),
                "display_position": player_info.get("display_position", ""),
                "primary_position": player_info.get("primary_position", ""),
                "position_type": player_info.get("position_type", ""),
                "uniform_number": player_info.get("uniform_number", ""),
                "status": player_info.get("status", ""),
                "image_url": player_info.get("image_url", ""),
                "headshot_url": headshot_url,
                "is_undroppable": self._safe_bool(player_info.get("is_undroppable")),
                "season": self._extract_season_from_league_key(player_info.get("league_key", "")),
                "eligible_positions": eligible_positions
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"标准化球员数据失败: {e}")
            return {}
    
    def _extract_eligible_positions(self, player_info: Dict) -> List[str]:
        """
        提取球员合适位置列表
        
        Args:
            player_info: 球员信息
            
        Returns:
            List[str]: 位置列表
        """
        positions = []
        
        try:
            # 查找eligible_positions字段
            eligible_positions = player_info.get("eligible_positions")
            if not eligible_positions:
                return positions
            
            if isinstance(eligible_positions, list):
                for pos_item in eligible_positions:
                    if isinstance(pos_item, dict) and "position" in pos_item:
                        position = pos_item["position"]
                        if isinstance(position, str) and position not in positions:
                            positions.append(position)
                    elif isinstance(pos_item, str) and pos_item not in positions:
                        positions.append(pos_item)
            elif isinstance(eligible_positions, dict):
                # 有时候eligible_positions是一个包含position数组的对象
                if "position" in eligible_positions:
                    pos_data = eligible_positions["position"]
                    if isinstance(pos_data, list):
                        for pos in pos_data:
                            if isinstance(pos, str) and pos not in positions:
                                positions.append(pos)
                    elif isinstance(pos_data, str) and pos_data not in positions:
                        positions.append(pos_data)
                        
        except Exception as e:
            logger.warning(f"提取球员位置失败: {e}")
        
        return positions
    
    def _extract_season_from_league_key(self, league_key: str) -> str:
        """
        从联盟键中提取赛季信息
        
        Args:
            league_key: 联盟键
            
        Returns:
            str: 赛季信息
        """
        try:
            # 联盟键格式通常是: game_key.l.league_id
            # 例如: 364.l.53472，其中364是游戏键，可能包含赛季信息
            if not league_key:
                return "unknown"
            
            # 简单处理：使用当前年份作为默认赛季
            # 在实际应用中，应该从联盟设置或其他地方获取准确的赛季信息
            from datetime import datetime
            current_year = datetime.now().year
            return str(current_year)
            
        except Exception:
            return "unknown"
    
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
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数
            
        Returns:
            List[Dict]: 提取的球员数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("league_key is required for player extraction")
        
        # 使用分页方式获取所有球员数据
        all_players = []
        start = 0
        max_iterations = 100  # 防止无限循环
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # 构建API URL
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
            if start > 0:
                url += f";start={start}"
            url += "?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                break
            
            # 解析球员数据
            batch_players = self._parse_players_response(response_data, league_key)
            if not batch_players:
                break
            
            all_players.extend(batch_players)
            
            # 如果这批数据少于页面大小，说明已经是最后一页
            if len(batch_players) < self.page_size:
                break
            
            start += self.page_size
            
            # 避免API限制，短暂等待
            self.client.rate_limiter.wait_if_needed()
        
        return all_players
    

    
    def _get_page_params(self, page: int) -> Dict[str, Any]:
        """
        获取分页参数
        
        Args:
            page: 页码（从0开始）
            
        Returns:
            Dict: 分页参数
        """
        start = page * self.page_size
        return {"start": start}
    
    def _extract_page_data(self, page_response: Any) -> List[Any]:
        """
        从分页响应中提取数据
        
        Args:
            page_response: 分页API响应
            
        Returns:
            List: 提取的数据列表
        """
        # 这里应该解析page_response并返回球员数据列表
        # 但由于我们已经在extract方法中实现了完整逻辑，这里返回空列表
        return []
    
    def _has_more_pages(self, page_response: Any) -> bool:
        """
        判断是否有更多页面
        
        Args:
            page_response: 分页API响应
            
        Returns:
            bool: 是否有更多页面
        """
        # 这里应该检查是否还有更多页面
        # 但由于我们已经在extract方法中实现了完整逻辑，这里返回False
        return False
    
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
            bool: 球员数据支持增量更新
        """
        return True 