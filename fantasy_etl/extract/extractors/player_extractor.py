"""
Player Extractor - 球员数据提取器

负责提取球员相关的原始数据
"""
from typing import Dict, List, Optional
import logging
from datetime import date

from .base_extractor import BaseExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class PlayerExtractor(BaseExtractor):
    """球员数据提取器 - 负责提取球员相关的原始数据"""
    
    def extract_league_players(self, league_key: str, start: int = 0) -> ExtractionResult:
        """提取联盟所有球员数据（分页）
        
        Args:
            league_key: 联盟键
            start: 分页起始位置
            
        Returns:
            ExtractionResult: 包含players数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_players")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的球员数据 (起始位置: {start})")
            
            # 构建端点
            endpoint = f"league/{league_key}/players"
            if start > 0:
                endpoint += f";start={start}"
            endpoint += "?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的球员数据",
                    metadata=self._build_metadata(
                        operation="extract_league_players",
                        league_key=league_key,
                        start=start
                    )
                )
            
            # 提取players数据
            players_data = self._extract_players_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的 {len(players_data)} 个球员")
            
            return ExtractionResult(
                success=True,
                data=players_data,
                total_records=len(players_data),
                metadata=self._build_metadata(
                    operation="extract_league_players",
                    league_key=league_key,
                    start=start,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 球员数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_players",
                    league_key=league_key,
                    start=start
                )
            )
    
    def extract_all_league_players(self, league_key: str, max_pages: int = 100) -> ExtractionResult:
        """提取联盟所有球员数据（自动分页）
        
        Args:
            league_key: 联盟键
            max_pages: 最大页数限制
            
        Returns:
            ExtractionResult: 包含所有players数据的提取结果
        """
        try:
            self.logger.info(f"开始分页提取联盟 {league_key} 的所有球员数据")
            
            all_players = []
            endpoint_template = f"league/{league_key}/players"
            
            # 使用基类的分页提取方法
            for page_data in self.extract_paginated(
                endpoint_template + ";start={start}?format=json",
                max_pages=max_pages,
                start_param="start"
            ):
                players_data = self._extract_players_from_response(page_data)
                all_players.extend(players_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的总共 {len(all_players)} 个球员")
            
            return ExtractionResult(
                success=True,
                data=all_players,
                total_records=len(all_players),
                metadata=self._build_metadata(
                    operation="extract_all_league_players",
                    league_key=league_key,
                    total_pages=max_pages
                )
            )
            
        except Exception as e:
            self.logger.error(f"分页提取联盟 {league_key} 球员数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_all_league_players",
                    league_key=league_key
                )
            )
    
    def extract_player_season_stats(self, league_key: str, player_keys: List[str]) -> ExtractionResult:
        """提取球员赛季统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            
        Returns:
            ExtractionResult: 包含player season stats数据的提取结果
        """
        try:
            if not league_key or not player_keys:
                return ExtractionResult(
                    success=False,
                    error="league_key和player_keys参数不能为空",
                    metadata=self._build_metadata(operation="extract_player_season_stats")
                )
            
            self.logger.info(f"开始提取 {len(player_keys)} 个球员的赛季统计数据")
            
            # 构建球员键字符串
            player_keys_str = ",".join(player_keys)
            endpoint = f"league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取球员赛季统计数据",
                    metadata=self._build_metadata(
                        operation="extract_player_season_stats",
                        league_key=league_key,
                        player_count=len(player_keys)
                    )
                )
            
            # 提取统计数据
            stats_data = self._extract_player_stats_from_response(response_data)
            
            self.logger.info(f"成功提取 {len(stats_data)} 个球员的赛季统计数据")
            
            return ExtractionResult(
                success=True,
                data=stats_data,
                total_records=len(stats_data),
                metadata=self._build_metadata(
                    operation="extract_player_season_stats",
                    league_key=league_key,
                    player_count=len(player_keys),
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取球员赛季统计数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_player_season_stats",
                    league_key=league_key
                )
            )
    
    def extract_player_daily_stats(self, league_key: str, player_keys: List[str], date_str: str) -> ExtractionResult:
        """提取球员日统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            date_str: 日期字符串 (YYYY-MM-DD)
            
        Returns:
            ExtractionResult: 包含player daily stats数据的提取结果
        """
        try:
            if not league_key or not player_keys or not date_str:
                return ExtractionResult(
                    success=False,
                    error="所有参数都不能为空",
                    metadata=self._build_metadata(operation="extract_player_daily_stats")
                )
            
            self.logger.info(f"开始提取 {len(player_keys)} 个球员 {date_str} 的日统计数据")
            
            # 构建球员键字符串
            player_keys_str = ",".join(player_keys)
            endpoint = f"league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取球员日统计数据",
                    metadata=self._build_metadata(
                        operation="extract_player_daily_stats",
                        league_key=league_key,
                        date=date_str,
                        player_count=len(player_keys)
                    )
                )
            
            # 提取统计数据
            stats_data = self._extract_player_stats_from_response(response_data)
            
            self.logger.info(f"成功提取 {len(stats_data)} 个球员 {date_str} 的日统计数据")
            
            return ExtractionResult(
                success=True,
                data=stats_data,
                total_records=len(stats_data),
                metadata=self._build_metadata(
                    operation="extract_player_daily_stats",
                    league_key=league_key,
                    date=date_str,
                    player_count=len(player_keys),
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取球员日统计数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_player_daily_stats",
                    league_key=league_key,
                    date=date_str
                )
            )
    
    def extract(self, operation: str, **kwargs) -> ExtractionResult:
        """统一的提取接口
        
        Args:
            operation: 操作类型 ('league_players', 'all_league_players', 'season_stats', 'daily_stats')
            **kwargs: 操作参数
            
        Returns:
            ExtractionResult: 提取结果
        """
        if operation == "league_players":
            return self.extract_league_players(
                kwargs.get("league_key"), 
                kwargs.get("start", 0)
            )
        elif operation == "all_league_players":
            return self.extract_all_league_players(
                kwargs.get("league_key"),
                kwargs.get("max_pages", 100)
            )
        elif operation == "season_stats":
            return self.extract_player_season_stats(
                kwargs.get("league_key"),
                kwargs.get("player_keys", [])
            )
        elif operation == "daily_stats":
            return self.extract_player_daily_stats(
                kwargs.get("league_key"),
                kwargs.get("player_keys", []),
                kwargs.get("date_str")
            )
        else:
            return ExtractionResult(
                success=False,
                error=f"不支持的操作类型: {operation}",
                metadata=self._build_metadata(operation=operation)
            )
    
    def _extract_players_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取players数据"""
        players = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
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
                return players
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                player_data = players_container.get(str(i), {})
                if "player" in player_data:
                    player_info = player_data["player"]
                    
                    # 处理嵌套的player信息
                    if isinstance(player_info, list) and len(player_info) > 0:
                        # 合并player信息
                        merged_player = {}
                        for item in player_info:
                            if isinstance(item, list):
                                for sub_item in item:
                                    if isinstance(sub_item, dict):
                                        merged_player.update(sub_item)
                            elif isinstance(item, dict):
                                merged_player.update(item)
                        
                        if merged_player.get("player_key"):
                            players.append(merged_player)
        
        except Exception as e:
            self.logger.error(f"提取players数据时出错: {str(e)}")
        
        return players
    
    def _extract_player_stats_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取player stats数据"""
        stats_data = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
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
                return stats_data
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                player_data = players_container.get(str(i), {})
                if "player" in player_data:
                    player_info = player_data["player"]
                    
                    if isinstance(player_info, list) and len(player_info) >= 2:
                        # 第一部分是球员基本信息
                        player_basic = player_info[0]
                        player_key = None
                        
                        if isinstance(player_basic, list):
                            for item in player_basic:
                                if isinstance(item, dict) and "player_key" in item:
                                    player_key = item["player_key"]
                                    break
                        
                        # 第二部分是统计数据
                        if len(player_info) > 1 and player_key:
                            stats_container = player_info[1]
                            if isinstance(stats_container, dict) and "player_stats" in stats_container:
                                player_stats = stats_container["player_stats"]
                                stats_data.append({
                                    "player_key": player_key,
                                    "stats": player_stats
                                })
        
        except Exception as e:
            self.logger.error(f"提取player stats数据时出错: {str(e)}")
        
        return stats_data
    
    def _count_records_in_response(self, response_data: Dict) -> int:
        """重写基类方法：计算players响应中的记录数量"""
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        return int(item["players"].get("count", 0))
            elif isinstance(league_data, dict) and "players" in league_data:
                return int(league_data["players"].get("count", 0))
            
            return 0
        except Exception:
            return 0 