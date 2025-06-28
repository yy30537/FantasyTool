#!/usr/bin/env python3
"""
球员统计数据提取器
提取球员的赛季统计和日期统计数据
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class PlayerStatsExtractor(BaseExtractor):
    """球员统计数据提取器
    
    提取球员统计数据，包括：
    - 赛季统计数据（season stats）
    - 日期统计数据（daily stats）
    - 周统计数据（weekly stats，主要用于NFL）
    
    支持批量处理和分页提取
    """
    
    def __init__(self, api_client: YahooAPIClient, batch_size: int = 25):
        """
        初始化球员统计数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
            batch_size: 批量处理大小，默认25（Yahoo API限制）
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="PlayerStatsExtractor",
            extractor_type=ExtractorType.STATISTICS
        )
        self.batch_size = batch_size
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，包含：
                - league_key: 联盟键（必需）
                - player_keys: 球员键列表（可选，如果不提供则获取所有球员）
                - stats_type: 统计类型 ('season', 'daily', 'week')
                - date: 日期（daily stats需要）
                - week: 周数（weekly stats需要）
                - season: 赛季
                
        Returns:
            List[Dict]: 提取的球员统计数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("PlayerStatsExtractor requires 'league_key' parameter")
        
        stats_type = params.get('stats_type', 'season')
        player_keys = params.get('player_keys')
        season = params.get('season', self._extract_season_from_league_key(league_key))
        
        # 如果没有提供球员键，获取联盟中的所有球员
        if not player_keys:
            player_keys = self._get_league_player_keys(league_key)
        
        if not player_keys:
            self.logger.warning(f"No player keys found for league {league_key}")
            return []
        
        # 根据统计类型提取数据
        if stats_type == 'season':
            return self._extract_season_stats(league_key, player_keys, season)
        elif stats_type == 'daily':
            date_obj = params.get('date')
            if not date_obj:
                raise ValueError("Daily stats extraction requires 'date' parameter")
            return self._extract_daily_stats(league_key, player_keys, season, date_obj)
        elif stats_type == 'week':
            week = params.get('week')
            if not week:
                raise ValueError("Weekly stats extraction requires 'week' parameter")
            return self._extract_weekly_stats(league_key, player_keys, season, week)
        else:
            raise ValueError(f"Unsupported stats_type: {stats_type}")
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["PlayerExtractor"]  # 依赖球员基本数据
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return True  # 球员统计数据经常更新
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 6 * 3600  # 6小时更新一次
    
    def _get_league_player_keys(self, league_key: str) -> List[str]:
        """
        获取联盟中所有球员的键
        
        Args:
            league_key: 联盟键
            
        Returns:
            List[str]: 球员键列表
        """
        try:
            # 获取联盟球员数据
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
            response_data = self.client.get(url)
            
            if not response_data:
                return []
            
            return self._extract_player_keys_from_response(response_data)
            
        except Exception as e:
            self.logger.error(f"Failed to get league player keys for {league_key}: {e}")
            return []
    
    def _extract_player_keys_from_response(self, response_data: Dict) -> List[str]:
        """从API响应中提取球员键"""
        player_keys = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                return player_keys
            
            players_count = int(players_container.get("count", 0))
            for i in range(players_count):
                str_index = str(i)
                if str_index in players_container:
                    player_data = players_container[str_index]
                    if "player" in player_data:
                        player_info = player_data["player"]
                        player_key = self._extract_player_key_from_info(player_info)
                        if player_key:
                            player_keys.append(player_key)
                            
        except Exception as e:
            self.logger.error(f"Failed to extract player keys from response: {e}")
            
        return player_keys
    
    def _extract_player_key_from_info(self, player_info) -> Optional[str]:
        """从球员信息中提取球员键"""
        try:
            if isinstance(player_info, list) and len(player_info) > 0:
                player_basic = player_info[0]
                if isinstance(player_basic, list):
                    for item in player_basic:
                        if isinstance(item, dict) and "player_key" in item:
                            return item["player_key"]
                elif isinstance(player_basic, dict) and "player_key" in player_basic:
                    return player_basic["player_key"]
        except Exception:
            pass
        return None
    
    def _extract_season_stats(self, league_key: str, player_keys: List[str], season: str) -> List[Dict[str, Any]]:
        """
        提取球员赛季统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            season: 赛季
            
        Returns:
            List[Dict]: 赛季统计数据
        """
        all_stats = []
        
        # 分批处理球员
        for i in range(0, len(player_keys), self.batch_size):
            batch_players = player_keys[i:i + self.batch_size]
            player_keys_str = ",".join(batch_players)
            
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    batch_stats = self._parse_season_stats_response(response_data, league_key, season)
                    all_stats.extend(batch_stats)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract season stats for batch {i//self.batch_size + 1}: {e}")
                continue
        
        return all_stats
    
    def _extract_daily_stats(self, league_key: str, player_keys: List[str], season: str, date_obj: date) -> List[Dict[str, Any]]:
        """
        提取球员日期统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            season: 赛季
            date_obj: 日期
            
        Returns:
            List[Dict]: 日期统计数据
        """
        all_stats = []
        date_str = date_obj.strftime('%Y-%m-%d')
        
        # 分批处理球员
        for i in range(0, len(player_keys), self.batch_size):
            batch_players = player_keys[i:i + self.batch_size]
            player_keys_str = ",".join(batch_players)
            
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    batch_stats = self._parse_daily_stats_response(response_data, league_key, season, date_obj)
                    all_stats.extend(batch_stats)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract daily stats for batch {i//self.batch_size + 1} on {date_str}: {e}")
                continue
        
        return all_stats
    
    def _extract_weekly_stats(self, league_key: str, player_keys: List[str], season: str, week: int) -> List[Dict[str, Any]]:
        """
        提取球员周统计数据（主要用于NFL）
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            season: 赛季
            week: 周数
            
        Returns:
            List[Dict]: 周统计数据
        """
        all_stats = []
        
        # 分批处理球员
        for i in range(0, len(player_keys), self.batch_size):
            batch_players = player_keys[i:i + self.batch_size]
            player_keys_str = ",".join(batch_players)
            
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=week;week={week}?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if response_data:
                    batch_stats = self._parse_weekly_stats_response(response_data, league_key, season, week)
                    all_stats.extend(batch_stats)
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract weekly stats for batch {i//self.batch_size + 1} for week {week}: {e}")
                continue
        
        return all_stats
    
    def _parse_season_stats_response(self, response_data: Dict, league_key: str, season: str) -> List[Dict[str, Any]]:
        """解析赛季统计API响应"""
        stats_data = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list):
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
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    continue
                
                # 提取球员基本信息
                player_key, editorial_player_key = self._extract_player_identifiers(player_info_list[0])
                if not player_key:
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if "player_stats" in stats_container:
                    player_stats = stats_container["player_stats"]
                    stats_dict = self._parse_stats_container(player_stats)
                    
                    if stats_dict:
                        stat_record = {
                            "player_key": player_key,
                            "editorial_player_key": editorial_player_key,
                            "league_key": league_key,
                            "season": season,
                            "stats_type": "season",
                            "stats_data": stats_dict
                        }
                        stats_data.append(stat_record)
                        
        except Exception as e:
            self.logger.error(f"Failed to parse season stats response: {e}")
        
        return stats_data
    
    def _parse_daily_stats_response(self, response_data: Dict, league_key: str, season: str, date_obj: date) -> List[Dict[str, Any]]:
        """解析日期统计API响应"""
        stats_data = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list):
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
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    continue
                
                # 提取球员基本信息
                player_key, editorial_player_key = self._extract_player_identifiers(player_info_list[0])
                if not player_key:
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if "player_stats" in stats_container:
                    player_stats = stats_container["player_stats"]
                    stats_dict = self._parse_stats_container(player_stats)
                    
                    if stats_dict:
                        stat_record = {
                            "player_key": player_key,
                            "editorial_player_key": editorial_player_key,
                            "league_key": league_key,
                            "season": season,
                            "date": date_obj,
                            "stats_type": "daily",
                            "stats_data": stats_dict
                        }
                        stats_data.append(stat_record)
                        
        except Exception as e:
            self.logger.error(f"Failed to parse daily stats response: {e}")
        
        return stats_data
    
    def _parse_weekly_stats_response(self, response_data: Dict, league_key: str, season: str, week: int) -> List[Dict[str, Any]]:
        """解析周统计API响应"""
        stats_data = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list):
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
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    continue
                
                # 提取球员基本信息
                player_key, editorial_player_key = self._extract_player_identifiers(player_info_list[0])
                if not player_key:
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if "player_stats" in stats_container:
                    player_stats = stats_container["player_stats"]
                    stats_dict = self._parse_stats_container(player_stats)
                    
                    if stats_dict:
                        stat_record = {
                            "player_key": player_key,
                            "editorial_player_key": editorial_player_key,
                            "league_key": league_key,
                            "season": season,
                            "week": week,
                            "stats_type": "weekly",
                            "stats_data": stats_dict
                        }
                        stats_data.append(stat_record)
                        
        except Exception as e:
            self.logger.error(f"Failed to parse weekly stats response: {e}")
        
        return stats_data
    
    def _extract_player_identifiers(self, player_basic_info) -> tuple:
        """提取球员标识信息"""
        player_key = None
        editorial_player_key = None
        
        try:
            if isinstance(player_basic_info, list):
                for item in player_basic_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_key = item["player_key"]
                        elif "editorial_player_key" in item:
                            editorial_player_key = item["editorial_player_key"]
            elif isinstance(player_basic_info, dict):
                player_key = player_basic_info.get("player_key")
                editorial_player_key = player_basic_info.get("editorial_player_key")
        except Exception:
            pass
        
        return player_key, editorial_player_key
    
    def _parse_stats_container(self, player_stats: Dict) -> Dict[str, Any]:
        """解析统计数据容器"""
        stats_dict = {}
        
        try:
            if "stats" in player_stats:
                stats_list = player_stats["stats"]
                if isinstance(stats_list, list):
                    for stat_item in stats_list:
                        if isinstance(stat_item, dict) and "stat" in stat_item:
                            stat_info = stat_item["stat"]
                            stat_id = stat_info.get("stat_id")
                            value = stat_info.get("value")
                            if stat_id is not None and value is not None:
                                stats_dict[str(stat_id)] = value
        except Exception as e:
            self.logger.error(f"Failed to parse stats container: {e}")
        
        return stats_dict
    
    def _extract_season_from_league_key(self, league_key: str) -> str:
        """从联盟键中提取赛季信息"""
        try:
            # 联盟键格式通常是 game_id.l.league_id
            parts = league_key.split('.')
            if len(parts) >= 1:
                game_id = parts[0]
                # 根据游戏ID推断赛季（这是简化处理）
                if game_id.startswith('41'):  # NBA/MLB等
                    return "2024"
                else:
                    return "2024"
            return "2024"
        except:
            return "2024"
    
    def extract_season_stats(self, league_key: str, player_keys: List[str] = None, season: str = None) -> ExtractionResult:
        """
        提取球员赛季统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表（可选）
            season: 赛季（可选）
            
        Returns:
            ExtractionResult: 包含赛季统计数据的提取结果
        """
        try:
            params = {
                'league_key': league_key,
                'stats_type': 'season'
            }
            if player_keys:
                params['player_keys'] = player_keys
            if season:
                params['season'] = season
            
            stats_data = self._extract_data(**params)
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=stats_data,
                total_records=len(stats_data),
                message=f"Successfully extracted season stats for {len(stats_data)} players in league {league_key}"
            )
            
        except Exception as e:
            self.logger.error(f"PlayerStatsExtractor season stats failed for league {league_key}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_daily_stats(self, league_key: str, date_obj: date, player_keys: List[str] = None, season: str = None) -> ExtractionResult:
        """
        提取球员日期统计数据
        
        Args:
            league_key: 联盟键
            date_obj: 日期
            player_keys: 球员键列表（可选）
            season: 赛季（可选）
            
        Returns:
            ExtractionResult: 包含日期统计数据的提取结果
        """
        try:
            params = {
                'league_key': league_key,
                'stats_type': 'daily',
                'date': date_obj
            }
            if player_keys:
                params['player_keys'] = player_keys
            if season:
                params['season'] = season
            
            stats_data = self._extract_data(**params)
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=stats_data,
                total_records=len(stats_data),
                message=f"Successfully extracted daily stats for {len(stats_data)} players on {date_obj}"
            )
            
        except Exception as e:
            self.logger.error(f"PlayerStatsExtractor daily stats failed for {date_obj}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )