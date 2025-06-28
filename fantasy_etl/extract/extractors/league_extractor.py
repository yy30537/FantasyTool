"""
League Data Extractor - 联盟数据提取器
===================================

提取联盟基本信息，对应数据库leagues表
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import LeagueData, ExtractionResult
from ..yahoo_client import YahooAPIClient

logger = logging.getLogger(__name__)


class LeagueExtractor(BaseExtractor):
    """
    联盟数据提取器
    
    功能：
    - 提取指定游戏下的所有联盟信息
    - 数据写入leagues表
    - 支持按游戏键筛选
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化联盟数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="LeagueExtractor",
            extractor_type=ExtractorType.CORE_ENTITY
        )
    
    def extract(self, game_keys: Optional[List[str]] = None, **kwargs) -> ExtractionResult:
        """
        提取联盟数据
        
        Args:
            game_keys: 游戏键列表，如果为None则提取所有游戏的联盟
            
        Returns:
            ExtractionResult: 提取结果，包含联盟数据列表
        """
        try:
            logger.info("开始提取联盟数据...")
            
            # 如果没有指定游戏键，先获取所有游戏
            if not game_keys:
                game_keys = self._get_available_game_keys()
                if not game_keys:
                    return ExtractionResult(
                        success=False,
                        data=[],
                        error_message="No available game keys found",
                        total_count=0
                    )
            
            # 提取每个游戏的联盟数据
            all_leagues = []
            for game_key in game_keys:
                leagues_data = self._extract_leagues_for_game(game_key)
                all_leagues.extend(leagues_data)
            
            # 验证和标准化数据
            validated_leagues = []
            for league_data in all_leagues:
                try:
                    league_model = LeagueData(**league_data)
                    validated_leagues.append(league_model)
                except Exception as e:
                    logger.warning(f"联盟数据验证失败: {league_data.get('league_key', 'unknown')}, 错误: {e}")
                    continue
            
            logger.info(f"成功提取 {len(validated_leagues)} 个联盟数据")
            
            return ExtractionResult(
                success=True,
                data=validated_leagues,
                total_count=len(validated_leagues),
                extraction_time=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"联盟数据提取失败: {e}")
            return ExtractionResult(
                success=False,
                data=[],
                error_message=str(e),
                total_count=0
            )
    
    def _get_available_game_keys(self) -> List[str]:
        """
        获取可用的游戏键列表
        
        Returns:
            List[str]: 游戏键列表
        """
        try:
            url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
            response_data = self.client.get(url)
            
            if not response_data:
                return []
            
            game_keys = []
            fantasy_content = response_data.get("fantasy_content", {})
            users_data = fantasy_content.get("users", {})
            user_data = users_data.get("0", {}).get("user", [])
            
            # 查找games容器
            games_container = None
            for item in user_data:
                if isinstance(item, dict) and "games" in item:
                    games_container = item["games"]
                    break
            
            if games_container:
                games_count = int(games_container.get("count", 0))
                for i in range(games_count):
                    game_index = str(i)
                    if game_index in games_container:
                        game_container = games_container[game_index]
                        if "game" in game_container:
                            game_data_list = game_container["game"]
                            if isinstance(game_data_list, list) and len(game_data_list) > 0:
                                game_info = game_data_list[0]
                                if isinstance(game_info, dict):
                                    game_key = game_info.get("game_key")
                                    game_type = game_info.get("type")
                                    # 只处理type='full'的游戏
                                    if game_key and game_type == "full":
                                        game_keys.append(game_key)
            
            logger.info(f"发现 {len(game_keys)} 个可用游戏")
            return game_keys
            
        except Exception as e:
            logger.error(f"获取游戏键失败: {e}")
            return []
    
    def _extract_leagues_for_game(self, game_key: str) -> List[Dict[str, Any]]:
        """
        提取指定游戏的联盟数据
        
        Args:
            game_key: 游戏键
            
        Returns:
            List[Dict]: 联盟数据列表
        """
        try:
            logger.info(f"提取游戏 {game_key} 的联盟数据...")
            
            # 构建API URL
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                logger.warning(f"游戏 {game_key} 的联盟数据获取失败")
                return []
            
            # 解析联盟数据
            leagues_data = self._parse_leagues_response(response_data, game_key)
            logger.info(f"游戏 {game_key} 提取到 {len(leagues_data)} 个联盟")
            
            return leagues_data
            
        except Exception as e:
            logger.error(f"提取游戏 {game_key} 联盟数据失败: {e}")
            return []
    
    def _parse_leagues_response(self, response_data: Dict, game_key: str) -> List[Dict[str, Any]]:
        """
        解析Yahoo API返回的联盟数据
        
        Args:
            response_data: API响应数据
            game_key: 游戏键
            
        Returns:
            List[Dict]: 标准化的联盟数据列表
        """
        leagues_list = []
        
        try:
            # 检查是否有错误
            if "error" in response_data:
                logger.warning(f"API返回错误: {response_data['error']}")
                return leagues_list
            
            fantasy_content = response_data.get("fantasy_content", {})
            users_data = fantasy_content.get("users", {})
            user_data = users_data.get("0", {}).get("user", [])
            
            # 查找games容器
            games_container = None
            for item in user_data:
                if isinstance(item, dict) and "games" in item:
                    games_container = item["games"]
                    break
            
            if not games_container:
                return leagues_list
            
            # 查找指定游戏的联盟数据
            games_count = int(games_container.get("count", 0))
            for i in range(games_count):
                game_index = str(i)
                if game_index not in games_container:
                    continue
                
                game_container = games_container[game_index]
                if "game" not in game_container:
                    continue
                
                game_data_list = game_container["game"]
                if not isinstance(game_data_list, list) or len(game_data_list) == 0:
                    continue
                
                # 验证是否是目标游戏
                current_game_key = None
                if isinstance(game_data_list[0], dict):
                    current_game_key = game_data_list[0].get("game_key")
                
                if current_game_key != game_key:
                    continue
                
                # 查找联盟数据
                if len(game_data_list) > 1 and isinstance(game_data_list[1], dict):
                    leagues_container = game_data_list[1].get("leagues")
                    if leagues_container:
                        leagues_count = int(leagues_container.get("count", 0))
                        
                        for j in range(leagues_count):
                            league_index = str(j)
                            if league_index not in leagues_container:
                                continue
                            
                            league_container = leagues_container[league_index]
                            if "league" not in league_container:
                                continue
                            
                            league_data_list = league_container["league"]
                            
                            # 合并联盟信息
                            league_info = {"game_key": game_key}  # 确保包含game_key
                            if isinstance(league_data_list, list):
                                for league_item in league_data_list:
                                    if isinstance(league_item, dict):
                                        league_info.update(league_item)
                            
                            # 标准化联盟数据
                            standardized_league = self._standardize_league_data(league_info)
                            if standardized_league:
                                leagues_list.append(standardized_league)
                
                break  # 找到目标游戏后退出
            
        except Exception as e:
            logger.error(f"解析联盟数据失败: {e}")
        
        return leagues_list
    
    def _standardize_league_data(self, league_info: Dict) -> Optional[Dict[str, Any]]:
        """
        标准化联盟数据
        
        Args:
            league_info: 原始联盟信息
            
        Returns:
            Optional[Dict]: 标准化的联盟数据，失败返回None
        """
        try:
            # 必须字段验证
            league_key = league_info.get("league_key")
            if not league_key:
                return None
            
            # 构建标准化数据
            standardized = {
                "league_key": league_key,
                "league_id": league_info.get("league_id", ""),
                "game_key": league_info.get("game_key", ""),
                "name": league_info.get("name", ""),
                "url": league_info.get("url", ""),
                "logo_url": league_info.get("logo_url", ""),
                "password": league_info.get("password", ""),
                "draft_status": league_info.get("draft_status", ""),
                "num_teams": self._safe_int(league_info.get("num_teams")),
                "edit_key": league_info.get("edit_key", ""),
                "weekly_deadline": league_info.get("weekly_deadline", ""),
                "league_update_timestamp": league_info.get("league_update_timestamp", ""),
                "scoring_type": league_info.get("scoring_type", ""),
                "league_type": league_info.get("league_type", ""),
                "renew": league_info.get("renew", ""),
                "renewed": league_info.get("renewed", ""),
                "felo_tier": league_info.get("felo_tier", ""),
                "iris_group_chat_id": league_info.get("iris_group_chat_id", ""),
                "short_invitation_url": league_info.get("short_invitation_url", ""),
                "allow_add_to_dl_extra_pos": self._safe_bool(league_info.get("allow_add_to_dl_extra_pos")),
                "is_pro_league": self._safe_bool(league_info.get("is_pro_league")),
                "is_cash_league": self._safe_bool(league_info.get("is_cash_league")),
                "current_week": league_info.get("current_week", ""),
                "start_week": league_info.get("start_week", ""),
                "start_date": league_info.get("start_date", ""),
                "end_week": league_info.get("end_week", ""),
                "end_date": league_info.get("end_date", ""),
                "is_finished": self._safe_bool(league_info.get("is_finished")),
                "is_plus_league": self._safe_bool(league_info.get("is_plus_league")),
                "game_code": league_info.get("game_code", ""),
                "season": league_info.get("season", ""),
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"标准化联盟数据失败: {e}")
            return None
    
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
            List[Dict]: 提取的联盟数据
        """
        game_keys = params.get('game_keys')
        
        # 如果没有指定游戏键，先获取所有游戏
        if not game_keys:
            game_keys = self._get_available_game_keys()
            if not game_keys:
                raise ValueError("No available game keys found")
        
        # 提取每个游戏的联盟数据
        all_leagues = []
        for game_key in game_keys:
            leagues_data = self._extract_leagues_for_game(game_key)
            all_leagues.extend(leagues_data)
        
        return all_leagues
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["GameExtractor"]  # 依赖游戏数据
    
    def supports_incremental_update(self) -> bool:
        """
        是否支持增量更新
        
        Returns:
            bool: 联盟数据支持增量更新
        """
        return True 