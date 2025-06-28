"""
Game Data Extractor - 游戏数据提取器
=====================================

提取用户可访问的所有游戏信息，对应数据库games表
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import GameData, ExtractionResult
from ..yahoo_client import YahooAPIClient

logger = logging.getLogger(__name__)


class GameExtractor(BaseExtractor):
    """
    游戏数据提取器
    
    功能：
    - 提取用户可访问的所有游戏信息
    - 数据写入games表
    - 支持增量更新
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化游戏数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="GameExtractor",
            extractor_type=ExtractorType.CORE_ENTITY
        )
    
    def extract(self, **kwargs) -> ExtractionResult:
        """
        提取游戏数据
        
        Returns:
            ExtractionResult: 提取结果，包含游戏数据列表
        """
        try:
            logger.info("开始提取游戏数据...")
            
            # 构建API URL
            url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                return ExtractionResult(
                    success=False,
                    data=[],
                    error_message="Failed to retrieve games data from API",
                    total_count=0
                )
            
            # 解析游戏数据
            games_data = self._parse_games_response(response_data)
            
            # 验证和标准化数据
            validated_games = []
            for game_data in games_data:
                try:
                    game_model = GameData(**game_data)
                    validated_games.append(game_model)
                except Exception as e:
                    logger.warning(f"游戏数据验证失败: {game_data.get('game_key', 'unknown')}, 错误: {e}")
                    continue
            
            logger.info(f"成功提取 {len(validated_games)} 个游戏数据")
            
            return ExtractionResult(
                success=True,
                data=validated_games,
                total_count=len(validated_games),
                extraction_time=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"游戏数据提取失败: {e}")
            return ExtractionResult(
                success=False,
                data=[],
                error_message=str(e),
                total_count=0
            )
    
    def _parse_games_response(self, response_data: Dict) -> List[Dict[str, Any]]:
        """
        解析Yahoo API返回的游戏数据
        
        Args:
            response_data: API响应数据
            
        Returns:
            List[Dict]: 标准化的游戏数据列表
        """
        games_list = []
        
        try:
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
                logger.warning("API响应中未找到games数据")
                return games_list
            
            games_count = int(games_container.get("count", 0))
            logger.info(f"发现 {games_count} 个游戏")
            
            # 提取每个游戏的信息
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
                
                # 合并游戏信息
                game_info = {}
                for game_item in game_data_list:
                    if isinstance(game_item, dict):
                        game_info.update(game_item)
                
                # 标准化游戏数据
                standardized_game = self._standardize_game_data(game_info)
                if standardized_game:
                    games_list.append(standardized_game)
            
        except Exception as e:
            logger.error(f"解析游戏数据失败: {e}")
        
        return games_list
    
    def _standardize_game_data(self, game_info: Dict) -> Optional[Dict[str, Any]]:
        """
        标准化游戏数据
        
        Args:
            game_info: 原始游戏信息
            
        Returns:
            Optional[Dict]: 标准化的游戏数据，失败返回None
        """
        try:
            # 必须字段验证
            game_key = game_info.get("game_key")
            if not game_key:
                return None
            
            # 构建标准化数据
            standardized = {
                "game_key": game_key,
                "game_id": game_info.get("game_id", ""),
                "name": game_info.get("name", ""),
                "code": game_info.get("code", ""),
                "type": game_info.get("type", ""),
                "url": game_info.get("url", ""),
                "season": game_info.get("season", ""),
                "is_registration_over": self._safe_bool(game_info.get("is_registration_over")),
                "is_game_over": self._safe_bool(game_info.get("is_game_over")),
                "is_offseason": self._safe_bool(game_info.get("is_offseason")),
                "editorial_season": game_info.get("editorial_season", ""),
                "picks_status": game_info.get("picks_status", ""),
                "contest_group_id": game_info.get("contest_group_id", ""),
                "scenario_generator": self._safe_bool(game_info.get("scenario_generator")),
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"标准化游戏数据失败: {e}")
            return None
    
    def _safe_bool(self, value: Any) -> bool:
        """
        安全地转换布尔值
        
        Args:
            value: 待转换的值
            
        Returns:
            bool: 转换后的布尔值
        """
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
            List[Dict]: 提取的游戏数据
        """
        # 构建API URL
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        
        # 调用API
        response_data = self.client.get(url)
        if not response_data:
            raise ValueError("Failed to retrieve games data from API")
        
        # 解析游戏数据
        return self._parse_games_response(response_data)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表（游戏数据无依赖）
        """
        return []
    
    def supports_incremental_update(self) -> bool:
        """
        是否支持增量更新
        
        Returns:
            bool: 游戏数据支持增量更新
        """
        return True