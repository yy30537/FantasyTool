"""
League Settings Extractor - 联盟设置提取器
=========================================

提取联盟详细设置信息，对应数据库league_settings表
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import LeagueSettingsData, ExtractionResult
from ..yahoo_client import YahooAPIClient

logger = logging.getLogger(__name__)


class SettingsExtractor(BaseExtractor):
    """
    联盟设置提取器
    
    功能：
    - 提取指定联盟的详细设置信息
    - 数据写入league_settings表
    - 支持联盟配置数据提取
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化联盟设置提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="SettingsExtractor",
            extractor_type=ExtractorType.AUXILIARY
        )
    
    def extract(self, league_key: str, **kwargs) -> ExtractionResult:
        """
        提取联盟设置数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 提取结果，包含联盟设置数据
        """
        try:
            logger.info(f"开始提取联盟 {league_key} 的设置数据...")
            
            # 构建API URL
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                return ExtractionResult(
                    success=False,
                    data=[],
                    error_message="Failed to retrieve league settings data from API",
                    total_count=0
                )
            
            # 解析联盟设置数据
            settings_data = self._parse_settings_response(response_data, league_key)
            
            # 验证和标准化数据
            validated_settings = []
            if settings_data:
                try:
                    settings_model = LeagueSettingsData(**settings_data)
                    validated_settings.append(settings_model)
                except Exception as e:
                    logger.warning(f"联盟设置数据验证失败: {league_key}, 错误: {e}")
            
            logger.info(f"成功提取联盟设置数据")
            
            return ExtractionResult(
                success=True,
                data=validated_settings,
                total_count=len(validated_settings),
                extraction_time=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"联盟设置数据提取失败: {e}")
            return ExtractionResult(
                success=False,
                data=[],
                error_message=str(e),
                total_count=0
            )
    
    def _parse_settings_response(self, response_data: Dict, league_key: str) -> Optional[Dict[str, Any]]:
        """
        解析Yahoo API返回的联盟设置数据
        
        Args:
            response_data: API响应数据
            league_key: 联盟键
            
        Returns:
            Optional[Dict]: 标准化的联盟设置数据，失败返回None
        """
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找settings容器
            settings_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings_container = item["settings"]
                        break
            
            if not settings_container:
                logger.warning("API响应中未找到settings数据")
                return None
            
            # 标准化设置数据
            standardized_settings = self._standardize_settings_data(settings_container, league_key)
            return standardized_settings
            
        except Exception as e:
            logger.error(f"解析联盟设置数据失败: {e}")
            return None
    
    def _standardize_settings_data(self, settings_info: Dict, league_key: str) -> Dict[str, Any]:
        """
        标准化联盟设置数据
        
        Args:
            settings_info: 原始设置信息
            league_key: 联盟键
            
        Returns:
            Dict: 标准化的联盟设置数据
        """
        try:
            # 提取阵容位置信息
            roster_positions = self._extract_roster_positions(settings_info)
            
            # 构建标准化数据
            standardized = {
                "league_key": league_key,
                "draft_type": settings_info.get("draft_type", ""),
                "is_auction_draft": self._safe_bool(settings_info.get("is_auction_draft")),
                "persistent_url": settings_info.get("persistent_url", ""),
                "uses_playoff": self._safe_bool(settings_info.get("uses_playoff")),
                "has_playoff_consolation_games": self._safe_bool(settings_info.get("has_playoff_consolation_games")),
                "playoff_start_week": settings_info.get("playoff_start_week", ""),
                "uses_playoff_reseeding": self._safe_bool(settings_info.get("uses_playoff_reseeding")),
                "uses_lock_eliminated_teams": self._safe_bool(settings_info.get("uses_lock_eliminated_teams")),
                "num_playoff_teams": self._safe_int(settings_info.get("num_playoff_teams")),
                "num_playoff_consolation_teams": self._safe_int(settings_info.get("num_playoff_consolation_teams")),
                "has_multiweek_championship": self._safe_bool(settings_info.get("has_multiweek_championship")),
                "waiver_type": settings_info.get("waiver_type", ""),
                "waiver_rule": settings_info.get("waiver_rule", ""),
                "uses_faab": self._safe_bool(settings_info.get("uses_faab")),
                "draft_time": settings_info.get("draft_time", ""),
                "draft_pick_time": settings_info.get("draft_pick_time", ""),
                "post_draft_players": settings_info.get("post_draft_players", ""),
                "max_teams": self._safe_int(settings_info.get("max_teams")),
                "waiver_time": settings_info.get("waiver_time", ""),
                "trade_end_date": settings_info.get("trade_end_date", ""),
                "trade_ratify_type": settings_info.get("trade_ratify_type", ""),
                "trade_reject_time": settings_info.get("trade_reject_time", ""),
                "player_pool": settings_info.get("player_pool", ""),
                "cant_cut_list": settings_info.get("cant_cut_list", ""),
                "draft_together": self._safe_bool(settings_info.get("draft_together")),
                "is_publicly_viewable": self._safe_bool(settings_info.get("is_publicly_viewable")),
                "can_trade_draft_picks": self._safe_bool(settings_info.get("can_trade_draft_picks")),
                "sendbird_channel_url": settings_info.get("sendbird_channel_url", ""),
                "roster_positions": roster_positions
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"标准化联盟设置数据失败: {e}")
            return {}
    
    def _extract_roster_positions(self, settings_info: Dict) -> List[Dict[str, Any]]:
        """
        提取阵容位置配置信息
        
        Args:
            settings_info: 设置信息
            
        Returns:
            List[Dict]: 阵容位置列表
        """
        positions = []
        
        try:
            roster_positions = settings_info.get("roster_positions")
            if not roster_positions:
                return positions
            
            if isinstance(roster_positions, list):
                for pos_item in roster_positions:
                    if isinstance(pos_item, dict) and "roster_position" in pos_item:
                        pos_info = pos_item["roster_position"]
                        if isinstance(pos_info, dict):
                            position_data = {
                                "position": pos_info.get("position", ""),
                                "position_type": pos_info.get("position_type", ""),
                                "count": self._safe_int(pos_info.get("count")),
                                "is_starting_position": pos_info.get("position") not in ["BN", "IL", "IR"]
                            }
                            positions.append(position_data)
            elif isinstance(roster_positions, dict):
                # 有时候roster_positions是一个对象而不是数组
                if "roster_position" in roster_positions:
                    pos_data = roster_positions["roster_position"]
                    if isinstance(pos_data, list):
                        for pos_info in pos_data:
                            if isinstance(pos_info, dict):
                                position_data = {
                                    "position": pos_info.get("position", ""),
                                    "position_type": pos_info.get("position_type", ""),
                                    "count": self._safe_int(pos_info.get("count")),
                                    "is_starting_position": pos_info.get("position") not in ["BN", "IL", "IR"]
                                }
                                positions.append(position_data)
                        
        except Exception as e:
            logger.warning(f"提取阵容位置信息失败: {e}")
        
        return positions
    
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
    
    def _extract_data(self, **params) -> Dict[str, Any]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数
            
        Returns:
            Dict: 提取的联盟设置数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("league_key is required for settings extraction")
        
        # 构建API URL
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        
        # 调用API
        response_data = self.client.get(url)
        if not response_data:
            raise ValueError("Failed to retrieve league settings data from API")
        
        # 解析联盟设置数据
        settings_data = self._parse_settings_response(response_data, league_key)
        if not settings_data:
            raise ValueError("Failed to parse league settings data")
        
        return settings_data
    

    
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
            bool: 联盟设置数据支持增量更新
        """
        return True
