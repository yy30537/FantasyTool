#!/usr/bin/env python3
"""
统计类别数据提取器
从联盟设置中提取和解析统计类别定义
"""

from typing import List, Dict, Any, Optional
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class StatCategoriesExtractor(BaseExtractor):
    """统计类别数据提取器
    
    从联盟设置API中提取统计类别定义，包含：
    - 统计类别ID和名称
    - 显示名称和缩写
    - 分组和排序信息
    - 位置类型和启用状态
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化统计类别数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="StatCategoriesExtractor",
            extractor_type=ExtractorType.METADATA
        )
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，需要包含league_key
            
        Returns:
            List[Dict]: 提取的统计类别数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("StatCategoriesExtractor requires 'league_key' parameter")
        
        # 构建API URL
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        
        # 调用API
        response_data = self.client.get(url)
        if not response_data:
            raise ValueError(f"Failed to retrieve league settings for {league_key}")
        
        # 解析统计类别数据
        return self._parse_stat_categories_response(response_data, league_key)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["LeagueExtractor"]  # 依赖联盟数据
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return False  # 统计类别定义通常在赛季开始时确定，不需要频繁更新
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 7 * 24 * 3600  # 一周更新一次
    
    def _parse_stat_categories_response(self, response_data: Dict, league_key: str) -> List[Dict[str, Any]]:
        """
        解析API响应中的统计类别数据
        
        Args:
            response_data: API响应数据
            league_key: 联盟键
            
        Returns:
            List[Dict]: 标准化的统计类别数据
        """
        stat_categories = []
        
        try:
            # 导航到league settings
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找settings容器
            settings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings_container = item["settings"]
                        break
            elif isinstance(league_data, dict) and "settings" in league_data:
                settings_container = league_data["settings"]
            
            if not settings_container:
                self.logger.warning(f"No settings found in league data for {league_key}")
                return stat_categories
            
            # 提取第一个settings对象
            settings_info = None
            if isinstance(settings_container, list) and len(settings_container) > 0:
                settings_info = settings_container[0]
            elif isinstance(settings_container, dict):
                settings_info = settings_container
            
            if not settings_info:
                self.logger.warning(f"No settings info found for {league_key}")
                return stat_categories
            
            # 提取stat_categories
            stat_categories_data = settings_info.get("stat_categories")
            if not stat_categories_data:
                self.logger.warning(f"No stat_categories found in settings for {league_key}")
                return stat_categories
            
            stats_list = stat_categories_data.get("stats", [])
            if not isinstance(stats_list, list):
                self.logger.warning(f"Invalid stats list format for {league_key}")
                return stat_categories
            
            # 解析每个统计类别
            for stat_item in stats_list:
                if not isinstance(stat_item, dict) or "stat" not in stat_item:
                    continue
                
                stat_info = stat_item["stat"]
                if not isinstance(stat_info, dict):
                    continue
                
                stat_id = stat_info.get("stat_id")
                if stat_id is None:
                    continue
                
                # 标准化统计类别数据
                category_data = {
                    "league_key": league_key,
                    "stat_id": int(stat_id),
                    "name": stat_info.get("name", ""),
                    "display_name": stat_info.get("display_name", ""),
                    "abbr": stat_info.get("abbr", ""),
                    "group_name": stat_info.get("group", ""),
                    "sort_order": self._safe_int(stat_info.get("sort_order", 0)),
                    "position_type": stat_info.get("position_type", ""),
                    "is_enabled": self._safe_bool(stat_info.get("enabled", "1")),
                    "is_only_display_stat": self._safe_bool(stat_info.get("is_only_display_stat", "0")),
                    "is_core_stat": self._is_core_stat(stat_id),
                    "core_stat_column": self._get_core_stat_column(stat_id)
                }
                
                stat_categories.append(category_data)
                
        except Exception as e:
            self.logger.error(f"Failed to parse stat categories for {league_key}: {e}")
            
        return stat_categories
    
    def _safe_int(self, value) -> int:
        """安全转换为整数"""
        try:
            if value is None or value == '':
                return 0
            return int(value)
        except (ValueError, TypeError):
            return 0
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        try:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip() in ('1', 'true', 'True', 'yes', 'Yes')
            if isinstance(value, (int, float)):
                return bool(value)
            return False
        except:
            return False
    
    def _is_core_stat(self, stat_id: int) -> bool:
        """判断是否为核心统计项"""
        # 基于Yahoo Fantasy Basketball的核心统计项
        core_stat_ids = {
            5,        # FG%
            8,        # FT%
            10,       # 3PTM
            12,       # PTS
            15,       # REB
            16,       # AST
            17,       # ST
            18,       # BLK
            19,       # TO
            9004003,  # FGM/A
            9007006   # FTM/A
        }
        return int(stat_id) in core_stat_ids
    
    def _get_core_stat_column(self, stat_id: int) -> Optional[str]:
        """获取核心统计项对应的数据库列名"""
        stat_column_mapping = {
            5: "field_goal_percentage",
            8: "free_throw_percentage", 
            10: "three_pointers_made",
            12: "points",  # 或 total_points (取决于表)
            15: "rebounds",  # 或 total_rebounds
            16: "assists",   # 或 total_assists
            17: "steals",    # 或 total_steals
            18: "blocks",    # 或 total_blocks
            19: "turnovers", # 或 total_turnovers
            9004003: "field_goals_made_attempted",  # 需要分解为两个字段
            9007006: "free_throws_made_attempted"   # 需要分解为两个字段
        }
        return stat_column_mapping.get(int(stat_id))
    
    def extract(self, league_key: str, **kwargs) -> ExtractionResult:
        """
        提取指定联盟的统计类别数据
        
        Args:
            league_key: 联盟键
            **kwargs: 其他参数
            
        Returns:
            ExtractionResult: 包含统计类别数据的提取结果
        """
        try:
            # 调用基类方法
            stat_categories_data = self._extract_data(league_key=league_key, **kwargs)
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=stat_categories_data,
                total_records=len(stat_categories_data),
                message=f"Successfully extracted {len(stat_categories_data)} stat categories for league {league_key}"
            )
            
        except Exception as e:
            self.logger.error(f"StatCategoriesExtractor failed for league {league_key}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
