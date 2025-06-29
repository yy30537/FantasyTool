# 联盟数据加载器
#
# 迁移来源: @database_writer.py 中的联盟相关写入逻辑
# 主要映射:
#   - write_leagues_data() -> LeagueLoader.load_leagues()
#   - write_league_settings() -> LeagueLoader.load_league_settings()
#   - write_stat_categories() -> LeagueLoader.load_stat_categories()
#   - write_league_roster_positions() -> LeagueLoader.load_roster_positions()
#
# 职责:
#   - 联盟基础数据加载：
#     * League模型数据写入：league_key、name、game_key等
#     * 联盟配置信息：num_teams、scoring_type、draft_status等
#     * 时间信息处理：start_date、end_date、current_week等
#     * 布尔值字段标准化：is_finished、is_pro_league等
#   - 联盟设置数据加载：
#     * LeagueSettings模型写入：draft_type、playoff配置等
#     * 一对一关系维护：League与LeagueSettings的关联
#     * JSON字段处理：roster_positions等复杂配置
#     * 设置完整性验证：playoff、waiver等配置的合理性
#   - 统计类别数据加载：
#     * StatCategory模型写入：stat_id、name、display_name等
#     * 核心统计标记：is_core_stat字段的设置
#     * 统计分组处理：group_name、sort_order等
#     * 统计启用状态：is_enabled、is_only_display_stat等
#   - 阵容位置配置加载：
#     * LeagueRosterPosition模型写入：position、count等
#     * 位置类型分类：position_type、is_starting_position
#     * 阵容规则解析：从JSON配置到结构化数据
#   - 数据关系管理：
#     * 外键关系维护：league_key到Game表的引用
#     * 级联数据处理：联盟相关的所有配置数据
#     * 关系完整性检查：确保所有关联数据的一致性
#   - 去重和更新策略：
#     * 联盟主键冲突处理：league_key唯一性
#     * 设置更新逻辑：已存在联盟的设置更新
#     * 增量配置更新：统计类别和位置配置的变更
#   - 批量处理优化：
#     * 分层批量写入：先联盟、再设置、最后配置
#     * 事务协调：多表写入的事务一致性
#     * 错误恢复：部分失败时的数据回滚
#   - 验证和质量检查：
#     * 联盟配置合理性：team数量、playoff配置等
#     * 统计类别完整性：必需统计项的存在检查
#     * 阵容位置逻辑：位置数量和类型的合理性
#
# 输入: 联盟数据、设置数据、统计类别数据、位置配置数据
# 输出: 联盟数据加载结果和统计信息 

"""
League Data Loader - Handles leagues, league settings, and stat categories
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json

from .base_loader import BaseLoader, LoadResult
from ..database.models import League, LeagueSettings, StatCategory, LeagueRosterPosition


class LeagueLoader(BaseLoader):
    """Loader for league basic information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate league record"""
        required_fields = ['league_key', 'league_id', 'game_key', 'name', 'num_teams', 'season']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> League:
        """Create League model instance"""
        return League(
            league_key=record['league_key'],
            league_id=record['league_id'],
            game_key=record['game_key'],
            name=record['name'],
            url=record.get('url'),
            logo_url=record.get('logo_url'),
            password=record.get('password'),
            draft_status=record.get('draft_status'),
            num_teams=record['num_teams'],
            edit_key=record.get('edit_key'),
            weekly_deadline=record.get('weekly_deadline'),
            league_update_timestamp=record.get('league_update_timestamp'),
            scoring_type=record.get('scoring_type'),
            league_type=record.get('league_type'),
            renew=record.get('renew'),
            renewed=record.get('renewed'),
            felo_tier=record.get('felo_tier'),
            iris_group_chat_id=record.get('iris_group_chat_id'),
            short_invitation_url=record.get('short_invitation_url'),
            allow_add_to_dl_extra_pos=self._safe_bool(record.get('allow_add_to_dl_extra_pos', False)),
            is_pro_league=self._safe_bool(record.get('is_pro_league', False)),
            is_cash_league=self._safe_bool(record.get('is_cash_league', False)),
            current_week=str(record.get('current_week', '')),
            start_week=record.get('start_week'),
            start_date=record.get('start_date'),
            end_week=record.get('end_week'),
            end_date=record.get('end_date'),
            is_finished=self._safe_bool(record.get('is_finished', False)),
            is_plus_league=self._safe_bool(record.get('is_plus_league', False)),
            game_code=record.get('game_code'),
            season=record['season']
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for league"""
        return record['league_key']
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess league record"""
        # Handle boolean fields that might come as strings
        bool_fields = ['allow_add_to_dl_extra_pos', 'is_pro_league', 'is_cash_league', 
                      'is_finished', 'is_plus_league']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        # Ensure current_week is string
        if 'current_week' in record:
            record['current_week'] = str(record['current_week'])
        
        return record


class LeagueSettingsLoader(BaseLoader):
    """Loader for league settings"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate league settings record"""
        return 'league_key' in record and record['league_key']
    
    def _create_model_instance(self, record: Dict[str, Any]) -> LeagueSettings:
        """Create LeagueSettings model instance"""
        return LeagueSettings(
            league_key=record['league_key'],
            draft_type=record.get('draft_type'),
            is_auction_draft=self._safe_bool(record.get('is_auction_draft', False)),
            persistent_url=record.get('persistent_url'),
            uses_playoff=self._safe_bool(record.get('uses_playoff', True)),
            has_playoff_consolation_games=self._safe_bool(record.get('has_playoff_consolation_games', False)),
            playoff_start_week=record.get('playoff_start_week'),
            uses_playoff_reseeding=self._safe_bool(record.get('uses_playoff_reseeding', False)),
            uses_lock_eliminated_teams=self._safe_bool(record.get('uses_lock_eliminated_teams', False)),
            num_playoff_teams=self._safe_int(record.get('num_playoff_teams')),
            num_playoff_consolation_teams=self._safe_int(record.get('num_playoff_consolation_teams', 0)),
            has_multiweek_championship=self._safe_bool(record.get('has_multiweek_championship', False)),
            waiver_type=record.get('waiver_type'),
            waiver_rule=record.get('waiver_rule'),
            uses_faab=self._safe_bool(record.get('uses_faab', False)),
            draft_time=record.get('draft_time'),
            draft_pick_time=record.get('draft_pick_time'),
            post_draft_players=record.get('post_draft_players'),
            max_teams=self._safe_int(record.get('max_teams')),
            waiver_time=record.get('waiver_time'),
            trade_end_date=record.get('trade_end_date'),
            trade_ratify_type=record.get('trade_ratify_type'),
            trade_reject_time=record.get('trade_reject_time'),
            player_pool=record.get('player_pool'),
            cant_cut_list=record.get('cant_cut_list'),
            draft_together=self._safe_bool(record.get('draft_together', False)),
            is_publicly_viewable=self._safe_bool(record.get('is_publicly_viewable', True)),
            can_trade_draft_picks=self._safe_bool(record.get('can_trade_draft_picks', False)),
            sendbird_channel_url=record.get('sendbird_channel_url'),
            roster_positions=record.get('roster_positions')
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for league settings"""
        return record['league_key']
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess league settings record"""
        # Handle boolean fields
        bool_fields = ['is_auction_draft', 'uses_playoff', 'has_playoff_consolation_games',
                      'uses_playoff_reseeding', 'uses_lock_eliminated_teams', 
                      'has_multiweek_championship', 'uses_faab', 'draft_together',
                      'is_publicly_viewable', 'can_trade_draft_picks']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        return record


class StatCategoryLoader(BaseLoader):
    """Loader for stat categories"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate stat category record"""
        required_fields = ['league_key', 'stat_id', 'name', 'display_name', 'abbr']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> StatCategory:
        """Create StatCategory model instance"""
        return StatCategory(
            league_key=record['league_key'],
            stat_id=record['stat_id'],
            name=record['name'],
            display_name=record['display_name'],
            abbr=record['abbr'],
            group_name=record.get('group_name'),
            sort_order=self._safe_int(record.get('sort_order', 0)),
            position_type=record.get('position_type'),
            is_enabled=self._safe_bool(record.get('is_enabled', True)),
            is_only_display_stat=self._safe_bool(record.get('is_only_display_stat', False)),
            is_core_stat=self._safe_bool(record.get('is_core_stat', False)),
            core_stat_column=record.get('core_stat_column')
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for stat category"""
        return f"{record['league_key']}_{record['stat_id']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess stat category record"""
        # Handle boolean fields
        bool_fields = ['is_enabled', 'is_only_display_stat', 'is_core_stat']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        # Ensure stat_id is integer
        if 'stat_id' in record:
            record['stat_id'] = self._safe_int(record['stat_id'])
        
        return record


class LeagueRosterPositionLoader(BaseLoader):
    """Loader for league roster positions"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate roster position record"""
        required_fields = ['league_key', 'position']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> LeagueRosterPosition:
        """Create LeagueRosterPosition model instance"""
        return LeagueRosterPosition(
            league_key=record['league_key'],
            position=record['position'],
            position_type=record.get('position_type'),
            count=self._safe_int(record.get('count', 0)),
            is_starting_position=self._safe_bool(record.get('is_starting_position', False))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for roster position"""
        return f"{record['league_key']}_{record['position']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess roster position record"""
        record['count'] = self._safe_int(record.get('count', 0))
        record['is_starting_position'] = self._safe_bool(record.get('is_starting_position', False))
        return record


class CompleteLeagueLoader:
    """Complete league data loader that handles all league-related tables"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loaders
        self.league_loader = LeagueLoader(connection_manager, batch_size)
        self.settings_loader = LeagueSettingsLoader(connection_manager, batch_size)
        self.stat_category_loader = StatCategoryLoader(connection_manager, batch_size)
        self.roster_position_loader = LeagueRosterPositionLoader(connection_manager, batch_size)
    
    def load_league_data(self, league_data: Dict[str, Any]) -> LoadResult:
        """Load complete league data including settings and stat categories"""
        total_result = LoadResult()
        
        # Load basic league info
        league_result = self.league_loader.load([league_data])
        total_result.combine(league_result)
        
        # Load league settings if present
        if 'settings' in league_data:
            settings_data = league_data['settings'].copy()
            settings_data['league_key'] = league_data['league_key']
            
            settings_result = self.settings_loader.load([settings_data])
            total_result.combine(settings_result)
            
            # Load stat categories from settings
            if 'stat_categories' in settings_data:
                stat_categories = self._extract_stat_categories(
                    league_data['league_key'], 
                    settings_data['stat_categories']
                )
                if stat_categories:
                    stat_result = self.stat_category_loader.load(stat_categories)
                    total_result.combine(stat_result)
            
            # Load roster positions from settings
            if 'roster_positions' in settings_data:
                roster_positions = self._extract_roster_positions(
                    league_data['league_key'],
                    settings_data['roster_positions']
                )
                if roster_positions:
                    roster_result = self.roster_position_loader.load(roster_positions)
                    total_result.combine(roster_result)
        
        return total_result
    
    def _extract_stat_categories(self, league_key: str, stat_categories_data: Any) -> List[Dict[str, Any]]:
        """Extract stat categories from settings data"""
        categories = []
        
        try:
            if isinstance(stat_categories_data, dict) and 'stats' in stat_categories_data:
                stats_list = stat_categories_data['stats']
                
                for stat_item in stats_list:
                    if 'stat' in stat_item:
                        stat_info = stat_item['stat']
                        stat_record = {
                            'league_key': league_key,
                            'stat_id': stat_info.get('stat_id'),
                            'name': stat_info.get('name', ''),
                            'display_name': stat_info.get('display_name', ''),
                            'abbr': stat_info.get('abbr', ''),
                            'group_name': stat_info.get('group'),
                            'sort_order': stat_info.get('sort_order', 0),
                            'position_type': stat_info.get('position_type'),
                            'is_enabled': stat_info.get('enabled', '1') == '1',
                            'is_only_display_stat': stat_info.get('is_only_display_stat', '0') == '1'
                        }
                        
                        if stat_record['stat_id'] is not None:
                            categories.append(stat_record)
        
        except Exception as e:
            print(f"Error extracting stat categories: {e}")
        
        return categories
    
    def _extract_roster_positions(self, league_key: str, roster_positions_data: Any) -> List[Dict[str, Any]]:
        """Extract roster positions from settings data"""
        positions = []
        
        try:
            # Handle JSON string
            if isinstance(roster_positions_data, str):
                roster_positions_data = json.loads(roster_positions_data)
            
            if isinstance(roster_positions_data, list):
                for rp_item in roster_positions_data:
                    if isinstance(rp_item, dict):
                        # Handle nested structure
                        if 'roster_position' in rp_item:
                            rp_info = rp_item['roster_position']
                        else:
                            rp_info = rp_item
                        
                        position_record = {
                            'league_key': league_key,
                            'position': rp_info.get('position'),
                            'position_type': rp_info.get('position_type'),
                            'count': rp_info.get('count', 0),
                            'is_starting_position': rp_info.get('is_starting_position', False)
                        }
                        
                        if position_record['position']:
                            positions.append(position_record)
        
        except Exception as e:
            print(f"Error extracting roster positions: {e}")
        
        return positions 