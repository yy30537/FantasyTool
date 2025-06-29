# 阵容数据加载器
#
# 迁移来源: @database_writer.py 中的阵容相关写入逻辑
# 主要映射:
#   - write_roster_daily() -> RosterLoader.load_roster_daily()
#   - RosterDaily模型写入逻辑 -> 阵容数据加载功能
#
# 职责:
#   - 每日阵容数据加载：
#     * RosterDaily模型数据写入：team_key、player_key、date等
#     * 时间维度信息：date、season、week字段
#     * 位置分配信息：selected_position、is_starting等
#     * 球员状态信息：player_status、injury_note等
#   - 阵容位置分类：
#     * 首发状态：is_starting基于selected_position判断
#     * 替补状态：is_bench (selected_position == 'BN')
#     * 伤病名单：is_injured_reserve (position in ['IL', 'IR'])
#     * 位置验证：selected_position的有效性检查
#   - Keeper和Fantasy信息：
#     * Keeper状态：is_keeper、keeper_cost处理
#     * Fantasy设置：is_prescoring、is_editable标记
#     * 成本信息：keeper_cost字符串处理
#   - 时间序列数据管理：
#     * 日期范围处理：单日vs多日阵容数据
#     * 历史数据保持：已有阵容记录的保护
#     * 增量更新：新增日期的阵容数据
#     * 时间戳管理：更新时间的记录
#   - 数据验证和清洗：
#     * 必需字段验证：team_key、player_key、date、league_key
#     * 日期格式验证：date字段的正确性
#     * 位置代码验证：selected_position的有效性
#     * 状态字段验证：各种布尔标记的一致性
#   - 关联数据处理：
#     * 外键约束：team_key、player_key、league_key的引用
#     * 数据完整性：阵容-球员-团队关系的一致性
#     * 赛季一致性：date与season的匹配验证
#   - 去重和冲突处理：
#     * 唯一约束：(team_key, player_key, date)的唯一性
#     * 更新vs插入：已存在阵容记录的更新策略
#     * 冲突解决：同一日期多个位置分配的处理
#   - 批量处理优化：
#     * 按日期分批：同一日期的阵容批量处理
#     * 按团队分组：同一团队的阵容数据聚合
#     * 事务管理：大批量阵容数据的事务边界
#     * 内存管理：大时间范围数据的内存控制
#   - 业务规则验证：
#     * 阵容规则检查：首发位置数量限制
#     * 球员可用性：球员是否属于指定团队
#     * 日期合理性：阵容日期在赛季范围内
#     * 位置合理性：球员是否能打指定位置
#   - 增量和历史数据：
#     * 历史快照：保持每日阵容的完整记录
#     * 变更追踪：阵容变化的识别和记录
#     * 数据回填：补充缺失日期的阵容数据
#   - 统计和报告：
#     * 处理统计：加载的阵容记录数量
#     * 时间覆盖：处理的日期范围统计
#     * 团队分布：各团队的阵容数据量
#     * 位置分布：各位置的使用统计
#
# 输入: 标准化的阵容数据 (Dict或List[Dict])
# 输出: 阵容数据加载结果和统计信息 

"""
Roster Data Loader - Handles daily roster information
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, date

from .base_loader import BaseLoader, LoadResult
from ..database.models import RosterDaily


class RosterDailyLoader(BaseLoader):
    """Loader for daily roster information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate roster record"""
        required_fields = ['team_key', 'player_key', 'league_key', 'date', 'season']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> RosterDaily:
        """Create RosterDaily model instance"""
        return RosterDaily(
            team_key=record['team_key'],
            player_key=record['player_key'],
            league_key=record['league_key'],
            date=record['date'],
            season=record['season'],
            week=self._safe_int(record.get('week')),
            selected_position=record.get('selected_position'),
            is_starting=self._safe_bool(record.get('is_starting', False)),
            is_bench=self._safe_bool(record.get('is_bench', False)),
            is_injured_reserve=self._safe_bool(record.get('is_injured_reserve', False)),
            player_status=record.get('player_status'),
            status_full=record.get('status_full'),
            injury_note=record.get('injury_note'),
            is_keeper=self._safe_bool(record.get('is_keeper', False)),
            keeper_cost=record.get('keeper_cost'),
            is_prescoring=self._safe_bool(record.get('is_prescoring', False)),
            is_editable=self._safe_bool(record.get('is_editable', False))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for roster record"""
        date_str = record['date'].strftime('%Y-%m-%d') if isinstance(record['date'], date) else str(record['date'])
        return f"{record['team_key']}_{record['player_key']}_{date_str}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess roster record"""
        # Handle date field
        if 'date' in record and record['date']:
            if isinstance(record['date'], str):
                try:
                    record['date'] = datetime.strptime(record['date'], '%Y-%m-%d').date()
                except ValueError:
                    print(f"Invalid date format: {record['date']}")
                    return record
        
        # Handle boolean fields
        bool_fields = ['is_starting', 'is_bench', 'is_injured_reserve', 
                      'is_keeper', 'is_prescoring', 'is_editable']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        # Handle integer fields
        if 'week' in record:
            record['week'] = self._safe_int(record['week'])
        
        # Determine position-based booleans if not explicitly set
        if 'selected_position' in record:
            selected_pos = record['selected_position']
            if selected_pos:
                if 'is_starting' not in record:
                    record['is_starting'] = selected_pos not in ['BN', 'IL', 'IR']
                if 'is_bench' not in record:
                    record['is_bench'] = selected_pos == 'BN'
                if 'is_injured_reserve' not in record:
                    record['is_injured_reserve'] = selected_pos in ['IL', 'IR']
        
        return record


class CompleteRosterLoader:
    """Complete roster data loader that handles daily roster information"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loader
        self.roster_loader = RosterDailyLoader(connection_manager, batch_size)
    
    def load_roster_data(self, roster_data: List[Dict[str, Any]]) -> LoadResult:
        """Load roster data for a specific date"""
        return self.roster_loader.load(roster_data)
    
    def load_team_roster_for_date(self, team_key: str, league_key: str, 
                                 roster_date: date, season: str,
                                 roster_entries: List[Dict[str, Any]]) -> LoadResult:
        """Load complete roster for a team on a specific date"""
        processed_entries = []
        
        for entry in roster_entries:
            processed_entry = entry.copy()
            processed_entry.update({
                'team_key': team_key,
                'league_key': league_key,
                'date': roster_date,
                'season': season
            })
            processed_entries.append(processed_entry)
        
        return self.roster_loader.load(processed_entries)
    
    def load_multi_team_roster_for_date(self, roster_data_by_team: Dict[str, List[Dict[str, Any]]], 
                                       league_key: str, roster_date: date, 
                                       season: str) -> LoadResult:
        """Load roster data for multiple teams on a specific date"""
        total_result = LoadResult()
        
        for team_key, roster_entries in roster_data_by_team.items():
            team_result = self.load_team_roster_for_date(
                team_key, league_key, roster_date, season, roster_entries
            )
            total_result.combine(team_result)
        
        return total_result
    
    def load_date_range_roster(self, team_key: str, league_key: str, 
                              season: str, roster_data_by_date: Dict[date, List[Dict[str, Any]]]) -> LoadResult:
        """Load roster data for a team across multiple dates"""
        total_result = LoadResult()
        
        for roster_date, roster_entries in roster_data_by_date.items():
            date_result = self.load_team_roster_for_date(
                team_key, league_key, roster_date, season, roster_entries
            )
            total_result.combine(date_result)
        
        return total_result 