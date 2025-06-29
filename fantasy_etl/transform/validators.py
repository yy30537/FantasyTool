# 数据验证器
#
# 迁移来源: 从旧脚本中的隐式验证逻辑中提取
# 主要职责: 将分散在各个脚本中的数据验证逻辑集中化
#
# 职责:
#   - 核心字段验证：
#     * 必需字段存在性检查：player_key、team_key、league_key等
#     * 字段格式验证：键值格式、日期格式等
#     * 数据类型验证：字符串、整数、布尔值等
#   - 业务规则验证：
#     * 联盟配置合理性：num_teams、playoff配置等  
#     * 球员信息一致性：位置、团队归属等
#     * 统计数据合理性：数值范围、逻辑关系等
#   - 关系约束验证：
#     * 外键关系：team_key存在于teams中等
#     * 数据一致性：日期范围、赛季信息等
#     * 引用完整性：球员-团队关系等
#   - 数据质量检查：
#     * 重复数据检测
#     * 缺失值处理策略
#     * 异常值识别
#   - 验证结果报告：
#     * 错误类型分类
#     * 严重程度评估
#     * 修复建议生成
#
# 输入: 各种标准化后的数据对象
# 输出: 验证结果对象，包含错误列表和警告信息 

import re
import logging
from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """验证错误严重程度"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationResult:
    """验证结果"""
    
    def __init__(self):
        self.is_valid = True
        self.errors = []
        self.warnings = []
        self.info = []
    
    def add_error(self, field: str, message: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
        """添加验证错误"""
        error_info = {
            'field': field,
            'message': message,
            'severity': severity.value,
            'timestamp': datetime.now()
        }
        
        if severity == ValidationSeverity.CRITICAL or severity == ValidationSeverity.ERROR:
            self.errors.append(error_info)
            self.is_valid = False
        elif severity == ValidationSeverity.WARNING:
            self.warnings.append(error_info)
        else:
            self.info.append(error_info)
    
    def get_all_issues(self) -> List[Dict]:
        """获取所有问题"""
        return self.errors + self.warnings + self.info
    
    def get_summary(self) -> Dict[str, int]:
        """获取问题摘要"""
        return {
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'total_info': len(self.info),
            'is_valid': self.is_valid
        }


class DataValidator:
    """数据验证器 - 集中化所有数据验证逻辑"""
    
    def __init__(self):
        """初始化数据验证器"""
        self.stats = {
            'validated_records': 0,
            'failed_validations': 0,
            'warnings_generated': 0,
            'errors_generated': 0
        }
        
        # 预定义的验证规则
        self._setup_validation_rules()
    
    def _setup_validation_rules(self):
        """设置验证规则"""
        # Yahoo Fantasy API键格式规则
        self.key_patterns = {
            'game_key': r'^\d+$',  # 纯数字
            'league_key': r'^\d+\.l\.\d+$',  # 格式: 游戏ID.l.联盟ID
            'team_key': r'^\d+\.l\.\d+\.t\.\d+$',  # 格式: 游戏ID.l.联盟ID.t.团队ID
            'player_key': r'^\d+\.p\.\d+$',  # 格式: 游戏ID.p.球员ID
            'transaction_key': r'^\d+\.l\.\d+\.tr\.\d+$'  # 格式: 游戏ID.l.联盟ID.tr.交易ID
        }
        
        # 统计值合理范围
        self.stat_ranges = {
            'field_goal_percentage': (0, 100),
            'free_throw_percentage': (0, 100),
            'three_pointers_made': (0, 20),  # 单场最多三分
            'total_points': (0, 100),  # 单场最多得分
            'total_rebounds': (0, 30),  # 单场最多篮板
            'total_assists': (0, 30),  # 单场最多助攻
            'total_steals': (0, 15),  # 单场最多抢断
            'total_blocks': (0, 15),  # 单场最多盖帽
            'total_turnovers': (0, 15)  # 单场最多失误
        }
        
        # 位置有效值
        self.valid_positions = {
            'PG', 'SG', 'SF', 'PF', 'C',  # 基本位置
            'G', 'F',  # 组合位置
            'BN', 'IL', 'IR', 'NA'  # 特殊位置
        }
        
        # 联盟设置合理范围
        self.league_settings_ranges = {
            'num_teams': (4, 20),  # 联盟团队数
            'max_teams': (4, 20),
            'scoring_type': {'head_to_head', 'roto'},
            'max_games_per_week': (1, 7)
        }
    
    # ===== 核心字段验证 =====
    
    def validate_required_fields(self, data: Dict[str, Any], 
                                required_fields: List[str]) -> ValidationResult:
        """验证必需字段存在性"""
        result = ValidationResult()
        
        for field in required_fields:
            if field not in data:
                result.add_error(field, f"必需字段 '{field}' 缺失", ValidationSeverity.ERROR)
            elif data[field] is None:
                result.add_error(field, f"必需字段 '{field}' 为空", ValidationSeverity.ERROR)
            elif isinstance(data[field], str) and data[field].strip() == '':
                result.add_error(field, f"必需字段 '{field}' 为空字符串", ValidationSeverity.ERROR)
        
        return result
    
    def validate_field_format(self, data: Dict[str, Any], 
                            field_formats: Dict[str, str]) -> ValidationResult:
        """验证字段格式"""
        result = ValidationResult()
        
        for field, pattern in field_formats.items():
            if field in data and data[field] is not None:
                value = str(data[field]).strip()
                if value and not re.match(pattern, value):
                    result.add_error(field, f"字段 '{field}' 格式不正确: {value}", ValidationSeverity.ERROR)
        
        return result
    
    def validate_data_types(self, data: Dict[str, Any], 
                          type_requirements: Dict[str, type]) -> ValidationResult:
        """验证数据类型"""
        result = ValidationResult()
        
        for field, expected_type in type_requirements.items():
            if field in data and data[field] is not None:
                value = data[field]
                if not isinstance(value, expected_type):
                    # 尝试类型转换
                    try:
                        if expected_type == int:
                            int(value)
                        elif expected_type == float:
                            float(value)
                        elif expected_type == bool:
                            if isinstance(value, str):
                                value.lower() in ('true', 'false', '1', '0')
                            else:
                                bool(value)
                    except (ValueError, TypeError):
                        result.add_error(field, 
                                       f"字段 '{field}' 类型错误: 期望 {expected_type.__name__}, 得到 {type(value).__name__}",
                                       ValidationSeverity.ERROR)
        
        return result
    
    def validate_yahoo_keys(self, data: Dict[str, Any]) -> ValidationResult:
        """验证Yahoo Fantasy API键格式"""
        result = ValidationResult()
        
        for key_type, pattern in self.key_patterns.items():
            if key_type in data and data[key_type] is not None:
                value = str(data[key_type]).strip()
                if value and not re.match(pattern, value):
                    result.add_error(key_type, 
                                   f"Yahoo API键格式错误: {value} 不匹配 {key_type} 格式",
                                   ValidationSeverity.ERROR)
        
        return result
    
    # ===== 业务规则验证 =====
    
    def validate_player_data(self, player_data: Dict[str, Any]) -> ValidationResult:
        """验证球员数据业务规则"""
        result = ValidationResult()
        
        # 必需字段检查
        required_fields = ['player_key', 'display_name']
        result_required = self.validate_required_fields(player_data, required_fields)
        result.errors.extend(result_required.errors)
        result.warnings.extend(result_required.warnings)
        
        # Yahoo键格式检查
        result_keys = self.validate_yahoo_keys(player_data)
        result.errors.extend(result_keys.errors)
        
        # 位置验证
        if 'eligible_positions' in player_data:
            positions = player_data['eligible_positions']
            if isinstance(positions, list):
                for pos in positions:
                    pos_str = pos if isinstance(pos, str) else pos.get('position') if isinstance(pos, dict) else None
                    if pos_str and pos_str not in self.valid_positions:
                        result.add_error('eligible_positions', 
                                       f"无效位置: {pos_str}",
                                       ValidationSeverity.WARNING)
        
        # 球员信息一致性检查
        if 'team_key' in player_data and player_data['team_key']:
            team_key = str(player_data['team_key'])
            if not re.match(self.key_patterns['team_key'], team_key):
                result.add_error('team_key', f"球员的team_key格式错误: {team_key}", ValidationSeverity.ERROR)
        
        self.stats['validated_records'] += 1
        if not result.is_valid:
            self.stats['failed_validations'] += 1
        
        return result
    
    def validate_team_data(self, team_data: Dict[str, Any]) -> ValidationResult:
        """验证团队数据业务规则"""
        result = ValidationResult()
        
        # 必需字段检查
        required_fields = ['team_key', 'name']
        result_required = self.validate_required_fields(team_data, required_fields)
        result.errors.extend(result_required.errors)
        
        # Yahoo键格式检查
        result_keys = self.validate_yahoo_keys(team_data)
        result.errors.extend(result_keys.errors)
        
        # 团队数据合理性检查
        if 'number_of_trades' in team_data:
            trades = team_data['number_of_trades']
            if isinstance(trades, (int, str)):
                try:
                    trades_int = int(trades)
                    if trades_int < 0 or trades_int > 50:  # 赛季交易次数合理范围
                        result.add_error('number_of_trades', 
                                       f"团队交易次数异常: {trades_int}",
                                       ValidationSeverity.WARNING)
                except ValueError:
                    result.add_error('number_of_trades', 
                                   f"团队交易次数格式错误: {trades}",
                                   ValidationSeverity.ERROR)
        
        # 管理员信息验证
        if 'managers' in team_data and isinstance(team_data['managers'], list):
            managers = team_data['managers']
            if len(managers) == 0:
                result.add_error('managers', "团队必须有至少一个管理员", ValidationSeverity.ERROR)
            elif len(managers) > 3:  # 通常一个团队不会有太多管理员
                result.add_error('managers', f"团队管理员过多: {len(managers)}", ValidationSeverity.WARNING)
        
        self.stats['validated_records'] += 1
        if not result.is_valid:
            self.stats['failed_validations'] += 1
        
        return result
    
    def validate_league_data(self, league_data: Dict[str, Any]) -> ValidationResult:
        """验证联盟数据业务规则"""
        result = ValidationResult()
        
        # 必需字段检查
        required_fields = ['league_key', 'name', 'season']
        result_required = self.validate_required_fields(league_data, required_fields)
        result.errors.extend(result_required.errors)
        
        # Yahoo键格式检查
        result_keys = self.validate_yahoo_keys(league_data)
        result.errors.extend(result_keys.errors)
        
        # 联盟设置验证
        if 'num_teams' in league_data:
            num_teams = league_data['num_teams']
            if isinstance(num_teams, (int, str)):
                try:
                    teams_int = int(num_teams)
                    min_teams, max_teams = self.league_settings_ranges['num_teams']
                    if teams_int < min_teams or teams_int > max_teams:
                        result.add_error('num_teams', 
                                       f"联盟团队数量异常: {teams_int} (合理范围: {min_teams}-{max_teams})",
                                       ValidationSeverity.WARNING)
                except ValueError:
                    result.add_error('num_teams', f"联盟团队数量格式错误: {num_teams}", ValidationSeverity.ERROR)
        
        # 赛季年份验证
        if 'season' in league_data:
            season = league_data['season']
            if isinstance(season, (int, str)):
                try:
                    season_int = int(season)
                    current_year = datetime.now().year
                    if season_int < 2000 or season_int > current_year + 1:
                        result.add_error('season', 
                                       f"联盟赛季年份异常: {season_int}",
                                       ValidationSeverity.WARNING)
                except ValueError:
                    result.add_error('season', f"联盟赛季格式错误: {season}", ValidationSeverity.ERROR)
        
        self.stats['validated_records'] += 1
        if not result.is_valid:
            self.stats['failed_validations'] += 1
        
        return result
    
    def validate_stats_data(self, stats_data: Dict[str, Any]) -> ValidationResult:
        """验证统计数据合理性"""
        result = ValidationResult()
        
        # 统计值范围检查
        for stat_name, value in stats_data.items():
            if stat_name in self.stat_ranges and value is not None:
                try:
                    stat_value = float(value)
                    min_val, max_val = self.stat_ranges[stat_name]
                    
                    if stat_value < min_val or stat_value > max_val:
                        result.add_error(stat_name, 
                                       f"统计值超出合理范围: {stat_value} (范围: {min_val}-{max_val})",
                                       ValidationSeverity.WARNING)
                except (ValueError, TypeError):
                    result.add_error(stat_name, f"统计值格式错误: {value}", ValidationSeverity.ERROR)
        
        # 投篮命中率逻辑验证
        if 'field_goals_made' in stats_data and 'field_goals_attempted' in stats_data:
            try:
                made = int(stats_data['field_goals_made'] or 0)
                attempted = int(stats_data['field_goals_attempted'] or 0)
                
                if made > attempted:
                    result.add_error('field_goals_made', 
                                   f"投篮命中数({made})不能大于投篮数({attempted})",
                                   ValidationSeverity.ERROR)
                
                if attempted > 0:
                    calculated_pct = (made / attempted) * 100
                    if 'field_goal_percentage' in stats_data and stats_data['field_goal_percentage'] is not None:
                        reported_pct = float(stats_data['field_goal_percentage'])
                        if abs(calculated_pct - reported_pct) > 1:  # 允许1%的误差
                            result.add_error('field_goal_percentage', 
                                           f"投篮命中率不一致: 计算值{calculated_pct:.1f}%, 报告值{reported_pct:.1f}%",
                                           ValidationSeverity.WARNING)
                            
            except (ValueError, TypeError) as e:
                result.add_error('field_goals', f"投篮数据格式错误: {e}", ValidationSeverity.ERROR)
        
        # 罚球命中率逻辑验证
        if 'free_throws_made' in stats_data and 'free_throws_attempted' in stats_data:
            try:
                made = int(stats_data['free_throws_made'] or 0)
                attempted = int(stats_data['free_throws_attempted'] or 0)
                
                if made > attempted:
                    result.add_error('free_throws_made', 
                                   f"罚球命中数({made})不能大于罚球数({attempted})",
                                   ValidationSeverity.ERROR)
                                   
            except (ValueError, TypeError) as e:
                result.add_error('free_throws', f"罚球数据格式错误: {e}", ValidationSeverity.ERROR)
        
        return result
    
    # ===== 关系约束验证 =====
    
    def validate_foreign_key_relationships(self, data: Dict[str, Any], 
                                         relationship_rules: Dict[str, Dict]) -> ValidationResult:
        """验证外键关系
        
        Args:
            data: 待验证数据
            relationship_rules: 关系规则，格式: {field: {table: str, exists_check: callable}}
        """
        result = ValidationResult()
        
        for field, rule in relationship_rules.items():
            if field in data and data[field] is not None:
                foreign_key = data[field]
                table_name = rule.get('table')
                exists_check = rule.get('exists_check')
                
                if exists_check and callable(exists_check):
                    try:
                        if not exists_check(foreign_key):
                            result.add_error(field, 
                                           f"外键关系验证失败: {field}={foreign_key} 在 {table_name} 中不存在",
                                           ValidationSeverity.ERROR)
                    except Exception as e:
                        result.add_error(field, 
                                       f"外键关系检查失败: {e}",
                                       ValidationSeverity.WARNING)
        
        return result
    
    def validate_data_consistency(self, new_data: Dict[str, Any], 
                                existing_data: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """验证数据一致性"""
        result = ValidationResult()
        
        if existing_data:
            # 检查关键字段是否发生了不应该的变更
            immutable_fields = ['player_key', 'team_key', 'league_key', 'game_key']
            
            for field in immutable_fields:
                if field in new_data and field in existing_data:
                    if new_data[field] != existing_data[field]:
                        result.add_error(field, 
                                       f"不可变字段发生变更: {existing_data[field]} -> {new_data[field]}",
                                       ValidationSeverity.ERROR)
        
        # 日期一致性检查
        if 'season' in new_data and 'date' in new_data:
            try:
                season = int(new_data['season'])
                date_obj = new_data['date']
                if isinstance(date_obj, str):
                    date_obj = datetime.strptime(date_obj[:10], '%Y-%m-%d').date()
                elif isinstance(date_obj, datetime):
                    date_obj = date_obj.date()
                
                if isinstance(date_obj, date):
                    # NBA赛季通常跨年：例如2023-24赛季从2023年10月到2024年4月
                    season_start_year = season
                    season_end_year = season + 1
                    
                    if not (date_obj.year == season_start_year or date_obj.year == season_end_year):
                        result.add_error('date', 
                                       f"日期与赛季不一致: {date_obj} 不在 {season}-{season_end_year} 赛季范围内",
                                       ValidationSeverity.WARNING)
                        
            except (ValueError, TypeError) as e:
                result.add_error('date', f"日期格式错误: {e}", ValidationSeverity.ERROR)
        
        return result
    
    # ===== 数据质量检查 =====
    
    def detect_duplicate_records(self, records: List[Dict[str, Any]], 
                               unique_fields: List[str]) -> ValidationResult:
        """检测重复记录"""
        result = ValidationResult()
        
        seen_combinations = set()
        duplicate_indices = []
        
        for i, record in enumerate(records):
            # 构建唯一键
            unique_key = tuple(record.get(field) for field in unique_fields)
            
            if unique_key in seen_combinations:
                duplicate_indices.append(i)
                result.add_error('duplicate_record', 
                               f"发现重复记录 (索引: {i}): {dict(zip(unique_fields, unique_key))}",
                               ValidationSeverity.WARNING)
            else:
                seen_combinations.add(unique_key)
        
        return result
    
    def validate_completeness(self, data: Dict[str, Any], 
                            expected_fields: List[str],
                            completeness_threshold: float = 0.8) -> ValidationResult:
        """验证数据完整性"""
        result = ValidationResult()
        
        total_fields = len(expected_fields)
        present_fields = sum(1 for field in expected_fields if field in data and data[field] is not None)
        completeness_ratio = present_fields / total_fields if total_fields > 0 else 0
        
        if completeness_ratio < completeness_threshold:
            result.add_error('completeness', 
                           f"数据完整性不足: {completeness_ratio:.2%} < {completeness_threshold:.2%}",
                           ValidationSeverity.WARNING)
        
        # 记录缺失字段
        missing_fields = [field for field in expected_fields if field not in data or data[field] is None]
        if missing_fields:
            result.add_error('missing_fields', 
                           f"缺失字段: {missing_fields}",
                           ValidationSeverity.INFO)
        
        return result
    
    # ===== 批量验证方法 =====
    
    def validate_player_record(self, player_data: Dict[str, Any]) -> ValidationResult:
        """验证单个球员记录"""
        result = ValidationResult()
        
        # 球员数据验证
        player_result = self.validate_player_data(player_data)
        result.errors.extend(player_result.errors)
        result.warnings.extend(player_result.warnings)
        result.info.extend(player_result.info)
        
        # 如果有统计数据，验证统计数据
        if 'stats' in player_data and isinstance(player_data['stats'], dict):
            stats_result = self.validate_stats_data(player_data['stats'])
            result.errors.extend(stats_result.errors)
            result.warnings.extend(stats_result.warnings)
            result.info.extend(stats_result.info)
        
        if result.errors:
            result.is_valid = False
            
        return result
    
    def validate_batch_records(self, records: List[Dict[str, Any]], 
                             record_type: str = 'player') -> Dict[str, ValidationResult]:
        """批量验证记录"""
        results = {}
        
        for i, record in enumerate(records):
            if record_type == 'player':
                result = self.validate_player_record(record)
            elif record_type == 'team':
                result = self.validate_team_data(record)
            elif record_type == 'league':
                result = self.validate_league_data(record)
            else:
                result = ValidationResult()
                result.add_error('unknown_type', f"未知记录类型: {record_type}", ValidationSeverity.ERROR)
            
            results[f"{record_type}_{i}"] = result
        
        return results
    
    def get_validation_summary(self, results: Dict[str, ValidationResult]) -> Dict[str, Any]:
        """获取验证摘要"""
        total_records = len(results)
        valid_records = sum(1 for result in results.values() if result.is_valid)
        invalid_records = total_records - valid_records
        
        total_errors = sum(len(result.errors) for result in results.values())
        total_warnings = sum(len(result.warnings) for result in results.values())
        total_info = sum(len(result.info) for result in results.values())
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': invalid_records,
            'success_rate': valid_records / total_records if total_records > 0 else 0,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'total_info': total_info,
            'validation_stats': self.stats.copy()
        }
    
    def get_validation_stats(self) -> Dict[str, int]:
        """获取验证统计信息"""
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        for key in self.stats:
            self.stats[key] = 0


# 全局验证器实例
default_validator = DataValidator()

# 便捷函数
def validate_player(player_data: Dict[str, Any]) -> ValidationResult:
    """球员数据验证便捷函数"""
    return default_validator.validate_player_record(player_data)

def validate_team(team_data: Dict[str, Any]) -> ValidationResult:
    """团队数据验证便捷函数"""
    return default_validator.validate_team_data(team_data)

def validate_league(league_data: Dict[str, Any]) -> ValidationResult:
    """联盟数据验证便捷函数"""
    return default_validator.validate_league_data(league_data)

def validate_yahoo_key(key: str, key_type: str) -> bool:
    """Yahoo API键验证便捷函数"""
    validator = DataValidator()
    return key_type in validator.key_patterns and re.match(validator.key_patterns[key_type], key) is not None 