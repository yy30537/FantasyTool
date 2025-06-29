# 数据质量检查器
#
# 迁移来源: 从旧脚本中提取隐式的数据质量检查逻辑
# 主要职责: 系统化的数据质量监控和报告
#
# 职责:
#   - 完整性检查：
#     * 数据覆盖度：所有期望的联盟、团队、球员是否存在
#     * 时间序列完整性：日期范围内的数据连续性
#     * 关系完整性：外键关系的一致性检查
#   - 准确性检查：
#     * 数据逻辑一致性：统计数据的合理性
#     * 跨表数据一致性：同一实体在不同表中的信息
#     * 业务规则符合性：联盟规则、球员规则等
#   - 及时性检查：
#     * 数据更新时效：最新数据的时间戳检查
#     * 历史数据稳定性：已有数据的变更监控
#     * 增量更新质量：新增数据的质量评估
#   - 唯一性检查：
#     * 主键唯一性验证
#     * 业务唯一性约束检查
#     * 重复数据检测和报告
#   - 有效性检查：
#     * 数据格式有效性：日期、数字、枚举值等
#     * 业务值有效性：统计范围、状态转换等
#     * 参照完整性：引用数据的存在性
#   - 质量度量：
#     * 数据质量评分计算
#     * 质量趋势分析
#     * 问题影响评估
#   - 质量报告：
#     * 质量问题分类和优先级
#     * 质量改进建议
#     * 质量监控仪表板数据
#
# 输入: 处理后的数据对象和质量规则配置
# 输出: 数据质量报告和问题清单 

import logging
import statistics
from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime, date, timedelta
from collections import defaultdict, Counter
from enum import Enum

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """数据质量等级"""
    EXCELLENT = "excellent"  # 90-100%
    GOOD = "good"           # 75-89%
    FAIR = "fair"           # 60-74%
    POOR = "poor"           # 40-59%
    CRITICAL = "critical"   # 0-39%


class QualityIssue:
    """质量问题"""
    
    def __init__(self, issue_type: str, severity: str, message: str, 
                affected_records: int = 0, field: Optional[str] = None):
        self.issue_type = issue_type
        self.severity = severity
        self.message = message
        self.affected_records = affected_records
        self.field = field
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'issue_type': self.issue_type,
            'severity': self.severity,
            'message': self.message,
            'affected_records': self.affected_records,
            'field': self.field,
            'timestamp': self.timestamp
        }


class QualityReport:
    """数据质量报告"""
    
    def __init__(self):
        self.overall_score = 0.0
        self.quality_level = QualityLevel.CRITICAL
        self.issues = []
        self.metrics = {}
        self.recommendations = []
        self.timestamp = datetime.now()
    
    def add_issue(self, issue: QualityIssue):
        """添加质量问题"""
        self.issues.append(issue)
    
    def add_metric(self, name: str, value: Any, target: Optional[Any] = None):
        """添加质量指标"""
        self.metrics[name] = {
            'value': value,
            'target': target,
            'timestamp': datetime.now()
        }
    
    def add_recommendation(self, recommendation: str):
        """添加改进建议"""
        self.recommendations.append(recommendation)
    
    def calculate_overall_score(self) -> float:
        """计算总体质量评分"""
        if not self.metrics:
            return 0.0
        
        scores = []
        
        # 基于各项指标计算评分
        for metric_name, metric_data in self.metrics.items():
            value = metric_data['value']
            target = metric_data.get('target')
            
            if isinstance(value, (int, float)) and isinstance(target, (int, float)):
                if target > 0:
                    score = min(100, (value / target) * 100)
                    scores.append(score)
            elif isinstance(value, (int, float)) and 0 <= value <= 100:
                scores.append(value)
        
        if scores:
            self.overall_score = sum(scores) / len(scores)
        else:
            self.overall_score = 0.0
        
        # 确定质量等级
        if self.overall_score >= 90:
            self.quality_level = QualityLevel.EXCELLENT
        elif self.overall_score >= 75:
            self.quality_level = QualityLevel.GOOD
        elif self.overall_score >= 60:
            self.quality_level = QualityLevel.FAIR
        elif self.overall_score >= 40:
            self.quality_level = QualityLevel.POOR
        else:
            self.quality_level = QualityLevel.CRITICAL
        
        return self.overall_score
    
    def get_summary(self) -> Dict[str, Any]:
        """获取报告摘要"""
        return {
            'overall_score': self.overall_score,
            'quality_level': self.quality_level.value,
            'total_issues': len(self.issues),
            'critical_issues': len([i for i in self.issues if i.severity == 'critical']),
            'error_issues': len([i for i in self.issues if i.severity == 'error']),
            'warning_issues': len([i for i in self.issues if i.severity == 'warning']),
            'total_metrics': len(self.metrics),
            'total_recommendations': len(self.recommendations),
            'timestamp': self.timestamp
        }


class DataQualityChecker:
    """数据质量检查器 - 系统化的数据质量监控和报告"""
    
    def __init__(self):
        """初始化数据质量检查器"""
        self.stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'issues_found': 0
        }
        
        # 质量标准阈值
        self.quality_thresholds = {
            'completeness': 0.95,    # 完整性阈值
            'accuracy': 0.98,        # 准确性阈值
            'consistency': 0.95,     # 一致性阈值
            'timeliness': 0.90,      # 及时性阈值
            'uniqueness': 0.99       # 唯一性阈值
        }
    
    # ===== 完整性检查 =====
    
    def check_data_completeness(self, records: List[Dict[str, Any]], 
                              expected_fields: List[str],
                              critical_fields: Optional[List[str]] = None) -> QualityReport:
        """检查数据完整性"""
        report = QualityReport()
        
        if not records:
            report.add_issue(QualityIssue('completeness', 'critical', '数据集为空', 0))
            report.add_metric('completeness_score', 0, 100)
            report.calculate_overall_score()
            return report
        
        total_records = len(records)
        critical_fields = critical_fields or []
        
        # 检查每个字段的完整性
        field_completeness = {}
        
        for field in expected_fields:
            present_count = sum(1 for record in records 
                              if field in record and record[field] is not None and record[field] != '')
            completeness_ratio = present_count / total_records
            field_completeness[field] = completeness_ratio
            
            # 检查是否低于阈值
            threshold = self.quality_thresholds['completeness']
            if completeness_ratio < threshold:
                severity = 'critical' if field in critical_fields else 'error'
                missing_count = total_records - present_count
                
                report.add_issue(QualityIssue(
                    'completeness', severity,
                    f"字段 '{field}' 完整性不足: {completeness_ratio:.2%} (缺失 {missing_count} 条记录)",
                    missing_count, field
                ))
        
        # 计算总体完整性
        overall_completeness = sum(field_completeness.values()) / len(expected_fields) if expected_fields else 0
        report.add_metric('completeness_score', overall_completeness * 100, 100)
        
        # 时间序列完整性检查
        if 'date' in expected_fields:
            date_completeness = self._check_time_series_completeness(records)
            report.add_metric('time_series_completeness', date_completeness * 100, 100)
            
            if date_completeness < self.quality_thresholds['completeness']:
                report.add_issue(QualityIssue(
                    'completeness', 'warning',
                    f"时间序列数据不完整: {date_completeness:.2%}",
                    int(total_records * (1 - date_completeness))
                ))
        
        # 关系完整性检查（外键）
        relationship_completeness = self._check_relationship_completeness(records)
        if relationship_completeness < 1.0:
            report.add_issue(QualityIssue(
                'completeness', 'error',
                f"关系完整性问题: {relationship_completeness:.2%}",
                int(total_records * (1 - relationship_completeness))
            ))
        
        report.calculate_overall_score()
        self._update_stats(report)
        return report
    
    def _check_time_series_completeness(self, records: List[Dict[str, Any]]) -> float:
        """检查时间序列数据完整性"""
        try:
            dates = []
            for record in records:
                if 'date' in record and record['date']:
                    date_obj = record['date']
                    if isinstance(date_obj, str):
                        date_obj = datetime.strptime(date_obj[:10], '%Y-%m-%d').date()
                    elif isinstance(date_obj, datetime):
                        date_obj = date_obj.date()
                    
                    if isinstance(date_obj, date):
                        dates.append(date_obj)
            
            if len(dates) < 2:
                return 1.0  # 数据太少，无法判断
            
            dates.sort()
            date_range = (dates[-1] - dates[0]).days + 1
            unique_dates = len(set(dates))
            
            return unique_dates / date_range if date_range > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"时间序列完整性检查失败: {e}")
            return 1.0
    
    def _check_relationship_completeness(self, records: List[Dict[str, Any]]) -> float:
        """检查关系完整性（简化版，实际应该连接数据库）"""
        # 这里简化实现，检查关键外键字段的存在性
        foreign_key_fields = ['team_key', 'league_key', 'player_key']
        
        total_checks = 0
        valid_checks = 0
        
        for record in records:
            for field in foreign_key_fields:
                if field in record:
                    total_checks += 1
                    if record[field] is not None and record[field] != '':
                        # 简化的格式检查（实际应该检查数据库中是否存在）
                        if self._is_valid_key_format(record[field], field):
                            valid_checks += 1
        
        return valid_checks / total_checks if total_checks > 0 else 1.0
    
    def _is_valid_key_format(self, key: str, key_type: str) -> bool:
        """检查键格式是否有效"""
        import re
        patterns = {
            'team_key': r'^\d+\.l\.\d+\.t\.\d+$',
            'league_key': r'^\d+\.l\.\d+$',
            'player_key': r'^\d+\.p\.\d+$'
        }
        
        pattern = patterns.get(key_type)
        if pattern:
            return re.match(pattern, str(key)) is not None
        return True
    
    # ===== 准确性检查 =====
    
    def check_data_accuracy(self, records: List[Dict[str, Any]]) -> QualityReport:
        """检查数据准确性"""
        report = QualityReport()
        
        if not records:
            report.add_metric('accuracy_score', 0, 100)
            report.calculate_overall_score()
            return report
        
        total_records = len(records)
        accuracy_issues = 0
        
        # 数据格式准确性
        format_accuracy = self._check_format_accuracy(records)
        accuracy_issues += total_records - format_accuracy['valid_records']
        
        # 业务规则准确性
        business_accuracy = self._check_business_rule_accuracy(records)
        accuracy_issues += business_accuracy['violations']
        
        # 统计数据逻辑准确性
        stats_accuracy = self._check_statistical_accuracy(records)
        accuracy_issues += stats_accuracy['invalid_stats']
        
        # 计算总体准确性
        overall_accuracy = max(0, (total_records - accuracy_issues) / total_records) if total_records > 0 else 0
        report.add_metric('accuracy_score', overall_accuracy * 100, 100)
        
        # 添加具体问题
        if format_accuracy['invalid_records'] > 0:
            report.add_issue(QualityIssue(
                'accuracy', 'error',
                f"数据格式错误: {format_accuracy['invalid_records']} 条记录",
                format_accuracy['invalid_records']
            ))
        
        if business_accuracy['violations'] > 0:
            report.add_issue(QualityIssue(
                'accuracy', 'warning',
                f"业务规则违反: {business_accuracy['violations']} 项",
                business_accuracy['violations']
            ))
        
        if stats_accuracy['invalid_stats'] > 0:
            report.add_issue(QualityIssue(
                'accuracy', 'error',
                f"统计数据逻辑错误: {stats_accuracy['invalid_stats']} 项",
                stats_accuracy['invalid_stats']
            ))
        
        report.calculate_overall_score()
        self._update_stats(report)
        return report
    
    def _check_format_accuracy(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """检查数据格式准确性"""
        valid_records = 0
        invalid_records = 0
        
        for record in records:
            is_valid = True
            
            # 检查关键字段格式
            for field, value in record.items():
                if field.endswith('_key') and value:
                    if not self._is_valid_key_format(value, field):
                        is_valid = False
                        break
                
                # 检查百分比字段
                if 'percentage' in field and value is not None:
                    try:
                        pct_val = float(value)
                        if pct_val < 0 or pct_val > 100:
                            is_valid = False
                            break
                    except (ValueError, TypeError):
                        is_valid = False
                        break
            
            if is_valid:
                valid_records += 1
            else:
                invalid_records += 1
        
        return {'valid_records': valid_records, 'invalid_records': invalid_records}
    
    def _check_business_rule_accuracy(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """检查业务规则准确性"""
        violations = 0
        
        for record in records:
            # 检查投篮命中率逻辑
            if all(k in record for k in ['field_goals_made', 'field_goals_attempted']):
                try:
                    made = int(record['field_goals_made'] or 0)
                    attempted = int(record['field_goals_attempted'] or 0)
                    if made > attempted:
                        violations += 1
                except (ValueError, TypeError):
                    violations += 1
            
            # 检查团队数量合理性
            if 'num_teams' in record:
                try:
                    num_teams = int(record['num_teams'])
                    if num_teams < 4 or num_teams > 20:
                        violations += 1
                except (ValueError, TypeError):
                    violations += 1
        
        return {'violations': violations}
    
    def _check_statistical_accuracy(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """检查统计数据准确性"""
        invalid_stats = 0
        
        for record in records:
            # 检查统计值范围
            stat_ranges = {
                'field_goal_percentage': (0, 100),
                'total_points': (0, 100),
                'total_rebounds': (0, 30),
                'total_assists': (0, 30)
            }
            
            for stat_name, (min_val, max_val) in stat_ranges.items():
                if stat_name in record and record[stat_name] is not None:
                    try:
                        value = float(record[stat_name])
                        if value < min_val or value > max_val:
                            invalid_stats += 1
                    except (ValueError, TypeError):
                        invalid_stats += 1
        
        return {'invalid_stats': invalid_stats}
    
    # ===== 及时性检查 =====
    
    def check_data_timeliness(self, records: List[Dict[str, Any]], 
                            expected_update_frequency: str = 'daily') -> QualityReport:
        """检查数据及时性"""
        report = QualityReport()
        
        if not records:
            report.add_metric('timeliness_score', 0, 100)
            report.calculate_overall_score()
            return report
        
        # 数据更新时效检查
        update_timeliness = self._check_update_timeliness(records, expected_update_frequency)
        report.add_metric('update_timeliness', update_timeliness * 100, 100)
        
        # 历史数据稳定性检查
        stability_score = self._check_historical_stability(records)
        report.add_metric('historical_stability', stability_score * 100, 100)
        
        # 增量更新质量检查
        incremental_quality = self._check_incremental_quality(records)
        report.add_metric('incremental_quality', incremental_quality * 100, 100)
        
        # 添加问题报告
        if update_timeliness < self.quality_thresholds['timeliness']:
            report.add_issue(QualityIssue(
                'timeliness', 'warning',
                f"数据更新不及时: {update_timeliness:.2%}",
                int(len(records) * (1 - update_timeliness))
            ))
        
        if stability_score < 0.9:
            report.add_issue(QualityIssue(
                'timeliness', 'warning',
                f"历史数据稳定性较差: {stability_score:.2%}",
                0
            ))
        
        report.calculate_overall_score()
        self._update_stats(report)
        return report
    
    def _check_update_timeliness(self, records: List[Dict[str, Any]], frequency: str) -> float:
        """检查数据更新时效性"""
        try:
            # 获取最新的时间戳
            latest_timestamps = []
            current_time = datetime.now()
            
            for record in records:
                for time_field in ['updated_at', 'created_at', 'timestamp', 'date']:
                    if time_field in record and record[time_field]:
                        timestamp = record[time_field]
                        if isinstance(timestamp, str):
                            try:
                                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            except:
                                continue
                        elif isinstance(timestamp, date):
                            timestamp = datetime.combine(timestamp, datetime.min.time())
                        
                        if isinstance(timestamp, datetime):
                            latest_timestamps.append(timestamp)
                        break
            
            if not latest_timestamps:
                return 1.0  # 无法判断，假设正常
            
            latest_timestamp = max(latest_timestamps)
            time_diff = current_time - latest_timestamp
            
            # 根据频率判断是否及时
            if frequency == 'daily':
                threshold = timedelta(days=1)
            elif frequency == 'weekly':
                threshold = timedelta(days=7)
            elif frequency == 'real-time':
                threshold = timedelta(hours=1)
            else:
                threshold = timedelta(days=1)
            
            return 1.0 if time_diff <= threshold else max(0, 1 - (time_diff.total_seconds() / threshold.total_seconds()))
            
        except Exception as e:
            logger.warning(f"及时性检查失败: {e}")
            return 1.0
    
    def _check_historical_stability(self, records: List[Dict[str, Any]]) -> float:
        """检查历史数据稳定性"""
        # 简化实现：检查关键字段的变化情况
        try:
            if len(records) < 2:
                return 1.0
            
            # 按时间排序（如果有时间字段）
            sorted_records = sorted(records, key=lambda x: x.get('date', ''), reverse=True)
            
            # 检查关键字段的稳定性
            stable_fields = 0
            total_fields = 0
            
            key_fields = ['player_key', 'team_key', 'league_key', 'display_name']
            
            for field in key_fields:
                if field in sorted_records[0]:
                    total_fields += 1
                    # 检查前10条记录的一致性
                    values = [r.get(field) for r in sorted_records[:10] if field in r]
                    if len(set(values)) <= 1:  # 值基本一致
                        stable_fields += 1
            
            return stable_fields / total_fields if total_fields > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"历史稳定性检查失败: {e}")
            return 1.0
    
    def _check_incremental_quality(self, records: List[Dict[str, Any]]) -> float:
        """检查增量更新质量"""
        # 简化实现：检查新增数据的质量
        try:
            if not records:
                return 1.0
            
            # 假设最新的记录是增量数据
            recent_records = records[:min(100, len(records))]
            
            quality_issues = 0
            total_checks = 0
            
            for record in recent_records:
                total_checks += 1
                
                # 检查关键字段是否缺失
                required_fields = ['player_key', 'team_key'] if 'player_key' in record else ['team_key', 'league_key']
                
                for field in required_fields:
                    if field not in record or not record[field]:
                        quality_issues += 1
                        break
            
            return max(0, (total_checks - quality_issues) / total_checks) if total_checks > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"增量质量检查失败: {e}")
            return 1.0
    
    # ===== 唯一性检查 =====
    
    def check_data_uniqueness(self, records: List[Dict[str, Any]], 
                            unique_constraints: List[List[str]]) -> QualityReport:
        """检查数据唯一性"""
        report = QualityReport()
        
        if not records:
            report.add_metric('uniqueness_score', 100, 100)
            report.calculate_overall_score()
            return report
        
        total_violations = 0
        total_records = len(records)
        
        for constraint in unique_constraints:
            violations = self._check_unique_constraint(records, constraint)
            total_violations += violations
            
            if violations > 0:
                report.add_issue(QualityIssue(
                    'uniqueness', 'error',
                    f"唯一性约束违反 {constraint}: {violations} 条重复记录",
                    violations
                ))
        
        # 主键唯一性检查
        primary_key_violations = self._check_primary_key_uniqueness(records)
        total_violations += primary_key_violations
        
        if primary_key_violations > 0:
            report.add_issue(QualityIssue(
                'uniqueness', 'critical',
                f"主键重复: {primary_key_violations} 条记录",
                primary_key_violations
            ))
        
        # 计算唯一性得分
        uniqueness_score = max(0, (total_records - total_violations) / total_records) if total_records > 0 else 1.0
        report.add_metric('uniqueness_score', uniqueness_score * 100, 100)
        
        report.calculate_overall_score()
        self._update_stats(report)
        return report
    
    def _check_unique_constraint(self, records: List[Dict[str, Any]], fields: List[str]) -> int:
        """检查唯一性约束"""
        seen_combinations = set()
        violations = 0
        
        for record in records:
            # 构建组合键
            key_values = tuple(record.get(field) for field in fields)
            
            # 跳过包含None的组合
            if None in key_values:
                continue
            
            if key_values in seen_combinations:
                violations += 1
            else:
                seen_combinations.add(key_values)
        
        return violations
    
    def _check_primary_key_uniqueness(self, records: List[Dict[str, Any]]) -> int:
        """检查主键唯一性"""
        # 根据记录类型确定主键
        primary_keys = {
            'player_key': ['player_key'],
            'team_key': ['team_key'],
            'league_key': ['league_key'],
            'transaction_key': ['transaction_key']
        }
        
        violations = 0
        
        for key_name, key_fields in primary_keys.items():
            if any(key_fields[0] in record for record in records):
                violations += self._check_unique_constraint(records, key_fields)
                break  # 只检查一种主键类型
        
        return violations
    
    # ===== 一致性检查 =====
    
    def check_data_consistency(self, records: List[Dict[str, Any]]) -> QualityReport:
        """检查数据一致性"""
        report = QualityReport()
        
        if not records:
            report.add_metric('consistency_score', 100, 100)
            report.calculate_overall_score()
            return report
        
        # 跨表数据一致性
        cross_table_consistency = self._check_cross_table_consistency(records)
        report.add_metric('cross_table_consistency', cross_table_consistency * 100, 100)
        
        # 业务规则一致性
        business_rule_consistency = self._check_business_rule_consistency(records)
        report.add_metric('business_rule_consistency', business_rule_consistency * 100, 100)
        
        # 数据格式一致性
        format_consistency = self._check_format_consistency(records)
        report.add_metric('format_consistency', format_consistency * 100, 100)
        
        # 添加问题报告
        if cross_table_consistency < self.quality_thresholds['consistency']:
            report.add_issue(QualityIssue(
                'consistency', 'error',
                f"跨表数据不一致: {cross_table_consistency:.2%}",
                int(len(records) * (1 - cross_table_consistency))
            ))
        
        if business_rule_consistency < self.quality_thresholds['consistency']:
            report.add_issue(QualityIssue(
                'consistency', 'warning',
                f"业务规则不一致: {business_rule_consistency:.2%}",
                int(len(records) * (1 - business_rule_consistency))
            ))
        
        report.calculate_overall_score()
        self._update_stats(report)
        return report
    
    def _check_cross_table_consistency(self, records: List[Dict[str, Any]]) -> float:
        """检查跨表数据一致性（简化版）"""
        # 简化实现：检查同一实体在不同记录中的信息一致性
        try:
            entity_info = defaultdict(dict)
            inconsistencies = 0
            total_checks = 0
            
            for record in records:
                # 检查球员信息一致性
                if 'player_key' in record:
                    player_key = record['player_key']
                    
                    # 检查球员姓名一致性
                    if 'display_name' in record:
                        total_checks += 1
                        if player_key in entity_info:
                            if 'display_name' in entity_info[player_key]:
                                if entity_info[player_key]['display_name'] != record['display_name']:
                                    inconsistencies += 1
                        else:
                            entity_info[player_key]['display_name'] = record['display_name']
            
            return max(0, (total_checks - inconsistencies) / total_checks) if total_checks > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"跨表一致性检查失败: {e}")
            return 1.0
    
    def _check_business_rule_consistency(self, records: List[Dict[str, Any]]) -> float:
        """检查业务规则一致性"""
        try:
            violations = 0
            total_checks = 0
            
            for record in records:
                # 检查赛季和日期的一致性
                if 'season' in record and 'date' in record:
                    total_checks += 1
                    try:
                        season = int(record['season'])
                        date_obj = record['date']
                        
                        if isinstance(date_obj, str):
                            date_obj = datetime.strptime(date_obj[:10], '%Y-%m-%d').date()
                        elif isinstance(date_obj, datetime):
                            date_obj = date_obj.date()
                        
                        if isinstance(date_obj, date):
                            # NBA赛季跨年检查
                            if not (date_obj.year == season or date_obj.year == season + 1):
                                violations += 1
                                
                    except (ValueError, TypeError):
                        violations += 1
            
            return max(0, (total_checks - violations) / total_checks) if total_checks > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"业务规则一致性检查失败: {e}")
            return 1.0
    
    def _check_format_consistency(self, records: List[Dict[str, Any]]) -> float:
        """检查数据格式一致性"""
        try:
            field_formats = defaultdict(set)
            inconsistent_fields = set()
            
            for record in records:
                for field, value in record.items():
                    if value is not None:
                        # 记录字段的数据类型
                        field_formats[field].add(type(value).__name__)
            
            # 检查每个字段是否有多种类型
            for field, types in field_formats.items():
                if len(types) > 1:
                    inconsistent_fields.add(field)
            
            total_fields = len(field_formats)
            consistent_fields = total_fields - len(inconsistent_fields)
            
            return consistent_fields / total_fields if total_fields > 0 else 1.0
            
        except Exception as e:
            logger.warning(f"格式一致性检查失败: {e}")
            return 1.0
    
    # ===== 综合质量评估 =====
    
    def comprehensive_quality_check(self, records: List[Dict[str, Any]], 
                                  config: Optional[Dict[str, Any]] = None) -> QualityReport:
        """综合数据质量检查"""
        config = config or {}
        
        # 获取配置参数
        expected_fields = config.get('expected_fields', [])
        critical_fields = config.get('critical_fields', [])
        unique_constraints = config.get('unique_constraints', [])
        update_frequency = config.get('update_frequency', 'daily')
        
        # 执行各项质量检查
        completeness_report = self.check_data_completeness(records, expected_fields, critical_fields)
        accuracy_report = self.check_data_accuracy(records)
        timeliness_report = self.check_data_timeliness(records, update_frequency)
        uniqueness_report = self.check_data_uniqueness(records, unique_constraints)
        consistency_report = self.check_data_consistency(records)
        
        # 合并报告
        comprehensive_report = QualityReport()
        
        # 合并所有指标
        all_reports = [completeness_report, accuracy_report, timeliness_report, 
                      uniqueness_report, consistency_report]
        
        for report in all_reports:
            comprehensive_report.issues.extend(report.issues)
            comprehensive_report.metrics.update(report.metrics)
        
        # 计算综合评分
        comprehensive_report.calculate_overall_score()
        
        # 生成改进建议
        self._generate_recommendations(comprehensive_report)
        
        return comprehensive_report
    
    def _generate_recommendations(self, report: QualityReport):
        """生成质量改进建议"""
        # 基于问题类型生成建议
        issue_types = Counter(issue.issue_type for issue in report.issues)
        
        if 'completeness' in issue_types:
            report.add_recommendation("增强数据收集流程，确保关键字段的完整性")
            report.add_recommendation("实施数据验证规则，在数据入库前进行完整性检查")
        
        if 'accuracy' in issue_types:
            report.add_recommendation("建立数据验证机制，加强输入数据的格式检查")
            report.add_recommendation("定期进行数据校验，识别和修正错误数据")
        
        if 'timeliness' in issue_types:
            report.add_recommendation("优化数据更新频率，建立自动化数据同步机制")
            report.add_recommendation("监控数据延迟，设置及时性告警")
        
        if 'uniqueness' in issue_types:
            report.add_recommendation("强化唯一性约束，防止重复数据插入")
            report.add_recommendation("定期执行去重操作，清理历史重复数据")
        
        if 'consistency' in issue_types:
            report.add_recommendation("建立数据一致性检查规则，确保跨系统数据同步")
            report.add_recommendation("统一数据标准和格式规范")
        
        # 基于质量等级生成建议
        if report.quality_level == QualityLevel.CRITICAL:
            report.add_recommendation("立即停止使用当前数据，进行全面数据清理")
        elif report.quality_level == QualityLevel.POOR:
            report.add_recommendation("制定数据质量改进计划，优先解决关键问题")
        elif report.quality_level == QualityLevel.FAIR:
            report.add_recommendation("持续监控数据质量，逐步提升数据标准")
    
    def _update_stats(self, report: QualityReport):
        """更新统计信息"""
        self.stats['total_checks'] += 1
        if report.quality_level in [QualityLevel.EXCELLENT, QualityLevel.GOOD]:
            self.stats['passed_checks'] += 1
        else:
            self.stats['failed_checks'] += 1
        
        self.stats['issues_found'] += len(report.issues)
    
    def get_quality_stats(self) -> Dict[str, int]:
        """获取质量检查统计信息"""
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        for key in self.stats:
            self.stats[key] = 0


# 全局质量检查器实例
default_quality_checker = DataQualityChecker()

# 便捷函数
def check_completeness(records: List[Dict[str, Any]], expected_fields: List[str]) -> QualityReport:
    """数据完整性检查便捷函数"""
    return default_quality_checker.check_data_completeness(records, expected_fields)

def check_accuracy(records: List[Dict[str, Any]]) -> QualityReport:
    """数据准确性检查便捷函数"""
    return default_quality_checker.check_data_accuracy(records)

def comprehensive_check(records: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None) -> QualityReport:
    """综合质量检查便捷函数"""
    return default_quality_checker.comprehensive_quality_check(records, config) 