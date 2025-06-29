#!/usr/bin/env python3
"""
统计数据标准化器

迁移来源: @database_writer.py 中的统计数据处理逻辑
主要映射:
  - _extract_core_player_season_stats() -> normalize_season_stats()
  - _extract_core_daily_stats() -> normalize_daily_stats()
  - _extract_team_weekly_stats() -> normalize_team_stats()
  - normalize_stat_id_mapping() -> normalize_stats_array()

职责:
  - 标准化统计数据格式：
    * 将API返回的stats数组转换为stat_id->value字典
    * 处理嵌套的stat结构：stats[i]["stat"]["stat_id/value"]
  - 统计类别映射：
    * 将Yahoo stat_id映射到标准统计项名称
    * 建立统计类别到数据库列的映射关系
  - 统计值类型转换：
    * 整数统计：得分、篮板、助攻等
    * 百分比统计：投篮命中率、罚球命中率等  
    * 复合统计：投篮made/attempted格式解析
  - 特殊值处理：
    * 空值、'-'、'N/A'等无效值处理
    * 百分比格式标准化：从字符串转为数值
  - 数据验证：
    * 统计值合理性检查
    * 数据类型验证

输入: Yahoo API stats数组 (List[Dict]) 或 统计值字典 (Dict[str, Any])
输出: 标准化的统计值字典 (Dict[str, Any])
"""

from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
from ..stats.stat_utils import (
    safe_int_conversion, safe_float_conversion, 
    parse_percentage_value, parse_made_attempted_stat,
    validate_stat_range, validate_made_attempted_logic,
    calculate_percentage, normalize_stat_id_mapping
)


# Yahoo Fantasy NBA 核心统计项映射
CORE_STAT_MAPPINGS = {
    # 投篮统计
    "9004003": {"type": "made_attempted", "field_prefix": "field_goals"},  # FGM/A
    "5": {"type": "percentage", "field": "field_goal_percentage"},         # FG%
    
    # 罚球统计
    "9007006": {"type": "made_attempted", "field_prefix": "free_throws"},  # FTM/A
    "8": {"type": "percentage", "field": "free_throw_percentage"},         # FT%
    
    # 其他统计
    "10": {"type": "integer", "field": "three_pointers_made"},            # 3PTM
    "12": {"type": "integer", "field": "points"},                         # PTS
    "15": {"type": "integer", "field": "rebounds"},                       # REB
    "16": {"type": "integer", "field": "assists"},                        # AST
    "17": {"type": "integer", "field": "steals"},                         # ST
    "18": {"type": "integer", "field": "blocks"},                         # BLK
    "19": {"type": "integer", "field": "turnovers"}                       # TO
}

# 赛季统计字段映射（累计字段）
SEASON_STAT_FIELD_MAPPINGS = {
    "points": "total_points",
    "rebounds": "total_rebounds", 
    "assists": "total_assists",
    "steals": "total_steals",
    "blocks": "total_blocks",
    "turnovers": "total_turnovers"
}


def normalize_stats_array(stats_list: List[Dict]) -> Dict[str, str]:
    """将API返回的stats数组转换为stat_id->value字典
    
    Args:
        stats_list: API返回的stats数组
                   格式: [{"stat": {"stat_id": "12", "value": "25"}}, ...]
        
    Returns:
        stat_id到value的映射字典
    """
    return normalize_stat_id_mapping(stats_list)


def normalize_season_stats(stats_data: Dict[str, Any]) -> Dict[str, Any]:
    """标准化球员赛季统计数据，提取完整的11个统计项
    
    Args:
        stats_data: 统计值字典，key为stat_id，value为统计值
        
    Returns:
        标准化的赛季统计数据字典
    """
    normalized_stats = {}
    
    try:
        # 处理每个核心统计项
        for stat_id, mapping in CORE_STAT_MAPPINGS.items():
            stat_value = stats_data.get(stat_id, '')
            
            if mapping["type"] == "made_attempted":
                # 处理made/attempted格式
                made, attempted = parse_made_attempted_stat(stat_value)
                field_prefix = mapping["field_prefix"]
                
                # 对于赛季统计，使用total_前缀
                made_field = f"{field_prefix}_made"
                attempted_field = f"{field_prefix}_attempted"
                
                normalized_stats[made_field] = made
                normalized_stats[attempted_field] = attempted
                
            elif mapping["type"] == "percentage":
                # 处理百分比
                field = mapping["field"]
                normalized_stats[field] = parse_percentage_value(stat_value)
                
            elif mapping["type"] == "integer":
                # 处理整数统计
                field = mapping["field"]
                # 对于赛季统计，某些字段需要添加total_前缀
                if field in SEASON_STAT_FIELD_MAPPINGS:
                    field = SEASON_STAT_FIELD_MAPPINGS[field]
                    
                normalized_stats[field] = safe_int_conversion(stat_value)
        
        # 验证数据合理性
        _validate_stats_consistency(normalized_stats)
        
        return normalized_stats
        
    except Exception as e:
        print(f"标准化赛季统计时出错: {e}")
        return {}


def normalize_daily_stats(stats_data: Dict[str, Any]) -> Dict[str, Any]:
    """标准化球员日统计数据，提取完整的11个统计项
    
    Args:
        stats_data: 统计值字典，key为stat_id，value为统计值
        
    Returns:
        标准化的日统计数据字典
    """
    normalized_stats = {}
    
    try:
        # 处理每个核心统计项
        for stat_id, mapping in CORE_STAT_MAPPINGS.items():
            stat_value = stats_data.get(stat_id, '')
            
            if mapping["type"] == "made_attempted":
                # 处理made/attempted格式
                made, attempted = parse_made_attempted_stat(stat_value)
                field_prefix = mapping["field_prefix"]
                
                made_field = f"{field_prefix}_made"
                attempted_field = f"{field_prefix}_attempted"
                
                normalized_stats[made_field] = made
                normalized_stats[attempted_field] = attempted
                
            elif mapping["type"] == "percentage":
                # 处理百分比
                field = mapping["field"]
                normalized_stats[field] = parse_percentage_value(stat_value)
                
            elif mapping["type"] == "integer":
                # 处理整数统计 - 日统计不使用total_前缀
                field = mapping["field"]
                normalized_stats[field] = safe_int_conversion(stat_value)
        
        # 验证数据合理性
        _validate_stats_consistency(normalized_stats)
        
        return normalized_stats
        
    except Exception as e:
        print(f"标准化日统计时出错: {e}")
        return {}


def normalize_team_stats(stats_data: Dict[str, Any]) -> Dict[str, Any]:
    """标准化团队统计数据，提取完整的11个统计项
    
    Args:
        stats_data: 团队统计数据，可能包含stats数组或已转换的字典
        
    Returns:
        标准化的团队统计数据字典
    """
    normalized_stats = {}
    
    try:
        # 处理API格式的团队统计数据
        if 'stats' in stats_data and isinstance(stats_data['stats'], list):
            # 先转换为字典格式
            stats_dict = normalize_stats_array(stats_data['stats'])
        else:
            # 已经是字典格式
            stats_dict = stats_data
        
        # 处理每个核心统计项
        for stat_id, mapping in CORE_STAT_MAPPINGS.items():
            stat_value = stats_dict.get(stat_id, '')
            
            if mapping["type"] == "made_attempted":
                # 处理made/attempted格式
                made, attempted = parse_made_attempted_stat(stat_value)
                field_prefix = mapping["field_prefix"]
                
                made_field = f"{field_prefix}_made"
                attempted_field = f"{field_prefix}_attempted"
                
                normalized_stats[made_field] = made
                normalized_stats[attempted_field] = attempted
                
            elif mapping["type"] == "percentage":
                # 处理百分比
                field = mapping["field"]
                normalized_stats[field] = parse_percentage_value(stat_value)
                
            elif mapping["type"] == "integer":
                # 处理整数统计 - 团队统计不使用total_前缀
                field = mapping["field"]
                normalized_stats[field] = safe_int_conversion(stat_value)
        
        # 验证数据合理性
        _validate_stats_consistency(normalized_stats)
        
        return normalized_stats
        
    except Exception as e:
        print(f"标准化团队统计时出错: {e}")
        return {}


def map_stat_id_to_name(stat_id: Union[str, int]) -> Optional[str]:
    """将Yahoo stat_id映射到标准统计项名称
    
    Args:
        stat_id: Yahoo统计项ID
        
    Returns:
        标准统计项名称，未知ID时返回None
    """
    stat_id_str = str(stat_id)
    
    # 基于核心统计映射生成名称映射
    stat_names = {
        "9004003": "Field Goals Made/Attempted",
        "5": "Field Goal Percentage",
        "9007006": "Free Throws Made/Attempted", 
        "8": "Free Throw Percentage",
        "10": "3-Point Shots Made",
        "12": "Points Scored",
        "15": "Total Rebounds",
        "16": "Assists",
        "17": "Steals",
        "18": "Blocked Shots",
        "19": "Turnovers"
    }
    
    return stat_names.get(stat_id_str)


def get_stat_field_mapping(stat_id: Union[str, int], is_season: bool = False) -> Optional[Dict]:
    """获取统计项的字段映射信息
    
    Args:
        stat_id: Yahoo统计项ID
        is_season: 是否为赛季统计（影响字段名前缀）
        
    Returns:
        字段映射信息字典，包含type、field等信息
    """
    stat_id_str = str(stat_id)
    mapping = CORE_STAT_MAPPINGS.get(stat_id_str)
    
    if not mapping:
        return None
    
    # 为赛季统计调整字段名
    if is_season and mapping["type"] == "integer":
        field = mapping["field"]
        if field in SEASON_STAT_FIELD_MAPPINGS:
            mapping = mapping.copy()  # 避免修改原始映射
            mapping["field"] = SEASON_STAT_FIELD_MAPPINGS[field]
    
    return mapping


def validate_normalized_stats(stats_data: Dict[str, Any]) -> Dict[str, bool]:
    """验证标准化后的统计数据合理性
    
    Args:
        stats_data: 标准化后的统计数据
        
    Returns:
        验证结果字典，每个验证项对应一个布尔值
    """
    validation_results = {
        "field_goals_logic": True,
        "free_throws_logic": True,
        "value_ranges": True,
        "data_completeness": True
    }
    
    try:
        # 验证投篮逻辑
        fg_made = stats_data.get("field_goals_made")
        fg_attempted = stats_data.get("field_goals_attempted")
        if fg_made is not None and fg_attempted is not None:
            validation_results["field_goals_logic"] = validate_made_attempted_logic(fg_made, fg_attempted)
        
        # 验证罚球逻辑
        ft_made = stats_data.get("free_throws_made")
        ft_attempted = stats_data.get("free_throws_attempted")
        if ft_made is not None and ft_attempted is not None:
            validation_results["free_throws_logic"] = validate_made_attempted_logic(ft_made, ft_attempted)
        
        # 验证数值范围
        for field, value in stats_data.items():
            if value is not None and isinstance(value, (int, float)):
                # 统计值不应为负数（除了某些特殊情况）
                if value < 0:
                    validation_results["value_ranges"] = False
                    break
        
        # 验证数据完整性（检查核心统计项）
        core_fields = ["points", "rebounds", "assists", "steals", "blocks", "turnovers"]
        missing_core_stats = sum(1 for field in core_fields if stats_data.get(field) is None)
        if missing_core_stats > len(core_fields) // 2:  # 超过一半核心统计缺失
            validation_results["data_completeness"] = False
    
    except Exception as e:
        print(f"验证统计数据时出错: {e}")
        # 验证出错时，将所有验证项标记为失败
        validation_results = {key: False for key in validation_results}
    
    return validation_results


def clean_invalid_stats(stats_data: Dict[str, Any]) -> Dict[str, Any]:
    """清理无效的统计值
    
    Args:
        stats_data: 原始统计数据
        
    Returns:
        清理后的统计数据
    """
    cleaned_stats = {}
    
    try:
        for field, value in stats_data.items():
            # 跳过None值
            if value is None:
                cleaned_stats[field] = None
                continue
            
            # 处理百分比字段
            if "percentage" in field:
                if isinstance(value, (int, float, Decimal)):
                    # 确保百分比在合理范围内
                    if 0 <= float(value) <= 100:
                        cleaned_stats[field] = value
                    else:
                        cleaned_stats[field] = None
                else:
                    cleaned_stats[field] = None
            
            # 处理整数字段
            elif field.endswith(("_made", "_attempted")) or field in ["points", "rebounds", "assists", "steals", "blocks", "turnovers"] or field.startswith("total_"):
                if isinstance(value, int) and value >= 0:
                    cleaned_stats[field] = value
                else:
                    cleaned_stats[field] = None
            
            # 其他字段保持原值
            else:
                cleaned_stats[field] = value
    
    except Exception as e:
        print(f"清理统计数据时出错: {e}")
        return stats_data
    
    return cleaned_stats


def extract_stats_summary(stats_data: Dict[str, Any]) -> Dict[str, Any]:
    """提取统计数据摘要信息
    
    Args:
        stats_data: 标准化后的统计数据
        
    Returns:
        统计数据摘要
    """
    try:
        summary = {
            "total_stats": len(stats_data),
            "non_null_stats": sum(1 for v in stats_data.values() if v is not None),
            "has_shooting_stats": False,
            "has_counting_stats": False,
            "calculated_percentages": {}
        }
        
        # 检查投篮统计
        if (stats_data.get("field_goals_made") is not None or 
            stats_data.get("free_throws_made") is not None):
            summary["has_shooting_stats"] = True
        
        # 检查计数统计
        counting_stats = ["points", "rebounds", "assists", "steals", "blocks", "turnovers"]
        if any(stats_data.get(stat) is not None for stat in counting_stats):
            summary["has_counting_stats"] = True
        
        # 计算命中率（用于验证）
        fg_made = stats_data.get("field_goals_made")
        fg_attempted = stats_data.get("field_goals_attempted")
        if fg_made is not None and fg_attempted is not None:
            calculated_fg_pct = calculate_percentage(fg_made, fg_attempted)
            summary["calculated_percentages"]["field_goal"] = calculated_fg_pct
        
        ft_made = stats_data.get("free_throws_made")
        ft_attempted = stats_data.get("free_throws_attempted")
        if ft_made is not None and ft_attempted is not None:
            calculated_ft_pct = calculate_percentage(ft_made, ft_attempted)
            summary["calculated_percentages"]["free_throw"] = calculated_ft_pct
        
        return summary
    
    except Exception as e:
        print(f"提取统计摘要时出错: {e}")
        return {"error": str(e)}


def _validate_stats_consistency(stats_data: Dict[str, Any]) -> bool:
    """内部函数：验证统计数据内部一致性
    
    Args:
        stats_data: 统计数据字典
        
    Returns:
        是否通过一致性检验
    """
    try:
        # 验证投篮统计一致性
        fg_made = stats_data.get("field_goals_made")
        fg_attempted = stats_data.get("field_goals_attempted")
        fg_pct = stats_data.get("field_goal_percentage")
        
        if fg_made is not None and fg_attempted is not None:
            if not validate_made_attempted_logic(fg_made, fg_attempted):
                print(f"警告: 投篮统计逻辑错误 - made: {fg_made}, attempted: {fg_attempted}")
        
        # 验证罚球统计一致性
        ft_made = stats_data.get("free_throws_made")
        ft_attempted = stats_data.get("free_throws_attempted")
        ft_pct = stats_data.get("free_throw_percentage")
        
        if ft_made is not None and ft_attempted is not None:
            if not validate_made_attempted_logic(ft_made, ft_attempted):
                print(f"警告: 罚球统计逻辑错误 - made: {ft_made}, attempted: {ft_attempted}")
        
        return True
    
    except Exception as e:
        print(f"验证统计一致性时出错: {e}")
        return False 