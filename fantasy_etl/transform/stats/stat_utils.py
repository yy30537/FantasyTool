#!/usr/bin/env python3
"""
统计数据工具函数

迁移来源: @database_writer.py 中的工具方法
主要映射:
  - _safe_int() -> safe_int_conversion()
  - _safe_float() -> safe_float_conversion()
  - _parse_percentage() -> parse_percentage_value()
  - _safe_bool() -> safe_bool_conversion()
  - 数据类型安全转换的工具函数集合

职责:
  - 安全整数转换：处理None、空字符串、异常情况
  - 安全浮点数转换：处理精度和格式问题
  - 百分比解析：多格式支持和标准化
  - 安全布尔转换：多类型支持
  - 复合统计解析：'made/attempted'格式拆分
  - 数据验证辅助：数值范围检查和逻辑关系验证

输入: 各种原始数据类型 (Any)
输出: 转换后的目标数据类型，失败时返回None或默认值
"""

from typing import Any, Optional, Tuple, Union
from decimal import Decimal, InvalidOperation


def safe_int_conversion(value: Any) -> Optional[int]:
    """安全转换为整数
    
    Args:
        value: 待转换的值
        
    Returns:
        转换后的整数，失败时返回None
    """
    try:
        if value is None or value == '':
            return None
        return int(float(value))  # 先转float再转int，处理'1.0'格式
    except (ValueError, TypeError):
        return None


def safe_float_conversion(value: Any) -> Optional[float]:
    """安全转换为浮点数
    
    Args:
        value: 待转换的值
        
    Returns:
        转换后的浮点数，失败时返回None
    """
    try:
        if value is None or value == '':
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_bool_conversion(value: Any) -> bool:
    """安全转换为布尔值
    
    Args:
        value: 待转换的值
        
    Returns:
        转换后的布尔值，失败时返回False
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes')
    if isinstance(value, (int, float)):
        return value != 0
    return False


def parse_percentage_value(pct_str: Any) -> Optional[Decimal]:
    """解析百分比字符串，返回标准化的Decimal值
    
    Args:
        pct_str: 百分比字符串，支持多种格式
                - '50%' -> 50.000
                - '0.5' -> 50.000  
                - '.500' -> 50.000
                - '-' -> None
                
    Returns:
        百分比值（0-100范围），保留3位小数，失败时返回None
    """
    try:
        if not pct_str or pct_str == '-' or pct_str == 'N/A':
            return None
        
        pct_str = str(pct_str).strip()
        
        # 移除%符号
        if '%' in pct_str:
            clean_value = pct_str.replace('%', '')
            val = safe_float_conversion(clean_value)
            return Decimal(str(round(val, 3))) if val is not None else None
        
        # 处理小数形式（如 .500 或 0.500）
        clean_value = safe_float_conversion(pct_str)
        if clean_value is not None:
            # 如果是小数形式（0-1），转换为百分比（0-100）
            if 0 <= clean_value <= 1:
                return Decimal(str(round(clean_value * 100, 3)))
            # 如果已经是百分比形式（0-100），直接返回
            elif 0 <= clean_value <= 100:
                return Decimal(str(round(clean_value, 3)))
        
        return None
    except (ValueError, TypeError, InvalidOperation):
        return None


def parse_made_attempted_stat(stat_value: str) -> Tuple[Optional[int], Optional[int]]:
    """解析'made/attempted'格式的复合统计
    
    Args:
        stat_value: 格式为'made/attempted'的字符串，如'5/10'
        
    Returns:
        (made, attempted)元组，解析失败时返回(None, None)
    """
    try:
        if not isinstance(stat_value, str) or '/' not in stat_value:
            return None, None
        
        parts = stat_value.split('/')
        if len(parts) != 2:
            return None, None
        
        made = safe_int_conversion(parts[0].strip())
        attempted = safe_int_conversion(parts[1].strip())
        
        return made, attempted
    except Exception:
        return None, None


def validate_stat_range(value: Optional[Union[int, float]], 
                       min_val: Optional[Union[int, float]] = None,
                       max_val: Optional[Union[int, float]] = None) -> bool:
    """验证统计值是否在合理范围内
    
    Args:
        value: 待验证的值
        min_val: 最小值（可选）
        max_val: 最大值（可选）
        
    Returns:
        值是否在合理范围内
    """
    if value is None:
        return True  # None值视为有效
    
    try:
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True
    except (TypeError, ValueError):
        return False


def validate_made_attempted_logic(made: Optional[int], attempted: Optional[int]) -> bool:
    """验证made/attempted统计的逻辑合理性
    
    Args:
        made: 命中数
        attempted: 尝试数
        
    Returns:
        是否符合逻辑（made <= attempted）
    """
    if made is None or attempted is None:
        return True  # 如果有None值，跳过验证
    
    try:
        return made <= attempted and made >= 0 and attempted >= 0
    except (TypeError, ValueError):
        return False


def safe_division(numerator: Optional[Union[int, float]], 
                 denominator: Optional[Union[int, float]]) -> Optional[float]:
    """安全除法计算
    
    Args:
        numerator: 分子
        denominator: 分母
        
    Returns:
        除法结果，分母为0或None时返回None
    """
    try:
        if numerator is None or denominator is None or denominator == 0:
            return None
        return float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def calculate_percentage(made: Optional[int], attempted: Optional[int]) -> Optional[Decimal]:
    """计算命中率百分比
    
    Args:
        made: 命中数
        attempted: 尝试数
        
    Returns:
        命中率百分比（0-100），计算失败时返回None
    """
    try:
        if made is None or attempted is None or attempted == 0:
            return None
        
        if not validate_made_attempted_logic(made, attempted):
            return None
        
        percentage = (made / attempted) * 100
        return Decimal(str(round(percentage, 3)))
    except (TypeError, ValueError, ZeroDivisionError, InvalidOperation):
        return None


def normalize_stat_id_mapping(stats_list: list) -> dict:
    """将API返回的stats数组转换为stat_id->value字典
    
    Args:
        stats_list: API返回的stats数组
                   格式: [{"stat": {"stat_id": "12", "value": "25"}}, ...]
        
    Returns:
        stat_id到value的映射字典
    """
    try:
        stats_dict = {}
        
        if not isinstance(stats_list, list):
            return stats_dict
        
        for stat_item in stats_list:
            if not isinstance(stat_item, dict) or 'stat' not in stat_item:
                continue
            
            stat_info = stat_item['stat']
            if not isinstance(stat_info, dict):
                continue
            
            stat_id = stat_info.get('stat_id')
            value = stat_info.get('value')
            
            if stat_id is not None:
                stats_dict[str(stat_id)] = value
        
        return stats_dict
    except Exception:
        return {}


def extract_position_string(position_data: Any) -> Optional[str]:
    """从位置数据中提取位置字符串
    
    Args:
        position_data: 位置数据，可以是字符串、字典或列表
        
    Returns:
        标准化的位置字符串，提取失败时返回None
    """
    if not position_data:
        return None
    
    if isinstance(position_data, str):
        return position_data.strip()
    
    if isinstance(position_data, dict):
        return position_data.get("position")
    
    if isinstance(position_data, list) and len(position_data) > 0:
        if isinstance(position_data[0], str):
            return position_data[0].strip()
        elif isinstance(position_data[0], dict):
            return position_data[0].get("position")
    
    return None


def is_starting_position(position: Optional[str]) -> bool:
    """判断是否为首发位置
    
    Args:
        position: 位置字符串
        
    Returns:
        是否为首发位置（非BN、IL、IR）
    """
    if not position:
        return False
    return position not in ['BN', 'IL', 'IR']


def is_bench_position(position: Optional[str]) -> bool:
    """判断是否为替补位置
    
    Args:
        position: 位置字符串
        
    Returns:
        是否为替补位置（BN）
    """
    return position == 'BN'


def is_injured_reserve_position(position: Optional[str]) -> bool:
    """判断是否为伤病名单位置
    
    Args:
        position: 位置字符串
        
    Returns:
        是否为伤病名单位置（IL或IR）
    """
    return position in ['IL', 'IR'] 