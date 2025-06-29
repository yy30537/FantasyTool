#!/usr/bin/env python3
"""
位置数据标准化器

迁移来源: @yahoo_api_data.py 中的位置相关标准化逻辑
主要映射:
  - _extract_position_string() -> normalize_position_data()
  - 位置信息提取和标准化逻辑

职责:
  - 标准化位置数据格式：处理字符串、字典、列表类型位置
  - 位置信息验证：检查位置代码有效性
  - 特殊位置处理：首发位置、替补位置、伤病名单
  - 位置类型分类：确定is_starting、is_bench、is_injured_reserve状态
  - 处理嵌套位置数据结构

输入: 原始位置数据 (Any: str/dict/list)
输出: 标准化的位置字符串 (Optional[str])
"""

from typing import Any, Optional, Dict, List
from ..stats.stat_utils import extract_position_string, is_starting_position, is_bench_position, is_injured_reserve_position


def normalize_position_data(position_data: Any) -> Optional[str]:
    """标准化位置数据格式
    
    Args:
        position_data: 原始位置数据，可以是字符串、字典或列表
        
    Returns:
        标准化的位置字符串，提取失败时返回None
    """
    return extract_position_string(position_data)


def validate_position_code(position: str) -> bool:
    """验证位置代码是否有效
    
    Args:
        position: 位置代码字符串
        
    Returns:
        位置代码是否有效
    """
    try:
        if not position:
            return False
        
        # NBA Fantasy常见位置代码
        valid_positions = {
            # 首发位置
            'PG', 'SG', 'G', 'SF', 'PF', 'F', 'C',
            # 特殊位置
            'UTIL', 'BN', 'IL', 'IR', 'NA'
        }
        
        return position.upper() in valid_positions
    
    except Exception:
        return False


def normalize_position_code(position: str) -> str:
    """标准化位置代码格式
    
    Args:
        position: 原始位置代码
        
    Returns:
        标准化的位置代码
    """
    try:
        if not position:
            return position
        
        # 转换为大写并清理空格
        normalized = position.strip().upper()
        
        # 处理常见的位置代码变体
        position_mappings = {
            'POINT GUARD': 'PG',
            'SHOOTING GUARD': 'SG',
            'SMALL FORWARD': 'SF',
            'POWER FORWARD': 'PF',
            'CENTER': 'C',
            'GUARD': 'G',
            'FORWARD': 'F',
            'UTILITY': 'UTIL',
            'BENCH': 'BN',
            'INJURED LIST': 'IL',
            'INJURED RESERVE': 'IR',
            'NOT ACTIVE': 'NA'
        }
        
        return position_mappings.get(normalized, normalized)
    
    except Exception:
        return position


def analyze_position_type(position: Optional[str]) -> Dict[str, bool]:
    """分析位置类型和状态
    
    Args:
        position: 位置字符串
        
    Returns:
        位置分析结果字典
    """
    try:
        analysis = {
            "is_starting": False,
            "is_bench": False,
            "is_injured_reserve": False,
            "is_valid": False,
            "is_active": False
        }
        
        if not position:
            return analysis
        
        normalized_position = normalize_position_code(position)
        
        # 验证位置有效性
        analysis["is_valid"] = validate_position_code(normalized_position)
        
        # 判断位置类型
        analysis["is_starting"] = is_starting_position(normalized_position)
        analysis["is_bench"] = is_bench_position(normalized_position)
        analysis["is_injured_reserve"] = is_injured_reserve_position(normalized_position)
        
        # 判断是否活跃（非伤病名单）
        analysis["is_active"] = not analysis["is_injured_reserve"]
        
        return analysis
    
    except Exception:
        return {
            "is_starting": False,
            "is_bench": False,
            "is_injured_reserve": False,
            "is_valid": False,
            "is_active": False
        }


def parse_multiple_positions(positions_data: Any) -> List[str]:
    """解析多个位置数据
    
    Args:
        positions_data: 位置数据，可能包含多个位置
        
    Returns:
        标准化的位置列表
    """
    positions = []
    
    try:
        if not positions_data:
            return positions
        
        if isinstance(positions_data, list):
            for pos_item in positions_data:
                normalized_pos = normalize_position_data(pos_item)
                if normalized_pos and normalized_pos not in positions:
                    positions.append(normalize_position_code(normalized_pos))
        else:
            # 单个位置
            normalized_pos = normalize_position_data(positions_data)
            if normalized_pos:
                positions.append(normalize_position_code(normalized_pos))
    
    except Exception as e:
        print(f"解析多个位置时出错: {e}")
    
    return positions


def get_primary_position(positions: List[str]) -> Optional[str]:
    """从位置列表中获取主要位置
    
    Args:
        positions: 位置列表
        
    Returns:
        主要位置，通常是第一个有效的首发位置
    """
    try:
        if not positions:
            return None
        
        # 优先返回首发位置
        for position in positions:
            if is_starting_position(position):
                return position
        
        # 如果没有首发位置，返回第一个位置
        return positions[0]
    
    except Exception:
        return None


def categorize_position_by_sport(position: str, sport: str = "nba") -> Dict[str, str]:
    """根据运动类型对位置进行分类
    
    Args:
        position: 位置代码
        sport: 运动类型，默认为"nba"
        
    Returns:
        位置分类信息
    """
    try:
        categorization = {
            "position": position,
            "category": "unknown",
            "position_type": "unknown",
            "sport": sport
        }
        
        if sport.lower() == "nba":
            # NBA位置分类
            position_categories = {
                # 后卫
                "PG": {"category": "guard", "position_type": "point_guard"},
                "SG": {"category": "guard", "position_type": "shooting_guard"},
                "G": {"category": "guard", "position_type": "guard"},
                
                # 前锋
                "SF": {"category": "forward", "position_type": "small_forward"},
                "PF": {"category": "forward", "position_type": "power_forward"},
                "F": {"category": "forward", "position_type": "forward"},
                
                # 中锋
                "C": {"category": "center", "position_type": "center"},
                
                # 特殊位置
                "UTIL": {"category": "utility", "position_type": "utility"},
                "BN": {"category": "bench", "position_type": "bench"},
                "IL": {"category": "injured", "position_type": "injured_list"},
                "IR": {"category": "injured", "position_type": "injured_reserve"},
                "NA": {"category": "inactive", "position_type": "not_active"}
            }
            
            if position in position_categories:
                categorization.update(position_categories[position])
        
        return categorization
    
    except Exception:
        return {
            "position": position,
            "category": "unknown",
            "position_type": "unknown",
            "sport": sport
        }


def validate_roster_position_assignment(player_position: str, 
                                      roster_position: str) -> bool:
    """验证球员位置与阵容位置的匹配性
    
    Args:
        player_position: 球员的实际位置
        roster_position: 阵容中分配的位置
        
    Returns:
        位置分配是否有效
    """
    try:
        if not player_position or not roster_position:
            return False
        
        # 标准化位置代码
        player_pos = normalize_position_code(player_position)
        roster_pos = normalize_position_code(roster_position)
        
        # 如果完全匹配
        if player_pos == roster_pos:
            return True
        
        # 检查位置兼容性
        position_compatibility = {
            # 后卫兼容性
            "PG": ["PG", "G", "UTIL"],
            "SG": ["SG", "G", "UTIL"],
            "G": ["PG", "SG", "G", "UTIL"],
            
            # 前锋兼容性
            "SF": ["SF", "F", "UTIL"],
            "PF": ["PF", "F", "UTIL"],
            "F": ["SF", "PF", "F", "UTIL"],
            
            # 中锋兼容性
            "C": ["C", "UTIL"],
            
            # 万能位置
            "UTIL": ["PG", "SG", "G", "SF", "PF", "F", "C", "UTIL"],
            
            # 特殊位置（任何球员都可以）
            "BN": ["PG", "SG", "G", "SF", "PF", "F", "C"],
            "IL": ["PG", "SG", "G", "SF", "PF", "F", "C"],
            "IR": ["PG", "SG", "G", "SF", "PF", "F", "C"],
            "NA": ["PG", "SG", "G", "SF", "PF", "F", "C"]
        }
        
        compatible_positions = position_compatibility.get(roster_pos, [])
        return player_pos in compatible_positions
    
    except Exception:
        return False


def extract_position_summary(positions_list: List[str]) -> Dict[str, Any]:
    """提取位置数据的摘要信息
    
    Args:
        positions_list: 位置列表
        
    Returns:
        位置摘要信息
    """
    try:
        summary = {
            "total_positions": len(positions_list),
            "position_counts": {},
            "categories": {},
            "starting_positions": 0,
            "bench_positions": 0,
            "injured_positions": 0,
            "valid_positions": 0
        }
        
        for position in positions_list:
            # 统计位置出现次数
            normalized_pos = normalize_position_code(position)
            if normalized_pos not in summary["position_counts"]:
                summary["position_counts"][normalized_pos] = 0
            summary["position_counts"][normalized_pos] += 1
            
            # 分析位置类型
            analysis = analyze_position_type(normalized_pos)
            if analysis["is_valid"]:
                summary["valid_positions"] += 1
            if analysis["is_starting"]:
                summary["starting_positions"] += 1
            elif analysis["is_bench"]:
                summary["bench_positions"] += 1
            elif analysis["is_injured_reserve"]:
                summary["injured_positions"] += 1
            
            # 统计位置分类
            categorization = categorize_position_by_sport(normalized_pos)
            category = categorization["category"]
            if category not in summary["categories"]:
                summary["categories"][category] = 0
            summary["categories"][category] += 1
        
        return summary
    
    except Exception as e:
        print(f"提取位置摘要时出错: {e}")
        return {"error": str(e)} 