#!/usr/bin/env python3
"""
球员统计数据转换器

迁移来源: @database_writer.py 中的球员统计处理逻辑
主要映射:
  - _extract_core_player_season_stats() -> transform_season_stats()
  - _extract_core_daily_stats() -> transform_daily_stats()
  - 球员统计数据的标准化转换逻辑

职责:
  - 赛季统计转换：
    * 从统计值字典提取11个核心统计项
    * 处理复合统计：FGM/A (9004003)、FTM/A (9007006)
    * 转换百分比统计：FG% (5)、FT% (8)
    * 提取单项统计：3PTM (10)、PTS (12)、REB (15)等
    * 累计统计字段映射：total_points、total_rebounds等
  - 日统计转换：
    * 相同的11个核心统计项提取
    * 日期特定的统计字段：points、rebounds（非total_）
    * 周数信息处理和验证
  - 统计数据验证：
    * 数值合理性检查：投篮命中不能超过尝试等
    * 百分比计算验证：计算值与API值的一致性
    * 统计项完整性：必需统计项的存在性
  - 数据类型转换：
    * 安全整数转换：处理字符串数字
    * 百分比标准化：统一为0-100范围的decimal值
    * 复合统计解析：'made/attempted'格式拆分
  - 特殊值处理：
    * 无效值标识：'-'、'N/A'等
    * 缺失统计项的默认值设置
    * 异常值标记和处理

输入: 统计值字典 (Dict[str, Any])
输出: 标准化的统计数据对象 (Dict)
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import date
from decimal import Decimal
from ..normalizers.stats_normalizer import (
    normalize_season_stats, normalize_daily_stats,
    validate_normalized_stats, clean_invalid_stats
)
from .stat_utils import (
    safe_int_conversion, parse_percentage_value,
    parse_made_attempted_stat, validate_made_attempted_logic,
    calculate_percentage
)


def transform_season_stats(player_key: str, editorial_player_key: str,
                          league_key: str, season: str, 
                          stats_data: Dict[str, Any]) -> Optional[Dict]:
    """转换球员赛季统计数据
    
    Args:
        player_key: 球员键
        editorial_player_key: 编辑球员键
        league_key: 联盟键
        season: 赛季
        stats_data: 统计值字典，key为stat_id，value为统计值
        
    Returns:
        标准化的赛季统计数据对象，转换失败时返回None
    """
    try:
        # 使用标准化器提取核心统计项
        core_stats = normalize_season_stats(stats_data)
        
        if not core_stats:
            print(f"球员 {player_key} 赛季统计标准化失败")
            return None
        
        # 构建完整的赛季统计对象
        season_stats = {
            # 基本信息
            "player_key": player_key,
            "editorial_player_key": editorial_player_key,
            "league_key": league_key,
            "season": season,
            
            # 投篮统计
            "field_goals_made": core_stats.get("field_goals_made"),
            "field_goals_attempted": core_stats.get("field_goals_attempted"),
            "field_goal_percentage": core_stats.get("field_goal_percentage"),
            
            # 罚球统计
            "free_throws_made": core_stats.get("free_throws_made"),
            "free_throws_attempted": core_stats.get("free_throws_attempted"),
            "free_throw_percentage": core_stats.get("free_throw_percentage"),
            
            # 其他统计
            "three_pointers_made": core_stats.get("three_pointers_made"),
            "total_points": core_stats.get("total_points"),
            "total_rebounds": core_stats.get("total_rebounds"),
            "total_assists": core_stats.get("total_assists"),
            "total_steals": core_stats.get("total_steals"),
            "total_blocks": core_stats.get("total_blocks"),
            "total_turnovers": core_stats.get("total_turnovers")
        }
        
        # 验证数据合理性
        validation_results = validate_normalized_stats(season_stats)
        if not all(validation_results.values()):
            print(f"警告: 球员 {player_key} 赛季统计验证失败: {validation_results}")
        
        # 清理无效数据
        season_stats = clean_invalid_stats(season_stats)
        
        # 添加计算统计项
        _add_calculated_stats(season_stats, is_season=True)
        
        return season_stats
    
    except Exception as e:
        print(f"转换球员赛季统计失败 {player_key}: {e}")
        return None


def transform_daily_stats(player_key: str, editorial_player_key: str,
                         league_key: str, season: str, date_obj: date,
                         stats_data: Dict[str, Any], 
                         week: Optional[int] = None) -> Optional[Dict]:
    """转换球员日统计数据
    
    Args:
        player_key: 球员键
        editorial_player_key: 编辑球员键
        league_key: 联盟键
        season: 赛季
        date_obj: 统计日期
        stats_data: 统计值字典，key为stat_id，value为统计值
        week: 周数（可选）
        
    Returns:
        标准化的日统计数据对象，转换失败时返回None
    """
    try:
        # 使用标准化器提取核心统计项
        core_stats = normalize_daily_stats(stats_data)
        
        if not core_stats:
            print(f"球员 {player_key} 日期 {date_obj} 统计标准化失败")
            return None
        
        # 构建完整的日统计对象
        daily_stats = {
            # 基本信息
            "player_key": player_key,
            "editorial_player_key": editorial_player_key,
            "league_key": league_key,
            "season": season,
            "date": date_obj,
            "week": week,
            
            # 投篮统计
            "field_goals_made": core_stats.get("field_goals_made"),
            "field_goals_attempted": core_stats.get("field_goals_attempted"),
            "field_goal_percentage": core_stats.get("field_goal_percentage"),
            
            # 罚球统计
            "free_throws_made": core_stats.get("free_throws_made"),
            "free_throws_attempted": core_stats.get("free_throws_attempted"),
            "free_throw_percentage": core_stats.get("free_throw_percentage"),
            
            # 其他统计
            "three_pointers_made": core_stats.get("three_pointers_made"),
            "points": core_stats.get("points"),
            "rebounds": core_stats.get("rebounds"),
            "assists": core_stats.get("assists"),
            "steals": core_stats.get("steals"),
            "blocks": core_stats.get("blocks"),
            "turnovers": core_stats.get("turnovers")
        }
        
        # 验证数据合理性
        validation_results = validate_normalized_stats(daily_stats)
        if not all(validation_results.values()):
            print(f"警告: 球员 {player_key} 日期 {date_obj} 统计验证失败: {validation_results}")
        
        # 清理无效数据
        daily_stats = clean_invalid_stats(daily_stats)
        
        # 添加计算统计项
        _add_calculated_stats(daily_stats, is_season=False)
        
        return daily_stats
    
    except Exception as e:
        print(f"转换球员日统计失败 {player_key}/{date_obj}: {e}")
        return None


def batch_transform_season_stats(players_stats: List[Dict]) -> List[Dict]:
    """批量转换球员赛季统计数据
    
    Args:
        players_stats: 球员统计数据列表，每个元素包含player_key和stats字典
        
    Returns:
        转换后的赛季统计数据列表
    """
    transformed_stats = []
    
    try:
        for player_stat in players_stats:
            if not isinstance(player_stat, dict):
                continue
            
            player_key = player_stat.get("player_key")
            editorial_player_key = player_stat.get("editorial_player_key", player_key)
            league_key = player_stat.get("league_key")
            season = player_stat.get("season")
            stats_data = player_stat.get("stats", {})
            
            if not all([player_key, league_key, season]):
                print(f"跳过不完整的球员统计记录: {player_key}")
                continue
            
            transformed_stat = transform_season_stats(
                player_key, editorial_player_key, league_key, season, stats_data
            )
            
            if transformed_stat:
                transformed_stats.append(transformed_stat)
    
    except Exception as e:
        print(f"批量转换球员赛季统计时出错: {e}")
    
    return transformed_stats


def batch_transform_daily_stats(players_stats: List[Dict], target_date: date) -> List[Dict]:
    """批量转换球员日统计数据
    
    Args:
        players_stats: 球员统计数据列表
        target_date: 目标日期
        
    Returns:
        转换后的日统计数据列表
    """
    transformed_stats = []
    
    try:
        for player_stat in players_stats:
            if not isinstance(player_stat, dict):
                continue
            
            player_key = player_stat.get("player_key")
            editorial_player_key = player_stat.get("editorial_player_key", player_key)
            league_key = player_stat.get("league_key")
            season = player_stat.get("season")
            week = player_stat.get("week")
            stats_data = player_stat.get("stats", {})
            
            if not all([player_key, league_key, season]):
                print(f"跳过不完整的球员统计记录: {player_key}")
                continue
            
            transformed_stat = transform_daily_stats(
                player_key, editorial_player_key, league_key, season, 
                target_date, stats_data, week
            )
            
            if transformed_stat:
                transformed_stats.append(transformed_stat)
    
    except Exception as e:
        print(f"批量转换球员日统计时出错: {e}")
    
    return transformed_stats


def validate_player_stats_consistency(stats_data: Dict) -> Dict[str, Any]:
    """验证球员统计数据的一致性和合理性
    
    Args:
        stats_data: 统计数据字典
        
    Returns:
        验证结果和详细信息
    """
    validation_result = {
        "is_valid": True,
        "issues": [],
        "warnings": [],
        "calculated_percentages": {}
    }
    
    try:
        # 验证投篮统计
        fg_made = stats_data.get("field_goals_made")
        fg_attempted = stats_data.get("field_goals_attempted")
        fg_pct = stats_data.get("field_goal_percentage")
        
        if fg_made is not None and fg_attempted is not None:
            if not validate_made_attempted_logic(fg_made, fg_attempted):
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"投篮逻辑错误: {fg_made}/{fg_attempted}")
            
            # 计算命中率并与API值比较
            calculated_fg_pct = calculate_percentage(fg_made, fg_attempted)
            validation_result["calculated_percentages"]["field_goal"] = calculated_fg_pct
            
            if fg_pct and calculated_fg_pct:
                diff = abs(float(fg_pct) - float(calculated_fg_pct))
                if diff > 1.0:  # 差异超过1%
                    validation_result["warnings"].append(
                        f"投篮命中率差异: API={fg_pct}%, 计算={calculated_fg_pct}%"
                    )
        
        # 验证罚球统计
        ft_made = stats_data.get("free_throws_made")
        ft_attempted = stats_data.get("free_throws_attempted")
        ft_pct = stats_data.get("free_throw_percentage")
        
        if ft_made is not None and ft_attempted is not None:
            if not validate_made_attempted_logic(ft_made, ft_attempted):
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"罚球逻辑错误: {ft_made}/{ft_attempted}")
            
            # 计算命中率并与API值比较
            calculated_ft_pct = calculate_percentage(ft_made, ft_attempted)
            validation_result["calculated_percentages"]["free_throw"] = calculated_ft_pct
            
            if ft_pct and calculated_ft_pct:
                diff = abs(float(ft_pct) - float(calculated_ft_pct))
                if diff > 1.0:  # 差异超过1%
                    validation_result["warnings"].append(
                        f"罚球命中率差异: API={ft_pct}%, 计算={calculated_ft_pct}%"
                    )
        
        # 验证数值范围
        numeric_fields = [
            "field_goals_made", "field_goals_attempted", "free_throws_made", 
            "free_throws_attempted", "three_pointers_made", "points", 
            "rebounds", "assists", "steals", "blocks", "turnovers"
        ]
        
        for field in numeric_fields:
            value = stats_data.get(field)
            if value is not None and value < 0:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"负数值: {field}={value}")
        
        # 验证百分比范围
        percentage_fields = ["field_goal_percentage", "free_throw_percentage"]
        for field in percentage_fields:
            value = stats_data.get(field)
            if value is not None:
                if value < 0 or value > 100:
                    validation_result["is_valid"] = False
                    validation_result["issues"].append(f"百分比超出范围: {field}={value}%")
    
    except Exception as e:
        validation_result["is_valid"] = False
        validation_result["issues"].append(f"验证过程出错: {e}")
    
    return validation_result


def calculate_derived_stats(stats_data: Dict) -> Dict[str, Any]:
    """计算派生统计项
    
    Args:
        stats_data: 基础统计数据
        
    Returns:
        派生统计项字典
    """
    derived_stats = {}
    
    try:
        # 计算真实投篮命中率 (TS%)
        # TS% = Points / (2 * (FGA + 0.44 * FTA))
        points = stats_data.get("points") or stats_data.get("total_points")
        fga = stats_data.get("field_goals_attempted")
        fta = stats_data.get("free_throws_attempted")
        
        if points and fga and fta:
            ts_denominator = 2 * (fga + 0.44 * fta)
            if ts_denominator > 0:
                true_shooting_pct = (points / ts_denominator) * 100
                derived_stats["true_shooting_percentage"] = round(true_shooting_pct, 3)
        
        # 计算有效投篮命中率 (eFG%)
        # eFG% = (FGM + 0.5 * 3PTM) / FGA
        fgm = stats_data.get("field_goals_made")
        three_ptm = stats_data.get("three_pointers_made")
        
        if fgm and fga and three_ptm:
            if fga > 0:
                effective_fg_pct = ((fgm + 0.5 * three_ptm) / fga) * 100
                derived_stats["effective_field_goal_percentage"] = round(effective_fg_pct, 3)
        
        # 计算球员效率值 (PER的简化版本)
        # 简化PER = (PTS + REB + AST + STL + BLK - TO - (FGA - FGM) - (FTA - FTM)) / GP
        rebounds = stats_data.get("rebounds") or stats_data.get("total_rebounds") or 0
        assists = stats_data.get("assists") or stats_data.get("total_assists") or 0
        steals = stats_data.get("steals") or stats_data.get("total_steals") or 0
        blocks = stats_data.get("blocks") or stats_data.get("total_blocks") or 0
        turnovers = stats_data.get("turnovers") or stats_data.get("total_turnovers") or 0
        points = points or 0
        
        ft_made = stats_data.get("free_throws_made")
        
        if fgm and fga and ft_made and fta:
            player_efficiency = (
                points + rebounds + assists + steals + blocks - turnovers 
                - (fga - fgm) - (fta - ft_made)
            )
            derived_stats["player_efficiency_rating"] = player_efficiency
    
    except Exception as e:
        print(f"计算派生统计时出错: {e}")
    
    return derived_stats


def extract_stats_comparison(stats1: Dict, stats2: Dict) -> Dict[str, Any]:
    """比较两组统计数据
    
    Args:
        stats1: 第一组统计数据
        stats2: 第二组统计数据
        
    Returns:
        比较结果字典
    """
    comparison = {
        "differences": {},
        "improvements": [],
        "declines": [],
        "percentage_changes": {}
    }
    
    try:
        comparable_fields = [
            "points", "total_points", "rebounds", "total_rebounds",
            "assists", "total_assists", "steals", "total_steals",
            "blocks", "total_blocks", "turnovers", "total_turnovers",
            "field_goal_percentage", "free_throw_percentage"
        ]
        
        for field in comparable_fields:
            val1 = stats1.get(field)
            val2 = stats2.get(field)
            
            if val1 is not None and val2 is not None:
                diff = val2 - val1
                comparison["differences"][field] = diff
                
                if diff > 0:
                    comparison["improvements"].append(field)
                elif diff < 0:
                    comparison["declines"].append(field)
                
                # 计算百分比变化
                if val1 != 0:
                    pct_change = (diff / val1) * 100
                    comparison["percentage_changes"][field] = round(pct_change, 2)
    
    except Exception as e:
        print(f"比较统计数据时出错: {e}")
        comparison["error"] = str(e)
    
    return comparison


def _add_calculated_stats(stats_data: Dict, is_season: bool = False) -> None:
    """添加计算统计项到统计数据中
    
    Args:
        stats_data: 统计数据字典（会被修改）
        is_season: 是否为赛季统计
    """
    try:
        # 计算命中率（用于验证）
        fg_made = stats_data.get("field_goals_made")
        fg_attempted = stats_data.get("field_goals_attempted")
        if fg_made is not None and fg_attempted is not None:
            calculated_fg_pct = calculate_percentage(fg_made, fg_attempted)
            if calculated_fg_pct:
                stats_data["calculated_field_goal_percentage"] = calculated_fg_pct
        
        ft_made = stats_data.get("free_throws_made")
        ft_attempted = stats_data.get("free_throws_attempted")
        if ft_made is not None and ft_attempted is not None:
            calculated_ft_pct = calculate_percentage(ft_made, ft_attempted)
            if calculated_ft_pct:
                stats_data["calculated_free_throw_percentage"] = calculated_ft_pct
        
        # 添加派生统计
        derived_stats = calculate_derived_stats(stats_data)
        stats_data.update(derived_stats)
    
    except Exception as e:
        print(f"添加计算统计时出错: {e}") 