#!/usr/bin/env python3
"""
团队统计数据转换器

迁移来源: @database_writer.py 中的团队统计处理逻辑
主要映射:
  - _extract_team_weekly_stats() -> transform_weekly_stats()
  - write_team_weekly_stats_from_matchup() -> transform_matchup_stats()
  - 团队统计数据的标准化转换逻辑

职责:
  - 团队周统计转换：
    * 从matchup数据中的team_stats提取统计信息
    * 处理与球员统计相同的11个核心统计项
    * 团队级别的复合统计：FGM/A、FTM/A
    * 团队百分比统计：FG%、FT%转换
    * 团队累计统计：points、rebounds、assists等
  - Matchup数据解析：
    * 从teams数据容器中提取特定团队的统计
    * 处理复杂嵌套结构：teams["0"]["teams"][i]["team"]
    * 定位目标团队：通过team_key匹配
    * 提取team_stats容器：从team[1]["team_stats"]
  - 统计数组处理：
    * 解析stats数组：stats[i]["stat"]结构
    * 构建stat_id到value的映射字典
    * 应用与球员统计相同的转换逻辑
  - 数据验证和清洗：
    * 团队统计合理性验证
    * 与对手数据的一致性检查
    * 缺失统计项的处理策略
  - 聚合计算：
    * 团队总分计算验证
    * 效率指标派生：团队投篮效率等
    * 对战优势统计：各项统计的相对表现

输入: Matchup数据中的team_stats (Dict)
输出: 标准化的团队周统计对象 (Dict)
"""

from typing import Dict, List, Optional, Any
from ..normalizers.stats_normalizer import normalize_team_stats, validate_normalized_stats
from .stat_utils import (
    safe_int_conversion, parse_percentage_value,
    parse_made_attempted_stat, calculate_percentage
)


def transform_weekly_stats(team_key: str, league_key: str, season: str,
                          week: int, team_stats_data: Dict) -> Optional[Dict]:
    """转换团队周统计数据
    
    Args:
        team_key: 团队键
        league_key: 联盟键
        season: 赛季
        week: 周数
        team_stats_data: 团队统计数据，来自matchup数据
        
    Returns:
        标准化的团队周统计数据对象，转换失败时返回None
    """
    try:
        # 使用标准化器提取核心统计项
        core_stats = normalize_team_stats(team_stats_data)
        
        if not core_stats:
            print(f"团队 {team_key} 周 {week} 统计标准化失败")
            return None
        
        # 构建完整的团队周统计对象
        weekly_stats = {
            # 基本信息
            "team_key": team_key,
            "league_key": league_key,
            "season": season,
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
        validation_results = validate_normalized_stats(weekly_stats)
        if not all(validation_results.values()):
            print(f"警告: 团队 {team_key} 周 {week} 统计验证失败: {validation_results}")
        
        # 添加团队特定的计算统计项
        _add_team_calculated_stats(weekly_stats)
        
        return weekly_stats
    
    except Exception as e:
        print(f"转换团队周统计失败 {team_key}/{week}: {e}")
        return None


def transform_matchup_stats(matchup_data: Dict, team_key: str) -> Optional[Dict]:
    """从matchup数据中提取团队统计信息
    
    Args:
        matchup_data: 完整的matchup数据
        team_key: 目标团队键
        
    Returns:
        团队统计数据字典，提取失败时返回None
    """
    try:
        # 从matchup数据中定位目标团队的统计数据
        team_stats_data = _extract_team_stats_from_matchup(matchup_data, team_key)
        
        if not team_stats_data:
            print(f"无法从matchup数据中提取团队 {team_key} 的统计")
            return None
        
        return team_stats_data
    
    except Exception as e:
        print(f"从matchup数据提取团队统计失败 {team_key}: {e}")
        return None


def _extract_team_stats_from_matchup(matchup_data: Dict, target_team_key: str) -> Optional[Dict]:
    """从matchup数据中提取指定团队的统计数据
    
    Args:
        matchup_data: matchup数据
        target_team_key: 目标团队键
        
    Returns:
        团队统计数据，未找到时返回None
    """
    try:
        if not matchup_data or "0" not in matchup_data:
            return None
        
        teams_container = matchup_data["0"].get("teams", {})
        teams_count = int(teams_container.get("count", 0))
        
        for i in range(teams_count):
            str_index = str(i)
            if str_index not in teams_container:
                continue
            
            team_container = teams_container[str_index]
            if "team" not in team_container:
                continue
            
            team_data = team_container["team"]
            
            # 从复杂的嵌套结构中提取 team_key 和 team_stats
            current_team_key = None
            team_stats = None
            
            if isinstance(team_data, list) and len(team_data) >= 2:
                # team_data[0] 包含团队基本信息
                team_info = team_data[0]
                if isinstance(team_info, list):
                    for info_item in team_info:
                        if isinstance(info_item, dict) and "team_key" in info_item:
                            current_team_key = info_item["team_key"]
                            break
                
                # team_data[1] 包含统计数据
                if len(team_data) > 1 and isinstance(team_data[1], dict):
                    team_stats_container = team_data[1]
                    if "team_stats" in team_stats_container:
                        team_stats = team_stats_container["team_stats"]
            
            # 如果找到了目标团队，返回其统计数据
            if current_team_key == target_team_key and team_stats:
                return team_stats
        
        return None
        
    except Exception as e:
        print(f"提取团队统计数据失败: {e}")
        return None


def batch_transform_weekly_stats(teams_stats: List[Dict]) -> List[Dict]:
    """批量转换团队周统计数据
    
    Args:
        teams_stats: 团队统计数据列表，每个元素包含team_key、week和stats数据
        
    Returns:
        转换后的团队周统计数据列表
    """
    transformed_stats = []
    
    try:
        for team_stat in teams_stats:
            if not isinstance(team_stat, dict):
                continue
            
            team_key = team_stat.get("team_key")
            league_key = team_stat.get("league_key")
            season = team_stat.get("season")
            week = team_stat.get("week")
            team_stats_data = team_stat.get("team_stats_data", {})
            
            if not all([team_key, league_key, season]) or week is None:
                print(f"跳过不完整的团队统计记录: {team_key}")
                continue
            
            transformed_stat = transform_weekly_stats(
                team_key, league_key, season, week, team_stats_data
            )
            
            if transformed_stat:
                transformed_stats.append(transformed_stat)
    
    except Exception as e:
        print(f"批量转换团队周统计时出错: {e}")
    
    return transformed_stats


def calculate_team_efficiency_metrics(team_stats: Dict) -> Dict[str, Any]:
    """计算团队效率指标
    
    Args:
        team_stats: 团队统计数据
        
    Returns:
        团队效率指标字典
    """
    efficiency_metrics = {}
    
    try:
        # 计算团队攻击效率 (Offensive Rating)
        # 简化版本：每100次进攻得分 = Points * 100 / Possessions
        points = team_stats.get("points", 0)
        fga = team_stats.get("field_goals_attempted", 0)
        fta = team_stats.get("free_throws_attempted", 0)
        turnovers = team_stats.get("turnovers", 0)
        
        # 简化的进攻次数估算
        if fga > 0 or fta > 0 or turnovers > 0:
            estimated_possessions = fga + 0.44 * fta + turnovers
            if estimated_possessions > 0:
                offensive_rating = (points / estimated_possessions) * 100
                efficiency_metrics["offensive_rating"] = round(offensive_rating, 2)
        
        # 计算团队投篮效率
        fg_made = team_stats.get("field_goals_made", 0)
        three_ptm = team_stats.get("three_pointers_made", 0)
        
        if fga > 0:
            # 有效投篮命中率 (eFG%)
            effective_fg_pct = ((fg_made + 0.5 * three_ptm) / fga) * 100
            efficiency_metrics["effective_field_goal_percentage"] = round(effective_fg_pct, 3)
        
        # 计算助攻率 (AST/TO ratio)
        assists = team_stats.get("assists", 0)
        if turnovers > 0:
            assist_to_turnover_ratio = assists / turnovers
            efficiency_metrics["assist_to_turnover_ratio"] = round(assist_to_turnover_ratio, 2)
        elif assists > 0:
            efficiency_metrics["assist_to_turnover_ratio"] = float('inf')  # 无失误情况
        
        # 计算篮板率（假设对手数据可用）
        rebounds = team_stats.get("rebounds", 0)
        if rebounds > 0:
            efficiency_metrics["rebounds_per_game"] = rebounds
    
    except Exception as e:
        print(f"计算团队效率指标时出错: {e}")
    
    return efficiency_metrics


def compare_team_performances(team1_stats: Dict, team2_stats: Dict) -> Dict[str, Any]:
    """比较两个团队的表现
    
    Args:
        team1_stats: 第一个团队的统计数据
        team2_stats: 第二个团队的统计数据
        
    Returns:
        比较结果字典
    """
    comparison = {
        "team1_advantages": [],
        "team2_advantages": [],
        "statistical_differences": {},
        "efficiency_comparison": {}
    }
    
    try:
        # 比较基础统计项
        comparable_fields = [
            "points", "rebounds", "assists", "steals", "blocks", "turnovers",
            "field_goal_percentage", "free_throw_percentage", "three_pointers_made"
        ]
        
        for field in comparable_fields:
            val1 = team1_stats.get(field, 0)
            val2 = team2_stats.get(field, 0)
            
            if val1 is not None and val2 is not None:
                diff = val1 - val2
                comparison["statistical_differences"][field] = diff
                
                # 对于失误，较少的值是优势
                if field == "turnovers":
                    if val1 < val2:
                        comparison["team1_advantages"].append(field)
                    elif val1 > val2:
                        comparison["team2_advantages"].append(field)
                else:
                    # 对于其他统计，较高的值是优势
                    if val1 > val2:
                        comparison["team1_advantages"].append(field)
                    elif val1 < val2:
                        comparison["team2_advantages"].append(field)
        
        # 比较效率指标
        team1_efficiency = calculate_team_efficiency_metrics(team1_stats)
        team2_efficiency = calculate_team_efficiency_metrics(team2_stats)
        
        comparison["efficiency_comparison"] = {
            "team1": team1_efficiency,
            "team2": team2_efficiency
        }
    
    except Exception as e:
        print(f"比较团队表现时出错: {e}")
        comparison["error"] = str(e)
    
    return comparison


def validate_team_stats_consistency(team_stats: Dict) -> Dict[str, Any]:
    """验证团队统计数据的一致性
    
    Args:
        team_stats: 团队统计数据
        
    Returns:
        验证结果字典
    """
    validation_result = {
        "is_valid": True,
        "issues": [],
        "warnings": []
    }
    
    try:
        # 验证投篮统计逻辑
        fg_made = team_stats.get("field_goals_made")
        fg_attempted = team_stats.get("field_goals_attempted")
        
        if fg_made is not None and fg_attempted is not None:
            if fg_made > fg_attempted:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"投篮命中数超过尝试数: {fg_made}/{fg_attempted}")
        
        # 验证罚球统计逻辑
        ft_made = team_stats.get("free_throws_made")
        ft_attempted = team_stats.get("free_throws_attempted")
        
        if ft_made is not None and ft_attempted is not None:
            if ft_made > ft_attempted:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"罚球命中数超过尝试数: {ft_made}/{ft_attempted}")
        
        # 验证数值范围
        numeric_fields = [
            "points", "rebounds", "assists", "steals", "blocks", "turnovers",
            "field_goals_made", "field_goals_attempted", "free_throws_made", 
            "free_throws_attempted", "three_pointers_made"
        ]
        
        for field in numeric_fields:
            value = team_stats.get(field)
            if value is not None and value < 0:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"负数值: {field}={value}")
        
        # 检查统计合理性（警告级别）
        points = team_stats.get("points", 0)
        if points > 200:  # NBA团队单场得分很少超过200
            validation_result["warnings"].append(f"异常高分: {points}分")
        
        rebounds = team_stats.get("rebounds", 0)
        if rebounds > 80:  # NBA团队单场篮板很少超过80
            validation_result["warnings"].append(f"异常高篮板: {rebounds}个")
    
    except Exception as e:
        validation_result["is_valid"] = False
        validation_result["issues"].append(f"验证过程出错: {e}")
    
    return validation_result


def extract_team_stats_summary(teams_stats: List[Dict]) -> Dict[str, Any]:
    """提取团队统计数据摘要
    
    Args:
        teams_stats: 团队统计数据列表
        
    Returns:
        团队统计摘要信息
    """
    try:
        summary = {
            "total_teams": len(teams_stats),
            "weeks_covered": set(),
            "average_stats": {},
            "top_performers": {},
            "statistical_ranges": {}
        }
        
        if not teams_stats:
            return summary
        
        # 统计覆盖的周数
        for team_stat in teams_stats:
            week = team_stat.get("week")
            if week is not None:
                summary["weeks_covered"].add(week)
        
        summary["weeks_covered"] = sorted(list(summary["weeks_covered"]))
        
        # 计算平均统计
        stat_fields = [
            "points", "rebounds", "assists", "steals", "blocks", "turnovers",
            "field_goal_percentage", "free_throw_percentage"
        ]
        
        for field in stat_fields:
            values = [team_stat.get(field, 0) for team_stat in teams_stats if team_stat.get(field) is not None]
            if values:
                summary["average_stats"][field] = round(sum(values) / len(values), 2)
                summary["statistical_ranges"][field] = {
                    "min": min(values),
                    "max": max(values)
                }
        
        # 找出各项统计的最高表现
        for field in stat_fields:
            if field == "turnovers":  # 失误越少越好
                best_stat = min(teams_stats, key=lambda x: x.get(field, float('inf')), default=None)
            else:  # 其他统计越高越好
                best_stat = max(teams_stats, key=lambda x: x.get(field, 0), default=None)
            
            if best_stat:
                summary["top_performers"][field] = {
                    "team_key": best_stat.get("team_key"),
                    "value": best_stat.get(field),
                    "week": best_stat.get("week")
                }
        
        return summary
    
    except Exception as e:
        print(f"提取团队统计摘要时出错: {e}")
        return {"error": str(e)}


def _add_team_calculated_stats(team_stats: Dict) -> None:
    """添加团队特定的计算统计项
    
    Args:
        team_stats: 团队统计数据字典（会被修改）
    """
    try:
        # 计算命中率（用于验证）
        fg_made = team_stats.get("field_goals_made")
        fg_attempted = team_stats.get("field_goals_attempted")
        if fg_made is not None and fg_attempted is not None and fg_attempted > 0:
            calculated_fg_pct = calculate_percentage(fg_made, fg_attempted)
            if calculated_fg_pct:
                team_stats["calculated_field_goal_percentage"] = calculated_fg_pct
        
        ft_made = team_stats.get("free_throws_made")
        ft_attempted = team_stats.get("free_throws_attempted")
        if ft_made is not None and ft_attempted is not None and ft_attempted > 0:
            calculated_ft_pct = calculate_percentage(ft_made, ft_attempted)
            if calculated_ft_pct:
                team_stats["calculated_free_throw_percentage"] = calculated_ft_pct
        
        # 添加团队效率指标
        efficiency_metrics = calculate_team_efficiency_metrics(team_stats)
        team_stats.update(efficiency_metrics)
    
    except Exception as e:
        print(f"添加团队计算统计时出错: {e}") 