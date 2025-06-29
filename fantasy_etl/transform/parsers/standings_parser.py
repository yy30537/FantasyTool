#!/usr/bin/env python3
"""
排名数据解析器

迁移来源: @database_writer.py 中的排名数据处理逻辑
主要映射:
  - 提取和解析Yahoo API排名响应数据
  - 标准化团队排名信息和统计数据

职责:
  - 排名数据解析：
    * 从复杂嵌套的standings响应中提取排名信息
    * 解析teams容器：teams["0"]["teams"][i]["team"]
    * 提取团队排名位置、胜负记录、统计数据
    * 处理不同排名类型：总排名、分区排名等
  - 团队排名信息提取：
    * 解析team_standings容器数据
    * 提取排名位置：rank、wins、losses、ties
    * 计算胜率和胜负百分比
    * 处理季后赛资格信息
  - 统计数据解析：
    * 解析team_stats容器
    * 提取累计统计：points_for、points_against
    * 计算统计排名：各项统计在联盟中的排名
    * 处理统计趋势信息
  - 排名分类处理：
    * 整体联盟排名
    * 分区内排名（如果适用）
    * 近期表现排名
    * 实力指标排名
  - 历史数据关联：
    * 排名变化追踪
    * 胜负记录历史
    * 统计表现趋势
  - 季后赛信息：
    * 季后赛种子排名
    * 季后赛资格状态
    * 剩余赛程影响分析

输入: Yahoo API standings响应数据 (Dict)
输出: 标准化的排名信息列表 (List[Dict])
"""

from typing import Dict, List, Optional, Any
from decimal import Decimal
from ..utils.stat_utils import safe_int_conversion, safe_float_conversion


def parse_standings_data(standings_response: Dict) -> List[Dict]:
    """解析排名数据响应
    
    Args:
        standings_response: Yahoo API standings响应数据
        
    Returns:
        标准化的团队排名信息列表
    """
    try:
        standings = []
        
        if not standings_response or "teams" not in standings_response:
            return standings
        
        teams_container = standings_response["teams"]
        
        # 处理不同的teams结构
        if isinstance(teams_container, dict) and "0" in teams_container:
            # 标准嵌套结构
            teams_data = teams_container["0"].get("teams", {})
            teams_count = int(teams_data.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if "team" in team_container:
                    team_standings = parse_single_team_standings(team_container["team"])
                    if team_standings:
                        standings.append(team_standings)
        
        elif isinstance(teams_container, list):
            # 直接的团队列表
            for team_item in teams_container:
                team_standings = parse_single_team_standings(team_item)
                if team_standings:
                    standings.append(team_standings)
        
        # 按排名位置排序
        standings.sort(key=lambda x: x.get("rank", float('inf')))
        
        return standings
    
    except Exception as e:
        print(f"解析排名数据失败: {e}")
        return []


def parse_single_team_standings(team_data: Any) -> Optional[Dict]:
    """解析单个团队的排名信息
    
    Args:
        team_data: 团队数据，可能是复杂的嵌套结构
        
    Returns:
        标准化的团队排名信息，解析失败时返回None
    """
    try:
        if not team_data:
            return None
        
        team_standings = {
            # 基本团队信息
            "team_key": None,
            "team_id": None,
            "team_name": None,
            "team_logo": None,
            
            # 排名信息
            "rank": None,
            "wins": None,
            "losses": None,
            "ties": None,
            "win_percentage": None,
            
            # 统计信息
            "points_for": None,
            "points_against": None,
            "points_difference": None,
            
            # 季后赛信息
            "playoff_seed": None,
            "clinched_playoffs": False,
            "eliminated": False,
            
            # 附加信息
            "streak": None,
            "division_rank": None,
            "waiver_priority": None
        }
        
        # 处理不同的team数据结构
        if isinstance(team_data, list):
            # 嵌套列表结构
            for team_item in team_data:
                if isinstance(team_item, dict):
                    _extract_team_basic_info(team_standings, team_item)
                    _extract_standings_info(team_standings, team_item)
                    _extract_team_stats_info(team_standings, team_item)
        
        elif isinstance(team_data, dict):
            # 直接字典结构
            _extract_team_basic_info(team_standings, team_data)
            _extract_standings_info(team_standings, team_data)
            _extract_team_stats_info(team_standings, team_data)
        
        # 计算派生字段
        _calculate_derived_standings_fields(team_standings)
        
        # 验证必需信息
        if team_standings["team_key"]:
            return team_standings
        
        return None
    
    except Exception as e:
        print(f"解析团队排名信息失败: {e}")
        return None


def _extract_team_basic_info(team_standings: Dict, team_data: Dict) -> None:
    """提取团队基本信息
    
    Args:
        team_standings: 团队排名信息字典（会被修改）
        team_data: 原始团队数据
    """
    try:
        # 基本身份信息
        if "team_key" in team_data:
            team_standings["team_key"] = team_data["team_key"]
        
        if "team_id" in team_data:
            team_standings["team_id"] = safe_int_conversion(team_data["team_id"])
        
        # 团队名称
        if "name" in team_data:
            team_standings["team_name"] = team_data["name"]
        
        # 团队图标
        if "team_logos" in team_data:
            team_logos = team_data["team_logos"]
            if isinstance(team_logos, dict) and "0" in team_logos:
                logo_data = team_logos["0"]
                if isinstance(logo_data, dict) and "team_logo" in logo_data:
                    logo_info = logo_data["team_logo"]
                    if isinstance(logo_info, dict) and "url" in logo_info:
                        team_standings["team_logo"] = logo_info["url"]
        
        # Waiver Priority
        if "waiver_priority" in team_data:
            team_standings["waiver_priority"] = safe_int_conversion(team_data["waiver_priority"])
    
    except Exception as e:
        print(f"提取团队基本信息失败: {e}")


def _extract_standings_info(team_standings: Dict, team_data: Dict) -> None:
    """提取排名信息
    
    Args:
        team_standings: 团队排名信息字典（会被修改）
        team_data: 原始团队数据
    """
    try:
        # 直接的排名字段
        if "team_standings" in team_data:
            standings_data = team_data["team_standings"]
            
            if isinstance(standings_data, dict):
                # 排名位置
                if "rank" in standings_data:
                    team_standings["rank"] = safe_int_conversion(standings_data["rank"])
                
                # 胜负记录
                if "outcome_totals" in standings_data:
                    outcome_totals = standings_data["outcome_totals"]
                    if isinstance(outcome_totals, dict):
                        team_standings["wins"] = safe_int_conversion(outcome_totals.get("wins"))
                        team_standings["losses"] = safe_int_conversion(outcome_totals.get("losses"))
                        team_standings["ties"] = safe_int_conversion(outcome_totals.get("ties"))
                
                # 胜率
                if "win_percentage" in standings_data:
                    team_standings["win_percentage"] = safe_float_conversion(standings_data["win_percentage"])
                
                # 得失分
                if "points_for" in standings_data:
                    team_standings["points_for"] = safe_float_conversion(standings_data["points_for"])
                
                if "points_against" in standings_data:
                    team_standings["points_against"] = safe_float_conversion(standings_data["points_against"])
                
                # 季后赛信息
                if "playoff_seed" in standings_data:
                    team_standings["playoff_seed"] = safe_int_conversion(standings_data["playoff_seed"])
                
                if "clinched_playoffs" in standings_data:
                    team_standings["clinched_playoffs"] = bool(standings_data["clinched_playoffs"])
                
                if "eliminated" in standings_data:
                    team_standings["eliminated"] = bool(standings_data["eliminated"])
                
                # 连胜/连败
                if "streak" in standings_data:
                    streak_data = standings_data["streak"]
                    if isinstance(streak_data, dict):
                        streak_type = streak_data.get("type", "")
                        streak_value = safe_int_conversion(streak_data.get("value", 0))
                        if streak_type and streak_value:
                            team_standings["streak"] = f"{streak_type}{streak_value}"
                    else:
                        team_standings["streak"] = str(streak_data)
                
                # 分区排名
                if "division_rank" in standings_data:
                    team_standings["division_rank"] = safe_int_conversion(standings_data["division_rank"])
    
    except Exception as e:
        print(f"提取排名信息失败: {e}")


def _extract_team_stats_info(team_standings: Dict, team_data: Dict) -> None:
    """提取团队统计信息
    
    Args:
        team_standings: 团队排名信息字典（会被修改）
        team_data: 原始团队数据
    """
    try:
        if "team_stats" not in team_data:
            return
        
        team_stats = team_data["team_stats"]
        
        # 如果team_stats包含stats数组，提取相关统计
        if isinstance(team_stats, dict) and "stats" in team_stats:
            stats_data = team_stats["stats"]
            
            # 转换stats数组为字典格式
            stats_dict = {}
            if isinstance(stats_data, list):
                for stat_item in stats_data:
                    if isinstance(stat_item, dict) and "stat" in stat_item:
                        stat = stat_item["stat"]
                        if isinstance(stat, dict):
                            stat_id = stat.get("stat_id")
                            stat_value = stat.get("value")
                            if stat_id and stat_value is not None:
                                stats_dict[str(stat_id)] = stat_value
            
            # 提取特定的统计项（根据Yahoo Fantasy的stat_id）
            # 这些ID可能因运动项目而异，这里使用常见的篮球统计
            
            # 总得分相关统计
            if "60" in stats_dict:  # 假设60是总得分的stat_id
                team_standings["season_points"] = safe_float_conversion(stats_dict["60"])
            
            # 其他相关统计可以在这里添加
            
        # 直接的统计字段
        for field in ["points_for", "points_against"]:
            if field in team_stats:
                team_standings[field] = safe_float_conversion(team_stats[field])
    
    except Exception as e:
        print(f"提取团队统计信息失败: {e}")


def _calculate_derived_standings_fields(team_standings: Dict) -> None:
    """计算派生的排名字段
    
    Args:
        team_standings: 团队排名信息字典（会被修改）
    """
    try:
        # 计算胜率（如果没有提供的话）
        wins = team_standings.get("wins")
        losses = team_standings.get("losses")
        ties = team_standings.get("ties", 0)
        
        if wins is not None and losses is not None and team_standings.get("win_percentage") is None:
            total_games = wins + losses + ties
            if total_games > 0:
                # 在有平局的情况下，平局算0.5胜
                effective_wins = wins + (ties * 0.5)
                win_percentage = effective_wins / total_games
                team_standings["win_percentage"] = round(win_percentage, 3)
        
        # 计算得失分差
        points_for = team_standings.get("points_for")
        points_against = team_standings.get("points_against")
        
        if points_for is not None and points_against is not None:
            team_standings["points_difference"] = round(points_for - points_against, 2)
    
    except Exception as e:
        print(f"计算派生排名字段失败: {e}")


def calculate_standings_statistics(standings: List[Dict]) -> Dict[str, Any]:
    """计算排名统计信息
    
    Args:
        standings: 团队排名列表
        
    Returns:
        排名统计信息字典
    """
    try:
        if not standings:
            return {}
        
        stats = {
            "total_teams": len(standings),
            "playoff_teams": 0,
            "eliminated_teams": 0,
            "average_wins": 0,
            "average_points_for": 0,
            "average_points_against": 0,
            "highest_scoring_team": None,
            "lowest_scoring_team": None,
            "best_record": None,
            "worst_record": None,
            "tightest_race": []
        }
        
        # 收集数据
        wins_list = []
        points_for_list = []
        points_against_list = []
        
        for team in standings:
            # 统计季后赛状态
            if team.get("clinched_playoffs"):
                stats["playoff_teams"] += 1
            
            if team.get("eliminated"):
                stats["eliminated_teams"] += 1
            
            # 收集胜场数据
            wins = team.get("wins")
            if wins is not None:
                wins_list.append(wins)
            
            # 收集得分数据
            points_for = team.get("points_for")
            if points_for is not None:
                points_for_list.append(points_for)
            
            points_against = team.get("points_against")
            if points_against is not None:
                points_against_list.append(points_against)
        
        # 计算平均值
        if wins_list:
            stats["average_wins"] = round(sum(wins_list) / len(wins_list), 1)
        
        if points_for_list:
            stats["average_points_for"] = round(sum(points_for_list) / len(points_for_list), 1)
        
        if points_against_list:
            stats["average_points_against"] = round(sum(points_against_list) / len(points_against_list), 1)
        
        # 找出极值
        if standings:
            # 最高得分团队
            highest_scoring = max(standings, key=lambda x: x.get("points_for", 0))
            stats["highest_scoring_team"] = {
                "team_key": highest_scoring["team_key"],
                "team_name": highest_scoring.get("team_name"),
                "points_for": highest_scoring.get("points_for")
            }
            
            # 最低得分团队
            lowest_scoring = min(standings, key=lambda x: x.get("points_for", float('inf')))
            stats["lowest_scoring_team"] = {
                "team_key": lowest_scoring["team_key"],
                "team_name": lowest_scoring.get("team_name"),
                "points_for": lowest_scoring.get("points_for")
            }
            
            # 最佳战绩
            best_record = max(standings, key=lambda x: (
                x.get("wins", 0),
                -x.get("losses", 0),
                x.get("points_for", 0)
            ))
            stats["best_record"] = {
                "team_key": best_record["team_key"],
                "team_name": best_record.get("team_name"),
                "wins": best_record.get("wins"),
                "losses": best_record.get("losses"),
                "win_percentage": best_record.get("win_percentage")
            }
            
            # 最差战绩
            worst_record = min(standings, key=lambda x: (
                x.get("wins", 0),
                -x.get("losses", 0),
                x.get("points_for", 0)
            ))
            stats["worst_record"] = {
                "team_key": worst_record["team_key"],
                "team_name": worst_record.get("team_name"),
                "wins": worst_record.get("wins"),
                "losses": worst_record.get("losses"),
                "win_percentage": worst_record.get("win_percentage")
            }
        
        # 找出竞争最激烈的排名段
        stats["tightest_race"] = _find_tightest_races(standings)
        
        return stats
    
    except Exception as e:
        print(f"计算排名统计失败: {e}")
        return {"error": str(e)}


def _find_tightest_races(standings: List[Dict]) -> List[Dict]:
    """找出竞争最激烈的排名段
    
    Args:
        standings: 排名列表
        
    Returns:
        竞争激烈的排名段列表
    """
    tight_races = []
    
    try:
        if len(standings) < 2:
            return tight_races
        
        # 按胜场数分组
        by_wins = {}
        for team in standings:
            wins = team.get("wins")
            if wins is not None:
                if wins not in by_wins:
                    by_wins[wins] = []
                by_wins[wins].append(team)
        
        # 找出胜场数相同的团队组
        for wins, teams in by_wins.items():
            if len(teams) > 1:
                # 按得分差距排序
                teams_sorted = sorted(teams, key=lambda x: x.get("points_for", 0), reverse=True)
                
                # 计算得分差距
                if len(teams_sorted) >= 2:
                    highest_points = teams_sorted[0].get("points_for", 0)
                    lowest_points = teams_sorted[-1].get("points_for", 0)
                    points_spread = highest_points - lowest_points
                    
                    tight_races.append({
                        "wins": wins,
                        "teams_count": len(teams),
                        "teams": [{"team_key": t["team_key"], "team_name": t.get("team_name"), 
                                 "points_for": t.get("points_for")} for t in teams_sorted],
                        "points_spread": round(points_spread, 2)
                    })
        
        # 按参与团队数排序
        tight_races.sort(key=lambda x: x["teams_count"], reverse=True)
        
        return tight_races[:3]  # 返回前3个最激烈的竞争
    
    except Exception as e:
        print(f"找出激烈竞争失败: {e}")
        return []


def filter_standings_by_criteria(standings: List[Dict], criteria: Dict) -> List[Dict]:
    """根据条件过滤排名数据
    
    Args:
        standings: 排名列表
        criteria: 过滤条件
        
    Returns:
        过滤后的排名列表
    """
    try:
        filtered_standings = []
        
        for team_standing in standings:
            if _matches_standings_criteria(team_standing, criteria):
                filtered_standings.append(team_standing)
        
        return filtered_standings
    
    except Exception as e:
        print(f"过滤排名数据失败: {e}")
        return standings


def _matches_standings_criteria(team_standing: Dict, criteria: Dict) -> bool:
    """检查团队排名是否匹配过滤条件
    
    Args:
        team_standing: 团队排名信息
        criteria: 过滤条件
        
    Returns:
        是否匹配
    """
    try:
        # 排名范围过滤
        if "rank_range" in criteria:
            rank = team_standing.get("rank")
            if rank is not None:
                min_rank, max_rank = criteria["rank_range"]
                if rank < min_rank or rank > max_rank:
                    return False
        
        # 胜场数过滤
        if "min_wins" in criteria:
            wins = team_standing.get("wins")
            if wins is None or wins < criteria["min_wins"]:
                return False
        
        if "max_losses" in criteria:
            losses = team_standing.get("losses")
            if losses is None or losses > criteria["max_losses"]:
                return False
        
        # 季后赛状态过滤
        if "playoff_status" in criteria:
            status = criteria["playoff_status"]
            if status == "clinched":
                if not team_standing.get("clinched_playoffs"):
                    return False
            elif status == "eliminated":
                if not team_standing.get("eliminated"):
                    return False
            elif status == "competing":
                if (team_standing.get("clinched_playoffs") or 
                    team_standing.get("eliminated")):
                    return False
        
        # 得分范围过滤
        if "points_for_range" in criteria:
            points_for = team_standing.get("points_for")
            if points_for is not None:
                min_points, max_points = criteria["points_for_range"]
                if points_for < min_points or points_for > max_points:
                    return False
        
        return True
    
    except Exception as e:
        print(f"检查排名匹配条件失败: {e}")
        return False


def extract_standings_summary(standings: List[Dict]) -> Dict[str, Any]:
    """提取排名数据摘要信息
    
    Args:
        standings: 排名列表
        
    Returns:
        排名摘要信息
    """
    try:
        summary = {
            "total_teams": len(standings),
            "standings_date": None,  # 可以从API响应中获取
            "league_statistics": calculate_standings_statistics(standings),
            "top_teams": [],
            "bottom_teams": [],
            "playoff_picture": {
                "clinched": [],
                "eliminated": [],
                "competing": []
            }
        }
        
        if not standings:
            return summary
        
        # 前几名团队
        summary["top_teams"] = [
            {
                "rank": team.get("rank"),
                "team_key": team["team_key"],
                "team_name": team.get("team_name"),
                "wins": team.get("wins"),
                "losses": team.get("losses"),
                "points_for": team.get("points_for")
            }
            for team in standings[:5]  # 前5名
        ]
        
        # 后几名团队
        summary["bottom_teams"] = [
            {
                "rank": team.get("rank"),
                "team_key": team["team_key"],
                "team_name": team.get("team_name"),
                "wins": team.get("wins"),
                "losses": team.get("losses"),
                "points_for": team.get("points_for")
            }
            for team in standings[-5:]  # 后5名
        ]
        
        # 季后赛情况
        for team in standings:
            team_info = {
                "team_key": team["team_key"],
                "team_name": team.get("team_name"),
                "rank": team.get("rank")
            }
            
            if team.get("clinched_playoffs"):
                summary["playoff_picture"]["clinched"].append(team_info)
            elif team.get("eliminated"):
                summary["playoff_picture"]["eliminated"].append(team_info)
            else:
                summary["playoff_picture"]["competing"].append(team_info)
        
        return summary
    
    except Exception as e:
        print(f"提取排名摘要失败: {e}")
        return {"error": str(e)}


def validate_standings_data(standings: List[Dict]) -> Dict[str, Any]:
    """验证排名数据的一致性
    
    Args:
        standings: 排名列表
        
    Returns:
        验证结果字典
    """
    try:
        validation_result = {
            "is_valid": True,
            "issues": [],
            "warnings": []
        }
        
        if not standings:
            validation_result["warnings"].append("排名列表为空")
            return validation_result
        
        # 检查排名连续性
        ranks = [team.get("rank") for team in standings if team.get("rank") is not None]
        if ranks:
            ranks.sort()
            expected_ranks = list(range(1, len(ranks) + 1))
            if ranks != expected_ranks:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"排名不连续: {ranks}")
        
        # 检查重复的team_key
        team_keys = [team.get("team_key") for team in standings if team.get("team_key")]
        if len(team_keys) != len(set(team_keys)):
            validation_result["is_valid"] = False
            validation_result["issues"].append("存在重复的团队键")
        
        # 检查胜负逻辑
        for team in standings:
            wins = team.get("wins")
            losses = team.get("losses")
            win_pct = team.get("win_percentage")
            
            if wins is not None and losses is not None and win_pct is not None:
                total_games = wins + losses
                if total_games > 0:
                    expected_pct = wins / total_games
                    if abs(win_pct - expected_pct) > 0.01:  # 允许小的舍入误差
                        validation_result["warnings"].append(
                            f"团队 {team.get('team_key')} 胜率计算不匹配"
                        )
        
        return validation_result
    
    except Exception as e:
        validation_result = {
            "is_valid": False,
            "issues": [f"验证过程出错: {e}"],
            "warnings": []
        }
        return validation_result 