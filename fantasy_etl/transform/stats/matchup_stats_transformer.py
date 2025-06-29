#!/usr/bin/env python3
"""
对战统计数据转换器

迁移来源: @database_writer.py 中的对战数据处理逻辑
主要映射:
  - write_team_weekly_stats_from_matchup() -> transform_matchup_data()
  - Matchup数据结构解析逻辑

职责:
  - Matchup数据结构解析：
    * 从复杂嵌套的matchup响应中提取团队信息
    * 解析teams容器：teams["0"]["teams"][i]["team"]结构
    * 识别对战双方：通过count确定团队数量
    * 提取团队基本信息和统计数据
  - 对战结果分析：
    * 计算各项统计的胜负关系
    * 识别关键统计差异：得分、命中率等
    * 生成对战摘要和亮点
  - 团队数据配对：
    * 确保对战数据的完整性
    * 验证对战双方数据一致性
    * 处理缺失或不完整的数据
  - 胜负预测分析：
    * 基于统计差异预测胜负
    * 计算关键指标权重
    * 生成胜负概率估算
  - 数据标准化：
    * 应用统一的统计转换逻辑
    * 确保与团队统计转换器的一致性
    * 添加对战特定的元数据

输入: Yahoo API matchup响应数据 (Dict)
输出: 标准化的对战数据和分析结果 (Dict)
"""

from typing import Dict, List, Optional, Any, Tuple
from ..parsers.matchup_parser import parse_matchup_data, extract_matchup_summary
from .team_stats_transformer import (
    transform_weekly_stats, transform_matchup_stats,
    compare_team_performances, calculate_team_efficiency_metrics
)


def transform_matchup_data(matchup_data: Dict, league_key: str, 
                          season: str, week: int) -> Optional[Dict]:
    """转换完整的对战数据
    
    Args:
        matchup_data: Yahoo API matchup响应数据
        league_key: 联盟键
        season: 赛季
        week: 周数
        
    Returns:
        标准化的对战数据对象，转换失败时返回None
    """
    try:
        # 使用解析器提取基本对战信息
        parsed_matchup = parse_matchup_data(matchup_data)
        
        if not parsed_matchup:
            print(f"解析matchup数据失败，联盟 {league_key} 周 {week}")
            return None
        
        # 提取对战双方的团队信息
        teams_info = _extract_teams_from_matchup(matchup_data)
        
        if len(teams_info) != 2:
            print(f"对战数据不完整，应有2个团队，实际 {len(teams_info)} 个")
            return None
        
        team1, team2 = teams_info
        
        # 转换双方的统计数据
        team1_stats = None
        team2_stats = None
        
        if team1.get("team_stats"):
            team1_stats = transform_weekly_stats(
                team1["team_key"], league_key, season, week, team1["team_stats"]
            )
        
        if team2.get("team_stats"):
            team2_stats = transform_weekly_stats(
                team2["team_key"], league_key, season, week, team2["team_stats"]
            )
        
        # 构建完整的对战数据对象
        matchup_result = {
            # 基本信息
            "league_key": league_key,
            "season": season,
            "week": week,
            "matchup_id": parsed_matchup.get("matchup_id"),
            
            # 对战双方信息
            "team1": {
                "team_key": team1["team_key"],
                "team_name": team1.get("team_name"),
                "stats": team1_stats
            },
            "team2": {
                "team_key": team2["team_key"],
                "team_name": team2.get("team_name"),
                "stats": team2_stats
            },
            
            # 原始解析数据
            "raw_matchup_data": parsed_matchup
        }
        
        # 添加对战分析
        if team1_stats and team2_stats:
            matchup_analysis = analyze_matchup_performance(team1_stats, team2_stats)
            matchup_result["analysis"] = matchup_analysis
        
        return matchup_result
    
    except Exception as e:
        print(f"转换对战数据失败 {league_key}/{week}: {e}")
        return None


def _extract_teams_from_matchup(matchup_data: Dict) -> List[Dict]:
    """从matchup数据中提取团队信息
    
    Args:
        matchup_data: matchup响应数据
        
    Returns:
        团队信息列表，每个元素包含team_key、team_name和team_stats
    """
    teams_info = []
    
    try:
        if not matchup_data or "0" not in matchup_data:
            return teams_info
        
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
            
            # 提取团队信息
            team_info = _parse_team_from_matchup_data(team_data)
            if team_info:
                teams_info.append(team_info)
    
    except Exception as e:
        print(f"提取团队信息失败: {e}")
    
    return teams_info


def _parse_team_from_matchup_data(team_data: Any) -> Optional[Dict]:
    """从团队数据中提取信息
    
    Args:
        team_data: 团队数据，可能是复杂的嵌套结构
        
    Returns:
        团队信息字典，解析失败时返回None
    """
    try:
        team_info = {
            "team_key": None,
            "team_name": None,
            "team_stats": None
        }
        
        if isinstance(team_data, list) and len(team_data) >= 2:
            # team_data[0] 包含团队基本信息
            team_basic_info = team_data[0]
            if isinstance(team_basic_info, list):
                for info_item in team_basic_info:
                    if isinstance(info_item, dict):
                        if "team_key" in info_item:
                            team_info["team_key"] = info_item["team_key"]
                        if "name" in info_item:
                            team_info["team_name"] = info_item["name"]
            elif isinstance(team_basic_info, dict):
                team_info["team_key"] = team_basic_info.get("team_key")
                team_info["team_name"] = team_basic_info.get("name")
            
            # team_data[1] 包含统计数据
            if len(team_data) > 1 and isinstance(team_data[1], dict):
                team_stats_container = team_data[1]
                if "team_stats" in team_stats_container:
                    team_info["team_stats"] = team_stats_container["team_stats"]
        
        # 验证必需信息
        if team_info["team_key"]:
            return team_info
        
        return None
    
    except Exception as e:
        print(f"解析团队数据失败: {e}")
        return None


def analyze_matchup_performance(team1_stats: Dict, team2_stats: Dict) -> Dict[str, Any]:
    """分析对战双方的表现
    
    Args:
        team1_stats: 第一个团队的统计数据
        team2_stats: 第二个团队的统计数据
        
    Returns:
        对战分析结果字典
    """
    try:
        # 使用团队比较功能
        comparison = compare_team_performances(team1_stats, team2_stats)
        
        # 计算胜负概率
        win_probability = calculate_win_probability(team1_stats, team2_stats)
        
        # 识别关键统计差异
        key_differences = identify_key_differences(team1_stats, team2_stats)
        
        # 生成对战亮点
        highlights = generate_matchup_highlights(team1_stats, team2_stats, comparison)
        
        analysis = {
            "comparison": comparison,
            "win_probability": win_probability,
            "key_differences": key_differences,
            "highlights": highlights,
            "predicted_winner": _predict_winner(team1_stats, team2_stats, win_probability)
        }
        
        return analysis
    
    except Exception as e:
        print(f"分析对战表现失败: {e}")
        return {"error": str(e)}


def calculate_win_probability(team1_stats: Dict, team2_stats: Dict) -> Dict[str, float]:
    """计算双方胜负概率
    
    Args:
        team1_stats: 第一个团队统计
        team2_stats: 第二个团队统计
        
    Returns:
        胜负概率字典
    """
    try:
        # 定义关键统计权重
        stat_weights = {
            "points": 0.25,
            "field_goal_percentage": 0.20,
            "free_throw_percentage": 0.15,
            "rebounds": 0.15,
            "assists": 0.10,
            "steals": 0.05,
            "blocks": 0.05,
            "turnovers": -0.05  # 失误越多越不利
        }
        
        team1_score = 0
        team2_score = 0
        
        for stat, weight in stat_weights.items():
            val1 = team1_stats.get(stat, 0)
            val2 = team2_stats.get(stat, 0)
            
            if val1 > val2:
                team1_score += weight
            elif val2 > val1:
                team2_score += weight
        
        # 标准化到概率
        total_score = abs(team1_score) + abs(team2_score)
        if total_score > 0:
            team1_prob = (team1_score + total_score/2) / total_score
            team2_prob = 1 - team1_prob
        else:
            team1_prob = 0.5
            team2_prob = 0.5
        
        return {
            "team1_win_probability": round(max(0, min(1, team1_prob)), 3),
            "team2_win_probability": round(max(0, min(1, team2_prob)), 3)
        }
    
    except Exception as e:
        print(f"计算胜负概率失败: {e}")
        return {"team1_win_probability": 0.5, "team2_win_probability": 0.5}


def identify_key_differences(team1_stats: Dict, team2_stats: Dict) -> List[Dict[str, Any]]:
    """识别关键统计差异
    
    Args:
        team1_stats: 第一个团队统计
        team2_stats: 第二个团队统计
        
    Returns:
        关键差异列表
    """
    key_differences = []
    
    try:
        # 重要统计项及其阈值
        important_stats = {
            "points": {"threshold": 10, "name": "得分"},
            "field_goal_percentage": {"threshold": 5, "name": "投篮命中率"},
            "free_throw_percentage": {"threshold": 10, "name": "罚球命中率"},
            "rebounds": {"threshold": 5, "name": "篮板"},
            "assists": {"threshold": 3, "name": "助攻"},
            "turnovers": {"threshold": 2, "name": "失误"}
        }
        
        for stat, config in important_stats.items():
            val1 = team1_stats.get(stat, 0)
            val2 = team2_stats.get(stat, 0)
            
            if val1 is None or val2 is None:
                continue
            
            diff = abs(val1 - val2)
            
            if diff >= config["threshold"]:
                key_differences.append({
                    "stat": stat,
                    "stat_name": config["name"],
                    "team1_value": val1,
                    "team2_value": val2,
                    "difference": diff,
                    "advantage": "team1" if val1 > val2 else "team2"
                })
        
        # 按差异大小排序
        key_differences.sort(key=lambda x: x["difference"], reverse=True)
        
        return key_differences[:5]  # 只返回前5个最大差异
    
    except Exception as e:
        print(f"识别关键差异失败: {e}")
        return []


def generate_matchup_highlights(team1_stats: Dict, team2_stats: Dict, 
                               comparison: Dict) -> List[str]:
    """生成对战亮点描述
    
    Args:
        team1_stats: 第一个团队统计
        team2_stats: 第二个团队统计
        comparison: 团队比较结果
        
    Returns:
        亮点描述列表
    """
    highlights = []
    
    try:
        # 得分亮点
        team1_points = team1_stats.get("points", 0)
        team2_points = team2_stats.get("points", 0)
        
        if team1_points > team2_points:
            highlights.append(f"Team1 以 {team1_points}-{team2_points} 在得分上领先")
        elif team2_points > team1_points:
            highlights.append(f"Team2 以 {team2_points}-{team1_points} 在得分上领先")
        else:
            highlights.append(f"双方得分持平，均为 {team1_points} 分")
        
        # 投篮效率亮点
        team1_fg_pct = team1_stats.get("field_goal_percentage", 0)
        team2_fg_pct = team2_stats.get("field_goal_percentage", 0)
        
        if abs(team1_fg_pct - team2_fg_pct) >= 5:
            if team1_fg_pct > team2_fg_pct:
                highlights.append(f"Team1 投篮命中率 {team1_fg_pct}% 明显优于 Team2 的 {team2_fg_pct}%")
            else:
                highlights.append(f"Team2 投篮命中率 {team2_fg_pct}% 明显优于 Team1 的 {team1_fg_pct}%")
        
        # 篮板优势
        team1_rebounds = team1_stats.get("rebounds", 0)
        team2_rebounds = team2_stats.get("rebounds", 0)
        
        if abs(team1_rebounds - team2_rebounds) >= 5:
            if team1_rebounds > team2_rebounds:
                highlights.append(f"Team1 在篮板上占据优势 {team1_rebounds}-{team2_rebounds}")
            else:
                highlights.append(f"Team2 在篮板上占据优势 {team2_rebounds}-{team1_rebounds}")
        
        # 失误对比
        team1_turnovers = team1_stats.get("turnovers", 0)
        team2_turnovers = team2_stats.get("turnovers", 0)
        
        if abs(team1_turnovers - team2_turnovers) >= 2:
            if team1_turnovers < team2_turnovers:
                highlights.append(f"Team1 保护球更好，失误 {team1_turnovers} 次 vs Team2 的 {team2_turnovers} 次")
            else:
                highlights.append(f"Team2 保护球更好，失误 {team2_turnovers} 次 vs Team1 的 {team1_turnovers} 次")
        
        return highlights[:3]  # 只返回前3个亮点
    
    except Exception as e:
        print(f"生成对战亮点失败: {e}")
        return []


def _predict_winner(team1_stats: Dict, team2_stats: Dict, 
                   win_probability: Dict) -> Dict[str, Any]:
    """预测获胜方
    
    Args:
        team1_stats: 第一个团队统计
        team2_stats: 第二个团队统计
        win_probability: 胜负概率
        
    Returns:
        预测结果字典
    """
    try:
        team1_prob = win_probability.get("team1_win_probability", 0.5)
        team2_prob = win_probability.get("team2_win_probability", 0.5)
        
        if team1_prob > team2_prob:
            predicted_winner = "team1"
            confidence = team1_prob
        else:
            predicted_winner = "team2"
            confidence = team2_prob
        
        # 确定预测置信度级别
        if confidence >= 0.7:
            confidence_level = "高"
        elif confidence >= 0.6:
            confidence_level = "中等"
        else:
            confidence_level = "低"
        
        return {
            "predicted_winner": predicted_winner,
            "confidence": round(confidence, 3),
            "confidence_level": confidence_level
        }
    
    except Exception as e:
        print(f"预测获胜方失败: {e}")
        return {
            "predicted_winner": "unknown",
            "confidence": 0.5,
            "confidence_level": "无法预测"
        }


def batch_transform_matchups(matchups_data: List[Dict], league_key: str, 
                           season: str) -> List[Dict]:
    """批量转换对战数据
    
    Args:
        matchups_data: 对战数据列表，每个元素包含week和matchup_data
        league_key: 联盟键
        season: 赛季
        
    Returns:
        转换后的对战数据列表
    """
    transformed_matchups = []
    
    try:
        for matchup_item in matchups_data:
            if not isinstance(matchup_item, dict):
                continue
            
            week = matchup_item.get("week")
            matchup_data = matchup_item.get("matchup_data", {})
            
            if week is None or not matchup_data:
                print(f"跳过不完整的对战记录: 周 {week}")
                continue
            
            transformed_matchup = transform_matchup_data(
                matchup_data, league_key, season, week
            )
            
            if transformed_matchup:
                transformed_matchups.append(transformed_matchup)
    
    except Exception as e:
        print(f"批量转换对战数据时出错: {e}")
    
    return transformed_matchups


def extract_matchup_trends(matchups_data: List[Dict]) -> Dict[str, Any]:
    """提取对战趋势分析
    
    Args:
        matchups_data: 对战数据列表
        
    Returns:
        趋势分析结果
    """
    try:
        trends = {
            "weeks_analyzed": len(matchups_data),
            "total_matchups": 0,
            "average_scores": {"team1": 0, "team2": 0},
            "close_games": 0,  # 分差小于10分的比赛
            "blowouts": 0,     # 分差大于30分的比赛
            "stat_trends": {}
        }
        
        if not matchups_data:
            return trends
        
        total_team1_points = 0
        total_team2_points = 0
        stat_sums = {}
        
        for matchup in matchups_data:
            trends["total_matchups"] += 1
            
            team1_stats = matchup.get("team1", {}).get("stats", {})
            team2_stats = matchup.get("team2", {}).get("stats", {})
            
            # 统计得分
            team1_points = team1_stats.get("points", 0)
            team2_points = team2_stats.get("points", 0)
            
            total_team1_points += team1_points
            total_team2_points += team2_points
            
            # 分析比赛类型
            point_diff = abs(team1_points - team2_points)
            if point_diff <= 10:
                trends["close_games"] += 1
            elif point_diff >= 30:
                trends["blowouts"] += 1
            
            # 收集其他统计趋势
            for stat in ["rebounds", "assists", "steals", "blocks", "turnovers"]:
                if stat not in stat_sums:
                    stat_sums[stat] = {"team1": 0, "team2": 0}
                
                stat_sums[stat]["team1"] += team1_stats.get(stat, 0)
                stat_sums[stat]["team2"] += team2_stats.get(stat, 0)
        
        # 计算平均值
        if trends["total_matchups"] > 0:
            trends["average_scores"]["team1"] = round(total_team1_points / trends["total_matchups"], 1)
            trends["average_scores"]["team2"] = round(total_team2_points / trends["total_matchups"], 1)
            
            for stat, values in stat_sums.items():
                trends["stat_trends"][stat] = {
                    "team1_avg": round(values["team1"] / trends["total_matchups"], 1),
                    "team2_avg": round(values["team2"] / trends["total_matchups"], 1)
                }
        
        return trends
    
    except Exception as e:
        print(f"提取对战趋势失败: {e}")
        return {"error": str(e)} 