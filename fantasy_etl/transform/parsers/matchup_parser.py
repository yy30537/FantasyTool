#!/usr/bin/env python3
"""
对战数据解析器

迁移来源: @database_writer.py 中的对战数据处理逻辑
主要映射:
  - 提取和解析Yahoo API对战响应数据
  - 从复杂的matchup结构中提取团队对战信息

职责:
  - Matchup数据解析：
    * 从复杂嵌套的matchup响应中提取对战信息
    * 解析teams容器：teams["0"]["teams"][i]["team"]
    * 识别对战双方和对战基本信息
    * 处理不同类型的对战：常规赛、季后赛等
  - 团队对战信息提取：
    * 解析每个团队在对战中的信息
    * 提取团队基本信息：team_key、team_name等
    * 获取团队在该对战中的统计数据
    * 处理team_stats容器中的统计信息
  - 对战状态解析：
    * 解析对战状态：ongoing、completed等
    * 提取对战周数和赛季信息
    * 处理对战时间和截止时间
  - 胜负结果处理：
    * 确定对战结果（如果已完成）
    * 计算得分差距
    * 识别胜方和负方
  - 数据结构标准化：
    * 将复杂的嵌套结构转换为标准格式
    * 统一字段命名和数据类型
    * 处理缺失数据和默认值
  - 对战摘要生成：
    * 生成对战基本摘要信息
    * 提取关键统计亮点
    * 计算对战竞争激烈程度

输入: Yahoo API matchup响应数据 (Dict)
输出: 标准化的对战信息字典 (Dict)
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from ..utils.stat_utils import safe_int_conversion, safe_float_conversion


def parse_matchup_data(matchup_response: Dict) -> Optional[Dict]:
    """解析对战数据响应
    
    Args:
        matchup_response: Yahoo API matchup响应数据
        
    Returns:
        标准化的对战信息，解析失败时返回None
    """
    try:
        if not matchup_response:
            return None
        
        # 初始化对战信息结构
        matchup_info = {
            "matchup_id": None,
            "week": None,
            "season": None,
            "status": None,
            "is_playoffs": False,
            "is_tied": False,
            "winner_team_key": None,
            "teams": [],
            "matchup_stats": {},
            "metadata": {}
        }
        
        # 提取基本对战信息
        _extract_matchup_basic_info(matchup_info, matchup_response)
        
        # 提取参与的团队信息
        teams_info = _extract_teams_from_matchup_response(matchup_response)
        matchup_info["teams"] = teams_info
        
        # 分析对战结果
        _analyze_matchup_result(matchup_info)
        
        # 提取对战统计摘要
        _extract_matchup_statistics(matchup_info)
        
        # 验证对战数据
        if not _validate_matchup_data(matchup_info):
            return None
        
        return matchup_info
    
    except Exception as e:
        print(f"解析对战数据失败: {e}")
        return None


def _extract_matchup_basic_info(matchup_info: Dict, matchup_response: Dict) -> None:
    """提取对战基本信息
    
    Args:
        matchup_info: 对战信息字典（会被修改）
        matchup_response: 原始对战响应数据
    """
    try:
        # 检查不同的响应结构
        if "0" in matchup_response:
            # 标准嵌套结构
            main_data = matchup_response["0"]
        else:
            # 直接结构
            main_data = matchup_response
        
        # 提取基本字段
        if "matchup_id" in main_data:
            matchup_info["matchup_id"] = safe_int_conversion(main_data["matchup_id"])
        
        if "week" in main_data:
            matchup_info["week"] = safe_int_conversion(main_data["week"])
        
        if "season" in main_data:
            matchup_info["season"] = str(main_data["season"])
        
        if "status" in main_data:
            matchup_info["status"] = main_data["status"]
        
        if "is_playoffs" in main_data:
            matchup_info["is_playoffs"] = bool(main_data["is_playoffs"])
        
        if "is_tied" in main_data:
            matchup_info["is_tied"] = bool(main_data["is_tied"])
        
        if "winner_team_key" in main_data:
            matchup_info["winner_team_key"] = main_data["winner_team_key"]
        
        # 提取元数据
        metadata_fields = [
            "matchup_grades", "is_consolation", "matchup_recap_title", 
            "matchup_recap_url", "stat_winners"
        ]
        
        for field in metadata_fields:
            if field in main_data:
                matchup_info["metadata"][field] = main_data[field]
    
    except Exception as e:
        print(f"提取对战基本信息失败: {e}")


def _extract_teams_from_matchup_response(matchup_response: Dict) -> List[Dict]:
    """从对战响应中提取团队信息
    
    Args:
        matchup_response: 对战响应数据
        
    Returns:
        团队信息列表
    """
    teams_info = []
    
    try:
        # 定位teams容器
        teams_container = None
        
        if "0" in matchup_response:
            main_data = matchup_response["0"]
            if "teams" in main_data:
                teams_container = main_data["teams"]
        elif "teams" in matchup_response:
            teams_container = matchup_response["teams"]
        
        if not teams_container:
            return teams_info
        
        # 处理不同的teams结构
        if isinstance(teams_container, dict) and "0" in teams_container:
            # 标准嵌套结构: teams["0"]["teams"][i]["team"]
            teams_data = teams_container["0"].get("teams", {})
            teams_count = int(teams_data.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if "team" in team_container:
                    team_info = _parse_team_from_matchup(team_container["team"])
                    if team_info:
                        teams_info.append(team_info)
        
        elif isinstance(teams_container, list):
            # 直接的团队列表
            for team_item in teams_container:
                team_info = _parse_team_from_matchup(team_item)
                if team_info:
                    teams_info.append(team_info)
    
    except Exception as e:
        print(f"提取团队信息失败: {e}")
    
    return teams_info


def _parse_team_from_matchup(team_data: Any) -> Optional[Dict]:
    """从对战中解析团队信息
    
    Args:
        team_data: 团队数据，可能是复杂的嵌套结构
        
    Returns:
        团队信息字典，解析失败时返回None
    """
    try:
        team_info = {
            "team_key": None,
            "team_id": None,
            "team_name": None,
            "team_logo": None,
            "managers": [],
            "points": None,
            "projected_points": None,
            "team_stats": {},
            "roster": []
        }
        
        # 处理不同的team数据结构
        if isinstance(team_data, list):
            # 嵌套列表结构，通常team_data[0]包含基本信息，team_data[1]包含统计
            for i, team_item in enumerate(team_data):
                if isinstance(team_item, dict):
                    if i == 0:
                        # 第一个元素通常是基本信息
                        _extract_team_basic_info_from_matchup(team_info, team_item)
                    elif i == 1:
                        # 第二个元素通常是统计信息
                        _extract_team_stats_from_matchup(team_info, team_item)
        
        elif isinstance(team_data, dict):
            # 直接字典结构
            _extract_team_basic_info_from_matchup(team_info, team_data)
            _extract_team_stats_from_matchup(team_info, team_data)
        
        # 验证必需信息
        if team_info["team_key"]:
            return team_info
        
        return None
    
    except Exception as e:
        print(f"解析对战团队信息失败: {e}")
        return None


def _extract_team_basic_info_from_matchup(team_info: Dict, team_data: Dict) -> None:
    """从对战中提取团队基本信息
    
    Args:
        team_info: 团队信息字典（会被修改）
        team_data: 原始团队数据
    """
    try:
        # 处理嵌套的基本信息
        if isinstance(team_data, list):
            # 如果team_data是列表，查找包含team_key的字典
            for item in team_data:
                if isinstance(item, dict) and "team_key" in item:
                    team_data = item
                    break
        
        if not isinstance(team_data, dict):
            return
        
        # 基本身份信息
        if "team_key" in team_data:
            team_info["team_key"] = team_data["team_key"]
        
        if "team_id" in team_data:
            team_info["team_id"] = safe_int_conversion(team_data["team_id"])
        
        # 团队名称
        if "name" in team_data:
            team_info["team_name"] = team_data["name"]
        
        # 团队图标
        if "team_logos" in team_data:
            team_logos = team_data["team_logos"]
            if isinstance(team_logos, dict) and "0" in team_logos:
                logo_data = team_logos["0"]
                if isinstance(logo_data, dict) and "team_logo" in logo_data:
                    logo_info = logo_data["team_logo"]
                    if isinstance(logo_info, dict) and "url" in logo_info:
                        team_info["team_logo"] = logo_info["url"]
        
        # 管理员信息
        if "managers" in team_data:
            managers_data = team_data["managers"]
            if isinstance(managers_data, dict) and "0" in managers_data:
                managers_container = managers_data["0"].get("managers", {})
                managers_count = int(managers_container.get("count", 0))
                
                for i in range(managers_count):
                    str_index = str(i)
                    if str_index in managers_container:
                        manager_data = managers_container[str_index]
                        if "manager" in manager_data:
                            manager_info = manager_data["manager"]
                            if isinstance(manager_info, dict):
                                team_info["managers"].append({
                                    "manager_id": manager_info.get("manager_id"),
                                    "nickname": manager_info.get("nickname"),
                                    "is_commissioner": manager_info.get("is_commissioner", False)
                                })
        
        # 得分信息（可能在基本信息中）
        if "team_points" in team_data:
            team_points = team_data["team_points"]
            if isinstance(team_points, dict):
                team_info["points"] = safe_float_conversion(team_points.get("total"))
        
        if "team_projected_points" in team_data:
            team_projected_points = team_data["team_projected_points"]
            if isinstance(team_projected_points, dict):
                team_info["projected_points"] = safe_float_conversion(team_projected_points.get("total"))
    
    except Exception as e:
        print(f"提取团队基本信息失败: {e}")


def _extract_team_stats_from_matchup(team_info: Dict, team_data: Dict) -> None:
    """从对战中提取团队统计信息
    
    Args:
        team_info: 团队信息字典（会被修改）
        team_data: 原始团队数据
    """
    try:
        # 查找team_stats
        team_stats = None
        
        if "team_stats" in team_data:
            team_stats = team_data["team_stats"]
        
        if team_stats:
            # 直接保存team_stats供后续处理
            team_info["team_stats"] = team_stats
        
        # 提取得分信息（可能在统计数据中）
        if "team_points" in team_data and team_info.get("points") is None:
            team_points = team_data["team_points"]
            if isinstance(team_points, dict):
                team_info["points"] = safe_float_conversion(team_points.get("total"))
        
        if "team_projected_points" in team_data and team_info.get("projected_points") is None:
            team_projected_points = team_data["team_projected_points"]
            if isinstance(team_projected_points, dict):
                team_info["projected_points"] = safe_float_conversion(team_projected_points.get("total"))
        
        # 提取roster信息
        if "roster" in team_data:
            roster_data = team_data["roster"]
            # roster解析比较复杂，这里先保存原始数据
            team_info["roster"] = roster_data
    
    except Exception as e:
        print(f"提取团队统计信息失败: {e}")


def _analyze_matchup_result(matchup_info: Dict) -> None:
    """分析对战结果
    
    Args:
        matchup_info: 对战信息字典（会被修改）
    """
    try:
        teams = matchup_info.get("teams", [])
        
        if len(teams) != 2:
            return
        
        team1, team2 = teams
        
        # 获取双方得分
        team1_points = team1.get("points")
        team2_points = team2.get("points")
        
        if team1_points is not None and team2_points is not None:
            # 计算得分差距
            points_difference = abs(team1_points - team2_points)
            matchup_info["points_difference"] = round(points_difference, 2)
            
            # 确定胜负
            if team1_points > team2_points:
                matchup_info["winner_team_key"] = team1["team_key"]
                matchup_info["loser_team_key"] = team2["team_key"]
                matchup_info["is_tied"] = False
            elif team2_points > team1_points:
                matchup_info["winner_team_key"] = team2["team_key"]
                matchup_info["loser_team_key"] = team1["team_key"]
                matchup_info["is_tied"] = False
            else:
                # 平局
                matchup_info["is_tied"] = True
                matchup_info["winner_team_key"] = None
                matchup_info["loser_team_key"] = None
            
            # 分析竞争激烈程度
            if points_difference <= 5:
                matchup_info["competitiveness"] = "very_close"
            elif points_difference <= 15:
                matchup_info["competitiveness"] = "close"
            elif points_difference <= 30:
                matchup_info["competitiveness"] = "moderate"
            else:
                matchup_info["competitiveness"] = "blowout"
    
    except Exception as e:
        print(f"分析对战结果失败: {e}")


def _extract_matchup_statistics(matchup_info: Dict) -> None:
    """提取对战统计摘要
    
    Args:
        matchup_info: 对战信息字典（会被修改）
    """
    try:
        teams = matchup_info.get("teams", [])
        
        if len(teams) != 2:
            return
        
        stats_summary = {
            "total_points": 0,
            "average_points": 0,
            "projected_total": 0,
            "projection_accuracy": None
        }
        
        total_actual = 0
        total_projected = 0
        valid_actual_count = 0
        valid_projected_count = 0
        
        for team in teams:
            points = team.get("points")
            projected = team.get("projected_points")
            
            if points is not None:
                total_actual += points
                valid_actual_count += 1
            
            if projected is not None:
                total_projected += projected
                valid_projected_count += 1
        
        # 计算总得分和平均得分
        if valid_actual_count > 0:
            stats_summary["total_points"] = round(total_actual, 2)
            stats_summary["average_points"] = round(total_actual / valid_actual_count, 2)
        
        # 计算预测准确性
        if valid_projected_count > 0:
            stats_summary["projected_total"] = round(total_projected, 2)
            
            if valid_actual_count > 0 and total_projected > 0:
                accuracy = (total_actual / total_projected) * 100
                stats_summary["projection_accuracy"] = round(accuracy, 1)
        
        matchup_info["matchup_stats"] = stats_summary
    
    except Exception as e:
        print(f"提取对战统计失败: {e}")


def _validate_matchup_data(matchup_info: Dict) -> bool:
    """验证对战数据的完整性
    
    Args:
        matchup_info: 对战信息
        
    Returns:
        验证是否通过
    """
    try:
        # 检查基本字段
        if not matchup_info.get("week"):
            print("对战缺少周数信息")
            return False
        
        # 检查团队信息
        teams = matchup_info.get("teams", [])
        if len(teams) != 2:
            print(f"对战应包含2个团队，实际 {len(teams)} 个")
            return False
        
        # 验证团队键
        for i, team in enumerate(teams):
            if not team.get("team_key"):
                print(f"团队 {i+1} 缺少team_key")
                return False
        
        return True
    
    except Exception as e:
        print(f"验证对战数据失败: {e}")
        return False


def extract_matchup_summary(matchup_info: Dict) -> Dict[str, Any]:
    """提取对战摘要信息
    
    Args:
        matchup_info: 对战信息
        
    Returns:
        对战摘要字典
    """
    try:
        teams = matchup_info.get("teams", [])
        
        summary = {
            "matchup_id": matchup_info.get("matchup_id"),
            "week": matchup_info.get("week"),
            "season": matchup_info.get("season"),
            "status": matchup_info.get("status"),
            "is_playoffs": matchup_info.get("is_playoffs", False),
            "teams_count": len(teams),
            "teams_summary": [],
            "result": {},
            "competitiveness": matchup_info.get("competitiveness"),
            "statistics": matchup_info.get("matchup_stats", {})
        }
        
        # 团队摘要
        for team in teams:
            team_summary = {
                "team_key": team.get("team_key"),
                "team_name": team.get("team_name"),
                "points": team.get("points"),
                "projected_points": team.get("projected_points"),
                "managers_count": len(team.get("managers", []))
            }
            summary["teams_summary"].append(team_summary)
        
        # 结果摘要
        if matchup_info.get("is_tied"):
            summary["result"] = {
                "result_type": "tie",
                "points_difference": 0
            }
        elif matchup_info.get("winner_team_key"):
            summary["result"] = {
                "result_type": "win",
                "winner_team_key": matchup_info["winner_team_key"],
                "loser_team_key": matchup_info.get("loser_team_key"),
                "points_difference": matchup_info.get("points_difference")
            }
        else:
            summary["result"] = {
                "result_type": "unknown"
            }
        
        return summary
    
    except Exception as e:
        print(f"提取对战摘要失败: {e}")
        return {"error": str(e)}


def parse_multiple_matchups(matchups_response: Dict) -> List[Dict]:
    """解析多个对战数据
    
    Args:
        matchups_response: 包含多个对战的响应数据
        
    Returns:
        对战信息列表
    """
    try:
        matchups = []
        
        # 检查响应结构
        if "matchup" in matchups_response:
            matchup_data = matchups_response["matchup"]
            
            if isinstance(matchup_data, list):
                # 多个对战
                for matchup_item in matchup_data:
                    parsed_matchup = parse_matchup_data(matchup_item)
                    if parsed_matchup:
                        matchups.append(parsed_matchup)
            else:
                # 单个对战
                parsed_matchup = parse_matchup_data(matchup_data)
                if parsed_matchup:
                    matchups.append(parsed_matchup)
        
        return matchups
    
    except Exception as e:
        print(f"解析多个对战数据失败: {e}")
        return []


def filter_matchups_by_criteria(matchups: List[Dict], criteria: Dict) -> List[Dict]:
    """根据条件过滤对战数据
    
    Args:
        matchups: 对战列表
        criteria: 过滤条件
        
    Returns:
        过滤后的对战列表
    """
    try:
        filtered_matchups = []
        
        for matchup in matchups:
            if _matches_matchup_criteria(matchup, criteria):
                filtered_matchups.append(matchup)
        
        return filtered_matchups
    
    except Exception as e:
        print(f"过滤对战数据失败: {e}")
        return matchups


def _matches_matchup_criteria(matchup: Dict, criteria: Dict) -> bool:
    """检查对战是否匹配过滤条件
    
    Args:
        matchup: 对战信息
        criteria: 过滤条件
        
    Returns:
        是否匹配
    """
    try:
        # 周数过滤
        if "weeks" in criteria:
            week = matchup.get("week")
            if week not in criteria["weeks"]:
                return False
        
        # 季后赛过滤
        if "is_playoffs" in criteria:
            is_playoffs = matchup.get("is_playoffs", False)
            if is_playoffs != criteria["is_playoffs"]:
                return False
        
        # 团队过滤
        if "team_keys" in criteria:
            teams = matchup.get("teams", [])
            matchup_team_keys = {team.get("team_key") for team in teams}
            
            if not matchup_team_keys.intersection(set(criteria["team_keys"])):
                return False
        
        # 竞争激烈程度过滤
        if "competitiveness" in criteria:
            competitiveness = matchup.get("competitiveness")
            if competitiveness != criteria["competitiveness"]:
                return False
        
        # 得分范围过滤
        if "points_range" in criteria:
            teams = matchup.get("teams", [])
            total_points = sum(team.get("points", 0) for team in teams)
            
            min_points, max_points = criteria["points_range"]
            if total_points < min_points or total_points > max_points:
                return False
        
        return True
    
    except Exception as e:
        print(f"检查对战匹配条件失败: {e}")
        return False


def calculate_matchup_trends(matchups: List[Dict]) -> Dict[str, Any]:
    """计算对战趋势分析
    
    Args:
        matchups: 对战列表
        
    Returns:
        趋势分析结果
    """
    try:
        trends = {
            "total_matchups": len(matchups),
            "weeks_covered": set(),
            "average_total_points": 0,
            "competitiveness_distribution": {},
            "blowout_rate": 0,
            "close_game_rate": 0,
            "projection_accuracy": {
                "average_accuracy": 0,
                "overperformance_rate": 0
            }
        }
        
        if not matchups:
            return trends
        
        total_points_sum = 0
        projection_accuracies = []
        competitiveness_counts = {}
        close_games = 0
        blowouts = 0
        
        for matchup in matchups:
            # 收集周数
            week = matchup.get("week")
            if week:
                trends["weeks_covered"].add(week)
            
            # 收集得分数据
            matchup_stats = matchup.get("matchup_stats", {})
            total_points = matchup_stats.get("total_points", 0)
            total_points_sum += total_points
            
            # 收集预测准确性
            accuracy = matchup_stats.get("projection_accuracy")
            if accuracy:
                projection_accuracies.append(accuracy)
            
            # 统计竞争激烈程度
            competitiveness = matchup.get("competitiveness")
            if competitiveness:
                competitiveness_counts[competitiveness] = competitiveness_counts.get(competitiveness, 0) + 1
                
                if competitiveness in ["very_close", "close"]:
                    close_games += 1
                elif competitiveness == "blowout":
                    blowouts += 1
        
        # 计算平均值
        if len(matchups) > 0:
            trends["average_total_points"] = round(total_points_sum / len(matchups), 2)
            trends["close_game_rate"] = round((close_games / len(matchups)) * 100, 1)
            trends["blowout_rate"] = round((blowouts / len(matchups)) * 100, 1)
        
        # 设置周数覆盖
        trends["weeks_covered"] = sorted(list(trends["weeks_covered"]))
        
        # 设置竞争激烈程度分布
        trends["competitiveness_distribution"] = competitiveness_counts
        
        # 计算预测准确性
        if projection_accuracies:
            avg_accuracy = sum(projection_accuracies) / len(projection_accuracies)
            trends["projection_accuracy"]["average_accuracy"] = round(avg_accuracy, 1)
            
            overperformances = sum(1 for acc in projection_accuracies if acc > 100)
            overperformance_rate = (overperformances / len(projection_accuracies)) * 100
            trends["projection_accuracy"]["overperformance_rate"] = round(overperformance_rate, 1)
        
        return trends
    
    except Exception as e:
        print(f"计算对战趋势失败: {e}")
        return {"error": str(e)} 