#!/usr/bin/env python3
"""
团队数据解析器

迁移来源: @yahoo_api_data.py 中的团队相关解析逻辑
主要映射:
  - _extract_team_data_from_api() -> parse_team_data()
  - _extract_team_keys_from_data() -> parse_team_keys()
  - 团队基本信息提取逻辑

职责:
  - 解析Yahoo API返回的teams数据结构
  - 提取team_key、team_id、name、url等核心字段
  - 处理团队Logo信息：team_logos数组解析
  - 解析roster_adds信息：coverage_value、value字段
  - 处理managers数据：管理员信息提取
  - 布尔值字段标准化：clinched_playoffs、has_draft_grade等
  - 数字字段安全转换：number_of_trades等

输入: Yahoo API teams响应 (JSON)
输出: 标准化的团队数据列表 (List[Dict])，包含managers信息
"""

from typing import Dict, List, Optional, Any
from ..stats.stat_utils import safe_bool_conversion, safe_int_conversion


def parse_teams_data(teams_data: Dict) -> List[Dict]:
    """解析Yahoo API返回的teams数据
    
    Args:
        teams_data: Yahoo API teams响应
        
    Returns:
        标准化的团队数据列表
    """
    teams_list = []
    
    try:
        fantasy_content = teams_data["fantasy_content"]
        league_data = fantasy_content["league"]
        
        teams_container = None
        if isinstance(league_data, list) and len(league_data) > 1:
            for item in league_data:
                if isinstance(item, dict) and "teams" in item:
                    teams_container = item["teams"]
                    break
        
        if not teams_container:
            return teams_list
        
        teams_count = int(teams_container.get("count", 0))
        for i in range(teams_count):
            str_index = str(i)
            if str_index not in teams_container:
                continue
            
            team_container = teams_container[str_index]
            team_data = team_container["team"]
            
            # 处理团队数据
            parsed_team = parse_single_team(team_data)
            if parsed_team:
                teams_list.append(parsed_team)
    
    except Exception as e:
        print(f"解析团队数据时出错: {e}")
    
    return teams_list


def parse_single_team(team_data: List) -> Optional[Dict]:
    """从API团队数据中提取团队信息
    
    Args:
        team_data: API返回的单个团队数据列表
        
    Returns:
        标准化的团队数据字典，解析失败时返回None
    """
    try:
        # team_data[0] 应该是一个包含多个字典的列表
        if not isinstance(team_data, list) or len(team_data) == 0:
            return None
        
        team_info_list = team_data[0]
        if not isinstance(team_info_list, list):
            return None
        
        # 提取团队基本信息
        team_dict = {}
        managers_data = []
        
        for item in team_info_list:
            if isinstance(item, dict):
                if "managers" in item:
                    managers_data = item["managers"]
                elif "team_logos" in item and item["team_logos"]:
                    # 处理team logo
                    if len(item["team_logos"]) > 0 and "team_logo" in item["team_logos"][0]:
                        team_dict["team_logo_url"] = item["team_logos"][0]["team_logo"].get("url")
                elif "roster_adds" in item:
                    # 处理roster adds
                    roster_adds = item["roster_adds"]
                    team_dict["roster_adds_week"] = roster_adds.get("coverage_value")
                    team_dict["roster_adds_value"] = roster_adds.get("value")
                elif "clinched_playoffs" in item:
                    team_dict["clinched_playoffs"] = safe_bool_conversion(item["clinched_playoffs"])
                elif "has_draft_grade" in item:
                    team_dict["has_draft_grade"] = safe_bool_conversion(item["has_draft_grade"])
                elif "number_of_trades" in item:
                    # 处理可能是字符串的数字字段
                    team_dict["number_of_trades"] = safe_int_conversion(item["number_of_trades"]) or 0
                else:
                    # 直接更新其他字段
                    team_dict.update(item)
        
        # 添加managers数据
        team_dict["managers"] = parse_managers_data(managers_data)
        
        # 验证必要字段
        if not team_dict.get("team_key"):
            return None
        
        # 标准化数据类型
        standardized_team = standardize_team_data(team_dict)
        
        return standardized_team
        
    except Exception as e:
        print(f"解析单个团队时出错: {e}")
        return None


def parse_managers_data(managers_data: List) -> List[Dict]:
    """解析团队管理员数据
    
    Args:
        managers_data: 管理员原始数据列表
        
    Returns:
        标准化的管理员数据列表
    """
    parsed_managers = []
    
    try:
        for manager_item in managers_data:
            if isinstance(manager_item, dict) and "manager" in manager_item:
                manager_info = manager_item["manager"]
            else:
                manager_info = manager_item
            
            if not isinstance(manager_info, dict):
                continue
            
            if not manager_info.get("manager_id"):
                continue
            
            parsed_manager = {
                "manager_id": manager_info["manager_id"],
                "nickname": manager_info.get("nickname"),
                "guid": manager_info.get("guid"),
                "is_commissioner": safe_bool_conversion(manager_info.get("is_commissioner", False)),
                "email": manager_info.get("email"),
                "image_url": manager_info.get("image_url"),
                "felo_score": manager_info.get("felo_score"),
                "felo_tier": manager_info.get("felo_tier")
            }
            
            parsed_managers.append(parsed_manager)
    
    except Exception as e:
        print(f"解析管理员数据时出错: {e}")
    
    return parsed_managers


def standardize_team_data(team_dict: Dict) -> Dict:
    """标准化团队数据类型和字段
    
    Args:
        team_dict: 原始团队数据字典
        
    Returns:
        标准化的团队数据字典
    """
    try:
        standardized = {
            "team_key": team_dict.get("team_key"),
            "team_id": team_dict.get("team_id"),
            "name": team_dict.get("name"),
            "url": team_dict.get("url"),
            "team_logo_url": team_dict.get("team_logo_url"),
            "division_id": team_dict.get("division_id"),
            "waiver_priority": safe_int_conversion(team_dict.get("waiver_priority")),
            "faab_balance": team_dict.get("faab_balance"),
            "number_of_moves": safe_int_conversion(team_dict.get("number_of_moves")) or 0,
            "number_of_trades": safe_int_conversion(team_dict.get("number_of_trades")) or 0,
            "roster_adds_week": str(team_dict.get("roster_adds_week", "")),
            "roster_adds_value": team_dict.get("roster_adds_value"),
            "clinched_playoffs": safe_bool_conversion(team_dict.get("clinched_playoffs", False)),
            "has_draft_grade": safe_bool_conversion(team_dict.get("has_draft_grade", False)),
            "managers": team_dict.get("managers", [])
        }
        
        return standardized
    
    except Exception as e:
        print(f"标准化团队数据时出错: {e}")
        return team_dict


def parse_team_keys(teams_data: Dict) -> List[str]:
    """从团队数据中提取团队键
    
    Args:
        teams_data: 团队数据响应
        
    Returns:
        团队键列表
    """
    team_keys = []
    
    try:
        fantasy_content = teams_data["fantasy_content"]
        league_data = fantasy_content["league"]
        
        teams_container = None
        if isinstance(league_data, list) and len(league_data) > 1:
            for item in league_data:
                if isinstance(item, dict) and "teams" in item:
                    teams_container = item["teams"]
                    break
        
        if teams_container:
            teams_count = int(teams_container.get("count", 0))
            for i in range(teams_count):
                str_index = str(i)
                if str_index in teams_container:
                    team_container = teams_container[str_index]
                    if "team" in team_container:
                        team_data = team_container["team"]
                        # 修复：team_data[0] 是一个字典列表，不是嵌套列表
                        if (isinstance(team_data, list) and 
                            len(team_data) > 0 and 
                            isinstance(team_data[0], list)):
                            # 从team_data[0]列表中查找包含team_key的字典
                            for team_item in team_data[0]:
                                if isinstance(team_item, dict) and "team_key" in team_item:
                                    team_key = team_item["team_key"]
                                    if team_key:
                                        team_keys.append(team_key)
                                    break
    
    except Exception as e:
        print(f"提取团队键时出错: {e}")
    
    return team_keys


def validate_team_data(team_data: Dict) -> bool:
    """验证团队数据的完整性和合理性
    
    Args:
        team_data: 解析后的团队数据
        
    Returns:
        数据是否有效
    """
    try:
        # 检查必需字段
        required_fields = ["team_key", "team_id", "name"]
        for field in required_fields:
            if field not in team_data or not team_data[field]:
                return False
        
        # 检查数值字段合理性
        number_of_moves = team_data.get("number_of_moves", 0)
        if not isinstance(number_of_moves, int) or number_of_moves < 0:
            print(f"警告: 团队移动次数异常 {number_of_moves}")
        
        number_of_trades = team_data.get("number_of_trades", 0)
        if not isinstance(number_of_trades, int) or number_of_trades < 0:
            print(f"警告: 团队交易次数异常 {number_of_trades}")
        
        # 检查管理员数据
        managers = team_data.get("managers", [])
        if not isinstance(managers, list):
            print("警告: 管理员数据格式异常")
            return False
        
        # 至少要有一个管理员
        if len(managers) == 0:
            print("警告: 团队没有管理员")
        
        return True
    
    except Exception:
        return False


def filter_teams_by_criteria(teams_list: List[Dict], 
                            has_clinched_playoffs: Optional[bool] = None,
                            min_moves: Optional[int] = None,
                            has_commissioner: Optional[bool] = None) -> List[Dict]:
    """根据条件过滤团队列表
    
    Args:
        teams_list: 团队数据列表
        has_clinched_playoffs: 是否锁定季后赛
        min_moves: 最小移动次数
        has_commissioner: 是否包含联盟委员
        
    Returns:
        过滤后的团队数据列表
    """
    filtered_teams = []
    
    try:
        for team in teams_list:
            # 过滤季后赛状态
            if has_clinched_playoffs is not None and team.get("clinched_playoffs") != has_clinched_playoffs:
                continue
            
            # 过滤移动次数
            if min_moves and team.get("number_of_moves", 0) < min_moves:
                continue
            
            # 过滤是否包含委员
            if has_commissioner is not None:
                managers = team.get("managers", [])
                team_has_commissioner = any(manager.get("is_commissioner", False) for manager in managers)
                if team_has_commissioner != has_commissioner:
                    continue
            
            # 验证数据完整性
            if validate_team_data(team):
                filtered_teams.append(team)
    
    except Exception as e:
        print(f"过滤团队数据时出错: {e}")
    
    return filtered_teams


def extract_team_summary(teams_list: List[Dict]) -> Dict[str, Any]:
    """提取团队数据的摘要信息
    
    Args:
        teams_list: 团队数据列表
        
    Returns:
        团队摘要信息
    """
    try:
        summary = {
            "total_teams": len(teams_list),
            "clinched_playoffs": 0,
            "total_moves": 0,
            "total_trades": 0,
            "teams_with_commissioners": 0,
            "avg_moves_per_team": 0,
            "avg_trades_per_team": 0
        }
        
        for team in teams_list:
            # 统计季后赛锁定
            if team.get("clinched_playoffs"):
                summary["clinched_playoffs"] += 1
            
            # 统计移动和交易
            moves = team.get("number_of_moves", 0)
            trades = team.get("number_of_trades", 0)
            summary["total_moves"] += moves
            summary["total_trades"] += trades
            
            # 统计包含委员的团队
            managers = team.get("managers", [])
            if any(manager.get("is_commissioner", False) for manager in managers):
                summary["teams_with_commissioners"] += 1
        
        # 计算平均值
        if len(teams_list) > 0:
            summary["avg_moves_per_team"] = round(summary["total_moves"] / len(teams_list), 1)
            summary["avg_trades_per_team"] = round(summary["total_trades"] / len(teams_list), 1)
        
        return summary
    
    except Exception as e:
        print(f"提取团队摘要时出错: {e}")
        return {"error": str(e)} 