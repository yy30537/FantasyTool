#!/usr/bin/env python3
"""
球员数据解析器

迁移来源: @yahoo_api_data.py 中的球员相关解析逻辑
主要映射:
  - _extract_player_info_from_league_data() -> parse_league_players()
  - _normalize_player_info() -> 移至 normalizers/player_normalizer.py
  - 球员基本信息提取逻辑

职责:
  - 解析Yahoo API返回的players数据结构
  - 提取player_key、player_id、editorial_player_key等核心字段
  - 处理球员姓名信息：full_name、first_name、last_name
  - 解析团队信息：editorial_team_key、editorial_team_full_name等
  - 处理头像信息：headshot URL提取
  - 解析位置信息：display_position、position_type
  - 提取合适位置信息：eligible_positions数组
  - 处理球员状态和属性字段

输入: Yahoo API league players响应 (JSON)
输出: 标准化的球员数据列表 (List[Dict])，包含eligible_positions
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from ..stats.stat_utils import safe_bool_conversion


def parse_league_players(players_data: Dict) -> List[Dict]:
    """从联盟球员数据中提取球员信息
    
    Args:
        players_data: Yahoo API league players响应
        
    Returns:
        标准化的球员数据列表
    """
    players = []
    
    try:
        fantasy_content = players_data["fantasy_content"]
        league_data = fantasy_content["league"]
        
        players_container = None
        if isinstance(league_data, list) and len(league_data) > 1:
            for item in league_data:
                if isinstance(item, dict) and "players" in item:
                    players_container = item["players"]
                    break
        elif isinstance(league_data, dict) and "players" in league_data:
            players_container = league_data["players"]
        
        if not players_container:
            return players
        
        total_count = int(players_container.get("count", 0))
        
        for i in range(total_count):
            player_index = str(i)
            if player_index not in players_container:
                continue
                
            player_data = players_container[player_index]
            if "player" not in player_data:
                continue
            
            player_info_list = player_data["player"]
            if isinstance(player_info_list, list) and len(player_info_list) > 0:
                player_basic_info = player_info_list[0]
                
                parsed_player = parse_single_player(player_basic_info)
                if parsed_player:
                    players.append(parsed_player)
    
    except Exception as e:
        print(f"解析联盟球员数据时出错: {e}")
    
    return players


def parse_single_player(player_basic_info: Any) -> Optional[Dict]:
    """解析单个球员的基本信息
    
    Args:
        player_basic_info: 球员基本信息，可能是列表或字典
        
    Returns:
        标准化的球员数据字典，解析失败时返回None
    """
    try:
        merged_info = {}
        
        if isinstance(player_basic_info, list):
            for info_item in player_basic_info:
                if isinstance(info_item, dict):
                    merged_info.update(info_item)
        elif isinstance(player_basic_info, dict):
            merged_info = player_basic_info.copy()
        else:
            return None
        
        if not merged_info:
            return None
        
        # 基本字段验证
        if not merged_info.get("player_key"):
            return None
        
        # 标准化球员信息
        normalized_player = normalize_player_basic_info(merged_info)
        
        return normalized_player
        
    except Exception as e:
        print(f"解析单个球员时出错: {e}")
        return None


def normalize_player_basic_info(player_info: Dict) -> Dict:
    """标准化球员基本信息
    
    Args:
        player_info: 原始球员信息字典
        
    Returns:
        标准化的球员信息字典
    """
    normalized = {}
    
    try:
        # 基本标识信息
        normalized["player_key"] = player_info.get("player_key")
        normalized["player_id"] = player_info.get("player_id")
        normalized["editorial_player_key"] = player_info.get("editorial_player_key")
        
        # 处理姓名信息
        if "name" in player_info:
            name_info = player_info["name"]
            if isinstance(name_info, dict):
                normalized["full_name"] = name_info.get("full")
                normalized["first_name"] = name_info.get("first")
                normalized["last_name"] = name_info.get("last")
        
        # 处理团队信息
        if "editorial_team_key" in player_info:
            normalized["current_team_key"] = player_info["editorial_team_key"]
        if "editorial_team_full_name" in player_info:
            normalized["current_team_name"] = player_info["editorial_team_full_name"]
        if "editorial_team_abbr" in player_info:
            normalized["current_team_abbr"] = player_info["editorial_team_abbr"]
        
        # 位置信息
        normalized["display_position"] = player_info.get("display_position")
        normalized["primary_position"] = player_info.get("primary_position")
        normalized["position_type"] = player_info.get("position_type")
        
        # 处理合适位置信息
        eligible_positions = player_info.get("eligible_positions", [])
        normalized["eligible_positions"] = parse_eligible_positions(eligible_positions)
        
        # 球员属性
        normalized["uniform_number"] = player_info.get("uniform_number")
        normalized["status"] = player_info.get("status")
        normalized["image_url"] = player_info.get("image_url")
        
        # 处理头像信息
        if "headshot" in player_info:
            headshot_info = player_info["headshot"]
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                normalized["headshot_url"] = headshot_info["url"]
        
        # 球员状态
        is_undroppable = player_info.get("is_undroppable", False)
        if isinstance(is_undroppable, str):
            normalized["is_undroppable"] = safe_bool_conversion(is_undroppable)
        else:
            normalized["is_undroppable"] = bool(is_undroppable)
        
        # 添加时间戳
        normalized["last_updated"] = datetime.now()
        
        return normalized
    
    except Exception as e:
        print(f"标准化球员信息时出错: {e}")
        return player_info


def parse_eligible_positions(positions_data: Any) -> List[str]:
    """解析球员合适位置信息
    
    Args:
        positions_data: 位置数据，可能是列表或其他格式
        
    Returns:
        位置字符串列表
    """
    positions = []
    
    try:
        if not positions_data:
            return positions
        
        if isinstance(positions_data, list):
            for pos_item in positions_data:
                if isinstance(pos_item, dict):
                    position = pos_item.get("position")
                    if position and position not in positions:
                        positions.append(position)
                elif isinstance(pos_item, str):
                    if pos_item not in positions:
                        positions.append(pos_item)
        elif isinstance(positions_data, str):
            # 单个位置字符串
            positions.append(positions_data)
    
    except Exception as e:
        print(f"解析合适位置时出错: {e}")
    
    return positions


def parse_player_stats_data(player_stats_data: Dict) -> Optional[Dict]:
    """解析球员统计数据
    
    Args:
        player_stats_data: 球员统计数据响应
        
    Returns:
        标准化的球员统计数据，解析失败时返回None
    """
    try:
        if not player_stats_data or "fantasy_content" not in player_stats_data:
            return None
        
        fantasy_content = player_stats_data["fantasy_content"]
        league_data = fantasy_content["league"]
        
        # 查找players容器
        players_container = None
        if isinstance(league_data, list) and len(league_data) > 1:
            for item in league_data:
                if isinstance(item, dict) and "players" in item:
                    players_container = item["players"]
                    break
        elif isinstance(league_data, dict) and "players" in league_data:
            players_container = league_data["players"]
        
        if not players_container:
            return None
        
        # 处理球员统计数据
        players_stats = []
        players_count = int(players_container.get("count", 0))
        
        for i in range(players_count):
            str_index = str(i)
            if str_index not in players_container:
                continue
            
            player_data = players_container[str_index]
            if "player" not in player_data:
                continue
            
            player_info_list = player_data["player"]
            if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                continue
            
            # 提取球员基本信息
            player_basic_info = player_info_list[0]
            player_key = None
            editorial_player_key = None
            
            if isinstance(player_basic_info, list):
                for item in player_basic_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_key = item["player_key"]
                        elif "editorial_player_key" in item:
                            editorial_player_key = item["editorial_player_key"]
            elif isinstance(player_basic_info, dict):
                player_key = player_basic_info.get("player_key")
                editorial_player_key = player_basic_info.get("editorial_player_key")
            
            if not player_key:
                continue
            
            # 提取统计数据
            stats_container = player_info_list[1]
            if not isinstance(stats_container, dict) or "player_stats" not in stats_container:
                continue
            
            player_stats = stats_container["player_stats"]
            if not isinstance(player_stats, dict) or "stats" not in player_stats:
                continue
            
            stats_list = player_stats["stats"]
            if not isinstance(stats_list, list):
                continue
            
            # 构建球员统计记录
            stats_record = {
                "player_key": player_key,
                "editorial_player_key": editorial_player_key or player_key,
                "stats": parse_stats_array(stats_list)
            }
            
            players_stats.append(stats_record)
        
        return {"players_stats": players_stats}
    
    except Exception as e:
        print(f"解析球员统计数据时出错: {e}")
        return None


def parse_stats_array(stats_list: List) -> Dict[str, str]:
    """解析统计数据数组为字典格式
    
    Args:
        stats_list: 统计数据列表
        
    Returns:
        stat_id到value的映射字典
    """
    stats_dict = {}
    
    try:
        for stat_item in stats_list:
            if "stat" in stat_item:
                stat_info = stat_item["stat"]
                stat_id = stat_info.get("stat_id")
                value = stat_info.get("value")
                if stat_id is not None:
                    stats_dict[str(stat_id)] = value
    
    except Exception as e:
        print(f"解析统计数组时出错: {e}")
    
    return stats_dict


def validate_player_data(player_data: Dict) -> bool:
    """验证球员数据的完整性和合理性
    
    Args:
        player_data: 解析后的球员数据
        
    Returns:
        数据是否有效
    """
    try:
        # 检查必需字段
        required_fields = ["player_key", "player_id"]
        for field in required_fields:
            if field not in player_data or not player_data[field]:
                return False
        
        # 检查姓名信息
        full_name = player_data.get("full_name")
        if not full_name or len(full_name.strip()) == 0:
            print(f"警告: 球员 {player_data.get('player_key')} 缺少姓名信息")
        
        # 检查位置信息
        display_position = player_data.get("display_position")
        if not display_position:
            print(f"警告: 球员 {player_data.get('player_key')} 缺少位置信息")
        
        # 检查eligible_positions格式
        eligible_positions = player_data.get("eligible_positions", [])
        if not isinstance(eligible_positions, list):
            print(f"警告: 球员 {player_data.get('player_key')} 合适位置格式异常")
            return False
        
        return True
    
    except Exception:
        return False


def filter_players_by_criteria(players_list: List[Dict], 
                              positions: Optional[List[str]] = None,
                              team_abbr: Optional[str] = None,
                              status: Optional[str] = None,
                              is_undroppable: Optional[bool] = None) -> List[Dict]:
    """根据条件过滤球员列表
    
    Args:
        players_list: 球员数据列表
        positions: 位置过滤条件
        team_abbr: 团队缩写过滤条件
        status: 球员状态过滤条件
        is_undroppable: 是否不可丢弃过滤条件
        
    Returns:
        过滤后的球员数据列表
    """
    filtered_players = []
    
    try:
        for player in players_list:
            # 过滤位置
            if positions:
                player_positions = player.get("eligible_positions", [])
                if not any(pos in positions for pos in player_positions):
                    continue
            
            # 过滤团队
            if team_abbr and player.get("current_team_abbr") != team_abbr:
                continue
            
            # 过滤状态
            if status and player.get("status") != status:
                continue
            
            # 过滤不可丢弃状态
            if is_undroppable is not None and player.get("is_undroppable") != is_undroppable:
                continue
            
            # 验证数据完整性
            if validate_player_data(player):
                filtered_players.append(player)
    
    except Exception as e:
        print(f"过滤球员数据时出错: {e}")
    
    return filtered_players


def extract_player_summary(players_list: List[Dict]) -> Dict[str, Any]:
    """提取球员数据的摘要信息
    
    Args:
        players_list: 球员数据列表
        
    Returns:
        球员摘要信息
    """
    try:
        summary = {
            "total_players": len(players_list),
            "positions": {},
            "teams": {},
            "statuses": {},
            "undroppable_players": 0
        }
        
        for player in players_list:
            # 统计位置
            eligible_positions = player.get("eligible_positions", [])
            for position in eligible_positions:
                if position not in summary["positions"]:
                    summary["positions"][position] = 0
                summary["positions"][position] += 1
            
            # 统计团队
            team_abbr = player.get("current_team_abbr", "unknown")
            if team_abbr not in summary["teams"]:
                summary["teams"][team_abbr] = 0
            summary["teams"][team_abbr] += 1
            
            # 统计状态
            status = player.get("status", "active")
            if status not in summary["statuses"]:
                summary["statuses"][status] = 0
            summary["statuses"][status] += 1
            
            # 统计不可丢弃球员
            if player.get("is_undroppable"):
                summary["undroppable_players"] += 1
        
        return summary
    
    except Exception as e:
        print(f"提取球员摘要时出错: {e}")
        return {"error": str(e)} 