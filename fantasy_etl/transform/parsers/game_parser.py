#!/usr/bin/env python3
"""
游戏数据解析器

迁移来源: @yahoo_api_data.py 中的游戏相关解析逻辑
主要映射:
  - _extract_game_keys() -> parse_game_keys()
  - 游戏数据提取和验证逻辑

职责:
  - 解析Yahoo API返回的games数据结构
  - 提取game_key、game_id、name、code等核心字段
  - 过滤type='full'的游戏
  - 数据格式标准化和验证

输入: Yahoo API games响应 (JSON)
输出: 标准化的游戏数据列表 (List[Dict])
"""

from typing import Dict, List, Optional, Any
from ..stats.stat_utils import safe_bool_conversion


def parse_game_keys(games_data: Dict) -> List[str]:
    """从游戏数据中提取游戏键（只包含type='full'的游戏）
    
    Args:
        games_data: Yahoo API返回的games数据
        
    Returns:
        游戏键列表，只包含type='full'的游戏
    """
    game_keys = []
    
    try:
        fantasy_content = games_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        games_count = int(games_container.get("count", 0))
        
        for i in range(games_count):
            str_index = str(i)
            if str_index not in games_container:
                continue
                
            game_container = games_container[str_index]
            game_data = game_container["game"]
            
            if isinstance(game_data, list) and len(game_data) > 0:
                game_info = game_data[0]
                game_key = game_info.get("game_key")
                game_type = game_info.get("type")
                
                if game_key and game_type == "full":
                    game_keys.append(game_key)
    
    except Exception as e:
        print(f"解析游戏键时出错: {e}")
    
    return game_keys


def parse_games_data(games_data: Dict) -> List[Dict]:
    """解析Yahoo API返回的games数据
    
    Args:
        games_data: Yahoo API games响应
        
    Returns:
        标准化的游戏数据列表
    """
    games_list = []
    
    try:
        fantasy_content = games_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        
        for key, game_data in games_container.items():
            if key == "count":
                continue
                
            if not isinstance(game_data, dict) or "game" not in game_data:
                continue
            
            game_info = game_data["game"]
            
            # 处理嵌套的game结构
            if isinstance(game_info, list) and len(game_info) > 0:
                game_info = game_info[0]
            
            if not isinstance(game_info, dict):
                continue
            
            # 提取游戏信息
            parsed_game = parse_single_game(game_info)
            if parsed_game:
                games_list.append(parsed_game)
    
    except Exception as e:
        print(f"解析游戏数据时出错: {e}")
    
    return games_list


def parse_single_game(game_info: Dict) -> Optional[Dict]:
    """解析单个游戏的信息
    
    Args:
        game_info: 单个游戏的原始数据
        
    Returns:
        标准化的游戏数据字典，解析失败时返回None
    """
    try:
        # 必需字段验证
        required_fields = ["game_key", "game_id", "name", "code", "season"]
        for field in required_fields:
            if field not in game_info:
                return None
        
        parsed_game = {
            "game_key": game_info["game_key"],
            "game_id": game_info["game_id"],
            "name": game_info["name"],
            "code": game_info["code"],
            "type": game_info.get("type"),
            "url": game_info.get("url"),
            "season": game_info["season"],
            "is_registration_over": safe_bool_conversion(game_info.get("is_registration_over", 0)),
            "is_game_over": safe_bool_conversion(game_info.get("is_game_over", 0)),
            "is_offseason": safe_bool_conversion(game_info.get("is_offseason", 0)),
            "editorial_season": game_info.get("editorial_season"),
            "picks_status": game_info.get("picks_status"),
            "contest_group_id": game_info.get("contest_group_id"),
            "scenario_generator": safe_bool_conversion(game_info.get("scenario_generator", 0))
        }
        
        return parsed_game
    
    except Exception as e:
        print(f"解析单个游戏时出错: {e}")
        return None


def validate_game_data(game_data: Dict) -> bool:
    """验证游戏数据的完整性和合理性
    
    Args:
        game_data: 解析后的游戏数据
        
    Returns:
        数据是否有效
    """
    try:
        # 检查必需字段
        required_fields = ["game_key", "game_id", "name", "code", "season"]
        for field in required_fields:
            if field not in game_data or not game_data[field]:
                return False
        
        # 检查game_key格式 (通常是数字)
        game_key = game_data["game_key"]
        if not game_key.isdigit():
            return False
        
        # 检查season格式 (通常是4位年份)
        season = game_data["season"]
        if not (season.isdigit() and len(season) == 4):
            return False
        
        # 检查code是否为已知的运动类型
        known_codes = ["nba", "nfl", "mlb", "nhl", "yahoops"]
        if game_data["code"] not in known_codes:
            # 不强制要求，只是警告
            print(f"警告: 未知的游戏代码 {game_data['code']}")
        
        return True
    
    except Exception:
        return False


def filter_games_by_criteria(games_list: List[Dict], 
                           game_type: str = "full",
                           sport_codes: Optional[List[str]] = None) -> List[Dict]:
    """根据条件过滤游戏列表
    
    Args:
        games_list: 游戏数据列表
        game_type: 游戏类型过滤条件，默认为'full'
        sport_codes: 运动类型代码列表，None表示不过滤
        
    Returns:
        过滤后的游戏数据列表
    """
    filtered_games = []
    
    try:
        for game in games_list:
            # 过滤游戏类型
            if game_type and game.get("type") != game_type:
                continue
            
            # 过滤运动类型
            if sport_codes and game.get("code") not in sport_codes:
                continue
            
            # 验证数据完整性
            if validate_game_data(game):
                filtered_games.append(game)
    
    except Exception as e:
        print(f"过滤游戏数据时出错: {e}")
    
    return filtered_games


def extract_game_summary(games_list: List[Dict]) -> Dict[str, Any]:
    """提取游戏数据的摘要信息
    
    Args:
        games_list: 游戏数据列表
        
    Returns:
        游戏摘要信息
    """
    try:
        summary = {
            "total_games": len(games_list),
            "sports": {},
            "seasons": set(),
            "active_games": 0,
            "finished_games": 0
        }
        
        for game in games_list:
            # 统计运动类型
            sport_code = game.get("code", "unknown")
            if sport_code not in summary["sports"]:
                summary["sports"][sport_code] = 0
            summary["sports"][sport_code] += 1
            
            # 统计赛季
            season = game.get("season")
            if season:
                summary["seasons"].add(season)
            
            # 统计游戏状态
            if game.get("is_game_over"):
                summary["finished_games"] += 1
            else:
                summary["active_games"] += 1
        
        # 转换set为list便于序列化
        summary["seasons"] = sorted(list(summary["seasons"]))
        
        return summary
    
    except Exception as e:
        print(f"提取游戏摘要时出错: {e}")
        return {"error": str(e)} 