#!/usr/bin/env python3
"""
联盟数据解析器

迁移来源: @yahoo_api_data.py 中的联盟相关解析逻辑
主要映射:
  - _extract_leagues_from_data() -> parse_leagues_data()
  - 联盟基本信息提取逻辑

职责:
  - 解析Yahoo API返回的leagues数据结构
  - 提取league_key、league_id、name、game_key等核心字段
  - 处理联盟配置信息：num_teams、scoring_type、draft_status等
  - 解析联盟时间信息：start_date、end_date、current_week等
  - 处理布尔值字段的标准化转换

输入: Yahoo API leagues响应 (JSON)
输出: 标准化的联盟数据列表 (List[Dict])
"""

from typing import Dict, List, Optional, Any
from ..stats.stat_utils import safe_bool_conversion, safe_int_conversion


def parse_leagues_data(leagues_data: Dict, game_key: str) -> List[Dict]:
    """从API返回的数据中提取联盟信息
    
    Args:
        leagues_data: Yahoo API leagues响应
        game_key: 游戏键，用于标识联盟所属游戏
        
    Returns:
        标准化的联盟数据列表
    """
    leagues = []
    
    try:
        if "error" in leagues_data:
            print(f"API返回错误: {leagues_data.get('error')}")
            return leagues
        
        fantasy_content = leagues_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        
        for i in range(int(games_container.get("count", 0))):
            str_index = str(i)
            if str_index not in games_container:
                continue
            
            game_container = games_container[str_index]
            game_data = game_container["game"]
            
            current_game_key = None
            if isinstance(game_data, list) and len(game_data) > 0:
                current_game_key = game_data[0].get("game_key")
            
            if current_game_key != game_key:
                continue
            
            if len(game_data) > 1 and "leagues" in game_data[1]:
                leagues_container = game_data[1]["leagues"]
                leagues_count = int(leagues_container.get("count", 0))
                
                for j in range(leagues_count):
                    str_league_index = str(j)
                    if str_league_index not in leagues_container:
                        continue
                    
                    league_container = leagues_container[str_league_index]
                    league_data = league_container["league"]
                    
                    parsed_league = parse_single_league(league_data, game_key)
                    if parsed_league:
                        leagues.append(parsed_league)
            break
    
    except Exception as e:
        print(f"解析联盟数据时出错: {e}")
    
    return leagues


def parse_single_league(league_data: Any, game_key: str) -> Optional[Dict]:
    """解析单个联盟的数据
    
    Args:
        league_data: 联盟原始数据，可能是列表或字典
        game_key: 游戏键
        
    Returns:
        标准化的联盟数据字典，解析失败时返回None
    """
    try:
        league_info = {}
        
        # 处理不同格式的league_data
        if isinstance(league_data, list):
            for item in league_data:
                if isinstance(item, dict):
                    league_info.update(item)
        elif isinstance(league_data, dict):
            league_info = league_data.copy()
        else:
            return None
        
        # 必需字段验证
        required_fields = ["league_key", "league_id", "name"]
        for field in required_fields:
            if field not in league_info:
                print(f"缺少必需字段: {field}")
                return None
        
        # 确保联盟信息包含game_key
        league_info["game_key"] = game_key
        
        # 标准化联盟数据
        parsed_league = {
            "league_key": league_info["league_key"],
            "league_id": league_info["league_id"],
            "game_key": game_key,
            "name": league_info["name"],
            "url": league_info.get("url"),
            "logo_url": league_info.get("logo_url") if league_info.get("logo_url") else None,
            "password": league_info.get("password"),
            "draft_status": league_info.get("draft_status"),
            "num_teams": safe_int_conversion(league_info.get("num_teams")) or 0,
            "edit_key": league_info.get("edit_key"),
            "weekly_deadline": league_info.get("weekly_deadline"),
            "league_update_timestamp": league_info.get("league_update_timestamp"),
            "scoring_type": league_info.get("scoring_type"),
            "league_type": league_info.get("league_type"),
            "renew": league_info.get("renew"),
            "renewed": league_info.get("renewed"),
            "felo_tier": league_info.get("felo_tier"),
            "iris_group_chat_id": league_info.get("iris_group_chat_id"),
            "short_invitation_url": league_info.get("short_invitation_url"),
            "allow_add_to_dl_extra_pos": safe_bool_conversion(league_info.get("allow_add_to_dl_extra_pos", 0)),
            "is_pro_league": safe_bool_conversion(league_info.get("is_pro_league", "0")),
            "is_cash_league": safe_bool_conversion(league_info.get("is_cash_league", "0")),
            "current_week": str(league_info.get("current_week", "")),
            "start_week": league_info.get("start_week"),
            "start_date": league_info.get("start_date"),
            "end_week": league_info.get("end_week"),
            "end_date": league_info.get("end_date"),
            "is_finished": safe_bool_conversion(league_info.get("is_finished", 0)),
            "is_plus_league": safe_bool_conversion(league_info.get("is_plus_league", "0")),
            "game_code": league_info.get("game_code"),
            "season": league_info.get("season")
        }
        
        return parsed_league
    
    except Exception as e:
        print(f"解析单个联盟时出错: {e}")
        return None


def validate_league_data(league_data: Dict) -> bool:
    """验证联盟数据的完整性和合理性
    
    Args:
        league_data: 解析后的联盟数据
        
    Returns:
        数据是否有效
    """
    try:
        # 检查必需字段
        required_fields = ["league_key", "league_id", "game_key", "name"]
        for field in required_fields:
            if field not in league_data or not league_data[field]:
                return False
        
        # 检查num_teams合理性
        num_teams = league_data.get("num_teams", 0)
        if not isinstance(num_teams, int) or num_teams < 2 or num_teams > 20:
            print(f"警告: 联盟团队数量异常 {num_teams}")
        
        # 检查draft_status有效性
        valid_draft_statuses = ["predraft", "postdraft", "indraft"]
        draft_status = league_data.get("draft_status")
        if draft_status and draft_status not in valid_draft_statuses:
            print(f"警告: 未知的draft状态 {draft_status}")
        
        # 检查scoring_type有效性
        valid_scoring_types = ["head", "points", "roto"]
        scoring_type = league_data.get("scoring_type")
        if scoring_type and scoring_type not in valid_scoring_types:
            print(f"警告: 未知的计分类型 {scoring_type}")
        
        return True
    
    except Exception:
        return False


def parse_league_settings_data(settings_data: Dict) -> Optional[Dict]:
    """解析联盟设置数据
    
    Args:
        settings_data: Yahoo API league settings响应
        
    Returns:
        标准化的联盟设置数据，解析失败时返回None
    """
    try:
        if not settings_data or "fantasy_content" not in settings_data:
            return None
        
        settings_info = settings_data["fantasy_content"]["league"][1]["settings"][0]
        
        parsed_settings = {
            "draft_type": settings_info.get("draft_type"),
            "is_auction_draft": safe_bool_conversion(settings_info.get("is_auction_draft", "0")),
            "persistent_url": settings_info.get("persistent_url"),
            "uses_playoff": safe_bool_conversion(settings_info.get("uses_playoff", "1")),
            "has_playoff_consolation_games": safe_bool_conversion(settings_info.get("has_playoff_consolation_games", False)),
            "playoff_start_week": settings_info.get("playoff_start_week"),
            "uses_playoff_reseeding": safe_bool_conversion(settings_info.get("uses_playoff_reseeding", 0)),
            "uses_lock_eliminated_teams": safe_bool_conversion(settings_info.get("uses_lock_eliminated_teams", 0)),
            "num_playoff_teams": safe_int_conversion(settings_info.get("num_playoff_teams", 0)),
            "num_playoff_consolation_teams": safe_int_conversion(settings_info.get("num_playoff_consolation_teams", 0)),
            "has_multiweek_championship": safe_bool_conversion(settings_info.get("has_multiweek_championship", 0)),
            "waiver_type": settings_info.get("waiver_type"),
            "waiver_rule": settings_info.get("waiver_rule"),
            "uses_faab": safe_bool_conversion(settings_info.get("uses_faab", "0")),
            "draft_time": settings_info.get("draft_time"),
            "draft_pick_time": settings_info.get("draft_pick_time"),
            "post_draft_players": settings_info.get("post_draft_players"),
            "max_teams": safe_int_conversion(settings_info.get("max_teams", 0)),
            "waiver_time": settings_info.get("waiver_time"),
            "trade_end_date": settings_info.get("trade_end_date"),
            "trade_ratify_type": settings_info.get("trade_ratify_type"),
            "trade_reject_time": settings_info.get("trade_reject_time"),
            "player_pool": settings_info.get("player_pool"),
            "cant_cut_list": settings_info.get("cant_cut_list"),
            "draft_together": safe_bool_conversion(settings_info.get("draft_together", 0)),
            "is_publicly_viewable": safe_bool_conversion(settings_info.get("is_publicly_viewable", "1")),
            "can_trade_draft_picks": safe_bool_conversion(settings_info.get("can_trade_draft_picks", "0")),
            "sendbird_channel_url": settings_info.get("sendbird_channel_url"),
            "roster_positions": settings_info.get("roster_positions"),
            "stat_categories": settings_info.get("stat_categories")
        }
        
        return parsed_settings
    
    except Exception as e:
        print(f"解析联盟设置时出错: {e}")
        return None


def parse_stat_categories_data(stat_categories_data: Dict) -> List[Dict]:
    """解析统计类别数据
    
    Args:
        stat_categories_data: 统计类别原始数据
        
    Returns:
        标准化的统计类别列表
    """
    categories = []
    
    try:
        if not stat_categories_data or 'stats' not in stat_categories_data:
            return categories
        
        stats_list = stat_categories_data['stats']
        
        for stat_item in stats_list:
            if 'stat' not in stat_item:
                continue
                
            stat_info = stat_item['stat']
            stat_id = stat_info.get('stat_id')
            
            if not stat_id:
                continue
            
            parsed_category = {
                "stat_id": safe_int_conversion(stat_id),
                "name": stat_info.get('name', ''),
                "display_name": stat_info.get('display_name', ''),
                "abbr": stat_info.get('abbr', ''),
                "group_name": stat_info.get('group', ''),
                "sort_order": safe_int_conversion(stat_info.get('sort_order', 0)),
                "position_type": stat_info.get('position_type', ''),
                "is_enabled": safe_bool_conversion(stat_info.get('enabled', '1')),
                "is_only_display_stat": safe_bool_conversion(stat_info.get('is_only_display_stat', '0'))
            }
            
            categories.append(parsed_category)
    
    except Exception as e:
        print(f"解析统计类别时出错: {e}")
    
    return categories


def parse_roster_positions_data(roster_positions_data: Any) -> List[Dict]:
    """解析联盟阵容位置配置
    
    Args:
        roster_positions_data: 阵容位置原始数据
        
    Returns:
        标准化的阵容位置配置列表
    """
    positions = []
    
    try:
        # 如果是字符串，尝试解析为JSON
        if isinstance(roster_positions_data, str):
            import json
            roster_positions_list = json.loads(roster_positions_data)
        else:
            roster_positions_list = roster_positions_data

        if not isinstance(roster_positions_list, list):
            return positions

        for rp_item in roster_positions_list:
            # 每个item结构可能是 {"roster_position": {...}}
            if isinstance(rp_item, dict) and "roster_position" in rp_item:
                rp_info = rp_item["roster_position"]
            elif isinstance(rp_item, dict):
                rp_info = rp_item
            else:
                continue

            position = rp_info.get("position")
            if not position:
                continue

            parsed_position = {
                "position": position,
                "position_type": rp_info.get("position_type"),
                "count": safe_int_conversion(rp_info.get("count")) or 0,
                "is_starting_position": safe_bool_conversion(rp_info.get("is_starting_position", False))
            }
            
            positions.append(parsed_position)
    
    except Exception as e:
        print(f"解析阵容位置时出错: {e}")
    
    return positions


def filter_leagues_by_criteria(leagues_list: List[Dict], 
                              draft_status: Optional[str] = None,
                              is_finished: Optional[bool] = None,
                              min_teams: Optional[int] = None) -> List[Dict]:
    """根据条件过滤联盟列表
    
    Args:
        leagues_list: 联盟数据列表
        draft_status: 选秀状态过滤条件
        is_finished: 是否已结束过滤条件
        min_teams: 最小团队数量
        
    Returns:
        过滤后的联盟数据列表
    """
    filtered_leagues = []
    
    try:
        for league in leagues_list:
            # 过滤选秀状态
            if draft_status and league.get("draft_status") != draft_status:
                continue
            
            # 过滤结束状态
            if is_finished is not None and league.get("is_finished") != is_finished:
                continue
            
            # 过滤团队数量
            if min_teams and league.get("num_teams", 0) < min_teams:
                continue
            
            # 验证数据完整性
            if validate_league_data(league):
                filtered_leagues.append(league)
    
    except Exception as e:
        print(f"过滤联盟数据时出错: {e}")
    
    return filtered_leagues


def extract_league_summary(leagues_list: List[Dict]) -> Dict[str, Any]:
    """提取联盟数据的摘要信息
    
    Args:
        leagues_list: 联盟数据列表
        
    Returns:
        联盟摘要信息
    """
    try:
        summary = {
            "total_leagues": len(leagues_list),
            "draft_statuses": {},
            "scoring_types": {},
            "seasons": set(),
            "finished_leagues": 0,
            "active_leagues": 0,
            "total_teams": 0,
            "avg_teams_per_league": 0
        }
        
        for league in leagues_list:
            # 统计选秀状态
            draft_status = league.get("draft_status", "unknown")
            if draft_status not in summary["draft_statuses"]:
                summary["draft_statuses"][draft_status] = 0
            summary["draft_statuses"][draft_status] += 1
            
            # 统计计分类型
            scoring_type = league.get("scoring_type", "unknown")
            if scoring_type not in summary["scoring_types"]:
                summary["scoring_types"][scoring_type] = 0
            summary["scoring_types"][scoring_type] += 1
            
            # 统计赛季
            season = league.get("season")
            if season:
                summary["seasons"].add(season)
            
            # 统计联盟状态
            if league.get("is_finished"):
                summary["finished_leagues"] += 1
            else:
                summary["active_leagues"] += 1
            
            # 统计团队数量
            num_teams = league.get("num_teams", 0)
            summary["total_teams"] += num_teams
        
        # 计算平均团队数
        if len(leagues_list) > 0:
            summary["avg_teams_per_league"] = round(summary["total_teams"] / len(leagues_list), 1)
        
        # 转换set为list便于序列化
        summary["seasons"] = sorted(list(summary["seasons"]))
        
        return summary
    
    except Exception as e:
        print(f"提取联盟摘要时出错: {e}")
        return {"error": str(e)} 