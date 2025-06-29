#!/usr/bin/env python3
"""
球员数据标准化器

迁移来源: @yahoo_api_data.py 中的球员标准化逻辑
主要映射:
  - _normalize_player_info() -> normalize_player_data()
  - 球员信息字段标准化逻辑

职责:
  - 标准化球员姓名信息：
    * 从name字典中提取full、first、last字段
    * 设置统一的姓名字段格式
  - 标准化团队信息：
    * editorial_team_key -> current_team_key
    * editorial_team_full_name -> current_team_name  
    * editorial_team_abbr -> current_team_abbr
  - 标准化头像信息：
    * 从headshot字典中提取url字段
    * 设置headshot_url字段
  - 添加元数据字段：
    * season：当前赛季标识
    * last_updated：更新时间戳
  - 数据类型标准化和字段重命名
  - 处理缺失值和默认值设置

输入: 原始球员数据字典 (Dict)
输出: 标准化的球员数据字典 (Dict)
"""

from typing import Dict, List, Optional, Any
from datetime import datetime


def normalize_player_data(player_info: Dict, season: Optional[str] = None) -> Dict:
    """标准化球员数据
    
    Args:
        player_info: 原始球员信息字典
        season: 赛季标识，可选
        
    Returns:
        标准化的球员数据字典
    """
    try:
        normalized = {}
        
        # 基本标识信息
        normalized["player_key"] = player_info.get("player_key")
        normalized["player_id"] = player_info.get("player_id")
        normalized["editorial_player_key"] = player_info.get("editorial_player_key")
        
        # 处理姓名信息
        _normalize_name_fields(player_info, normalized)
        
        # 处理团队信息
        _normalize_team_fields(player_info, normalized)
        
        # 处理头像信息
        _normalize_headshot_fields(player_info, normalized)
        
        # 处理位置信息
        _normalize_position_fields(player_info, normalized)
        
        # 处理球员属性
        _normalize_player_attributes(player_info, normalized)
        
        # 添加元数据字段
        _add_metadata_fields(player_info, normalized, season)
        
        # 验证必要字段
        if not _validate_required_fields(normalized):
            print(f"警告: 球员数据缺少必要字段 {normalized.get('player_key', 'unknown')}")
        
        return normalized
    
    except Exception as e:
        print(f"标准化球员数据时出错: {e}")
        return player_info


def _normalize_name_fields(player_info: Dict, normalized: Dict) -> None:
    """标准化球员姓名信息
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
    """
    try:
        # 处理姓名信息
        if "name" in player_info:
            name_info = player_info["name"]
            if isinstance(name_info, dict):
                normalized["full_name"] = name_info.get("full", "").strip()
                normalized["first_name"] = name_info.get("first", "").strip()
                normalized["last_name"] = name_info.get("last", "").strip()
            else:
                # 如果name不是字典，尝试作为全名处理
                normalized["full_name"] = str(name_info).strip() if name_info else ""
                normalized["first_name"] = ""
                normalized["last_name"] = ""
        else:
            # 尝试从其他可能的字段获取姓名
            normalized["full_name"] = player_info.get("full_name", "").strip()
            normalized["first_name"] = player_info.get("first_name", "").strip()
            normalized["last_name"] = player_info.get("last_name", "").strip()
        
        # 确保姓名字段不为None
        for field in ["full_name", "first_name", "last_name"]:
            if normalized.get(field) is None:
                normalized[field] = ""
    
    except Exception as e:
        print(f"标准化姓名字段时出错: {e}")
        normalized["full_name"] = ""
        normalized["first_name"] = ""
        normalized["last_name"] = ""


def _normalize_team_fields(player_info: Dict, normalized: Dict) -> None:
    """标准化团队信息
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
    """
    try:
        # 处理团队信息 - 映射editorial字段到current字段
        if "editorial_team_key" in player_info:
            normalized["current_team_key"] = player_info["editorial_team_key"]
        else:
            normalized["current_team_key"] = player_info.get("current_team_key")
        
        if "editorial_team_full_name" in player_info:
            normalized["current_team_name"] = player_info["editorial_team_full_name"]
        else:
            normalized["current_team_name"] = player_info.get("current_team_name")
        
        if "editorial_team_abbr" in player_info:
            normalized["current_team_abbr"] = player_info["editorial_team_abbr"]
        else:
            normalized["current_team_abbr"] = player_info.get("current_team_abbr")
    
    except Exception as e:
        print(f"标准化团队字段时出错: {e}")
        normalized["current_team_key"] = None
        normalized["current_team_name"] = None
        normalized["current_team_abbr"] = None


def _normalize_headshot_fields(player_info: Dict, normalized: Dict) -> None:
    """标准化头像信息
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
    """
    try:
        # 处理头像信息
        if "headshot" in player_info:
            headshot_info = player_info["headshot"]
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                normalized["headshot_url"] = headshot_info["url"]
            elif isinstance(headshot_info, str):
                # 如果headshot直接是URL字符串
                normalized["headshot_url"] = headshot_info
            else:
                normalized["headshot_url"] = None
        else:
            # 尝试从其他可能的字段获取头像URL
            normalized["headshot_url"] = player_info.get("headshot_url") or player_info.get("image_url")
    
    except Exception as e:
        print(f"标准化头像字段时出错: {e}")
        normalized["headshot_url"] = None


def _normalize_position_fields(player_info: Dict, normalized: Dict) -> None:
    """标准化位置信息
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
    """
    try:
        # 位置信息
        normalized["display_position"] = player_info.get("display_position")
        normalized["primary_position"] = player_info.get("primary_position")
        normalized["position_type"] = player_info.get("position_type")
        
        # 处理合适位置信息
        eligible_positions = player_info.get("eligible_positions", [])
        normalized["eligible_positions"] = _normalize_eligible_positions(eligible_positions)
    
    except Exception as e:
        print(f"标准化位置字段时出错: {e}")
        normalized["display_position"] = None
        normalized["primary_position"] = None
        normalized["position_type"] = None
        normalized["eligible_positions"] = []


def _normalize_player_attributes(player_info: Dict, normalized: Dict) -> None:
    """标准化球员属性
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
    """
    try:
        # 球员属性
        normalized["uniform_number"] = player_info.get("uniform_number")
        normalized["status"] = player_info.get("status")
        normalized["image_url"] = player_info.get("image_url")
        
        # 布尔值属性
        is_undroppable = player_info.get("is_undroppable", False)
        if isinstance(is_undroppable, str):
            normalized["is_undroppable"] = is_undroppable.strip().lower() in ('1', 'true', 'yes')
        else:
            normalized["is_undroppable"] = bool(is_undroppable)
    
    except Exception as e:
        print(f"标准化球员属性时出错: {e}")
        normalized["uniform_number"] = None
        normalized["status"] = None
        normalized["image_url"] = None
        normalized["is_undroppable"] = False


def _add_metadata_fields(player_info: Dict, normalized: Dict, season: Optional[str]) -> None:
    """添加元数据字段
    
    Args:
        player_info: 原始球员信息
        normalized: 标准化结果字典
        season: 赛季标识
    """
    try:
        # 添加赛季信息
        if season:
            normalized["season"] = season
        else:
            normalized["season"] = player_info.get("season", "unknown")
        
        # 添加时间戳
        if "last_updated" in player_info:
            normalized["last_updated"] = player_info["last_updated"]
        else:
            normalized["last_updated"] = datetime.now()
    
    except Exception as e:
        print(f"添加元数据字段时出错: {e}")
        normalized["season"] = "unknown"
        normalized["last_updated"] = datetime.now()


def _normalize_eligible_positions(eligible_positions: Any) -> List[str]:
    """标准化合适位置信息
    
    Args:
        eligible_positions: 位置数据，可能是列表或其他格式
        
    Returns:
        标准化的位置字符串列表
    """
    positions = []
    
    try:
        if not eligible_positions:
            return positions
        
        if isinstance(eligible_positions, list):
            for pos_item in eligible_positions:
                if isinstance(pos_item, dict):
                    position = pos_item.get("position")
                    if position and position not in positions:
                        positions.append(str(position).strip())
                elif isinstance(pos_item, str):
                    position = pos_item.strip()
                    if position and position not in positions:
                        positions.append(position)
        elif isinstance(eligible_positions, str):
            # 单个位置字符串
            position = eligible_positions.strip()
            if position:
                positions.append(position)
    
    except Exception as e:
        print(f"标准化合适位置时出错: {e}")
    
    return positions


def _validate_required_fields(player_data: Dict) -> bool:
    """验证必要字段是否存在
    
    Args:
        player_data: 标准化后的球员数据
        
    Returns:
        是否包含所有必要字段
    """
    try:
        required_fields = ["player_key", "player_id"]
        
        for field in required_fields:
            if not player_data.get(field):
                return False
        
        return True
    
    except Exception:
        return False


def batch_normalize_players(players_list: List[Dict], season: Optional[str] = None) -> List[Dict]:
    """批量标准化球员数据
    
    Args:
        players_list: 球员数据列表
        season: 赛季标识，可选
        
    Returns:
        标准化后的球员数据列表
    """
    normalized_players = []
    
    try:
        for player_info in players_list:
            if not isinstance(player_info, dict):
                continue
            
            normalized_player = normalize_player_data(player_info, season)
            if _validate_required_fields(normalized_player):
                normalized_players.append(normalized_player)
            else:
                print(f"跳过无效球员记录: {player_info.get('player_key', 'unknown')}")
    
    except Exception as e:
        print(f"批量标准化球员数据时出错: {e}")
    
    return normalized_players


def extract_player_changes(old_player: Dict, new_player: Dict) -> Dict[str, Any]:
    """提取球员数据变化
    
    Args:
        old_player: 旧的球员数据
        new_player: 新的球员数据
        
    Returns:
        数据变化摘要
    """
    changes = {
        "has_changes": False,
        "changed_fields": [],
        "team_change": False,
        "position_change": False,
        "status_change": False
    }
    
    try:
        # 检查关键字段变化
        key_fields = [
            "current_team_key", "current_team_name", "current_team_abbr",
            "display_position", "status", "uniform_number"
        ]
        
        for field in key_fields:
            old_value = old_player.get(field)
            new_value = new_player.get(field)
            
            if old_value != new_value:
                changes["has_changes"] = True
                changes["changed_fields"].append({
                    "field": field,
                    "old_value": old_value,
                    "new_value": new_value
                })
                
                # 标记特定类型的变化
                if field.startswith("current_team"):
                    changes["team_change"] = True
                elif field == "display_position":
                    changes["position_change"] = True
                elif field == "status":
                    changes["status_change"] = True
    
    except Exception as e:
        print(f"提取球员变化时出错: {e}")
        changes["error"] = str(e)
    
    return changes


def validate_player_data_integrity(player_data: Dict) -> Dict[str, bool]:
    """验证球员数据完整性
    
    Args:
        player_data: 球员数据字典
        
    Returns:
        验证结果字典
    """
    validation_results = {
        "has_required_fields": True,
        "has_name_info": True,
        "has_position_info": True,
        "has_team_info": True,
        "data_consistency": True
    }
    
    try:
        # 验证必要字段
        required_fields = ["player_key", "player_id"]
        for field in required_fields:
            if not player_data.get(field):
                validation_results["has_required_fields"] = False
                break
        
        # 验证姓名信息
        if not player_data.get("full_name"):
            validation_results["has_name_info"] = False
        
        # 验证位置信息
        if not player_data.get("display_position"):
            validation_results["has_position_info"] = False
        
        # 验证团队信息
        if not player_data.get("current_team_abbr"):
            validation_results["has_team_info"] = False
        
        # 验证数据一致性
        eligible_positions = player_data.get("eligible_positions", [])
        if not isinstance(eligible_positions, list):
            validation_results["data_consistency"] = False
    
    except Exception as e:
        print(f"验证球员数据完整性时出错: {e}")
        # 验证出错时，将所有验证项标记为失败
        validation_results = {key: False for key in validation_results}
    
    return validation_results


def extract_player_summary(players_list: List[Dict]) -> Dict[str, Any]:
    """提取球员数据摘要信息
    
    Args:
        players_list: 球员数据列表
        
    Returns:
        球员摘要信息
    """
    try:
        summary = {
            "total_players": len(players_list),
            "valid_players": 0,
            "teams": set(),
            "positions": set(),
            "status_counts": {},
            "missing_info": {
                "no_name": 0,
                "no_position": 0,
                "no_team": 0
            }
        }
        
        for player in players_list:
            # 统计有效球员
            if _validate_required_fields(player):
                summary["valid_players"] += 1
            
            # 统计团队
            team_abbr = player.get("current_team_abbr")
            if team_abbr:
                summary["teams"].add(team_abbr)
            else:
                summary["missing_info"]["no_team"] += 1
            
            # 统计位置
            positions = player.get("eligible_positions", [])
            for pos in positions:
                summary["positions"].add(pos)
            
            if not player.get("display_position"):
                summary["missing_info"]["no_position"] += 1
            
            # 统计状态
            status = player.get("status", "active")
            if status not in summary["status_counts"]:
                summary["status_counts"][status] = 0
            summary["status_counts"][status] += 1
            
            # 统计缺失姓名
            if not player.get("full_name"):
                summary["missing_info"]["no_name"] += 1
        
        # 转换set为list便于序列化
        summary["teams"] = sorted(list(summary["teams"]))
        summary["positions"] = sorted(list(summary["positions"]))
        
        return summary
    
    except Exception as e:
        print(f"提取球员摘要时出错: {e}")
        return {"error": str(e)} 