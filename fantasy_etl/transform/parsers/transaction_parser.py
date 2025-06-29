#!/usr/bin/env python3
"""
交易数据解析器

迁移来源: @yahoo_api_data.py 中的交易数据处理逻辑
主要映射:
  - 提取和解析Yahoo API交易响应数据
  - 标准化各种交易类型：add、drop、trade等

职责:
  - 交易数据解析：
    * 从复杂嵌套的transactions响应中提取交易信息
    * 解析transaction容器：transactions["0"]["transactions"][i]["transaction"]
    * 提取交易基本信息：type、status、timestamp等
    * 解析涉及的球员信息：players容器
  - 交易类型处理：
    * add：添加球员（从自由球员市场）
    * drop：释放球员（放到自由球员市场）
    * trade：球员交易（团队间交换）
    * add/drop：同时添加和释放球员
  - 球员信息提取：
    * 从players容器中提取球员基本信息
    * 解析球员键、姓名、位置信息
    * 处理复杂的嵌套结构：players["0"]["players"][i]["player"]
  - 交易状态解析：
    * successful：成功完成的交易
    * pending：等待处理的交易
    * failed：失败的交易
  - 团队信息关联：
    * 提取交易发起团队信息
    * 处理多团队交易场景
    * 关联团队键和联盟信息
  - 时间信息处理：
    * 解析交易时间戳
    * 标准化时间格式
    * 添加时区信息

输入: Yahoo API transactions响应数据 (Dict)
输出: 标准化的交易信息列表 (List[Dict])
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from ..utils.stat_utils import safe_int_conversion


def parse_transactions_data(transactions_response: Dict) -> List[Dict]:
    """解析交易数据响应
    
    Args:
        transactions_response: Yahoo API transactions响应数据
        
    Returns:
        标准化的交易信息列表
    """
    try:
        transactions = []
        
        if not transactions_response or "transaction" not in transactions_response:
            return transactions
        
        # 检查是否是单个交易还是交易列表
        transaction_data = transactions_response["transaction"]
        
        if isinstance(transaction_data, list):
            # 多个交易
            for transaction_item in transaction_data:
                parsed_transaction = parse_single_transaction(transaction_item)
                if parsed_transaction:
                    transactions.append(parsed_transaction)
        else:
            # 单个交易
            parsed_transaction = parse_single_transaction(transaction_data)
            if parsed_transaction:
                transactions.append(parsed_transaction)
        
        return transactions
    
    except Exception as e:
        print(f"解析交易数据失败: {e}")
        return []


def parse_single_transaction(transaction_data: Dict) -> Optional[Dict]:
    """解析单个交易信息
    
    Args:
        transaction_data: 单个交易的数据
        
    Returns:
        标准化的交易信息，解析失败时返回None
    """
    try:
        if not isinstance(transaction_data, dict):
            return None
        
        # 提取基本交易信息
        transaction_info = {
            "transaction_key": transaction_data.get("transaction_key"),
            "transaction_id": transaction_data.get("transaction_id"),
            "type": transaction_data.get("type"),
            "status": transaction_data.get("status"),
            "timestamp": _parse_transaction_timestamp(transaction_data.get("timestamp")),
            "faab_bid": safe_int_conversion(transaction_data.get("faab_bid")),
            "trade_note": transaction_data.get("trade_note")
        }
        
        # 提取涉及的球员信息
        players_info = _extract_players_from_transaction(transaction_data)
        transaction_info["players"] = players_info
        
        # 根据交易类型添加特定信息
        _add_type_specific_info(transaction_info, transaction_data)
        
        # 验证交易数据
        if not _validate_transaction_data(transaction_info):
            return None
        
        return transaction_info
    
    except Exception as e:
        print(f"解析单个交易失败: {e}")
        return None


def _extract_players_from_transaction(transaction_data: Dict) -> List[Dict]:
    """从交易数据中提取球员信息
    
    Args:
        transaction_data: 交易数据
        
    Returns:
        球员信息列表
    """
    players_info = []
    
    try:
        if "players" not in transaction_data:
            return players_info
        
        players_container = transaction_data["players"]
        
        # 处理不同的players结构
        if isinstance(players_container, dict) and "0" in players_container:
            # 标准嵌套结构
            players_data = players_container["0"].get("players", {})
            players_count = int(players_data.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_data:
                    continue
                
                player_container = players_data[str_index]
                if "player" in player_container:
                    player_info = _parse_player_from_transaction(player_container["player"])
                    if player_info:
                        players_info.append(player_info)
        
        elif isinstance(players_container, list):
            # 直接的球员列表
            for player_item in players_container:
                player_info = _parse_player_from_transaction(player_item)
                if player_info:
                    players_info.append(player_info)
    
    except Exception as e:
        print(f"提取交易球员信息失败: {e}")
    
    return players_info


def _parse_player_from_transaction(player_data: Any) -> Optional[Dict]:
    """从交易中解析球员信息
    
    Args:
        player_data: 球员数据，可能是复杂的嵌套结构
        
    Returns:
        球员信息字典，解析失败时返回None
    """
    try:
        player_info = {
            "player_key": None,
            "player_id": None,
            "name": None,
            "editorial_player_key": None,
            "editorial_team_key": None,
            "editorial_team_full_name": None,
            "editorial_team_abbr": None,
            "uniform_number": None,
            "display_position": None,
            "position_type": None,
            "eligible_positions": [],
            "transaction_data": {}
        }
        
        # 处理不同的player数据结构
        if isinstance(player_data, list):
            # 嵌套列表结构，通常player_data[0]包含基本信息
            for player_item in player_data:
                if isinstance(player_item, dict):
                    _extract_player_basic_info(player_info, player_item)
                    _extract_player_transaction_info(player_info, player_item)
        
        elif isinstance(player_data, dict):
            # 直接字典结构
            _extract_player_basic_info(player_info, player_data)
            _extract_player_transaction_info(player_info, player_data)
        
        # 验证必需信息
        if player_info["player_key"] or player_info["editorial_player_key"]:
            return player_info
        
        return None
    
    except Exception as e:
        print(f"解析交易球员信息失败: {e}")
        return None


def _extract_player_basic_info(player_info: Dict, player_data: Dict) -> None:
    """提取球员基本信息
    
    Args:
        player_info: 球员信息字典（会被修改）
        player_data: 原始球员数据
    """
    try:
        # 基本身份信息
        if "player_key" in player_data:
            player_info["player_key"] = player_data["player_key"]
        
        if "player_id" in player_data:
            player_info["player_id"] = safe_int_conversion(player_data["player_id"])
        
        # 姓名信息
        if "name" in player_data:
            name_data = player_data["name"]
            if isinstance(name_data, dict):
                player_info["name"] = name_data.get("full")
            else:
                player_info["name"] = str(name_data)
        
        # 编辑信息
        if "editorial_player_key" in player_data:
            player_info["editorial_player_key"] = player_data["editorial_player_key"]
        
        if "editorial_team_key" in player_data:
            player_info["editorial_team_key"] = player_data["editorial_team_key"]
        
        if "editorial_team_full_name" in player_data:
            player_info["editorial_team_full_name"] = player_data["editorial_team_full_name"]
        
        if "editorial_team_abbr" in player_data:
            player_info["editorial_team_abbr"] = player_data["editorial_team_abbr"]
        
        # 位置信息
        if "uniform_number" in player_data:
            player_info["uniform_number"] = safe_int_conversion(player_data["uniform_number"])
        
        if "display_position" in player_data:
            player_info["display_position"] = player_data["display_position"]
        
        if "position_type" in player_data:
            player_info["position_type"] = player_data["position_type"]
        
        # 合适位置
        if "eligible_positions" in player_data:
            eligible_positions = player_data["eligible_positions"]
            if isinstance(eligible_positions, dict):
                # 解析位置数组
                positions = []
                position_count = int(eligible_positions.get("count", 0))
                for i in range(position_count):
                    str_index = str(i)
                    if str_index in eligible_positions:
                        position_data = eligible_positions[str_index]
                        if isinstance(position_data, dict) and "position" in position_data:
                            positions.append(position_data["position"])
                        else:
                            positions.append(str(position_data))
                player_info["eligible_positions"] = positions
            elif isinstance(eligible_positions, list):
                player_info["eligible_positions"] = eligible_positions
    
    except Exception as e:
        print(f"提取球员基本信息失败: {e}")


def _extract_player_transaction_info(player_info: Dict, player_data: Dict) -> None:
    """提取球员交易相关信息
    
    Args:
        player_info: 球员信息字典（会被修改）
        player_data: 原始球员数据
    """
    try:
        transaction_data = {}
        
        # 交易相关字段
        transaction_fields = [
            "source_type", "source_team_key", "source_team_name",
            "destination_type", "destination_team_key", "destination_team_name",
            "transaction_data"
        ]
        
        for field in transaction_fields:
            if field in player_data:
                transaction_data[field] = player_data[field]
        
        player_info["transaction_data"] = transaction_data
    
    except Exception as e:
        print(f"提取球员交易信息失败: {e}")


def _parse_transaction_timestamp(timestamp_data: Any) -> Optional[datetime]:
    """解析交易时间戳
    
    Args:
        timestamp_data: 时间戳数据，可能是字符串或整数
        
    Returns:
        解析后的datetime对象，解析失败时返回None
    """
    try:
        if not timestamp_data:
            return None
        
        if isinstance(timestamp_data, str):
            # 尝试解析ISO格式时间戳
            try:
                return datetime.fromisoformat(timestamp_data.replace('Z', '+00:00'))
            except ValueError:
                pass
            
            # 尝试解析Unix时间戳字符串
            try:
                unix_timestamp = int(timestamp_data)
                return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
            except ValueError:
                pass
        
        elif isinstance(timestamp_data, (int, float)):
            # Unix时间戳
            return datetime.fromtimestamp(timestamp_data, tz=timezone.utc)
        
        return None
    
    except Exception as e:
        print(f"解析交易时间戳失败: {e}")
        return None


def _add_type_specific_info(transaction_info: Dict, transaction_data: Dict) -> None:
    """根据交易类型添加特定信息
    
    Args:
        transaction_info: 交易信息字典（会被修改）
        transaction_data: 原始交易数据
    """
    try:
        transaction_type = transaction_info.get("type")
        
        if transaction_type == "add":
            # 添加球员特定信息
            transaction_info["added_players"] = [
                player for player in transaction_info.get("players", [])
                if player.get("transaction_data", {}).get("destination_type") == "team"
            ]
        
        elif transaction_type == "drop":
            # 释放球员特定信息
            transaction_info["dropped_players"] = [
                player for player in transaction_info.get("players", [])
                if player.get("transaction_data", {}).get("source_type") == "team"
            ]
        
        elif transaction_type == "add/drop":
            # 同时添加和释放
            players = transaction_info.get("players", [])
            transaction_info["added_players"] = [
                player for player in players
                if player.get("transaction_data", {}).get("destination_type") == "team"
            ]
            transaction_info["dropped_players"] = [
                player for player in players
                if player.get("transaction_data", {}).get("source_type") == "team"
            ]
        
        elif transaction_type == "trade":
            # 球员交易特定信息
            _extract_trade_details(transaction_info, transaction_data)
    
    except Exception as e:
        print(f"添加交易类型特定信息失败: {e}")


def _extract_trade_details(transaction_info: Dict, transaction_data: Dict) -> None:
    """提取交易详细信息
    
    Args:
        transaction_info: 交易信息字典（会被修改）
        transaction_data: 原始交易数据
    """
    try:
        # 按团队分组球员
        players_by_team = {}
        
        for player in transaction_info.get("players", []):
            source_team = player.get("transaction_data", {}).get("source_team_key")
            dest_team = player.get("transaction_data", {}).get("destination_team_key")
            
            if source_team:
                if source_team not in players_by_team:
                    players_by_team[source_team] = {"sent": [], "received": []}
                players_by_team[source_team]["sent"].append(player)
            
            if dest_team:
                if dest_team not in players_by_team:
                    players_by_team[dest_team] = {"sent": [], "received": []}
                players_by_team[dest_team]["received"].append(player)
        
        transaction_info["trade_details"] = {
            "teams_involved": list(players_by_team.keys()),
            "players_by_team": players_by_team
        }
    
    except Exception as e:
        print(f"提取交易详细信息失败: {e}")


def _validate_transaction_data(transaction_info: Dict) -> bool:
    """验证交易数据的完整性
    
    Args:
        transaction_info: 交易信息
        
    Returns:
        验证是否通过
    """
    try:
        # 检查必需字段
        required_fields = ["transaction_key", "type", "status"]
        for field in required_fields:
            if not transaction_info.get(field):
                print(f"交易缺少必需字段: {field}")
                return False
        
        # 验证交易类型
        valid_types = ["add", "drop", "add/drop", "trade"]
        if transaction_info["type"] not in valid_types:
            print(f"无效的交易类型: {transaction_info['type']}")
            return False
        
        # 验证交易状态
        valid_statuses = ["successful", "pending", "failed"]
        if transaction_info["status"] not in valid_statuses:
            print(f"无效的交易状态: {transaction_info['status']}")
            return False
        
        # 验证球员信息
        players = transaction_info.get("players", [])
        if not players:
            print("交易中没有球员信息")
            return False
        
        return True
    
    except Exception as e:
        print(f"验证交易数据失败: {e}")
        return False


def filter_transactions_by_criteria(transactions: List[Dict], 
                                   criteria: Dict) -> List[Dict]:
    """根据条件过滤交易数据
    
    Args:
        transactions: 交易列表
        criteria: 过滤条件
        
    Returns:
        过滤后的交易列表
    """
    try:
        filtered_transactions = []
        
        for transaction in transactions:
            if _matches_criteria(transaction, criteria):
                filtered_transactions.append(transaction)
        
        return filtered_transactions
    
    except Exception as e:
        print(f"过滤交易数据失败: {e}")
        return transactions


def _matches_criteria(transaction: Dict, criteria: Dict) -> bool:
    """检查交易是否匹配过滤条件
    
    Args:
        transaction: 交易信息
        criteria: 过滤条件
        
    Returns:
        是否匹配
    """
    try:
        # 交易类型过滤
        if "types" in criteria:
            if transaction.get("type") not in criteria["types"]:
                return False
        
        # 交易状态过滤
        if "statuses" in criteria:
            if transaction.get("status") not in criteria["statuses"]:
                return False
        
        # 时间范围过滤
        if "start_date" in criteria or "end_date" in criteria:
            transaction_time = transaction.get("timestamp")
            if transaction_time:
                if "start_date" in criteria:
                    if transaction_time < criteria["start_date"]:
                        return False
                
                if "end_date" in criteria:
                    if transaction_time > criteria["end_date"]:
                        return False
        
        # 球员过滤
        if "player_keys" in criteria:
            transaction_players = transaction.get("players", [])
            transaction_player_keys = {
                player.get("player_key") for player in transaction_players
                if player.get("player_key")
            }
            
            if not transaction_player_keys.intersection(set(criteria["player_keys"])):
                return False
        
        return True
    
    except Exception as e:
        print(f"检查交易匹配条件失败: {e}")
        return False


def extract_transaction_summary(transactions: List[Dict]) -> Dict[str, Any]:
    """提取交易数据摘要信息
    
    Args:
        transactions: 交易列表
        
    Returns:
        交易摘要信息
    """
    try:
        summary = {
            "total_transactions": len(transactions),
            "by_type": {},
            "by_status": {},
            "date_range": {"earliest": None, "latest": None},
            "most_active_players": [],
            "total_players_involved": 0
        }
        
        if not transactions:
            return summary
        
        # 统计各种类型和状态
        all_player_keys = set()
        player_transaction_count = {}
        timestamps = []
        
        for transaction in transactions:
            # 统计类型
            transaction_type = transaction.get("type", "unknown")
            summary["by_type"][transaction_type] = summary["by_type"].get(transaction_type, 0) + 1
            
            # 统计状态
            status = transaction.get("status", "unknown")
            summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
            
            # 收集时间戳
            timestamp = transaction.get("timestamp")
            if timestamp:
                timestamps.append(timestamp)
            
            # 统计球员参与度
            players = transaction.get("players", [])
            for player in players:
                player_key = player.get("player_key")
                if player_key:
                    all_player_keys.add(player_key)
                    player_transaction_count[player_key] = player_transaction_count.get(player_key, 0) + 1
        
        # 设置总参与球员数
        summary["total_players_involved"] = len(all_player_keys)
        
        # 设置时间范围
        if timestamps:
            summary["date_range"]["earliest"] = min(timestamps)
            summary["date_range"]["latest"] = max(timestamps)
        
        # 找出最活跃的球员（参与交易最多的）
        if player_transaction_count:
            sorted_players = sorted(
                player_transaction_count.items(),
                key=lambda x: x[1],
                reverse=True
            )
            summary["most_active_players"] = sorted_players[:10]  # 前10名
        
        return summary
    
    except Exception as e:
        print(f"提取交易摘要失败: {e}")
        return {"error": str(e)} 