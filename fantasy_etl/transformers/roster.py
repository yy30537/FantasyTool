"""
Roster数据转换器
处理球员名单相关的数据转换
"""

from typing import Optional, List, Dict
from .core import CoreTransformers


class RosterTransformers:
    """Roster数据转换器"""
    
    def __init__(self):
        """初始化转换器"""
        self.core_transformer = CoreTransformers()
    
    def transform_roster_data(self, roster_data: Dict, team_key: str) -> Optional[List[Dict]]:
        """
        从原始roster数据转换为标准化格式 (纯转换，不写入数据库)
        
        迁移自: archive/yahoo_api_data.py transform_roster_data() 第518行
        
        Args:
            roster_data: 原始Yahoo API roster响应数据
            team_key: 团队标识符
            
        Returns:
            转换后的roster数据列表，或None如果转换失败
        """
        if not roster_data:
            return None
        
        try:
            # 解析roster数据
            fantasy_content = roster_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team", [])
            
            # 查找roster信息
            roster_info = None
            for item in team_data:
                if isinstance(item, dict) and "roster" in item:
                    roster_info = item["roster"]
                    break
            
            if not roster_info:
                return None
            
            # 提取coverage信息（日期）
            coverage_type = roster_info.get("coverage_type", "")
            coverage_value = roster_info.get("coverage_value", "")
            
            # 提取球员列表
            players_container = roster_info.get("0", {}).get("players", {})
            players_count = int(players_container.get("count", 0))
            
            roster_list = []
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                player_data = player_container.get("player", [])
                
                # 合并球员信息
                player_info = {}
                selected_position_info = {}
                
                for item in player_data:
                    if isinstance(item, dict):
                        # 检查是否是selected_position信息
                        if "selected_position" in item:
                            selected_position_info = item["selected_position"][0] if isinstance(item["selected_position"], list) else item["selected_position"]
                        else:
                            player_info.update(item)
                
                if not player_info:
                    continue
                
                # 提取基本信息
                player_key = player_info.get("player_key")
                player_id = player_info.get("player_id")
                
                # 提取姓名
                name_info = player_info.get("name", {})
                if isinstance(name_info, dict):
                    full_name = name_info.get("full", "")
                else:
                    full_name = player_info.get("full_name", "")
                
                # 提取位置信息
                eligible_positions = player_info.get("eligible_positions", [])
                if isinstance(eligible_positions, list):
                    position_list = eligible_positions
                else:
                    # 尝试从其他格式提取位置
                    position_list = []
                    if isinstance(eligible_positions, dict) and "position" in eligible_positions:
                        positions = eligible_positions["position"]
                        if isinstance(positions, list):
                            position_list = positions
                        elif isinstance(positions, str):
                            position_list = [positions]
                
                # 提取选定位置
                selected_position = selected_position_info.get("position", "")
                if not selected_position and position_list:
                    selected_position = self.core_transformer.transform_position_string(position_list[0])
                
                # 提取其他信息
                is_flex = selected_position_info.get("is_flex", 0)
                has_recent_player_notes = player_info.get("has_recent_player_notes", 0)
                status = player_info.get("status", "")
                status_full = player_info.get("status_full", "")
                
                # 构建roster entry
                roster_entry = {
                    "team_key": team_key,
                    "player_key": player_key,
                    "player_id": player_id,
                    "full_name": full_name,
                    "selected_position": selected_position,
                    "eligible_positions": position_list,
                    "is_flex": is_flex,
                    "has_recent_player_notes": has_recent_player_notes,
                    "status": status,
                    "status_full": status_full,
                    "coverage_type": coverage_type,
                    "coverage_value": coverage_value
                }
                
                # 检查是否是keeper
                is_keeper = player_info.get("is_keeper")
                if is_keeper:
                    roster_entry["is_keeper"] = 1
                    roster_entry["keeper_cost"] = player_info.get("keeper_cost")
                else:
                    roster_entry["is_keeper"] = 0
                    roster_entry["keeper_cost"] = None
                
                # 添加是否可编辑标志
                is_editable = player_info.get("is_editable", 1)
                roster_entry["is_editable"] = is_editable
                
                roster_list.append(roster_entry)
            
            return roster_list
            
        except Exception as e:
            print(f"转换roster数据时出错: {e}")
            return None

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def transform_roster_data(raw_data: List[Dict]) -> List[Dict]:
    """转换阵容数据"""
    transformer = RosterTransformers()
    # 假设raw_data是单个团队的阵容数据
    return transformer.transform_roster_data({}, "")

def transform_roster_positions(raw_data: List[Dict]) -> List[Dict]:
    """转换阵容位置数据"""
    transformer = RosterTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_lineup_data(raw_data: Dict) -> Dict:
    """转换阵容数据"""
    transformer = RosterTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data