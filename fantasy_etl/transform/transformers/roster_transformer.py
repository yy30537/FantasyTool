"""
Roster Transformer - 阵容数据转换器

负责将Yahoo Fantasy API返回的阵容原始数据转换为标准化格式
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .base_transformer import BaseTransformer, TransformResult

logger = logging.getLogger(__name__)


class RosterTransformer(BaseTransformer):
    """阵容数据转换器"""
    
    def transform(self, raw_data: Dict) -> TransformResult:
        """转换单个阵容数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据结构
            if not self._validate_roster_structure(raw_data, result):
                return result
            
            # 提取基本阵容信息
            roster_info = self._extract_roster_info(raw_data, result)
            if not roster_info:
                result.add_error("roster_info", raw_data, "无法提取阵容基本信息")
                return result
            
            # 提取球员列表
            players_list = self._extract_players_list(raw_data, result)
            if not players_list:
                result.add_error("players_list", raw_data, "无法提取球员列表")
                return result
            
            # 转换球员数据
            transformed_players = []
            for player_data in players_list:
                transformed_player = self._transform_player_roster_data(player_data, roster_info, result)
                if transformed_player:
                    transformed_players.append(transformed_player)
            
            if not transformed_players:
                result.add_error("players", players_list, "没有有效的球员数据")
                return result
            
            # 构建最终结果
            result.data = {
                "roster_info": roster_info,
                "players": transformed_players,
                "total_players": len(transformed_players)
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", raw_data, f"转换异常: {str(e)}")
            
        return result
    
    def _validate_roster_structure(self, raw_data: Dict, result: TransformResult) -> bool:
        """验证阵容数据结构"""
        required_keys = ["fantasy_content"]
        errors = self.validate_required_fields(raw_data, required_keys)
        
        if errors:
            result.errors.extend(errors)
            return False
        
        fantasy_content = raw_data["fantasy_content"]
        if not isinstance(fantasy_content, dict) or "team" not in fantasy_content:
            result.add_error("fantasy_content.team", fantasy_content, "缺少team数据")
            return False
        
        return True
    
    def _extract_roster_info(self, raw_data: Dict, result: TransformResult) -> Optional[Dict]:
        """提取阵容基本信息"""
        try:
            fantasy_content = raw_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 在team_data中查找roster信息
            roster_info = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "roster" in item:
                        roster_info = item["roster"]
                        break
            elif isinstance(team_data, dict) and "roster" in team_data:
                roster_info = team_data["roster"]
            
            if not roster_info:
                return None
            
            # 提取和标准化信息
            coverage_date = self.clean_string(roster_info.get("date"))
            is_prescoring = self.safe_bool(roster_info.get("is_prescoring", False))
            is_editable = self.safe_bool(roster_info.get("is_editable", False))
            
            # 验证日期格式
            parsed_date = self.parse_date(coverage_date)
            if not parsed_date:
                result.add_warning("coverage_date", coverage_date, f"日期格式可能有问题: {coverage_date}")
            
            return {
                "coverage_date": coverage_date,
                "parsed_date": parsed_date,
                "is_prescoring": is_prescoring,
                "is_editable": is_editable
            }
            
        except Exception as e:
            result.add_error("roster_info", raw_data, f"提取阵容信息失败: {str(e)}")
            return None
    
    def _extract_players_list(self, raw_data: Dict, result: TransformResult) -> Optional[List[Dict]]:
        """提取球员列表数据"""
        try:
            fantasy_content = raw_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 查找roster中的players容器
            players_container = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "roster" in item:
                        roster_info = item["roster"]
                        if "0" in roster_info and "players" in roster_info["0"]:
                            players_container = roster_info["0"]["players"]
                        break
            
            if not players_container:
                return None
            
            players_count = self.safe_int(players_container.get("count", 0), 0)
            if players_count == 0:
                result.add_warning("players_count", players_count, "球员数量为0")
                return []
            
            players_list = []
            for i in range(players_count):
                str_index = str(i)
                if str_index in players_container:
                    player_data = players_container[str_index]
                    if "player" in player_data:
                        players_list.append(player_data["player"])
            
            return players_list
            
        except Exception as e:
            result.add_error("players_list", raw_data, f"提取球员列表失败: {str(e)}")
            return None
    
    def _transform_player_roster_data(self, player_data: Any, roster_info: Dict, result: TransformResult) -> Optional[Dict]:
        """转换单个球员的阵容数据"""
        try:
            if not isinstance(player_data, list) or len(player_data) == 0:
                return None
            
            # 提取球员基本信息和位置信息
            player_info = player_data[0]
            position_data = player_data[1] if len(player_data) > 1 else {}
            
            # 合并球员信息
            player_dict = {}
            
            # 处理球员基本信息
            if isinstance(player_info, list):
                for item in player_info:
                    if isinstance(item, dict):
                        player_dict.update(item)
            elif isinstance(player_info, dict):
                player_dict.update(player_info)
            
            # 处理位置信息
            if isinstance(position_data, dict):
                player_dict.update(position_data)
            
            # 提取和验证关键字段
            player_key = self.clean_string(player_dict.get("player_key"))
            if not player_key:
                result.add_warning("player_key", player_dict, "球员缺少player_key")
                return None
            
            # 构建标准化的阵容条目
            roster_entry = {
                # 基本标识信息
                "player_key": player_key,
                "coverage_date": roster_info["coverage_date"],
                "parsed_date": roster_info["parsed_date"],
                
                # 阵容状态信息
                "is_prescoring": roster_info["is_prescoring"],
                "is_editable": roster_info["is_editable"],
                
                # 球员状态信息
                "status": self.clean_string(player_dict.get("status")),
                "status_full": self.clean_string(player_dict.get("status_full")),
                "injury_note": self.clean_string(player_dict.get("injury_note")),
                
                # 位置信息
                "selected_position": self.extract_position_string(player_dict.get("selected_position")),
                
                # Keeper信息
                "is_keeper": False,
                "keeper_cost": None,
                "kept": False
            }
            
            # 处理keeper信息
            self._process_keeper_info(player_dict, roster_entry, result)
            
            # 推导位置状态
            self._derive_position_status(roster_entry)
            
            return roster_entry
            
        except Exception as e:
            result.add_error("player_transform", player_data, f"转换球员数据失败: {str(e)}")
            return None
    
    def _process_keeper_info(self, player_dict: Dict, roster_entry: Dict, result: TransformResult):
        """处理keeper信息"""
        try:
            if "is_keeper" in player_dict:
                keeper_info = player_dict["is_keeper"]
                if isinstance(keeper_info, dict):
                    roster_entry["is_keeper"] = self.safe_bool(keeper_info.get("status", False))
                    
                    cost = keeper_info.get("cost")
                    if cost is not None and str(cost).strip():
                        roster_entry["keeper_cost"] = str(cost)
                    
                    roster_entry["kept"] = self.safe_bool(keeper_info.get("kept", False))
                elif isinstance(keeper_info, bool):
                    roster_entry["is_keeper"] = keeper_info
                else:
                    # 尝试转换为布尔值
                    roster_entry["is_keeper"] = self.safe_bool(keeper_info)
                    
        except Exception as e:
            result.add_warning("keeper_info", player_dict.get("is_keeper"), f"处理keeper信息失败: {str(e)}")
    
    def _derive_position_status(self, roster_entry: Dict):
        """根据selected_position推导位置状态"""
        selected_position = roster_entry.get("selected_position")
        
        # 默认状态
        roster_entry["is_starting"] = False
        roster_entry["is_bench"] = False
        roster_entry["is_injured_reserve"] = False
        
        if selected_position:
            if selected_position == 'BN':
                roster_entry["is_bench"] = True
            elif selected_position in ['IL', 'IR']:
                roster_entry["is_injured_reserve"] = True
            else:
                # 其他位置都视为首发
                roster_entry["is_starting"] = True 