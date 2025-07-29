"""
Player数据转换器
处理球员相关的数据转换
"""

from typing import Dict, List
from datetime import datetime


class PlayerTransformers:
    """Player数据转换器"""
    
    def transform_player_info_from_league_data(self, players_data: Dict) -> List[Dict]:
        """
        从联盟球员数据中提取球员信息
        
        迁移自: archive/yahoo_api_data.py _extract_player_info_from_league_data() 第798行
        """
        players = []
        
        try:
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 找到players容器
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            
            if not players_container:
                return players
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                if "player" not in player_container:
                    continue
                
                player_info_list = player_container["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) == 0:
                    continue
                
                # 合并player信息
                player_info = {}
                for item in player_info_list:
                    if isinstance(item, dict):
                        player_info.update(item)
                
                if player_info:
                    players.append(player_info)
                    
        except Exception as e:
            print(f"提取球员信息时出错: {e}")
        
        return players
        
    def transform_player_info(self, player_info: Dict, league_key: str, season: str) -> Dict:
        """
        标准化球员信息
        
        迁移自: archive/yahoo_api_data.py _normalize_player_info() 第832行
        """
        normalized = {}
        
        # 基本信息
        normalized['player_key'] = player_info.get('player_key')
        normalized['player_id'] = player_info.get('player_id')
        normalized['editorial_player_key'] = player_info.get('editorial_player_key')
        normalized['editorial_team_key'] = player_info.get('editorial_team_key')
        normalized['editorial_team_full_name'] = player_info.get('editorial_team_full_name')
        normalized['editorial_team_abbr'] = player_info.get('editorial_team_abbr')
        
        # 姓名处理
        name_info = player_info.get('name', {})
        if isinstance(name_info, dict):
            normalized['first_name'] = name_info.get('first', '')
            normalized['last_name'] = name_info.get('last', '')
            normalized['full_name'] = name_info.get('full', '')
        else:
            # 如果name不是字典，尝试从其他字段获取
            normalized['first_name'] = player_info.get('first_name', '')
            normalized['last_name'] = player_info.get('last_name', '')
            normalized['full_name'] = player_info.get('full_name', '')
        
        # 其他信息
        normalized['uniform_number'] = player_info.get('uniform_number')
        normalized['display_position'] = player_info.get('display_position')
        normalized['position_type'] = player_info.get('position_type')
        
        # 头像信息
        headshot_info = player_info.get('headshot', {})
        if isinstance(headshot_info, dict):
            normalized['headshot_url'] = headshot_info.get('url')
        else:
            normalized['headshot_url'] = player_info.get('image_url')
        
        # 状态信息
        normalized['is_undroppable'] = player_info.get('is_undroppable', 0)
        normalized['has_player_notes'] = player_info.get('has_player_notes', 0)
        normalized['has_recent_player_notes'] = player_info.get('has_recent_player_notes', 0)
        
        # 添加联盟和赛季信息
        normalized['league_key'] = league_key
        normalized['season'] = season
        
        # 添加时间戳
        normalized['created_at'] = datetime.now()
        normalized['updated_at'] = datetime.now()
        
        return normalized