"""
Team数据转换器
处理团队相关的数据转换
"""

from typing import Optional, Dict, List


class TeamTransformers:
    """Team数据转换器"""
    
    # ============================================================================
    # 团队基础数据转换
    # ============================================================================
    
    def transform_team_keys_from_data(self, teams_data: Dict) -> List[str]:
        """
        从团队数据中转换提取团队键
        
        迁移自: archive/yahoo_api_data.py transform_team_keys_from_data() 第667行
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
        
    def transform_team_data_from_api(self, team_data: List) -> Optional[Dict]:
        """
        从API团队数据中提取团队信息
        
        迁移自: archive/yahoo_api_data.py transform_team_data_from_api() 第413行
        """
        if not team_data or not isinstance(team_data, list):
            return None
        
        team_info = {}
        managers = []
        
        try:
            # 第一个元素通常包含团队基本信息
            if len(team_data) > 0 and isinstance(team_data[0], list):
                for item in team_data[0]:
                    if isinstance(item, dict):
                        # 提取团队基本信息
                        for key in ['team_key', 'team_id', 'name', 'url', 'team_logo', 
                                   'waiver_priority', 'number_of_moves', 'number_of_trades',
                                   'clinched_playoffs', 'draft_grade', 'draft_recap_url']:
                            if key in item:
                                team_info[key] = item[key]
                        
                        # 处理team_logos
                        if 'team_logos' in item and isinstance(item['team_logos'], list):
                            for logo_item in item['team_logos']:
                                if isinstance(logo_item, dict) and 'team_logo' in logo_item:
                                    logo_info = logo_item['team_logo']
                                    if isinstance(logo_info, dict) and 'url' in logo_info:
                                        team_info['team_logo_url'] = logo_info['url']
                                        break
                        
                        # 处理roster_adds
                        if 'roster_adds' in item:
                            roster_adds = item['roster_adds']
                            if isinstance(roster_adds, dict):
                                team_info['roster_adds_coverage_type'] = roster_adds.get('coverage_type')
                                team_info['roster_adds_coverage_value'] = roster_adds.get('coverage_value')
                                team_info['roster_adds_value'] = roster_adds.get('value')
                        
                        # 处理managers
                        if 'managers' in item and isinstance(item['managers'], list):
                            for manager_item in item['managers']:
                                if isinstance(manager_item, dict) and 'manager' in manager_item:
                                    manager_info = manager_item['manager']
                                    if isinstance(manager_info, dict):
                                        managers.append({
                                            'manager_id': manager_info.get('manager_id'),
                                            'nickname': manager_info.get('nickname'),
                                            'guid': manager_info.get('guid'),
                                            'email': manager_info.get('email'),
                                            'image_url': manager_info.get('image_url')
                                        })
        
        except Exception as e:
            print(f"提取团队数据时出错: {e}")
            return None
        
        # 添加管理员信息到团队数据
        if managers:
            team_info['managers'] = managers
        
        return team_info
    
    # ============================================================================
    # 团队排名和统计转换
    # ============================================================================
    
    def transform_team_standings_info(self, team_data) -> Optional[Dict]:
        """
        从团队数据中提取排名信息
        
        迁移自: archive/yahoo_api_data.py transform_team_standings_info() 第1100行
        """
        if not team_data:
            return None
        
        try:
            # 检查是否是列表格式
            if isinstance(team_data, list):
                # 递归搜索team_standings
                for item in team_data:
                    if isinstance(item, dict):
                        if 'team_standings' in item:
                            return item['team_standings']
                        # 递归搜索
                        result = self.transform_team_standings_info(item)
                        if result:
                            return result
                    elif isinstance(item, list):
                        # 递归搜索列表
                        result = self.transform_team_standings_info(item)
                        if result:
                            return result
            
            # 检查是否是字典格式
            elif isinstance(team_data, dict):
                if 'team_standings' in team_data:
                    return team_data['team_standings']
                # 递归搜索字典的值
                for key, value in team_data.items():
                    if isinstance(value, (dict, list)):
                        result = self.transform_team_standings_info(value)
                        if result:
                            return result
        
        except Exception as e:
            print(f"提取团队排名信息时出错: {e}")
        
        return None
        
    def transform_team_stats_from_matchup_data(self, teams_data: Dict, target_team_key: str) -> Optional[Dict]:
        """
        从对战数据中提取目标团队的统计数据
        
        迁移自: archive/yahoo_api_data.py transform_team_stats_from_matchup_data() 第1672行
        """
        try:
            # teams_data 应该包含 '0' 和 '1' 两个团队
            for team_index in ['0', '1']:
                if team_index not in teams_data:
                    continue
                
                team_container = teams_data[team_index]
                if 'team' not in team_container:
                    continue
                
                team_info = team_container['team']
                
                # 查找team_key
                team_key = None
                team_stats = None
                
                if isinstance(team_info, list):
                    for item in team_info:
                        if isinstance(item, list):
                            # 处理嵌套列表格式
                            for sub_item in item:
                                if isinstance(sub_item, dict) and 'team_key' in sub_item:
                                    team_key = sub_item['team_key']
                        elif isinstance(item, dict):
                            if 'team_stats' in item:
                                team_stats = item['team_stats']
                
                # 如果找到目标团队，返回其统计数据
                if team_key == target_team_key and team_stats:
                    return team_stats
        
        except Exception as e:
            print(f"从对战数据提取团队统计失败: {e}")
        
        return None
        
    def transform_team_stats_from_matchup(self, matchup_data: Dict, target_team_key: str) -> Optional[Dict]:
        """
        从对战数据中提取团队统计
        
        迁移自: archive/yahoo_api_data.py transform_team_stats_from_matchup() 第2514行
        """
        try:
            # 查找teams数据
            teams = matchup_data.get('teams', [])
            
            for team in teams:
                if isinstance(team, dict):
                    team_data = team.get('team', {})
                    team_key = team_data.get('team_key')
                    
                    if team_key == target_team_key:
                        # 找到目标团队，提取统计数据
                        team_stats = team_data.get('team_stats', {})
                        return team_stats
                            
        except Exception as e:
            print(f"从对战数据提取团队统计失败: {e}")
        
        return None
    
    # ============================================================================
    # 对战数据转换
    # ============================================================================
    
    def transform_team_matchups(self, matchups_data: Dict, team_key: str) -> Optional[List[Dict]]:
        """
        从原始matchups数据转换为标准化格式 (纯转换，不写入数据库)
        
        迁移自: archive/yahoo_api_data.py transform_team_matchups() 第1556行
        """
        if not matchups_data:
            return None
        
        try:
            fantasy_content = matchups_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team", [])
            
            # 查找matchups容器
            matchups_container = None
            for item in team_data:
                if isinstance(item, dict) and "matchups" in item:
                    matchups_container = item["matchups"]
                    break
            
            if not matchups_container:
                return None
            
            matchups_count = int(matchups_container.get("count", 0))
            
            transformed_matchups = []
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_container = matchups_container[str_index]
                if "matchup" not in matchup_container:
                    continue
                
                matchup_info = matchup_container["matchup"]
                
                # 转换matchup数据
                transformed = {
                    "team_key": team_key,
                    "matchup_info": matchup_info
                }
                
                transformed_matchups.append(transformed)
            
            return transformed_matchups
            
        except Exception as e:
            print(f"转换matchups数据时出错: {e}")
            return None
        
    def transform_matchup_info(self, matchup_info, team_key: str) -> Optional[Dict]:
        """
        从对战数据中提取对战信息
        
        迁移自: archive/yahoo_api_data.py transform_matchup_info() 第1710行
        """
        if not matchup_info:
            return None
        
        try:
            # 提取基本信息
            week = matchup_info.get('week')
            week_start = matchup_info.get('week_start')
            week_end = matchup_info.get('week_end')
            status = matchup_info.get('status')
            is_playoffs = matchup_info.get('is_playoffs')
            is_consolation = matchup_info.get('is_consolation')
            
            # 处理布尔值
            if isinstance(is_playoffs, str):
                is_playoffs = is_playoffs == '1'
            if isinstance(is_consolation, str):
                is_consolation = is_consolation == '1'
            
            # 提取胜负信息
            is_tied = matchup_info.get('is_tied', 0)
            if isinstance(is_tied, str):
                is_tied = is_tied == '1'
            
            winner_team_key = matchup_info.get('winner_team_key')
            
            # 判断是否获胜
            is_winner = None
            if winner_team_key:
                is_winner = winner_team_key == team_key
            elif is_tied:
                is_winner = None  # 平局
            
            # 提取teams数据中的详细信息
            teams_data = matchup_info.get('teams', {})
            team_points = None
            opponent_team_key = None
            opponent_points = None
            
            # 处理teams数据
            if teams_data:
                for team_idx in ['0', '1']:
                    if team_idx in teams_data:
                        team_container = teams_data[team_idx]
                        if 'team' in team_container:
                            team_info = team_container['team']
                            current_team_key = None
                            current_points = None
                            
                            # 提取team_key和points
                            if isinstance(team_info, list):
                                for item in team_info:
                                    if isinstance(item, list):
                                        for sub_item in item:
                                            if isinstance(sub_item, dict):
                                                if 'team_key' in sub_item:
                                                    current_team_key = sub_item['team_key']
                                                if 'team_points' in sub_item:
                                                    team_points_data = sub_item['team_points']
                                                    if isinstance(team_points_data, dict):
                                                        current_points = team_points_data.get('total')
                            
                            # 判断是当前团队还是对手
                            if current_team_key == team_key:
                                team_points = current_points
                            else:
                                opponent_team_key = current_team_key
                                opponent_points = current_points
            
            # 构建转换后的数据
            transformed_info = {
                'week': self._safe_int(week),
                'week_start': week_start,
                'week_end': week_end,
                'status': status,
                'is_playoffs': is_playoffs,
                'is_consolation': is_consolation,
                'is_tied': is_tied,
                'is_winner': is_winner,
                'winner_team_key': winner_team_key,
                'team_points': self._safe_float(team_points),
                'opponent_team_key': opponent_team_key,
                'opponent_points': self._safe_float(opponent_points)
            }
            
            return transformed_info
            
        except Exception as e:
            print(f"提取对战信息时出错: {e}")
            return None
        
    def transform_team_matchup_details(self, teams_data, target_team_key: str) -> Optional[Dict]:
        """
        从teams数据中提取当前团队的对战详情
        
        迁移自: archive/yahoo_api_data.py transform_team_matchup_details() 第1813行
        """
        if not teams_data:
            return None
        
        try:
            team_info = None
            opponent_info = None
            
            # 遍历teams数据（通常包含 '0' 和 '1' 两个团队）
            for team_idx in ['0', '1']:
                if team_idx not in teams_data:
                    continue
                
                team_container = teams_data[team_idx]
                if 'team' not in team_container:
                    continue
                
                team_data = team_container['team']
                
                # 提取团队信息
                current_team_key = None
                current_team_points = None
                current_team_name = None
                
                if isinstance(team_data, list):
                    for item in team_data:
                        if isinstance(item, list):
                            for sub_item in item:
                                if isinstance(sub_item, dict):
                                    if 'team_key' in sub_item:
                                        current_team_key = sub_item['team_key']
                                    if 'name' in sub_item:
                                        current_team_name = sub_item['name']
                                    if 'team_points' in sub_item:
                                        points_data = sub_item['team_points']
                                        if isinstance(points_data, dict):
                                            current_team_points = points_data.get('total')
                
                # 判断是目标团队还是对手
                if current_team_key == target_team_key:
                    team_info = {
                        'team_key': current_team_key,
                        'team_name': current_team_name,
                        'team_points': self._safe_float(current_team_points)
                    }
                else:
                    opponent_info = {
                        'opponent_team_key': current_team_key,
                        'opponent_team_name': current_team_name,
                        'opponent_points': self._safe_float(current_team_points)
                    }
            
            # 如果找到了团队信息，合并数据
            if team_info:
                result = team_info.copy()
                if opponent_info:
                    result.update(opponent_info)
                    
                    # 基于积分判断胜负
                    if result['team_points'] is not None and result['opponent_points'] is not None:
                        if result['team_points'] > result['opponent_points']:
                            result['is_winner'] = True
                        elif result['team_points'] < result['opponent_points']:
                            result['is_winner'] = False
                        else:
                            result['is_winner'] = None  # 平局
                
                return result
                
        except Exception as e:
            print(f"提取对战详情时出错: {e}")
        
        return None
        
    def transform_team_weekly_stats(self, matchup_info: Dict, team_key: str) -> Optional[Dict]:
        """
        从matchup数据中转换团队周统计数据 (纯转换，不写入数据库)
        
        迁移自: archive/yahoo_api_data.py transform_team_weekly_stats() 第1629行
        """
        if not matchup_info:
            return None
        
        try:
            # 从matchup中的teams数据提取统计数据
            teams_data = matchup_info.get('teams', {})
            
            if teams_data:
                # 调用现有方法提取统计数据
                team_stats = self.transform_team_stats_from_matchup_data(teams_data, team_key)
                
                if team_stats:
                    # 提取其他信息
                    week = matchup_info.get('week')
                    is_playoffs = matchup_info.get('is_playoffs', False)
                    
                    # 构建转换后的数据
                    transformed_stats = {
                        'team_key': team_key,
                        'week': self._safe_int(week),
                        'is_playoffs': is_playoffs,
                        'stats': team_stats
                    }
                    
                    return transformed_stats
                    
        except Exception as e:
            print(f"转换团队周统计时出错: {e}")
        
        return None
    
    # ============================================================================
    # 辅助函数
    # ============================================================================
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def transform_team_data(raw_data: Dict) -> Dict:
    """转换团队数据"""
    transformer = TeamTransformers()
    return transformer.transform_team_data_from_api(raw_data)

def transform_team_stats(raw_data: Dict) -> Dict:
    """转换团队统计"""
    transformer = TeamTransformers()
    return transformer.transform_team_stats_from_matchup_data(raw_data, "")

def transform_matchup_data(raw_data: Dict) -> Dict:
    """转换对战数据"""
    transformer = TeamTransformers()
    return transformer.transform_matchup_info(raw_data, "")

def transform_manager_data(raw_data: Dict) -> Dict:
    """转换管理员数据"""
    transformer = TeamTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data