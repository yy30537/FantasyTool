"""
统计数据转换器
处理各种统计数据的转换逻辑
"""

from typing import Dict, Optional, List
from datetime import date


class StatsTransformers:
    """统计数据转换器"""
    
    # ============================================================================
    # 从 archive/database_writer.py 迁移的统计转换函数
    # ============================================================================
    
    def transform_core_team_weekly_stats(self, categories_won: int, win: Optional[bool] = None) -> Dict:
        """
        从matchup数据中提取核心统计项
        
        迁移自: archive/database_writer.py transform_core_team_weekly_stats() 第777行
        """
        return {
            'categories_won': categories_won,
            'is_winner': 1 if win else 0 if win is not None else None
        }
        
    def transform_team_season_stats(self, stats_data: Dict) -> Dict:
        """
        从团队赛季统计数据中提取完整统计项
        
        迁移自: archive/database_writer.py transform_team_season_stats() 第1893行
        """
        season_stats = {}
        
        try:
            # 提取排名和战绩信息
            team_standings = stats_data.get('team_standings', {})
            season_stats['rank'] = self._safe_int(team_standings.get('rank'))
            season_stats['playoff_seed'] = team_standings.get('playoff_seed')
            
            # 提取胜负记录
            outcome_totals = team_standings.get('outcome_totals', {})
            season_stats['wins'] = self._safe_int(outcome_totals.get('wins'))
            season_stats['losses'] = self._safe_int(outcome_totals.get('losses'))
            season_stats['ties'] = self._safe_int(outcome_totals.get('ties'))
            season_stats['percentage'] = self._safe_float(outcome_totals.get('percentage'))
            
            # 提取分区战绩
            divisional_outcome_totals = team_standings.get('divisional_outcome_totals', {})
            season_stats['divisional_wins'] = self._safe_int(divisional_outcome_totals.get('wins'))
            season_stats['divisional_losses'] = self._safe_int(divisional_outcome_totals.get('losses'))
            season_stats['divisional_ties'] = self._safe_int(divisional_outcome_totals.get('ties'))
            
            # 提取积分信息
            points_for = team_standings.get('points_for')
            if points_for:
                season_stats['points_for'] = self._safe_float(points_for)
            else:
                season_stats['points_for'] = None
            
            points_against = team_standings.get('points_against')
            if points_against:
                season_stats['points_against'] = self._safe_float(points_against)
            else:
                season_stats['points_against'] = None
                
        except Exception as e:
            print(f"提取团队赛季统计失败: {e}")
        
        return season_stats
        
    def transform_team_weekly_stats_from_stats_data(self, stats_data: Dict) -> Dict:
        """
        从团队周统计数据中提取完整的11个统计项
        
        迁移自: archive/database_writer.py transform_team_weekly_stats_from_stats_data() 第1930行
        """
        weekly_stats = {}
        
        try:
            # 首先构建stat_id到值的映射
            stat_map = {}
            stats_list = stats_data.get('stats', [])
            if isinstance(stats_list, list):
                for stat in stats_list:
                    if isinstance(stat, dict) and 'stat' in stat:
                        stat_info = stat['stat']
                        stat_id = stat_info.get('stat_id')
                        value = stat_info.get('value')
                        if stat_id and value is not None:
                            stat_map[str(stat_id)] = value
            
            # 提取11个核心统计项
            
            # 1. Field Goals Made / Attempted (FGM/A)
            field_goals_data = stat_map.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    weekly_stats['field_goals_made'] = self._safe_int(made.strip())
                    weekly_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    weekly_stats['field_goals_made'] = None
                    weekly_stats['field_goals_attempted'] = None
            else:
                weekly_stats['field_goals_made'] = None
                weekly_stats['field_goals_attempted'] = None
            
            # 2. Field Goal Percentage (FG%)
            fg_pct_str = stat_map.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                weekly_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                weekly_stats['field_goal_percentage'] = None
            
            # 3. Free Throws Made / Attempted (FTM/A)
            free_throws_data = stat_map.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    weekly_stats['free_throws_made'] = self._safe_int(made.strip())
                    weekly_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    weekly_stats['free_throws_made'] = None
                    weekly_stats['free_throws_attempted'] = None
            else:
                weekly_stats['free_throws_made'] = None
                weekly_stats['free_throws_attempted'] = None
            
            # 4. Free Throw Percentage (FT%)
            ft_pct_str = stat_map.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                weekly_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                weekly_stats['free_throw_percentage'] = None
            
            # 5. 3-point Shots Made (3PTM)
            weekly_stats['three_pointers_made'] = self._safe_int(stat_map.get('10'))
            
            # 6. Points Scored (PTS)
            weekly_stats['total_points'] = self._safe_int(stat_map.get('12'))
            
            # 7. Total Rebounds (REB)
            weekly_stats['total_rebounds'] = self._safe_int(stat_map.get('15'))
            
            # 8. Assists (AST)
            weekly_stats['total_assists'] = self._safe_int(stat_map.get('16'))
            
            # 9. Steals (ST)
            weekly_stats['total_steals'] = self._safe_int(stat_map.get('17'))
            
            # 10. Blocked Shots (BLK)
            weekly_stats['total_blocks'] = self._safe_int(stat_map.get('18'))
            
            # 11. Turnovers (TO)
            weekly_stats['total_turnovers'] = self._safe_int(stat_map.get('19'))
            
        except Exception as e:
            print(f"提取团队周统计失败: {e}")
        
        return weekly_stats
        
    def transform_core_player_season_stats(self, stats_data: Dict) -> Dict:
        """
        从球员赛季统计数据中提取完整的11个统计项
        
        迁移自: archive/database_writer.py transform_player_season_stats() 第487行
        """
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_data.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_data.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['total_points'] = self._safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['total_rebounds'] = self._safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['total_assists'] = self._safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['total_steals'] = self._safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['total_blocks'] = self._safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['total_turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"提取核心赛季统计失败: {e}")
        
        return core_stats
        
    def transform_core_daily_stats(self, stats_data: Dict) -> Dict:
        """
        从统计数据中提取完整的11个日期统计项
        
        迁移自: archive/database_writer.py transform_player_daily_stats() 第629行
        """
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_data.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_data.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['total_points'] = self._safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['total_rebounds'] = self._safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['total_assists'] = self._safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['total_steals'] = self._safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['total_blocks'] = self._safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['total_turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"提取核心日统计失败: {e}")
        
        return core_stats
    
    # ============================================================================
    # 从 archive/yahoo_api_data.py 迁移的统计处理函数
    # ============================================================================
    
    def transform_player_season_stats_batch(self, stats_data: Dict) -> List[Dict]:
        """
        处理球员赛季统计数据批次
        
        迁移自: archive/yahoo_api_data.py _process_player_season_stats_data() 第1975行
        返回转换后的统计数据列表
        """
        transformed_stats = []
        
        try:
            fantasy_content = stats_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 找到players容器
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            
            if not players_container:
                return transformed_stats
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                player_data = player_container.get("player", [])
                
                # 合并球员信息和统计数据
                player_info = {}
                player_stats_raw = {}
                
                for item in player_data:
                    if isinstance(item, dict):
                        if "player_stats" in item:
                            stats_info = item["player_stats"]
                            if isinstance(stats_info, dict) and "stats" in stats_info:
                                stats_list = stats_info["stats"]
                                if isinstance(stats_list, list):
                                    for stat in stats_list:
                                        if isinstance(stat, dict) and "stat" in stat:
                                            stat_detail = stat["stat"]
                                            stat_id = stat_detail.get("stat_id")
                                            value = stat_detail.get("value")
                                            if stat_id and value is not None:
                                                player_stats_raw[str(stat_id)] = value
                        else:
                            player_info.update(item)
                
                if player_info and player_stats_raw:
                    # 提取核心统计
                    core_stats = self.transform_core_player_season_stats(player_stats_raw)
                    
                    # 组合球员信息和统计
                    transformed_stat = {
                        'player_key': player_info.get('player_key'),
                        'editorial_player_key': player_info.get('editorial_player_key'),
                        'stats': core_stats
                    }
                    transformed_stats.append(transformed_stat)
                    
        except Exception as e:
            print(f"处理球员赛季统计批次失败: {e}")
        
        return transformed_stats
        
    def transform_player_daily_stats_batch(self, stats_data: Dict, stats_date: date) -> List[Dict]:
        """
        处理球员日统计数据批次
        
        迁移自: archive/yahoo_api_data.py _process_player_daily_stats_data() 第2086行
        返回转换后的统计数据列表
        """
        transformed_stats = []
        
        try:
            fantasy_content = stats_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 找到players容器
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            
            if not players_container:
                return transformed_stats
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                player_data = player_container.get("player", [])
                
                # 合并球员信息和统计数据
                player_info = {}
                player_stats_raw = {}
                
                for item in player_data:
                    if isinstance(item, dict):
                        if "player_stats" in item:
                            stats_info = item["player_stats"]
                            if isinstance(stats_info, dict) and "stats" in stats_info:
                                stats_list = stats_info["stats"]
                                if isinstance(stats_list, list):
                                    for stat in stats_list:
                                        if isinstance(stat, dict) and "stat" in stat:
                                            stat_detail = stat["stat"]
                                            stat_id = stat_detail.get("stat_id")
                                            value = stat_detail.get("value")
                                            if stat_id and value is not None:
                                                player_stats_raw[str(stat_id)] = value
                        else:
                            player_info.update(item)
                
                if player_info and player_stats_raw:
                    # 提取核心统计
                    core_stats = self.transform_core_daily_stats(player_stats_raw)
                    
                    # 组合球员信息和统计
                    transformed_stat = {
                        'player_key': player_info.get('player_key'),
                        'editorial_player_key': player_info.get('editorial_player_key'),
                        'stats_date': stats_date,
                        'stats': core_stats
                    }
                    transformed_stats.append(transformed_stat)
                    
        except Exception as e:
            print(f"处理球员日统计批次失败: {e}")
        
        return transformed_stats
        
    def transform_matchup_to_weekly_stats(self, team_key: str, week: int, opponent_team_key: str, 
                                        is_playoffs: bool, is_winner: Optional[bool], team_points: int, 
                                        matchup_data: Dict) -> Dict:
        """
        将对战记录转换为周统计数据
        
        迁移自: archive/yahoo_api_data.py _process_matchup_to_weekly_stats() 第2491行
        返回转换后的周统计数据
        """
        # 从matchup数据中提取团队统计
        team_stats = self.transform_team_stats_from_matchup(matchup_data, team_key)
        
        if not team_stats:
            return None
        
        # 计算获胜的统计类别数量
        categories_won = self.count_categories_won(matchup_data, team_key)
        
        # 构建周统计数据
        weekly_stats = {
            'team_key': team_key,
            'week': week,
            'opponent_team_key': opponent_team_key,
            'is_playoffs': is_playoffs,
            'is_winner': is_winner,
            'team_points': team_points,
            'categories_won': categories_won,
            'stats': team_stats
        }
        
        return weekly_stats
        
    def transform_standing_to_season_stats(self, standing: Dict) -> Dict:
        """
        将排名记录转换为赛季统计数据
        
        迁移自: archive/yahoo_api_data.py _process_standing_to_season_stats() 第2626行
        返回转换后的赛季统计数据
        """
        # 提取赛季统计
        season_stats = self.transform_team_season_stats({'team_standings': standing})
        
        # 构建完整的赛季统计记录
        transformed_stats = {
            'team_key': standing.get('team_key'),
            'rank': standing.get('rank'),
            'playoff_seed': standing.get('playoff_seed'),
            'wins': standing.get('wins', 0),
            'losses': standing.get('losses', 0),
            'ties': standing.get('ties', 0),
            'win_percentage': standing.get('win_percentage', 0.0),
            'games_back': standing.get('games_back', '-'),
            'divisional_wins': standing.get('divisional_wins', 0),
            'divisional_losses': standing.get('divisional_losses', 0),
            'divisional_ties': standing.get('divisional_ties', 0),
            'stats': season_stats
        }
        
        return transformed_stats
        
    def count_categories_won(self, matchup_data: Dict, team_key: str) -> int:
        """
        计算团队在对战中获胜的统计类别数量
        
        迁移自: archive/yahoo_api_data.py _count_categories_won() 第2563行
        """
        categories_won = 0
        
        try:
            # 从matchup数据中提取stat_winners信息
            stat_winners = matchup_data.get('stat_winners', [])
            
            if isinstance(stat_winners, list):
                for winner in stat_winners:
                    if isinstance(winner, dict):
                        stat_winner = winner.get('stat_winner', {})
                        winner_team_key = stat_winner.get('winner_team_key')
                        if winner_team_key == team_key:
                            categories_won += 1
                            
        except Exception as e:
            print(f"计算获胜类别数量失败: {e}")
        
        return categories_won
        
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
                        if team_stats:
                            return self.transform_team_weekly_stats_from_stats_data(team_stats)
                            
        except Exception as e:
            print(f"从对战数据提取团队统计失败: {e}")
        
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
    
    def _parse_percentage(self, pct_str) -> Optional[float]:
        """解析百分比字符串，返回百分比值（0-100）"""
        if not pct_str:
            return None
        
        try:
            # 移除百分号（如果有）
            pct_str = str(pct_str).strip().rstrip('%')
            
            # 转换为浮点数
            value = float(pct_str)
            
            # 如果值小于1，假设是小数形式（如0.500），转换为百分比
            if value <= 1.0:
                value = value * 100
            
            # 四舍五入到一位小数
            return round(value, 1)
            
        except (ValueError, TypeError):
            return None

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def transform_stat_categories(raw_data: List[Dict]) -> List[Dict]:
    """转换统计类别"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_scoring_settings(raw_data: Dict) -> Dict:
    """转换计分设置"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_weekly_stats(raw_data: Dict) -> Dict:
    """转换周统计"""
    transformer = StatsTransformers()
    return transformer.transform_team_weekly_stats_from_stats_data(raw_data)

def transform_season_stats(raw_data: Dict) -> Dict:
    """转换赛季统计"""
    transformer = StatsTransformers()
    return transformer.transform_team_season_stats(raw_data)

def transform_projected_stats(raw_data: Dict) -> Dict:
    """转换预测统计"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_matchup_grades(raw_data: Dict) -> Dict:
    """转换对战评分"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_trade_data(raw_data: Dict) -> Dict:
    """转换交易数据"""
    # 这应该在core transformers中
    from .core import CoreTransformers
    transformer = CoreTransformers()
    return transformer.transform_transaction_data(raw_data)

def transform_schedule_data(raw_data: List[Dict]) -> List[Dict]:
    """转换赛程数据"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_game_weeks(raw_data: List[Dict]) -> List[Dict]:
    """转换游戏周数据"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_metadata(raw_data: Dict) -> Dict:
    """转换元数据"""
    transformer = StatsTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data