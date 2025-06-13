"""
Team Transformer - 团队数据转换器

负责将Yahoo Fantasy API返回的团队相关原始数据转换为标准化格式
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .base_transformer import BaseTransformer, TransformResult

logger = logging.getLogger(__name__)


class TeamTransformer(BaseTransformer):
    """团队数据转换器"""
    
    def transform(self, raw_data: Dict) -> TransformResult:
        """转换团队数据（通用方法）"""
        result = TransformResult(success=False)
        result.add_error("transform", raw_data, "请使用具体的转换方法")
        return result
    
    def transform_team_weekly_stats_from_matchup(self, matchup_info: Dict, team_key: str) -> TransformResult:
        """
        从matchup数据中转换团队周统计数据
        
        Args:
            matchup_info: Yahoo API返回的matchup信息
            team_key: 目标团队键
            
        Returns:
            TransformResult: 转换结果，包含标准化的团队周统计数据
        """
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not self._validate_matchup_structure(matchup_info, result):
                return result
            
            # 提取基本信息
            week = matchup_info.get("week")
            if week is None:
                result.add_error("week", matchup_info, "缺少week信息")
                return result
            
            # 提取团队统计数据
            team_stats_data = self._extract_team_stats_from_matchup(matchup_info, team_key, result)
            if not team_stats_data:
                result.add_error("team_stats", matchup_info, f"无法提取团队 {team_key} 的统计数据")
                return result
            
            # 转换统计数据
            transformed_stats = self._transform_team_stats_data(team_stats_data, result)
            if not transformed_stats:
                result.add_error("stats_transform", team_stats_data, "统计数据转换失败")
                return result
            
            # 构建最终结果
            result.data = {
                "team_key": team_key,
                "week": week,
                "stats": transformed_stats,
                "raw_stats": team_stats_data  # 保留原始数据用于调试
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", matchup_info, f"转换异常: {str(e)}")
            
        return result
    
    def _validate_matchup_structure(self, matchup_info: Dict, result: TransformResult) -> bool:
        """验证matchup数据结构"""
        if not isinstance(matchup_info, dict):
            result.add_error("matchup_info", matchup_info, "matchup_info必须是字典")
            return False
        
        # 检查是否包含teams数据
        teams_data = matchup_info.get("0", {}).get("teams", {})
        if not teams_data:
            result.add_error("teams_data", matchup_info, "缺少teams数据")
            return False
        
        return True
    
    def _extract_team_stats_from_matchup(self, matchup_info: Dict, target_team_key: str, result: TransformResult) -> Optional[Dict]:
        """从matchup数据中提取目标团队的统计数据"""
        try:
            teams_data = matchup_info.get("0", {}).get("teams", {})
            teams_count = self.safe_int(teams_data.get("count", 0), 0)
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if "team" not in team_container:
                    continue
                
                team_info = team_container["team"]
                
                # 提取team_key
                current_team_key = self._extract_team_key_from_info(team_info)
                
                # 如果找到目标团队，提取统计数据
                if current_team_key == target_team_key:
                    return self._extract_team_stats_container(team_info)
            
            return None
            
        except Exception as e:
            result.add_error("extract_stats", matchup_info, f"提取统计数据失败: {str(e)}")
            return None
    
    def _extract_team_key_from_info(self, team_info: Any) -> Optional[str]:
        """从team info中提取team_key"""
        try:
            if isinstance(team_info, list) and len(team_info) >= 1:
                team_basic_info = team_info[0]
                if isinstance(team_basic_info, list):
                    for info_item in team_basic_info:
                        if isinstance(info_item, dict) and "team_key" in info_item:
                            return self.clean_string(info_item["team_key"])
                elif isinstance(team_basic_info, dict) and "team_key" in team_basic_info:
                    return self.clean_string(team_basic_info["team_key"])
            
            return None
            
        except Exception:
            return None
    
    def _extract_team_stats_container(self, team_info: Any) -> Optional[Dict]:
        """从team info中提取统计数据容器"""
        try:
            if isinstance(team_info, list) and len(team_info) > 1:
                team_stats_container = team_info[1]
                if isinstance(team_stats_container, dict) and "team_stats" in team_stats_container:
                    return team_stats_container["team_stats"]
            
            return None
            
        except Exception:
            return None
    
    def _transform_team_stats_data(self, team_stats_data: Dict, result: TransformResult) -> Optional[Dict]:
        """转换团队统计数据"""
        try:
            if not isinstance(team_stats_data, dict):
                return None
            
            transformed_stats = {}
            
            # 处理stats列表
            if "stats" in team_stats_data:
                stats_list = team_stats_data["stats"]
                if isinstance(stats_list, list):
                    # 转换为字典格式，便于后续处理
                    stats_dict = {}
                    for stat_item in stats_list:
                        if isinstance(stat_item, dict) and "stat" in stat_item:
                            stat_info = stat_item["stat"]
                            stat_id = stat_info.get("stat_id")
                            value = stat_info.get("value")
                            
                            if stat_id is not None:
                                stats_dict[str(stat_id)] = value
                    
                    transformed_stats["stats"] = stats_dict
                    transformed_stats["stats_count"] = len(stats_dict)
            
            # 处理coverage信息
            if "coverage_type" in team_stats_data:
                transformed_stats["coverage_type"] = self.clean_string(team_stats_data["coverage_type"])
            
            if "coverage_value" in team_stats_data:
                transformed_stats["coverage_value"] = self.safe_int(team_stats_data["coverage_value"])
            
            # 处理team_points信息
            if "team_points" in team_stats_data:
                team_points = team_stats_data["team_points"]
                if isinstance(team_points, dict):
                    # 提取总分
                    total_points = team_points.get("total")
                    if total_points is not None:
                        transformed_stats["team_points_total"] = self.safe_float(total_points, 0.0)
                    
                    # 提取coverage信息
                    coverage_type = team_points.get("coverage_type")
                    if coverage_type:
                        transformed_stats["points_coverage_type"] = self.clean_string(coverage_type)
                    
                    coverage_value = team_points.get("coverage_value")
                    if coverage_value is not None:
                        transformed_stats["points_coverage_value"] = self.safe_int(coverage_value)
                else:
                    # 直接是数字
                    transformed_stats["team_points_total"] = self.safe_float(team_points, 0.0)
            
            return transformed_stats if transformed_stats else None
            
        except Exception as e:
            result.add_error("stats_transform", team_stats_data, f"统计数据转换异常: {str(e)}")
            return None
    
    def transform_team_matchup_info(self, matchup_info: Dict, team_key: str) -> TransformResult:
        """
        转换团队对战信息
        
        Args:
            matchup_info: Yahoo API返回的matchup信息
            team_key: 目标团队键
            
        Returns:
            TransformResult: 转换结果，包含标准化的对战信息
        """
        result = TransformResult(success=False)
        
        try:
            # 提取基本对战信息
            matchup_data = self._extract_basic_matchup_info(matchup_info, team_key, result)
            if not matchup_data:
                return result
            
            # 提取对战详情
            matchup_details = self._extract_matchup_details(matchup_info, team_key, result)
            if matchup_details:
                matchup_data.update(matchup_details)
            
            result.data = matchup_data
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", matchup_info, f"对战信息转换异常: {str(e)}")
            
        return result
    
    def _extract_basic_matchup_info(self, matchup_info: Any, team_key: str, result: TransformResult) -> Optional[Dict]:
        """提取基本对战信息"""
        try:
            matchup_data = {}
            
            # 处理不同的输入格式
            if isinstance(matchup_info, list):
                for item in matchup_info:
                    if isinstance(item, dict):
                        matchup_data.update(self._extract_matchup_fields(item))
            elif isinstance(matchup_info, dict):
                matchup_data.update(self._extract_matchup_fields(matchup_info))
            
            if not matchup_data.get("week"):
                result.add_error("week", matchup_info, "缺少week信息")
                return None
            
            return matchup_data
            
        except Exception as e:
            result.add_error("basic_matchup", matchup_info, f"提取基本对战信息失败: {str(e)}")
            return None
    
    def _extract_matchup_fields(self, item: Dict) -> Dict:
        """提取对战字段"""
        return {
            "week": self.safe_int(item.get("week")),
            "week_start": self.clean_string(item.get("week_start")),
            "week_end": self.clean_string(item.get("week_end")),
            "status": self.clean_string(item.get("status")),
            "is_playoffs": self.safe_bool(item.get("is_playoffs", False)),
            "is_consolation": self.safe_bool(item.get("is_consolation", False)),
            "is_matchup_of_week": self.safe_bool(item.get("is_matchup_of_week", False)),
            "is_tied": self.safe_bool(item.get("is_tied", False)),
            "winner_team_key": self.clean_string(item.get("winner_team_key"))
        }
    
    def _extract_matchup_details(self, matchup_info: Dict, team_key: str, result: TransformResult) -> Optional[Dict]:
        """提取对战详情"""
        try:
            # 这里可以扩展更多的对战详情提取逻辑
            details = {}
            
            # 判断胜负
            winner_team_key = matchup_info.get("winner_team_key")
            if winner_team_key:
                details["is_winner"] = (winner_team_key == team_key)
            
            return details
            
        except Exception as e:
            result.add_warning("matchup_details", matchup_info, f"提取对战详情失败: {str(e)}")
            return {}
    
    def batch_transform_team_weekly_stats(self, matchups_data: List[Dict], team_key: str) -> List[TransformResult]:
        """批量转换团队周统计数据"""
        results = []
        
        for matchup_info in matchups_data:
            result = self.transform_team_weekly_stats_from_matchup(matchup_info, team_key)
            results.append(result)
        
        return results
    
    def transform_team_season_stats(self, standings_data: Dict) -> TransformResult:
        """转换团队赛季统计数据（从standings数据）"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not isinstance(standings_data, dict):
                result.add_error("standings_data", standings_data, "standings数据必须是字典格式")
                return result
            
            # 提取团队赛季统计数据
            season_stats = self._extract_team_season_stats(standings_data, result)
            
            if not season_stats:
                result.add_error("season_stats", standings_data, "无法提取团队赛季统计数据")
                return result
            
            result.data = {
                "season_stats": season_stats,
                "raw_data": standings_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", standings_data, f"团队赛季统计转换异常: {str(e)}")
            
        return result
    
    def transform_team_weekly_stats(self, team_stats_data: Dict) -> TransformResult:
        """转换团队周统计数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not isinstance(team_stats_data, dict):
                result.add_error("team_stats_data", team_stats_data, "团队统计数据必须是字典格式")
                return result
            
            # 提取团队周统计数据
            weekly_stats = self._extract_team_weekly_stats(team_stats_data, result)
            
            if not weekly_stats:
                result.add_error("weekly_stats", team_stats_data, "无法提取团队周统计数据")
                return result
            
            result.data = {
                "weekly_stats": weekly_stats,
                "raw_data": team_stats_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["stats_count"] = len([v for v in weekly_stats.values() if v is not None])
            
        except Exception as e:
            result.add_error("transform", team_stats_data, f"团队周统计转换异常: {str(e)}")
            
        return result
    
    def _extract_team_season_stats(self, stats_data: Dict, result: TransformResult) -> Dict:
        """从团队standings数据中提取赛季统计项"""
        core_stats = {}
        
        try:
            # 从 team_standings 中提取排名和战绩信息
            team_standings = stats_data.get('team_standings', {})
            if isinstance(team_standings, dict):
                core_stats['rank'] = self.safe_int(team_standings.get('rank'))
                core_stats['playoff_seed'] = self.clean_string(team_standings.get('playoff_seed'))
                core_stats['games_back'] = self.clean_string(team_standings.get('games_back'))
                
                # 从 outcome_totals 中提取战绩
                outcome_totals = team_standings.get('outcome_totals', {})
                if isinstance(outcome_totals, dict):
                    core_stats['wins'] = self.safe_int(outcome_totals.get('wins'))
                    core_stats['losses'] = self.safe_int(outcome_totals.get('losses'))
                    core_stats['ties'] = self.safe_int(outcome_totals.get('ties'))
                    core_stats['win_percentage'] = self.safe_float(outcome_totals.get('percentage'))
                
                # 从 divisional_outcome_totals 中提取分区战绩
                divisional_totals = team_standings.get('divisional_outcome_totals', {})
                if isinstance(divisional_totals, dict):
                    core_stats['divisional_wins'] = self.safe_int(divisional_totals.get('wins'))
                    core_stats['divisional_losses'] = self.safe_int(divisional_totals.get('losses'))
                    core_stats['divisional_ties'] = self.safe_int(divisional_totals.get('ties'))
            
            # 从 team_points 中提取总积分
            team_points = stats_data.get('team_points', {})
            if isinstance(team_points, dict):
                core_stats['team_points_total'] = self.safe_float(team_points.get('total'))
            elif team_points is not None:
                core_stats['team_points_total'] = self.safe_float(team_points)
                
        except Exception as e:
            result.add_error("extract_season_stats", stats_data, f"提取团队赛季统计失败: {str(e)}")
        
        return core_stats
    
    def _extract_team_weekly_stats(self, stats_data: Dict, result: TransformResult) -> Dict:
        """从团队周统计数据中提取完整的11个统计项"""
        core_stats = {}
        
        try:
            # 从 stats 数组中提取统计数据
            stats_list = stats_data.get('stats', [])
            if not isinstance(stats_list, list):
                return core_stats
            
            # 构建 stat_id 到 value 的映射
            stats_dict = {}
            for stat_item in stats_list:
                if isinstance(stat_item, dict) and 'stat' in stat_item:
                    stat_info = stat_item['stat']
                    stat_id = stat_info.get('stat_id')
                    value = stat_info.get('value')
                    if stat_id is not None:
                        stats_dict[str(stat_id)] = value
            
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self.safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self.safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_dict.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self.safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self.safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_dict.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self.safe_int(stats_dict.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self.safe_int(stats_dict.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self.safe_int(stats_dict.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['assists'] = self.safe_int(stats_dict.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['steals'] = self.safe_int(stats_dict.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self.safe_int(stats_dict.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self.safe_int(stats_dict.get('19'))
            
        except Exception as e:
            result.add_error("extract_weekly_stats", stats_data, f"提取团队周统计失败: {str(e)}")
        
        return core_stats
    
    def _parse_percentage(self, pct_str: str) -> Optional[float]:
        """解析百分比字符串"""
        if not pct_str or pct_str == '-':
            return None
        
        try:
            # 移除百分号
            clean_str = pct_str.replace('%', '').strip()
            
            # 转换为浮点数
            value = float(clean_str)
            
            # 如果值大于1，假设是百分比形式（如50.0），转换为小数形式
            if value > 1:
                value = value / 100.0
            
            return round(value, 3)
            
        except (ValueError, TypeError):
            return None 