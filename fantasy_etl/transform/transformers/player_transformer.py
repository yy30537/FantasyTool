"""
Player Transformer - 球员数据转换器

负责将Yahoo Fantasy API返回的球员相关原始数据转换为标准化格式
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .base_transformer import BaseTransformer, TransformResult

logger = logging.getLogger(__name__)


class PlayerTransformer(BaseTransformer):
    """球员数据转换器"""
    
    def transform(self, raw_data: Dict) -> TransformResult:
        """转换球员基本信息数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not self._validate_player_structure(raw_data, result):
                return result
            
            # 提取和转换球员基本信息
            player_info = self._extract_player_basic_info(raw_data, result)
            if not player_info:
                result.add_error("player_info", raw_data, "无法提取球员基本信息")
                return result
            
            # 标准化球员信息
            standardized_player = self._standardize_player_info(player_info, result)
            
            result.data = standardized_player
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", raw_data, f"转换异常: {str(e)}")
            
        return result
    
    def transform_player_season_stats(self, stats_data: Dict) -> TransformResult:
        """转换球员赛季统计数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证统计数据结构
            if not isinstance(stats_data, dict):
                result.add_error("stats_data", stats_data, "统计数据必须是字典格式")
                return result
            
            # 提取核心赛季统计项
            core_stats = self._extract_core_player_season_stats(stats_data, result)
            
            if not core_stats:
                result.add_error("core_stats", stats_data, "无法提取核心统计数据")
                return result
            
            result.data = {
                "season_stats": core_stats,
                "raw_stats": stats_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["stats_count"] = len([v for v in core_stats.values() if v is not None])
            
        except Exception as e:
            result.add_error("transform", stats_data, f"赛季统计转换异常: {str(e)}")
            
        return result
    
    def transform_player_daily_stats(self, stats_data: Dict, stats_date: date) -> TransformResult:
        """转换球员日统计数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证统计数据结构
            if not isinstance(stats_data, dict):
                result.add_error("stats_data", stats_data, "统计数据必须是字典格式")
                return result
            
            # 提取核心日统计项
            core_stats = self._extract_core_player_daily_stats(stats_data, result)
            
            if not core_stats:
                result.add_error("core_stats", stats_data, "无法提取核心统计数据")
                return result
            
            result.data = {
                "daily_stats": core_stats,
                "stats_date": stats_date,
                "raw_stats": stats_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["stats_count"] = len([v for v in core_stats.values() if v is not None])
            
        except Exception as e:
            result.add_error("transform", stats_data, f"日统计转换异常: {str(e)}")
            
        return result
    
    def _validate_player_structure(self, raw_data: Dict, result: TransformResult) -> bool:
        """验证球员数据结构"""
        if not isinstance(raw_data, dict):
            result.add_error("raw_data", raw_data, "原始数据必须是字典格式")
            return False
        
        # 检查是否包含球员基本信息
        if "player_key" not in raw_data and "editorial_player_key" not in raw_data:
            result.add_error("player_key", raw_data, "缺少球员标识信息")
            return False
        
        return True
    
    def _extract_player_basic_info(self, raw_data: Dict, result: TransformResult) -> Optional[Dict]:
        """提取球员基本信息"""
        try:
            player_info = {}
            
            # 提取基本标识信息
            player_info["player_key"] = self.clean_string(raw_data.get("player_key"))
            player_info["player_id"] = self.clean_string(raw_data.get("player_id"))
            player_info["editorial_player_key"] = self.clean_string(raw_data.get("editorial_player_key"))
            
            # 处理姓名信息
            if "name" in raw_data:
                name_info = raw_data["name"]
                if isinstance(name_info, dict):
                    player_info["full_name"] = self.clean_string(name_info.get("full"))
                    player_info["first_name"] = self.clean_string(name_info.get("first"))
                    player_info["last_name"] = self.clean_string(name_info.get("last"))
                elif isinstance(name_info, str):
                    player_info["full_name"] = self.clean_string(name_info)
            
            # 处理团队信息
            player_info["current_team_key"] = self.clean_string(raw_data.get("editorial_team_key"))
            player_info["current_team_name"] = self.clean_string(raw_data.get("editorial_team_full_name"))
            player_info["current_team_abbr"] = self.clean_string(raw_data.get("editorial_team_abbr"))
            
            # 处理位置信息
            player_info["display_position"] = self.clean_string(raw_data.get("display_position"))
            player_info["primary_position"] = self.clean_string(raw_data.get("primary_position"))
            player_info["position_type"] = self.clean_string(raw_data.get("position_type"))
            
            # 处理其他信息
            player_info["uniform_number"] = self.clean_string(raw_data.get("uniform_number"))
            player_info["status"] = self.clean_string(raw_data.get("status"))
            player_info["is_undroppable"] = self.safe_bool(raw_data.get("is_undroppable", False))
            
            # 处理头像信息
            if "headshot" in raw_data:
                headshot_info = raw_data["headshot"]
                if isinstance(headshot_info, dict) and "url" in headshot_info:
                    player_info["headshot_url"] = self.clean_string(headshot_info["url"])
            
            player_info["image_url"] = self.clean_string(raw_data.get("image_url"))
            
            return player_info
            
        except Exception as e:
            result.add_error("extract_basic", raw_data, f"提取球员基本信息失败: {str(e)}")
            return None
    
    def _standardize_player_info(self, player_info: Dict, result: TransformResult) -> Dict:
        """标准化球员信息"""
        try:
            standardized = {}
            
            # 必需字段
            standardized["player_key"] = player_info.get("player_key")
            standardized["player_id"] = player_info.get("player_id")
            standardized["editorial_player_key"] = player_info.get("editorial_player_key")
            
            # 姓名信息
            standardized["full_name"] = player_info.get("full_name")
            standardized["first_name"] = player_info.get("first_name")
            standardized["last_name"] = player_info.get("last_name")
            
            # 团队信息
            standardized["current_team_key"] = player_info.get("current_team_key")
            standardized["current_team_name"] = player_info.get("current_team_name")
            standardized["current_team_abbr"] = player_info.get("current_team_abbr")
            
            # 位置信息
            standardized["display_position"] = player_info.get("display_position")
            standardized["primary_position"] = player_info.get("primary_position")
            standardized["position_type"] = player_info.get("position_type")
            
            # 其他信息
            standardized["uniform_number"] = player_info.get("uniform_number")
            standardized["status"] = player_info.get("status")
            standardized["is_undroppable"] = player_info.get("is_undroppable", False)
            standardized["headshot_url"] = player_info.get("headshot_url")
            standardized["image_url"] = player_info.get("image_url")
            
            # 添加时间戳
            standardized["last_updated"] = datetime.now()
            
            return standardized
            
        except Exception as e:
            result.add_error("standardize", player_info, f"标准化球员信息失败: {str(e)}")
            return {}
    
    def _extract_core_player_season_stats(self, stats_data: Dict, result: TransformResult) -> Dict:
        """从球员赛季统计数据中提取完整的11个统计项"""
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
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
                    core_stats['free_throws_made'] = self.safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self.safe_int(attempted.strip())
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
            core_stats['three_pointers_made'] = self.safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['total_points'] = self.safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['total_rebounds'] = self.safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['total_assists'] = self.safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['total_steals'] = self.safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['total_blocks'] = self.safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['total_turnovers'] = self.safe_int(stats_data.get('19'))
            
        except Exception as e:
            result.add_error("extract_season_stats", stats_data, f"提取核心赛季统计失败: {str(e)}")
        
        return core_stats
    
    def _extract_core_player_daily_stats(self, stats_data: Dict, result: TransformResult) -> Dict:
        """从统计数据中提取完整的11个日期统计项"""
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
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
                    core_stats['free_throws_made'] = self.safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self.safe_int(attempted.strip())
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
            core_stats['three_pointers_made'] = self.safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self.safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self.safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['assists'] = self.safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['steals'] = self.safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self.safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self.safe_int(stats_data.get('19'))
            
        except Exception as e:
            result.add_error("extract_daily_stats", stats_data, f"提取核心日期统计失败: {str(e)}")
        
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
    
    def batch_transform_player_season_stats(self, stats_list: List[Dict]) -> List[TransformResult]:
        """批量转换球员赛季统计数据"""
        results = []
        
        for stats_data in stats_list:
            result = self.transform_player_season_stats(stats_data)
            results.append(result)
        
        return results
    
    def batch_transform_player_daily_stats(self, stats_list: List[Dict], stats_dates: List[date]) -> List[TransformResult]:
        """批量转换球员日统计数据"""
        results = []
        
        for i, stats_data in enumerate(stats_list):
            stats_date = stats_dates[i] if i < len(stats_dates) else date.today()
            result = self.transform_player_daily_stats(stats_data, stats_date)
            results.append(result)
        
        return results 