"""
League Transformer - 联盟数据转换器

负责将Yahoo Fantasy API返回的联盟相关原始数据转换为标准化格式
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .base_transformer import BaseTransformer, TransformResult

logger = logging.getLogger(__name__)


class LeagueTransformer(BaseTransformer):
    """联盟数据转换器"""
    
    def transform(self, raw_data: Dict) -> TransformResult:
        """转换联盟基本信息数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not self._validate_league_structure(raw_data, result):
                return result
            
            # 提取和转换联盟基本信息
            league_info = self._extract_league_basic_info(raw_data, result)
            if not league_info:
                result.add_error("league_info", raw_data, "无法提取联盟基本信息")
                return result
            
            # 标准化联盟信息
            standardized_league = self._standardize_league_info(league_info, result)
            
            result.data = standardized_league
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", raw_data, f"转换异常: {str(e)}")
            
        return result
    
    def transform_league_settings(self, settings_data: Dict) -> TransformResult:
        """转换联盟设置数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证设置数据结构
            if not isinstance(settings_data, dict):
                result.add_error("settings_data", settings_data, "设置数据必须是字典格式")
                return result
            
            # 提取联盟设置信息
            league_settings = self._extract_league_settings(settings_data, result)
            
            if not league_settings:
                result.add_error("league_settings", settings_data, "无法提取联盟设置数据")
                return result
            
            result.data = {
                "league_settings": league_settings,
                "raw_data": settings_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("transform", settings_data, f"联盟设置转换异常: {str(e)}")
            
        return result
    
    def transform_league_standings(self, standings_data: Dict) -> TransformResult:
        """转换联盟排名数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证排名数据结构
            if not isinstance(standings_data, dict):
                result.add_error("standings_data", standings_data, "排名数据必须是字典格式")
                return result
            
            # 提取联盟排名信息
            league_standings = self._extract_league_standings(standings_data, result)
            
            if not league_standings:
                result.add_error("league_standings", standings_data, "无法提取联盟排名数据")
                return result
            
            result.data = {
                "league_standings": league_standings,
                "raw_data": standings_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["teams_count"] = len(league_standings)
            
        except Exception as e:
            result.add_error("transform", standings_data, f"联盟排名转换异常: {str(e)}")
            
        return result
    
    def transform_stat_categories(self, stat_categories_data: Dict) -> TransformResult:
        """转换统计类别数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证统计类别数据结构
            if not isinstance(stat_categories_data, dict):
                result.add_error("stat_categories_data", stat_categories_data, "统计类别数据必须是字典格式")
                return result
            
            # 提取统计类别信息
            stat_categories = self._extract_stat_categories(stat_categories_data, result)
            
            if not stat_categories:
                result.add_error("stat_categories", stat_categories_data, "无法提取统计类别数据")
                return result
            
            result.data = {
                "stat_categories": stat_categories,
                "raw_data": stat_categories_data
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["categories_count"] = len(stat_categories)
            
        except Exception as e:
            result.add_error("transform", stat_categories_data, f"统计类别转换异常: {str(e)}")
            
        return result
    
    def _validate_league_structure(self, raw_data: Dict, result: TransformResult) -> bool:
        """验证联盟数据结构"""
        if not isinstance(raw_data, dict):
            result.add_error("raw_data", raw_data, "原始数据必须是字典格式")
            return False
        
        # 检查是否包含联盟基本信息
        if "league_key" not in raw_data and "league_id" not in raw_data:
            result.add_error("league_key", raw_data, "缺少联盟标识信息")
            return False
        
        return True
    
    def _extract_league_basic_info(self, raw_data: Dict, result: TransformResult) -> Optional[Dict]:
        """提取联盟基本信息"""
        try:
            league_info = {}
            
            # 提取基本标识信息
            league_info["league_key"] = self.clean_string(raw_data.get("league_key"))
            league_info["league_id"] = self.clean_string(raw_data.get("league_id"))
            league_info["game_key"] = self.clean_string(raw_data.get("game_key"))
            
            # 提取联盟基本信息
            league_info["name"] = self.clean_string(raw_data.get("name"))
            league_info["url"] = self.clean_string(raw_data.get("url"))
            league_info["logo_url"] = self.clean_string(raw_data.get("logo_url"))
            league_info["password"] = self.clean_string(raw_data.get("password"))
            league_info["draft_status"] = self.clean_string(raw_data.get("draft_status"))
            league_info["num_teams"] = self.safe_int(raw_data.get("num_teams"))
            league_info["edit_key"] = self.clean_string(raw_data.get("edit_key"))
            league_info["weekly_deadline"] = self.clean_string(raw_data.get("weekly_deadline"))
            league_info["league_update_timestamp"] = self.clean_string(raw_data.get("league_update_timestamp"))
            league_info["scoring_type"] = self.clean_string(raw_data.get("scoring_type"))
            league_info["league_type"] = self.clean_string(raw_data.get("league_type"))
            league_info["renew"] = self.clean_string(raw_data.get("renew"))
            league_info["renewed"] = self.clean_string(raw_data.get("renewed"))
            league_info["felo_tier"] = self.clean_string(raw_data.get("felo_tier"))
            league_info["iris_group_chat_id"] = self.clean_string(raw_data.get("iris_group_chat_id"))
            league_info["short_invitation_url"] = self.clean_string(raw_data.get("short_invitation_url"))
            league_info["allow_add_to_dl_extra_pos"] = self.safe_bool(raw_data.get("allow_add_to_dl_extra_pos", False))
            league_info["is_pro_league"] = self.safe_bool(raw_data.get("is_pro_league", False))
            league_info["is_cash_league"] = self.safe_bool(raw_data.get("is_cash_league", False))
            league_info["current_week"] = self.safe_int(raw_data.get("current_week"))
            league_info["start_week"] = self.safe_int(raw_data.get("start_week"))
            league_info["start_date"] = self.clean_string(raw_data.get("start_date"))
            league_info["end_week"] = self.safe_int(raw_data.get("end_week"))
            league_info["end_date"] = self.clean_string(raw_data.get("end_date"))
            league_info["is_finished"] = self.safe_bool(raw_data.get("is_finished", False))
            league_info["is_plus_league"] = self.safe_bool(raw_data.get("is_plus_league", False))
            league_info["game_code"] = self.clean_string(raw_data.get("game_code"))
            league_info["season"] = self.clean_string(raw_data.get("season"))
            
            return league_info
            
        except Exception as e:
            result.add_error("extract_basic", raw_data, f"提取联盟基本信息失败: {str(e)}")
            return None
    
    def _standardize_league_info(self, league_info: Dict, result: TransformResult) -> Dict:
        """标准化联盟信息"""
        try:
            standardized = {}
            
            # 必需字段
            standardized["league_key"] = league_info.get("league_key")
            standardized["league_id"] = league_info.get("league_id")
            standardized["game_key"] = league_info.get("game_key")
            
            # 联盟基本信息
            standardized["name"] = league_info.get("name")
            standardized["url"] = league_info.get("url")
            standardized["logo_url"] = league_info.get("logo_url")
            standardized["password"] = league_info.get("password")
            standardized["draft_status"] = league_info.get("draft_status")
            standardized["num_teams"] = league_info.get("num_teams")
            standardized["edit_key"] = league_info.get("edit_key")
            standardized["weekly_deadline"] = league_info.get("weekly_deadline")
            standardized["league_update_timestamp"] = league_info.get("league_update_timestamp")
            standardized["scoring_type"] = league_info.get("scoring_type")
            standardized["league_type"] = league_info.get("league_type")
            standardized["renew"] = league_info.get("renew")
            standardized["renewed"] = league_info.get("renewed")
            standardized["felo_tier"] = league_info.get("felo_tier")
            standardized["iris_group_chat_id"] = league_info.get("iris_group_chat_id")
            standardized["short_invitation_url"] = league_info.get("short_invitation_url")
            standardized["allow_add_to_dl_extra_pos"] = league_info.get("allow_add_to_dl_extra_pos", False)
            standardized["is_pro_league"] = league_info.get("is_pro_league", False)
            standardized["is_cash_league"] = league_info.get("is_cash_league", False)
            standardized["current_week"] = league_info.get("current_week")
            standardized["start_week"] = league_info.get("start_week")
            standardized["start_date"] = league_info.get("start_date")
            standardized["end_week"] = league_info.get("end_week")
            standardized["end_date"] = league_info.get("end_date")
            standardized["is_finished"] = league_info.get("is_finished", False)
            standardized["is_plus_league"] = league_info.get("is_plus_league", False)
            standardized["game_code"] = league_info.get("game_code")
            standardized["season"] = league_info.get("season")
            
            # 添加时间戳
            standardized["last_updated"] = datetime.now()
            
            return standardized
            
        except Exception as e:
            result.add_error("standardize", league_info, f"标准化联盟信息失败: {str(e)}")
            return {}
    
    def _extract_league_settings(self, settings_data: Dict, result: TransformResult) -> Optional[Dict]:
        """提取联盟设置信息"""
        try:
            settings = {}
            
            # 从fantasy_content中提取设置数据
            fantasy_content = settings_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找settings容器
            settings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings_container = item["settings"]
                        break
            elif isinstance(league_data, dict) and "settings" in league_data:
                settings_container = league_data["settings"]
            
            if not settings_container:
                return None
            
            # 提取设置信息
            settings["draft_type"] = self.clean_string(settings_container.get("draft_type"))
            settings["is_auction_draft"] = self.safe_bool(settings_container.get("is_auction_draft", False))
            settings["scoring_type"] = self.clean_string(settings_container.get("scoring_type"))
            settings["persistent_url"] = self.clean_string(settings_container.get("persistent_url"))
            settings["uses_playoff"] = self.safe_bool(settings_container.get("uses_playoff", False))
            settings["has_playoff_consolation_games"] = self.safe_bool(settings_container.get("has_playoff_consolation_games", False))
            settings["playoff_start_week"] = self.safe_int(settings_container.get("playoff_start_week"))
            settings["uses_playoff_reseeding"] = self.safe_bool(settings_container.get("uses_playoff_reseeding", False))
            settings["uses_lock_eliminated_teams"] = self.safe_bool(settings_container.get("uses_lock_eliminated_teams", False))
            settings["num_playoff_teams"] = self.safe_int(settings_container.get("num_playoff_teams"))
            settings["num_playoff_consolation_teams"] = self.safe_int(settings_container.get("num_playoff_consolation_teams"))
            settings["has_multiweek_championship"] = self.safe_bool(settings_container.get("has_multiweek_championship", False))
            settings["waiver_type"] = self.clean_string(settings_container.get("waiver_type"))
            settings["waiver_rule"] = self.clean_string(settings_container.get("waiver_rule"))
            settings["uses_faab"] = self.safe_bool(settings_container.get("uses_faab", False))
            settings["draft_time"] = self.clean_string(settings_container.get("draft_time"))
            settings["draft_pick_time"] = self.safe_int(settings_container.get("draft_pick_time"))
            settings["post_draft_players"] = self.clean_string(settings_container.get("post_draft_players"))
            settings["max_teams"] = self.safe_int(settings_container.get("max_teams"))
            settings["waiver_time"] = self.safe_int(settings_container.get("waiver_time"))
            settings["trade_end_date"] = self.clean_string(settings_container.get("trade_end_date"))
            settings["trade_ratify_type"] = self.clean_string(settings_container.get("trade_ratify_type"))
            settings["trade_reject_time"] = self.safe_int(settings_container.get("trade_reject_time"))
            settings["player_pool"] = self.clean_string(settings_container.get("player_pool"))
            settings["cant_cut_list"] = self.clean_string(settings_container.get("cant_cut_list"))
            settings["can_trade_draft_picks"] = self.safe_bool(settings_container.get("can_trade_draft_picks", False))
            settings["sendbird_channel_url"] = self.clean_string(settings_container.get("sendbird_channel_url"))
            
            return settings
            
        except Exception as e:
            result.add_error("extract_settings", settings_data, f"提取联盟设置失败: {str(e)}")
            return None
    
    def _extract_league_standings(self, standings_data: Dict, result: TransformResult) -> Optional[List[Dict]]:
        """提取联盟排名信息"""
        try:
            standings_list = []
            
            # 从fantasy_content中提取排名数据
            fantasy_content = standings_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找standings容器
            standings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "standings" in item:
                        standings_container = item["standings"]
                        break
            elif isinstance(league_data, dict) and "standings" in league_data:
                standings_container = league_data["standings"]
            
            if not standings_container:
                return None
            
            # 查找teams容器
            teams_container = None
            if isinstance(standings_container, list):
                for item in standings_container:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(standings_container, dict) and "teams" in standings_container:
                teams_container = standings_container["teams"]
            
            if not teams_container:
                return None
            
            teams_count = self.safe_int(teams_container.get("count", 0), 0)
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # 提取团队排名信息
                team_standing = self._extract_team_standing_info(team_data, result)
                if team_standing:
                    standings_list.append(team_standing)
            
            return standings_list
            
        except Exception as e:
            result.add_error("extract_standings", standings_data, f"提取联盟排名失败: {str(e)}")
            return None
    
    def _extract_team_standing_info(self, team_data: Any, result: TransformResult) -> Optional[Dict]:
        """从team数据中提取排名信息"""
        try:
            team_key = None
            team_standings = None
            team_points = None
            
            # team_data是复杂的嵌套结构，需要递归提取
            def extract_from_data(data, target_key):
                if isinstance(data, dict):
                    if target_key in data:
                        return data[target_key]
                    for value in data.values():
                        result = extract_from_data(value, target_key)
                        if result is not None:
                            return result
                elif isinstance(data, list):
                    for item in data:
                        result = extract_from_data(item, target_key)
                        if result is not None:
                            return result
                return None
            
            team_key = extract_from_data(team_data, "team_key")
            team_standings = extract_from_data(team_data, "team_standings")
            team_points = extract_from_data(team_data, "team_points")
            
            if not team_key:
                return None
            
            standing_info = {
                "team_key": team_key,
                "rank": None,
                "playoff_seed": None,
                "wins": 0,
                "losses": 0,
                "ties": 0,
                "win_percentage": 0.0,
                "games_back": "-",
                "divisional_wins": 0,
                "divisional_losses": 0,
                "divisional_ties": 0,
                "team_points_total": 0.0
            }
            
            # 提取standings数据
            if isinstance(team_standings, dict):
                standing_info["rank"] = self.safe_int(team_standings.get("rank"))
                standing_info["playoff_seed"] = self.clean_string(team_standings.get("playoff_seed"))
                standing_info["games_back"] = self.clean_string(team_standings.get("games_back", "-"))
                
                outcome_totals = team_standings.get("outcome_totals", {})
                if isinstance(outcome_totals, dict):
                    standing_info["wins"] = self.safe_int(outcome_totals.get("wins"), 0)
                    standing_info["losses"] = self.safe_int(outcome_totals.get("losses"), 0)
                    standing_info["ties"] = self.safe_int(outcome_totals.get("ties"), 0)
                    standing_info["win_percentage"] = self.safe_float(outcome_totals.get("percentage"), 0.0)
                
                divisional_outcome = team_standings.get("divisional_outcome_totals", {})
                if isinstance(divisional_outcome, dict):
                    standing_info["divisional_wins"] = self.safe_int(divisional_outcome.get("wins"), 0)
                    standing_info["divisional_losses"] = self.safe_int(divisional_outcome.get("losses"), 0)
                    standing_info["divisional_ties"] = self.safe_int(divisional_outcome.get("ties"), 0)
            
            # 提取team_points数据
            if isinstance(team_points, dict):
                standing_info["team_points_total"] = self.safe_float(team_points.get("total"), 0.0)
            elif team_points is not None:
                standing_info["team_points_total"] = self.safe_float(team_points, 0.0)
            
            return standing_info
            
        except Exception as e:
            result.add_error("extract_team_standing", team_data, f"提取团队排名信息失败: {str(e)}")
            return None
    
    def _extract_stat_categories(self, stat_categories_data: Dict, result: TransformResult) -> Optional[List[Dict]]:
        """提取统计类别信息"""
        try:
            categories_list = []
            
            # 从fantasy_content中提取统计类别数据
            fantasy_content = stat_categories_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", [])
            
            # 查找settings容器中的stat_categories
            settings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings_container = item["settings"]
                        break
            elif isinstance(league_data, dict) and "settings" in league_data:
                settings_container = league_data["settings"]
            
            if not settings_container:
                return None
            
            stat_categories_container = settings_container.get("stat_categories")
            if not stat_categories_container:
                return None
            
            # 查找stats容器
            stats_container = None
            if isinstance(stat_categories_container, dict) and "stats" in stat_categories_container:
                stats_container = stat_categories_container["stats"]
            
            if not stats_container:
                return None
            
            stats_count = self.safe_int(stats_container.get("count", 0), 0)
            
            for i in range(stats_count):
                str_index = str(i)
                if str_index not in stats_container:
                    continue
                
                stat_container = stats_container[str_index]
                if "stat" not in stat_container:
                    continue
                
                stat_data = stat_container["stat"]
                
                # 提取统计类别信息
                category_info = {
                    "stat_id": self.safe_int(stat_data.get("stat_id")),
                    "enabled": self.safe_bool(stat_data.get("enabled", True)),
                    "name": self.clean_string(stat_data.get("name")),
                    "display_name": self.clean_string(stat_data.get("display_name")),
                    "sort_order": self.safe_int(stat_data.get("sort_order")),
                    "position_type": self.clean_string(stat_data.get("position_type")),
                    "stat_position_types": stat_data.get("stat_position_types", []),
                    "is_only_display_stat": self.safe_bool(stat_data.get("is_only_display_stat", False))
                }
                
                categories_list.append(category_info)
            
            return categories_list
            
        except Exception as e:
            result.add_error("extract_stat_categories", stat_categories_data, f"提取统计类别失败: {str(e)}")
            return None
    
    def batch_transform_league_standings(self, standings_list: List[Dict]) -> List[TransformResult]:
        """批量转换联盟排名数据"""
        results = []
        
        for standings_data in standings_list:
            result = self.transform_league_standings(standings_data)
            results.append(result)
        
        return results 