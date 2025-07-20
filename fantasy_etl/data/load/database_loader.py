"""
数据库加载器
重构自archive/database_writer.py，提供数据库写入功能
"""
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_
from dataclasses import dataclass

from ...core.database import (
    Game, League, LeagueSettings, Team, Manager, Player, StatCategory,
    PlayerEligiblePosition, PlayerSeasonStats, PlayerDailyStats,
    TeamStatsWeekly, LeagueStandings, TeamMatchups,
    RosterDaily, Transaction, TransactionPlayer, DateDimension,
    LeagueRosterPosition
)
from ...core.database.connection_manager import db_manager

@dataclass
class LoadResult:
    """加载结果数据类"""
    success: bool
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_message: Optional[str] = None

class DatabaseLoader:
    """数据库加载器"""
    
    def __init__(self, batch_size: int = 100):
        """初始化数据库加载器
        
        Args:
            batch_size: 批量处理大小，默认100
        """
        self.batch_size = batch_size
        
        # 统计计数器
        self.stats = {
            'games': 0,
            'leagues': 0,
            'teams': 0,
            'managers': 0,
            'players': 0,
            'player_eligible_positions': 0,
            'stat_categories': 0,
            'player_season_stats': 0,
            'player_daily_stats': 0,
            'team_stats_weekly': 0,
            'league_standings': 0,
            'team_matchups': 0,
            'roster_daily': 0,
            'transactions': 0,
            'transaction_players': 0,
            'league_settings': 0,
            'date_dimension': 0,
            'league_roster_positions': 0
        }
    
    def get_stats_summary(self) -> str:
        """获取统计摘要"""
        return (f"统计: 游戏({self.stats['games']}) 联盟({self.stats['leagues']}) "
                f"团队({self.stats['teams']}) 球员({self.stats['players']}) "
                f"交易({self.stats['transactions']}) 交易球员({self.stats['transaction_players']}) "
                f"名单({self.stats['roster_daily']}) 赛季统计({self.stats['player_season_stats']}) "
                f"日统计({self.stats['player_daily_stats']}) 团队周统计({self.stats['team_stats_weekly']})")
    
    def load_games_data(self, games_data: Dict) -> LoadResult:
        """加载游戏数据"""
        if not games_data:
            return LoadResult(success=False, error_message="无游戏数据")
        
        try:
            with db_manager.session_scope() as session:
                count = 0
                games = games_data["fantasy_content"]["users"]["0"]["user"][1]["games"]
                
                for key, game_data in games.items():
                    if key == "count":
                        continue
                        
                    if isinstance(game_data["game"], list):
                        game_info = game_data["game"][0]
                    else:
                        game_info = game_data["game"]
                    
                    # 检查是否已存在
                    existing = session.query(Game).filter_by(game_key=game_info["game_key"]).first()
                    if existing:
                        continue
                    
                    game = Game(
                        game_key=game_info["game_key"],
                        game_id=game_info["game_id"],
                        name=game_info["name"],
                        code=game_info["code"],
                        type=game_info.get("type"),
                        url=game_info.get("url"),
                        season=game_info["season"],
                        is_registration_over=bool(game_info.get("is_registration_over", 0)),
                        is_game_over=bool(game_info.get("is_game_over", 0)),
                        is_offseason=bool(game_info.get("is_offseason", 0)),
                        editorial_season=game_info.get("editorial_season"),
                        picks_status=game_info.get("picks_status"),
                        contest_group_id=game_info.get("contest_group_id"),
                        scenario_generator=bool(game_info.get("scenario_generator", 0))
                    )
                    
                    session.add(game)
                    count += 1
                
                self.stats['games'] += count
                return LoadResult(success=True, inserted_count=count)
                
        except Exception as e:
            return LoadResult(success=False, error_message=f"加载游戏数据失败: {str(e)}")
    
    def load_leagues_data(self, leagues_data: Dict) -> LoadResult:
        """加载联盟数据"""
        if not leagues_data:
            return LoadResult(success=False, error_message="无联盟数据")
        
        try:
            with db_manager.session_scope() as session:
                count = 0
                leagues = leagues_data["fantasy_content"]["users"]["0"]["user"][1]["games"]
                
                for game_key, game_data in leagues.items():
                    if game_key == "count":
                        continue
                        
                    if isinstance(game_data["game"], list):
                        game_info = game_data["game"][0]
                        leagues_list = game_data["game"][1].get("leagues", {})
                    else:
                        game_info = game_data["game"]
                        leagues_list = game_info.get("leagues", {})
                    
                    if not leagues_list:
                        continue
                    
                    for league_key, league_data in leagues_list.items():
                        if league_key == "count":
                            continue
                        
                        if isinstance(league_data["league"], list):
                            league_info = league_data["league"][0]
                        else:
                            league_info = league_data["league"]
                        
                        # 检查是否已存在
                        existing = session.query(League).filter_by(league_key=league_info["league_key"]).first()
                        if existing:
                            continue
                        
                        league = League(
                            league_key=league_info["league_key"],
                            league_id=league_info["league_id"],
                            game_key=game_info["game_key"],
                            name=league_info["name"],
                            url=league_info.get("url"),
                            logo_url=league_info.get("logo_url"),
                            password=league_info.get("password"),
                            draft_status=league_info.get("draft_status"),
                            num_teams=int(league_info.get("num_teams", 0)),
                            edit_key=league_info.get("edit_key"),
                            weekly_deadline=league_info.get("weekly_deadline"),
                            league_update_timestamp=league_info.get("league_update_timestamp"),
                            scoring_type=league_info.get("scoring_type"),
                            league_type=league_info.get("league_type"),
                            renew=league_info.get("renew"),
                            renewed=league_info.get("renewed"),
                            felo_tier=league_info.get("felo_tier"),
                            iris_group_chat_id=league_info.get("iris_group_chat_id"),
                            short_invitation_url=league_info.get("short_invitation_url"),
                            allow_add_to_dl_extra_pos=bool(league_info.get("allow_add_to_dl_extra_pos", 0)),
                            is_pro_league=bool(league_info.get("is_pro_league", 0)),
                            is_cash_league=bool(league_info.get("is_cash_league", 0)),
                            current_week=league_info.get("current_week"),
                            start_week=league_info.get("start_week"),
                            start_date=league_info.get("start_date"),
                            end_week=league_info.get("end_week"),
                            end_date=league_info.get("end_date"),
                            is_finished=bool(league_info.get("is_finished", 0)),
                            is_plus_league=bool(league_info.get("is_plus_league", 0)),
                            game_code=game_info.get("code"),
                            season=game_info.get("season")
                        )
                        
                        session.add(league)
                        count += 1
                
                self.stats['leagues'] += count
                return LoadResult(success=True, inserted_count=count)
                
        except Exception as e:
            return LoadResult(success=False, error_message=f"加载联盟数据失败: {str(e)}")
    
    def load_player_stats(self, player_key: str, editorial_player_key: str,
                         league_key: str, season: str, stats_data: Dict,
                         stat_type: str = "season", date_obj: Optional[date] = None,
                         week: Optional[int] = None) -> LoadResult:
        """加载球员统计数据
        
        Args:
            player_key: 球员key
            editorial_player_key: 编辑球员key
            league_key: 联盟key
            season: 赛季
            stats_data: 统计数据
            stat_type: 统计类型 ("season" 或 "daily")
            date_obj: 日期对象 (仅用于daily统计)
            week: 周数
        """
        if not stats_data:
            return LoadResult(success=False, error_message="无统计数据")
        
        try:
            with db_manager.session_scope() as session:
                # 解析统计数据
                parsed_stats = self._parse_stats_data(stats_data)
                if not parsed_stats:
                    return LoadResult(success=False, error_message="无法解析统计数据")
                
                if stat_type == "season":
                    # 季度统计
                    existing = session.query(PlayerSeasonStats).filter_by(
                        player_key=player_key, season=season
                    ).first()
                    
                    if existing:
                        # 更新现有记录
                        for key, value in parsed_stats.items():
                            setattr(existing, key, value)
                        existing.updated_at = datetime.utcnow()
                        return LoadResult(success=True, updated_count=1)
                    else:
                        # 创建新记录
                        stats_record = PlayerSeasonStats(
                            player_key=player_key,
                            editorial_player_key=editorial_player_key,
                            league_key=league_key,
                            season=season,
                            **parsed_stats
                        )
                        session.add(stats_record)
                        self.stats['player_season_stats'] += 1
                        return LoadResult(success=True, inserted_count=1)
                
                elif stat_type == "daily" and date_obj:
                    # 日统计
                    existing = session.query(PlayerDailyStats).filter_by(
                        player_key=player_key, date=date_obj
                    ).first()
                    
                    if existing:
                        # 更新现有记录
                        for key, value in parsed_stats.items():
                            setattr(existing, key, value)
                        existing.updated_at = datetime.utcnow()
                        return LoadResult(success=True, updated_count=1)
                    else:
                        # 创建新记录
                        stats_record = PlayerDailyStats(
                            player_key=player_key,
                            editorial_player_key=editorial_player_key,
                            league_key=league_key,
                            season=season,
                            date=date_obj,
                            week=week,
                            **parsed_stats
                        )
                        session.add(stats_record)
                        self.stats['player_daily_stats'] += 1
                        return LoadResult(success=True, inserted_count=1)
                else:
                    return LoadResult(success=False, error_message="无效的统计类型或缺少日期")
                    
        except Exception as e:
            return LoadResult(success=False, error_message=f"加载球员统计失败: {str(e)}")
    
    def _parse_stats_data(self, stats_data: Dict) -> Optional[Dict]:
        """解析统计数据到标准格式"""
        try:
            parsed = {}
            
            # 解析各种统计项
            for stat in stats_data.get("stats", []):
                stat_id = int(stat.get("stat_id", 0))
                value = stat.get("value", "")
                
                # 根据stat_id映射到相应字段
                if stat_id == 5:  # Field Goal Percentage
                    parsed["field_goal_percentage"] = float(value) if value else None
                elif stat_id == 8:  # Free Throw Percentage  
                    parsed["free_throw_percentage"] = float(value) if value else None
                elif stat_id == 10:  # 3-point Shots Made
                    parsed["three_pointers_made"] = int(value) if value else None
                elif stat_id == 12:  # Points
                    parsed["points"] = int(value) if value else None
                elif stat_id == 15:  # Rebounds
                    parsed["rebounds"] = int(value) if value else None
                elif stat_id == 16:  # Assists
                    parsed["assists"] = int(value) if value else None
                elif stat_id == 17:  # Steals
                    parsed["steals"] = int(value) if value else None
                elif stat_id == 18:  # Blocks
                    parsed["blocks"] = int(value) if value else None
                elif stat_id == 19:  # Turnovers
                    parsed["turnovers"] = int(value) if value else None
                elif stat_id == 9004003:  # Field Goals Made/Attempted
                    if "/" in value:
                        made, attempted = value.split("/")
                        parsed["field_goals_made"] = int(made.strip()) if made.strip() else None
                        parsed["field_goals_attempted"] = int(attempted.strip()) if attempted.strip() else None
                elif stat_id == 9007006:  # Free Throws Made/Attempted
                    if "/" in value:
                        made, attempted = value.split("/")
                        parsed["free_throws_made"] = int(made.strip()) if made.strip() else None
                        parsed["free_throws_attempted"] = int(attempted.strip()) if attempted.strip() else None
            
            return parsed if parsed else None
            
        except Exception as e:
            print(f"解析统计数据失败: {e}")
            return None
    
    def load_roster_daily_data(self, roster_entries: List[Dict]) -> bool:
        """加载每日阵容数据"""
        if not roster_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for roster_entry in roster_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(RosterDaily).filter_by(
                            team_key=roster_entry["team_key"],
                            player_key=roster_entry["player_key"],
                            date=roster_entry["roster_date"]
                        ).first()
                        
                        if existing:
                            # 更新现有记录
                            existing.selected_position = roster_entry["selected_position"]
                            existing.player_status = roster_entry["player_status"]
                            existing.status_full = roster_entry["status_full"] 
                            existing.injury_note = roster_entry["injury_note"]
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            roster_daily = RosterDaily(
                                team_key=roster_entry["team_key"],
                                player_key=roster_entry["player_key"],
                                league_key=roster_entry["league_key"],
                                date=roster_entry["roster_date"],
                                season=roster_entry["season"],
                                selected_position=roster_entry["selected_position"],
                                is_starting=roster_entry["selected_position"] not in ['BN', 'IL', 'IR'] if roster_entry["selected_position"] else False,
                                is_bench=roster_entry["selected_position"] == 'BN' if roster_entry["selected_position"] else False,
                                is_injured_reserve=roster_entry["selected_position"] in ['IL', 'IR'] if roster_entry["selected_position"] else False,
                                player_status=roster_entry["player_status"],
                                status_full=roster_entry["status_full"],
                                injury_note=roster_entry["injury_note"]
                            )
                            session.add(roster_daily)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理阵容记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['roster_daily'] = self.stats.get('roster_daily', 0) + inserted_count
                
                print(f"  💾 阵容数据保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存阵容数据失败: {e}")
            return False
    
    def load_players_daily_stats_batch(self, stats_entries: List[Dict]) -> bool:
        """批量加载球员日统计数据"""
        if not stats_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for stats_entry in stats_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(PlayerDailyStats).filter_by(
                            player_key=stats_entry["player_key"],
                            date=stats_entry["date"]
                        ).first()
                        
                        # 提取核心统计项
                        parsed_stats = self._parse_stats_data_from_dict(stats_entry["stats_data"])
                        
                        if existing:
                            # 更新现有记录
                            for key, value in parsed_stats.items():
                                setattr(existing, key, value)
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            daily_stats = PlayerDailyStats(
                                player_key=stats_entry["player_key"],
                                editorial_player_key=stats_entry["editorial_player_key"],
                                league_key=stats_entry["league_key"],
                                season=stats_entry["season"],
                                date=stats_entry["date"],
                                **parsed_stats
                            )
                            session.add(daily_stats)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理球员统计记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['player_daily_stats'] = self.stats.get('player_daily_stats', 0) + inserted_count
                
                print(f"    💾 球员日统计保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存球员日统计数据失败: {e}")
            return False
    
    def _parse_stats_data_from_dict(self, stats_dict: Dict) -> Dict:
        """从统计字典中解析数据到标准格式"""
        try:
            parsed = {}
            
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    parsed['field_goals_made'] = self._safe_int(made.strip())
                    parsed['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['field_goals_made'] = None
                    parsed['field_goals_attempted'] = None
            else:
                parsed['field_goals_made'] = None
                parsed['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_dict.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                parsed['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                parsed['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    parsed['free_throws_made'] = self._safe_int(made.strip())
                    parsed['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['free_throws_made'] = None
                    parsed['free_throws_attempted'] = None
            else:
                parsed['free_throws_made'] = None
                parsed['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_dict.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                parsed['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                parsed['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            parsed['three_pointers_made'] = self._safe_int(stats_dict.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            parsed['points'] = self._safe_int(stats_dict.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            parsed['rebounds'] = self._safe_int(stats_dict.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            parsed['assists'] = self._safe_int(stats_dict.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            parsed['steals'] = self._safe_int(stats_dict.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            parsed['blocks'] = self._safe_int(stats_dict.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            parsed['turnovers'] = self._safe_int(stats_dict.get('19'))
            
            return parsed
            
        except Exception as e:
            print(f"解析统计字典失败: {e}")
            return {}
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        try:
            if value is None or value == '' or value == '-':
                return None
            return int(float(value))  # 先转float再转int，处理'1.0'格式
        except (ValueError, TypeError):
            return None
    
    def _parse_percentage(self, pct_str) -> Optional[float]:
        """解析百分比字符串，返回百分比值（0-100）"""
        try:
            if not pct_str or pct_str == '-':
                return None
            
            pct_str = str(pct_str).strip()
            
            # 移除%符号
            if '%' in pct_str:
                clean_value = pct_str.replace('%', '')
                val = self._safe_float(clean_value)
                return round(val, 3) if val is not None else None
            
            # 处理小数形式（如 .500 或 0.500）
            clean_value = self._safe_float(pct_str)
            if clean_value is not None:
                # 如果是小数形式（0-1），转换为百分比（0-100）
                if 0 <= clean_value <= 1:
                    return round(clean_value * 100, 3)
                # 如果已经是百分比形式（0-100），直接返回
                elif 0 <= clean_value <= 100:
                    return round(clean_value, 3)
            
            return None
        except (ValueError, TypeError):
            return None
    
    def load_players_season_stats_batch(self, stats_entries: List[Dict]) -> bool:
        """批量加载球员赛季统计数据"""
        if not stats_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for stats_entry in stats_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(PlayerSeasonStats).filter_by(
                            player_key=stats_entry["player_key"],
                            season=stats_entry["season"]
                        ).first()
                        
                        # 提取核心统计项  
                        parsed_stats = self._parse_season_stats_data_from_dict(stats_entry["stats_data"])
                        
                        if existing:
                            # 更新现有记录
                            for key, value in parsed_stats.items():
                                setattr(existing, key, value)
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            season_stats = PlayerSeasonStats(
                                player_key=stats_entry["player_key"],
                                editorial_player_key=stats_entry["editorial_player_key"],
                                league_key=stats_entry["league_key"],
                                season=stats_entry["season"],
                                **parsed_stats
                            )
                            session.add(season_stats)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理球员赛季统计记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['player_season_stats'] = self.stats.get('player_season_stats', 0) + inserted_count
                
                print(f"    💾 球员赛季统计保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存球员赛季统计数据失败: {e}")
            return False
    
    def _parse_season_stats_data_from_dict(self, stats_dict: Dict) -> Dict:
        """从统计字典中解析赛季数据到标准格式"""
        try:
            parsed = {}
            
            # 完整的11个统计项（基于Yahoo stat_categories），赛季统计使用total前缀
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    parsed['field_goals_made'] = self._safe_int(made.strip())
                    parsed['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['field_goals_made'] = None
                    parsed['field_goals_attempted'] = None
            else:
                parsed['field_goals_made'] = None
                parsed['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_dict.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                parsed['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                parsed['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    parsed['free_throws_made'] = self._safe_int(made.strip())
                    parsed['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['free_throws_made'] = None
                    parsed['free_throws_attempted'] = None
            else:
                parsed['free_throws_made'] = None
                parsed['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_dict.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                parsed['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                parsed['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            parsed['three_pointers_made'] = self._safe_int(stats_dict.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS) - 赛季使用total前缀
            parsed['total_points'] = self._safe_int(stats_dict.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            parsed['total_rebounds'] = self._safe_int(stats_dict.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            parsed['total_assists'] = self._safe_int(stats_dict.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            parsed['total_steals'] = self._safe_int(stats_dict.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            parsed['total_blocks'] = self._safe_int(stats_dict.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            parsed['total_turnovers'] = self._safe_int(stats_dict.get('19'))
            
            return parsed
            
        except Exception as e:
            print(f"解析赛季统计字典失败: {e}")
            return {}
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        try:
            if value is None or value == '' or value == '-':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def load_team_matchups_batch(self, matchups_entries: List[Dict]) -> bool:
        """批量加载团队对战数据"""
        if not matchups_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for matchup_entry in matchups_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(TeamMatchups).filter_by(
                            team_key=matchup_entry["team_key"],
                            week=matchup_entry["week"],
                            season=matchup_entry["season"]
                        ).first()
                        
                        if existing:
                            # 更新现有记录
                            existing.opponent_team_key = matchup_entry["opponent_team_key"]
                            existing.is_playoffs = matchup_entry["is_playoffs"]
                            existing.is_consolation = matchup_entry["is_consolation"]
                            existing.is_tied = matchup_entry["is_tied"]
                            existing.winner_team_key = matchup_entry["winner_team_key"]
                            existing.is_matchup_recap_available = matchup_entry["is_matchup_recap_available"]
                            existing.matchup_recap_url = matchup_entry["matchup_recap_url"]
                            existing.matchup_recap_title = matchup_entry["matchup_recap_title"]
                            existing.matchup_grades = matchup_entry["matchup_grades"]
                            existing.projected_team_points = matchup_entry["projected_team_points"]
                            existing.projected_opponent_points = matchup_entry["projected_opponent_points"]
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            team_matchup = TeamMatchups(
                                league_key=matchup_entry["league_key"],
                                team_key=matchup_entry["team_key"],
                                opponent_team_key=matchup_entry["opponent_team_key"],
                                week=matchup_entry["week"],
                                season=matchup_entry["season"],
                                is_playoffs=matchup_entry["is_playoffs"],
                                is_consolation=matchup_entry["is_consolation"],
                                is_tied=matchup_entry["is_tied"],
                                winner_team_key=matchup_entry["winner_team_key"],
                                is_matchup_recap_available=matchup_entry["is_matchup_recap_available"],
                                matchup_recap_url=matchup_entry["matchup_recap_url"],
                                matchup_recap_title=matchup_entry["matchup_recap_title"],
                                matchup_grades=matchup_entry["matchup_grades"],
                                projected_team_points=matchup_entry["projected_team_points"],
                                projected_opponent_points=matchup_entry["projected_opponent_points"]
                            )
                            session.add(team_matchup)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理团队对战记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['team_matchups'] = self.stats.get('team_matchups', 0) + inserted_count
                
                print(f"    💾 团队对战数据保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存团队对战数据失败: {e}")
            return False
    
    def load_team_weekly_stats_batch(self, weekly_stats_entries: List[Dict]) -> bool:
        """批量加载团队每周统计数据"""
        if not weekly_stats_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for stats_entry in weekly_stats_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(TeamStatsWeekly).filter_by(
                            team_key=stats_entry["team_key"],
                            week=stats_entry["week"],
                            season=stats_entry["season"]
                        ).first()
                        
                        # 提取核心统计项
                        parsed_stats = self._parse_team_stats_data_from_dict(stats_entry["stats_data"])
                        
                        if existing:
                            # 更新现有记录
                            for key, value in parsed_stats.items():
                                setattr(existing, key, value)
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            weekly_stats = TeamStatsWeekly(
                                league_key=stats_entry["league_key"],
                                team_key=stats_entry["team_key"],
                                week=stats_entry["week"],
                                season=stats_entry["season"],
                                **parsed_stats
                            )
                            session.add(weekly_stats)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理团队每周统计记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['team_stats_weekly'] = self.stats.get('team_stats_weekly', 0) + inserted_count
                
                print(f"    💾 团队每周统计保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存团队每周统计数据失败: {e}")
            return False
    
    def _parse_team_stats_data_from_dict(self, stats_dict: Dict) -> Dict:
        """从统计字典中解析团队数据到标准格式"""
        try:
            parsed = {}
            
            # 完整的11个统计项（基于Yahoo stat_categories），团队统计
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    parsed['field_goals_made'] = self._safe_int(made.strip())
                    parsed['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['field_goals_made'] = None
                    parsed['field_goals_attempted'] = None
            else:
                parsed['field_goals_made'] = None
                parsed['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_dict.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                parsed['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                parsed['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    parsed['free_throws_made'] = self._safe_int(made.strip())
                    parsed['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['free_throws_made'] = None
                    parsed['free_throws_attempted'] = None
            else:
                parsed['free_throws_made'] = None
                parsed['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_dict.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                parsed['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                parsed['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            parsed['three_pointers_made'] = self._safe_int(stats_dict.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            parsed['points'] = self._safe_int(stats_dict.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            parsed['rebounds'] = self._safe_int(stats_dict.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            parsed['assists'] = self._safe_int(stats_dict.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            parsed['steals'] = self._safe_int(stats_dict.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            parsed['blocks'] = self._safe_int(stats_dict.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            parsed['turnovers'] = self._safe_int(stats_dict.get('19'))
            
            return parsed
            
        except Exception as e:
            print(f"解析团队统计字典失败: {e}")
            return {}
    
    def load_league_standings_batch(self, standings_entries: List[Dict]) -> bool:
        """批量加载联盟排名数据"""
        if not standings_entries:
            return True
        
        try:
            with db_manager.session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for standings_entry in standings_entries:
                    try:
                        # 检查是否已存在
                        existing = session.query(LeagueStandings).filter_by(
                            team_key=standings_entry["team_key"],
                            season=standings_entry["season"]
                        ).first()
                        
                        if existing:
                            # 更新现有记录
                            existing.rank = standings_entry["rank"]
                            existing.wins = standings_entry["wins"]
                            existing.losses = standings_entry["losses"]
                            existing.ties = standings_entry["ties"]
                            existing.win_percentage = standings_entry["win_percentage"]
                            existing.points_for = standings_entry["points_for"]
                            existing.points_against = standings_entry["points_against"]
                            existing.updated_at = datetime.utcnow()
                            updated_count += 1
                        else:
                            # 创建新记录
                            league_standings = LeagueStandings(
                                league_key=standings_entry["league_key"],
                                team_key=standings_entry["team_key"],
                                season=standings_entry["season"],
                                rank=standings_entry["rank"],
                                wins=standings_entry["wins"],
                                losses=standings_entry["losses"],
                                ties=standings_entry["ties"],
                                win_percentage=standings_entry["win_percentage"],
                                points_for=standings_entry["points_for"],
                                points_against=standings_entry["points_against"]
                            )
                            session.add(league_standings)
                            inserted_count += 1
                            
                    except Exception as e:
                        print(f"处理联盟排名记录失败: {e}")
                        continue
                
                # 更新统计
                self.stats['league_standings'] = self.stats.get('league_standings', 0) + inserted_count
                
                print(f"    💾 联盟排名数据保存: {inserted_count} 新增, {updated_count} 更新")
                return True
                
        except Exception as e:
            print(f"保存联盟排名数据失败: {e}")
            return False