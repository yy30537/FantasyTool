#!/usr/bin/env python3
"""
数据库直接写入器 - 将Yahoo API数据直接写入数据库
支持时间序列数据和增量更新
"""

import os
import sys
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model import (
    create_database_engine, create_tables, recreate_tables, get_session,
    Game, League, LeagueSettings, Team, Manager, Player, StatCategory,
    PlayerEligiblePosition, PlayerSeasonStats, PlayerDailyStats,
    TeamStats, MatchupStatWinners,
    RosterDaily, Transaction, TransactionPlayer, DateDimension
)

class FantasyDatabaseWriter:
    """Yahoo Fantasy数据库直接写入器"""
    
    def __init__(self, batch_size: int = 100):
        """初始化数据库写入器
        
        Args:
            batch_size: 批量写入大小，默认100
        """
        self.batch_size = batch_size
        self.engine = create_database_engine()
        
        # 检查并修复表结构问题
        if self._check_table_structure_issues():
            print("🔧 检测到数据库表结构问题，正在修复...")
            recreate_tables(self.engine)
        else:
            create_tables(self.engine)
            
        self.session = get_session(self.engine)
        
        # 统计计数器
        self.stats = {
            'games': 0,
            'leagues': 0,
            'teams': 0,
            'managers': 0,
            'players': 0,
            'player_eligible_positions': 0,
            'stat_categories': 0,
            'player_season_stats': 0,      # 新的混合存储表
            'player_daily_stats': 0,       # 新的混合存储表
            'team_stats': 0,               # 新的混合存储表
            'matchup_stat_winners': 0,
            'roster_daily': 0,
            'transactions': 0,
            'transaction_players': 0,
            'league_settings': 0,
            'date_dimension': 0
        }
    
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
    
    def _check_table_structure_issues(self) -> bool:
        """检查数据库表结构是否存在问题
        
        Returns:
            bool: True表示存在问题需要重新创建表，False表示正常
        """
        try:
            # 创建一个临时session来检查表结构
            temp_session = get_session(self.engine)
            
            try:
                # 测试新的统计值表
                print("🔍 检查 PlayerSeasonStats 表结构...")
                result = temp_session.query(PlayerSeasonStats).first()
                
                print("🔍 检查 PlayerDailyStats 表结构...")
                result = temp_session.query(PlayerDailyStats).first()
                
                print("🔍 检查 TeamStats 表结构...")
                result = temp_session.query(TeamStats).first()
                
                # 测试 DateDimension 表的新字段
                print("🔍 检查 DateDimension 表结构...")
                result = temp_session.query(DateDimension).first()
                
                # 测试 Transaction 表的新字段
                print("🔍 检查 Transaction 表结构...")
                result = temp_session.query(Transaction).first()
                
                temp_session.close()
                print("✓ 表结构检查通过")
                return False  # 没有问题
                
            except Exception as e:
                temp_session.rollback()
                temp_session.close()
                error_msg = str(e).lower()
                print(f"🔍 检测到表结构问题: {e}")
                
                # 检查是否是列不存在或表结构相关错误
                if any(keyword in error_msg for keyword in [
                    "does not exist", "undefinedcolumn", "no such column", 
                    "unknown column", "column", "table", "relation"
                ]):
                    print("🔧 需要重新创建数据库表")
                    return True  # 需要重新创建表
                return False  # 其他类型的错误，不重新创建表
                
        except Exception as e:
            print(f"检查表结构时出错: {e}")
            return True  # 安全起见，重新创建表
    
    def get_stats_summary(self) -> str:
        """获取统计摘要"""
        return (f"统计: 游戏({self.stats['games']}) 联盟({self.stats['leagues']}) "
                f"团队({self.stats['teams']}) 球员({self.stats['players']}) "
                f"交易({self.stats['transactions']}) 交易球员({self.stats['transaction_players']}) "
                f"名单({self.stats['roster_daily']}) 赛季统计({self.stats['player_season_stats']}) "
                f"日期统计({self.stats['player_daily_stats']}) 团队统计({self.stats['team_stats']})")
    
    # ===== 便利方法：支持旧接口 =====
    
    def write_player_season_stats(self, player_key: str, editorial_player_key: str,
                                 league_key: str, stats_data: Dict, season: str) -> bool:
        """写入球员赛季统计（旧接口兼容）"""
        count = self.write_player_season_stat_values(
            player_key=player_key,
            editorial_player_key=editorial_player_key,
            league_key=league_key,
            season=season,
            stats_data=stats_data
        )
        return count > 0
    
    def write_player_daily_stats(self, player_key: str, editorial_player_key: str,
                                league_key: str, stats_data: Dict, season: str,
                                stats_date: date, week: Optional[int] = None) -> bool:
        """写入球员日期统计（旧接口兼容）"""
        count = self.write_player_daily_stat_values(
            player_key=player_key,
            editorial_player_key=editorial_player_key,
            league_key=league_key,
            season=season,
            date_obj=stats_date,
            stats_data=stats_data,
            week=week
        )
        return count > 0
    
    # ===== 基础数据写入方法 =====
    
    def write_games_data(self, games_data: Dict) -> int:
        """写入游戏数据"""
        if not games_data:
            return 0
        
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
            existing = self.session.query(Game).filter_by(game_key=game_info["game_key"]).first()
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
            self.session.add(game)
            count += 1
        
        self.session.commit()
        self.stats['games'] += count
        return count
    
    def write_leagues_data(self, leagues_data: Dict) -> int:
        """写入联盟数据"""
        if not leagues_data:
            return 0
        
        count = 0
        for game_key, leagues in leagues_data.items():
            for league_info in leagues:
                # 检查是否已存在
                existing = self.session.query(League).filter_by(league_key=league_info["league_key"]).first()
                if existing:
                    continue
                
                league = League(
                    league_key=league_info["league_key"],
                    league_id=league_info["league_id"],
                    game_key=game_key,
                    name=league_info["name"],
                    url=league_info.get("url"),
                    logo_url=league_info.get("logo_url") if league_info.get("logo_url") else None,
                    password=league_info.get("password"),
                    draft_status=league_info.get("draft_status"),
                    num_teams=league_info["num_teams"],
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
                    is_pro_league=bool(int(league_info.get("is_pro_league", "0"))),
                    is_cash_league=bool(int(league_info.get("is_cash_league", "0"))),
                    current_week=str(league_info.get("current_week", "")),
                    start_week=league_info.get("start_week"),
                    start_date=league_info.get("start_date"),
                    end_week=league_info.get("end_week"),
                    end_date=league_info.get("end_date"),
                    is_finished=bool(league_info.get("is_finished", 0)),
                    is_plus_league=bool(int(league_info.get("is_plus_league", "0"))),
                    game_code=league_info.get("game_code"),
                    season=league_info.get("season")
                )
                self.session.add(league)
                count += 1
        
        self.session.commit()
        self.stats['leagues'] += count
        return count
    
    def write_league_settings(self, league_key: str, settings_data: Dict) -> bool:
        """写入联盟设置"""
        if not settings_data or "fantasy_content" not in settings_data:
            return False
        
        try:
            settings_info = settings_data["fantasy_content"]["league"][1]["settings"][0]
            
            # 检查是否已存在
            existing = self.session.query(LeagueSettings).filter_by(league_key=league_key).first()
            if existing:
                return False
            
            settings = LeagueSettings(
                league_key=league_key,
                draft_type=settings_info.get("draft_type"),
                is_auction_draft=bool(int(settings_info.get("is_auction_draft", "0"))),
                persistent_url=settings_info.get("persistent_url"),
                uses_playoff=bool(int(settings_info.get("uses_playoff", "1"))),
                has_playoff_consolation_games=settings_info.get("has_playoff_consolation_games", False),
                playoff_start_week=settings_info.get("playoff_start_week"),
                uses_playoff_reseeding=bool(settings_info.get("uses_playoff_reseeding", 0)),
                uses_lock_eliminated_teams=bool(settings_info.get("uses_lock_eliminated_teams", 0)),
                num_playoff_teams=int(settings_info.get("num_playoff_teams", 0)) if settings_info.get("num_playoff_teams") else None,
                num_playoff_consolation_teams=settings_info.get("num_playoff_consolation_teams", 0),
                has_multiweek_championship=bool(settings_info.get("has_multiweek_championship", 0)),
                waiver_type=settings_info.get("waiver_type"),
                waiver_rule=settings_info.get("waiver_rule"),
                uses_faab=bool(int(settings_info.get("uses_faab", "0"))),
                draft_time=settings_info.get("draft_time"),
                draft_pick_time=settings_info.get("draft_pick_time"),
                post_draft_players=settings_info.get("post_draft_players"),
                max_teams=int(settings_info.get("max_teams", 0)) if settings_info.get("max_teams") else None,
                waiver_time=settings_info.get("waiver_time"),
                trade_end_date=settings_info.get("trade_end_date"),
                trade_ratify_type=settings_info.get("trade_ratify_type"),
                trade_reject_time=settings_info.get("trade_reject_time"),
                player_pool=settings_info.get("player_pool"),
                cant_cut_list=settings_info.get("cant_cut_list"),
                draft_together=bool(settings_info.get("draft_together", 0)),
                is_publicly_viewable=bool(int(settings_info.get("is_publicly_viewable", "1"))),
                can_trade_draft_picks=bool(int(settings_info.get("can_trade_draft_picks", "0"))),
                sendbird_channel_url=settings_info.get("sendbird_channel_url"),
                roster_positions=settings_info.get("roster_positions"),
                stat_categories=settings_info.get("stat_categories")
            )
            self.session.add(settings)
            self.session.commit()
            self.stats['league_settings'] += 1
            
            # 提取并写入统计类别
            stat_categories = settings_info.get("stat_categories")
            if stat_categories:
                categories_count = self.write_stat_categories(league_key, stat_categories)
                print(f"✓ 提取并写入 {categories_count} 个统计类别定义")
            
            return True
            
        except Exception as e:
            print(f"写入联盟设置失败 {league_key}: {e}")
            self.session.rollback()
            return False
    
    # ===== 统计类别管理方法 =====
    
    def write_stat_categories(self, league_key: str, stat_categories_data: Dict) -> int:
        """写入统计类别定义到数据库"""
        count = 0
        
        try:
            if not stat_categories_data or 'stats' not in stat_categories_data:
                return 0
            
            stats_list = stat_categories_data['stats']
            
            for stat_item in stats_list:
                if 'stat' not in stat_item:
                    continue
                    
                stat_info = stat_item['stat']
                stat_id = stat_info.get('stat_id')
                
                if not stat_id:
                    continue
                
            # 检查是否已存在
                existing = self.session.query(StatCategory).filter_by(
                    league_key=league_key,
                    stat_id=stat_id
                ).first()
                
                if existing:
                    # 更新现有记录
                    existing.name = stat_info.get('name', '')
                    existing.display_name = stat_info.get('display_name', '')
                    existing.abbr = stat_info.get('abbr', '')
                    existing.group_name = stat_info.get('group', '')
                    existing.sort_order = int(stat_info.get('sort_order', 0))
                    existing.position_type = stat_info.get('position_type', '')
                    existing.is_enabled = bool(int(stat_info.get('enabled', '1')))
                    existing.is_only_display_stat = bool(int(stat_info.get('is_only_display_stat', '0')))
                    existing.updated_at = datetime.utcnow()
                else:
                    # 创建新记录
                    stat_category = StatCategory(
                        league_key=league_key,
                        stat_id=stat_id,
                        name=stat_info.get('name', ''),
                        display_name=stat_info.get('display_name', ''),
                        abbr=stat_info.get('abbr', ''),
                        group_name=stat_info.get('group', ''),
                        sort_order=int(stat_info.get('sort_order', 0)),
                        position_type=stat_info.get('position_type', ''),
                        is_enabled=bool(int(stat_info.get('enabled', '1'))),
                        is_only_display_stat=bool(int(stat_info.get('is_only_display_stat', '0')))
                    )
                    self.session.add(stat_category)
                    count += 1
            
            self.session.commit()
            self.stats['stat_categories'] += count
            return count
            
        except Exception as e:
            print(f"写入统计类别失败: {e}")
            self.session.rollback()
            return 0
    
    def get_stat_category_info(self, league_key: str, stat_id: int) -> Optional[Dict]:
        """获取统计类别信息"""
        try:
            stat_cat = self.session.query(StatCategory).filter_by(
                league_key=league_key,
                stat_id=stat_id
            ).first()
            
            if stat_cat:
                return {
                    'name': stat_cat.name,
                    'display_name': stat_cat.display_name,
                    'abbr': stat_cat.abbr,
                    'group': stat_cat.group_name
                }
            return None
            
        except Exception as e:
            print(f"获取统计类别信息失败 {stat_id}: {e}")
            return None
    
    # ===== 混合存储统计值写入方法 =====
    
    def write_player_season_stat_values(self, player_key: str, editorial_player_key: str,
                                       league_key: str, season: str, stats_data: Dict) -> int:
        """写入球员赛季统计值（混合存储：JSON + 核心统计列）"""
        try:
            # 检查是否已存在
            existing = self.session.query(PlayerSeasonStats).filter_by(
                player_key=player_key,
                season=season
            ).first()
            
            # 提取核心统计项
            core_stats = self._extract_core_season_stats(stats_data)
            
            if existing:
                # 更新现有记录
                existing.stats_data = stats_data
                existing.total_points = core_stats.get('total_points')
                existing.games_played = core_stats.get('games_played')
                existing.avg_points = core_stats.get('avg_points')
                existing.total_assists = core_stats.get('total_assists')
                existing.total_rebounds = core_stats.get('total_rebounds')
                existing.total_steals = core_stats.get('total_steals')
                existing.total_blocks = core_stats.get('total_blocks')
                existing.field_goal_percentage = core_stats.get('field_goal_percentage')
                existing.free_throw_percentage = core_stats.get('free_throw_percentage')
                existing.three_point_percentage = core_stats.get('three_point_percentage')
                existing.updated_at = datetime.utcnow()
                self.stats['player_season_stats_updated'] = self.stats.get('player_season_stats_updated', 0) + 1
            else:
                # 创建新记录
                season_stats = PlayerSeasonStats(
                    player_key=player_key,
                    editorial_player_key=editorial_player_key,
                    league_key=league_key,
                    season=season,
                    stats_data=stats_data,
                    total_points=core_stats.get('total_points'),
                    games_played=core_stats.get('games_played'),
                    avg_points=core_stats.get('avg_points'),
                    total_assists=core_stats.get('total_assists'),
                    total_rebounds=core_stats.get('total_rebounds'),
                    total_steals=core_stats.get('total_steals'),
                    total_blocks=core_stats.get('total_blocks'),
                    field_goal_percentage=core_stats.get('field_goal_percentage'),
                    free_throw_percentage=core_stats.get('free_throw_percentage'),
                    three_point_percentage=core_stats.get('three_point_percentage')
                )
                self.session.add(season_stats)
                self.stats['player_season_stats'] = self.stats.get('player_season_stats', 0) + 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            print(f"写入球员赛季统计失败: {e}")
            self.session.rollback()
            return 0
    
    def _extract_core_season_stats(self, stats_data: Dict) -> Dict:
        """从统计数据中提取核心赛季统计项"""
        core_stats = {}
        
        try:
            # NBA统计项映射 (Yahoo stat_id -> 核心统计)
            total_points = self._safe_float(stats_data.get('9999'))  # Fantasy Points总分
            core_stats['total_points'] = total_points
            
            # 计算场次和平均分
            games_played = self._safe_int(stats_data.get('0'))  # 假设0是出场次数
            core_stats['games_played'] = games_played
            
            if total_points and games_played and games_played > 0:
                core_stats['avg_points'] = round(total_points / games_played, 2)
            
            # 其他核心统计
            core_stats['total_assists'] = self._safe_int(stats_data.get('5'))  # 助攻
            core_stats['total_rebounds'] = self._safe_int(stats_data.get('8'))  # 篮板
            core_stats['total_steals'] = self._safe_int(stats_data.get('6'))  # 抢断
            core_stats['total_blocks'] = self._safe_int(stats_data.get('7'))  # 盖帽
            
            # 百分比统计
            fg_made = self._safe_int(stats_data.get('0'))  # 投篮命中
            fg_attempted = self._safe_int(stats_data.get('1'))  # 投篮尝试
            if fg_made and fg_attempted and fg_attempted > 0:
                core_stats['field_goal_percentage'] = round((fg_made / fg_attempted) * 100, 1)
            
            ft_made = self._safe_int(stats_data.get('2'))  # 罚球命中
            ft_attempted = self._safe_int(stats_data.get('3'))  # 罚球尝试
            if ft_made and ft_attempted and ft_attempted > 0:
                core_stats['free_throw_percentage'] = round((ft_made / ft_attempted) * 100, 1)
            
            three_made = self._safe_int(stats_data.get('4'))  # 三分命中
            three_attempted = self._safe_int(stats_data.get('12'))  # 假设12是三分尝试
            if three_made and three_attempted and three_attempted > 0:
                core_stats['three_point_percentage'] = round((three_made / three_attempted) * 100, 1)
            
        except Exception as e:
            print(f"提取核心赛季统计失败: {e}")
        
        return core_stats
    
    def write_player_daily_stat_values(self, player_key: str, editorial_player_key: str,
                                     league_key: str, season: str, date_obj: date,
                                     stats_data: Dict, week: Optional[int] = None) -> int:
        """写入球员日期统计值（混合存储：JSON + 核心统计列）"""
        try:
            # 检查是否已存在
            existing = self.session.query(PlayerDailyStats).filter_by(
                player_key=player_key,
                date=date_obj
            ).first()
            
            # 提取核心统计项
            core_stats = self._extract_core_daily_stats(stats_data)
            
            if existing:
                # 更新现有记录
                existing.stats_data = stats_data
                existing.week = week
                existing.points = core_stats.get('points')
                existing.assists = core_stats.get('assists')
                existing.rebounds = core_stats.get('rebounds')
                existing.steals = core_stats.get('steals')
                existing.blocks = core_stats.get('blocks')
                existing.turnovers = core_stats.get('turnovers')
                existing.field_goals_made = core_stats.get('field_goals_made')
                existing.field_goals_attempted = core_stats.get('field_goals_attempted')
                existing.free_throws_made = core_stats.get('free_throws_made')
                existing.free_throws_attempted = core_stats.get('free_throws_attempted')
                existing.three_pointers_made = core_stats.get('three_pointers_made')
                existing.updated_at = datetime.utcnow()
                self.stats['player_daily_stats_updated'] = self.stats.get('player_daily_stats_updated', 0) + 1
            else:
                # 创建新记录
                daily_stats = PlayerDailyStats(
                    player_key=player_key,
                    editorial_player_key=editorial_player_key,
                    league_key=league_key,
                    season=season,
                    date=date_obj,
                    week=week,
                    stats_data=stats_data,
                    points=core_stats.get('points'),
                    assists=core_stats.get('assists'),
                    rebounds=core_stats.get('rebounds'),
                    steals=core_stats.get('steals'),
                    blocks=core_stats.get('blocks'),
                    turnovers=core_stats.get('turnovers'),
                    field_goals_made=core_stats.get('field_goals_made'),
                    field_goals_attempted=core_stats.get('field_goals_attempted'),
                    free_throws_made=core_stats.get('free_throws_made'),
                    free_throws_attempted=core_stats.get('free_throws_attempted'),
                    three_pointers_made=core_stats.get('three_pointers_made')
                )
                self.session.add(daily_stats)
                self.stats['player_daily_stats'] = self.stats.get('player_daily_stats', 0) + 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            print(f"写入球员日期统计失败: {e}")
            self.session.rollback()
            return 0
    
    def _extract_core_daily_stats(self, stats_data: Dict) -> Dict:
        """从统计数据中提取核心日期统计项"""
        core_stats = {}
        
        try:
            # NBA统计项映射 (Yahoo stat_id -> 核心统计)
            core_stats['points'] = self._safe_float(stats_data.get('9999'))  # Fantasy Points
            core_stats['assists'] = self._safe_int(stats_data.get('5'))  # 助攻
            core_stats['rebounds'] = self._safe_int(stats_data.get('8'))  # 篮板  
            core_stats['steals'] = self._safe_int(stats_data.get('6'))  # 抢断
            core_stats['blocks'] = self._safe_int(stats_data.get('7'))  # 盖帽
            core_stats['turnovers'] = self._safe_int(stats_data.get('9'))  # 失误
            core_stats['field_goals_made'] = self._safe_int(stats_data.get('0'))  # 投篮命中
            core_stats['field_goals_attempted'] = self._safe_int(stats_data.get('1'))  # 投篮尝试
            core_stats['free_throws_made'] = self._safe_int(stats_data.get('2'))  # 罚球命中
            core_stats['free_throws_attempted'] = self._safe_int(stats_data.get('3'))  # 罚球尝试
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('4'))  # 三分命中
            
        except Exception as e:
            print(f"提取核心日期统计失败: {e}")
        
        return core_stats
    
    def write_team_stat_values(self, team_key: str, league_key: str, season: str,
                             coverage_type: str, stats_data: Dict,
                             week: Optional[int] = None, date_obj: Optional[date] = None,
                             opponent_team_key: Optional[str] = None,
                             is_playoff: bool = False, win: Optional[bool] = None,
                             stats_winners: Optional[Dict] = None) -> int:
        """写入团队统计值（混合存储：JSON + 核心统计列）"""
        try:
            # 检查是否已存在
            existing = self.session.query(TeamStats).filter_by(
                team_key=team_key,
                coverage_type=coverage_type,
                season=season,
                week=week,
                date=date_obj
            ).first()
            
            # 提取核心统计项
            core_stats = self._extract_core_team_stats(stats_data)
            
            if existing:
                # 更新现有记录
                existing.stats_data = stats_data
                existing.stats_winners = stats_winners
                existing.total_points = core_stats.get('total_points')
                existing.wins = core_stats.get('wins')
                existing.losses = core_stats.get('losses')
                existing.ties = core_stats.get('ties')
                existing.win_percentage = core_stats.get('win_percentage')
                existing.opponent_team_key = opponent_team_key
                existing.is_playoff = is_playoff
                existing.win = win
                existing.updated_at = datetime.utcnow()
                self.stats['team_stats_updated'] = self.stats.get('team_stats_updated', 0) + 1
            else:
                # 创建新记录
                team_stats = TeamStats(
                    team_key=team_key,
                    league_key=league_key,
                    season=season,
                    coverage_type=coverage_type,
                    week=week,
                    date=date_obj,
                    stats_data=stats_data,
                    stats_winners=stats_winners,
                    total_points=core_stats.get('total_points'),
                    wins=core_stats.get('wins'),
                    losses=core_stats.get('losses'),
                    ties=core_stats.get('ties'),
                    win_percentage=core_stats.get('win_percentage'),
                    opponent_team_key=opponent_team_key,
                    is_playoff=is_playoff,
                    win=win
                )
                self.session.add(team_stats)
                self.stats['team_stats'] = self.stats.get('team_stats', 0) + 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            print(f"写入团队统计失败: {e}")
            self.session.rollback()
            return 0
    
    def _extract_core_team_stats(self, stats_data: Dict) -> Dict:
        """从统计数据中提取核心团队统计项"""
        core_stats = {}
        
        try:
            # 团队核心统计项
            core_stats['total_points'] = self._safe_float(stats_data.get('9999'))  # Fantasy Points
            core_stats['wins'] = self._safe_int(stats_data.get('60', '0'))  # 获胜数
            core_stats['losses'] = self._safe_int(stats_data.get('61', '0'))  # 失败数  
            core_stats['ties'] = self._safe_int(stats_data.get('62', '0'))  # 平局数
            
            # 计算胜率
            wins = core_stats['wins'] or 0
            losses = core_stats['losses'] or 0
            ties = core_stats['ties'] or 0
            total_games = wins + losses + ties
            
            if total_games > 0:
                core_stats['win_percentage'] = round((wins + ties * 0.5) / total_games * 100, 1)
            
        except Exception as e:
            print(f"提取核心团队统计失败: {e}")
        
        return core_stats
    
    def write_matchup_stat_winners(self, league_key: str, season: str, week: int, 
                                 stat_winners: List) -> int:
        """写入比赛统计类别获胜者（移除冗余字段）"""
        try:
            count = 0
            
            for stat_winner_item in stat_winners:
                if not isinstance(stat_winner_item, dict) or "stat_winner" not in stat_winner_item:
                    continue
                
                stat_winner = stat_winner_item["stat_winner"]
                if not isinstance(stat_winner, dict):
                    continue
                
                stat_id = stat_winner.get("stat_id")
                winner_team_key = stat_winner.get("winner_team_key")
                is_tied = bool(stat_winner.get("is_tied", 0))
                
                if not stat_id or not winner_team_key:
                    continue
                
                try:
                    # 检查是否已存在
                    existing = self.session.query(MatchupStatWinners).filter_by(
                        league_key=league_key,
                        week=week,
                        stat_id=int(stat_id)
                    ).first()
                    
                    if existing:
                        # 更新现有记录
                        existing.winner_team_key = winner_team_key
                        existing.is_tied = is_tied
                        existing.updated_at = datetime.utcnow()
                        self.stats['matchup_stat_winners_updated'] = self.stats.get('matchup_stat_winners_updated', 0) + 1
                    else:
                        # 创建新记录
                        winner_record = MatchupStatWinners(
                            league_key=league_key,
                            season=season,
                            week=week,
                            stat_id=int(stat_id),
                            winner_team_key=winner_team_key,
                            is_tied=is_tied
                        )
                        self.session.add(winner_record)
                        count += 1
                        
                except Exception as e:
                    print(f"处理获胜者统计 {stat_id} 失败: {e}")
                    continue
            
            if count > 0:
                self.session.commit()
                self.stats['matchup_stat_winners'] = self.stats.get('matchup_stat_winners', 0) + count
            
            return count
            
        except Exception as e:
            print(f"写入比赛统计获胜者失败: {e}")
            self.session.rollback()
            return 0
    
    def write_player_eligible_positions(self, player_key: str, positions: List) -> int:
        """写入球员合适位置"""
        count = 0
        
        try:
            # 先删除现有的位置记录
            self.session.query(PlayerEligiblePosition).filter_by(
                player_key=player_key
            ).delete()
            
            # 添加新的位置记录
            for position in positions:
                if isinstance(position, dict):
                    position_str = position.get('position', '')
                else:
                    position_str = str(position)
                
                if position_str:
                    eligible_pos = PlayerEligiblePosition(
                        player_key=player_key,
                        position=position_str
                    )
                    self.session.add(eligible_pos)
                    count += 1
            
            self.session.commit()
            self.stats['player_eligible_positions'] += count
            return count
            
        except Exception as e:
            print(f"写入球员合适位置失败 {player_key}: {e}")
            self.session.rollback()
            return 0

    def write_roster_daily(self, team_key: str, player_key: str, league_key: str,
                          roster_date: date, season: str,
                           week: Optional[int] = None,
                           selected_position: Optional[str] = None,
                           is_starting: bool = False,
                          is_bench: bool = False,
                          is_injured_reserve: bool = False,
                           player_status: Optional[str] = None,
                          status_full: Optional[str] = None,
                          injury_note: Optional[str] = None,
                          is_keeper: bool = False,
                          keeper_cost: Optional[str] = None,
                          is_prescoring: bool = False,
                          is_editable: bool = False) -> bool:
        """写入每日名单数据"""
        try:
            # 检查是否已存在 - 使用新的date列名
            existing = self.session.query(RosterDaily).filter_by(
                team_key=team_key,
                player_key=player_key,
                date=roster_date
            ).first()
            
            if existing:
                # 更新现有记录
                existing.selected_position = selected_position
                existing.is_starting = is_starting
                existing.is_bench = is_bench
                existing.is_injured_reserve = is_injured_reserve
                existing.player_status = player_status
                existing.status_full = status_full
                existing.injury_note = injury_note
                existing.is_keeper = is_keeper
                existing.keeper_cost = keeper_cost
                existing.is_prescoring = is_prescoring
                existing.is_editable = is_editable
                existing.week = week
                existing.updated_at = datetime.utcnow()
            else:
                # 创建新记录 - 使用新的date列名
                roster_daily = RosterDaily(
                    team_key=team_key,
                    player_key=player_key,
                    league_key=league_key,
                    date=roster_date,
                    season=season,
                    week=week,
                    selected_position=selected_position,
                    is_starting=is_starting,
                    is_bench=is_bench,
                    is_injured_reserve=is_injured_reserve,
                    player_status=player_status,
                    status_full=status_full,
                    injury_note=injury_note,
                    is_keeper=is_keeper,
                    keeper_cost=keeper_cost,
                    is_prescoring=is_prescoring,
                    is_editable=is_editable
                )
                self.session.add(roster_daily)
                self.stats['roster_daily'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入每日名单失败 {team_key}/{player_key}: {e}")
            self.session.rollback()
            return False
    
    def write_date_dimension(self, date_obj: date, league_key: str, season: str) -> bool:
        """写入日期维度数据"""
        try:
            # 检查是否已存在
            existing = self.session.query(DateDimension).filter_by(
                date=date_obj,
                league_key=league_key
            ).first()
            
            if existing:
                return True  # 已存在，不需要更新
            
            # 创建新记录
            date_dim = DateDimension(
                date=date_obj,
                league_key=league_key,
                season=season
            )
            self.session.add(date_dim)
            self.stats['date_dimension'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入日期维度失败 {date_obj}: {e}")
            self.session.rollback()
            return False
    
    def write_date_dimensions_batch(self, dates_data: List[Dict]) -> int:
        """批量写入赛季日期维度数据
        
        Args:
            dates_data: 包含日期信息的字典列表，每个字典应包含:
                - date: date对象
                - league_key: 联盟键  
                - season: 赛季
        """
        count = 0
        
        for date_data in dates_data:
            try:
                date_obj = date_data['date']
                league_key = date_data['league_key']
                
                # 检查是否已存在
                existing = self.session.query(DateDimension).filter_by(
                    date=date_obj,
                    league_key=league_key
                ).first()
                
                if existing:
                    continue
                
                date_dim = DateDimension(
                    date=date_obj,
                    league_key=league_key,
                    season=date_data.get('season', '2024')
                )
                
                self.session.add(date_dim)
                count += 1
                
                # 批量提交
                if count % self.batch_size == 0:
                    self.session.commit()
                    
            except Exception as e:
                print(f"写入日期维度失败 {date_data.get('date', 'unknown')}: {e}")
                self.session.rollback()
                continue
        
        # 最终提交
        try:
            if count > 0:
                self.session.commit()
                self.stats['date_dimension'] += count
        except Exception as e:
            print(f"批量提交日期维度失败: {e}")
            self.session.rollback()
        
        return count
    
    # ===== 批量写入方法 =====
    
    def write_players_batch(self, players_data: List[Dict], league_key: str) -> int:
        """批量写入球员数据"""
        count = 0
        
        for player_data in players_data:
            try:
                player_key = player_data.get("player_key")
                if not player_key:
                    continue
                
                # 检查是否已存在
                existing = self.session.query(Player).filter_by(player_key=player_key).first()
                if existing:
                    continue
                
                # 处理布尔值转换
                is_undroppable = player_data.get("is_undroppable", False)
                if isinstance(is_undroppable, str):
                    is_undroppable = bool(int(is_undroppable)) if is_undroppable.isdigit() else False
                elif is_undroppable is None:
                    is_undroppable = False
                
                player = Player(
                    player_key=player_key,
                    player_id=player_data.get("player_id"),
                    editorial_player_key=player_data.get("editorial_player_key"),
                    league_key=league_key,
                    full_name=player_data.get("full_name"),
                    first_name=player_data.get("first_name"),
                    last_name=player_data.get("last_name"),
                    current_team_key=player_data.get("current_team_key"),
                    current_team_name=player_data.get("current_team_name"),
                    current_team_abbr=player_data.get("current_team_abbr"),
                    display_position=player_data.get("display_position"),
                    primary_position=player_data.get("primary_position"),
                    position_type=player_data.get("position_type"),
                    uniform_number=player_data.get("uniform_number"),
                    status=player_data.get("status"),
                    image_url=player_data.get("image_url"),
                    headshot_url=player_data.get("headshot_url"),
                    is_undroppable=is_undroppable,
                    season=player_data.get("season"),
                    last_updated=datetime.now()
                )
                self.session.add(player)
                count += 1
                
                # 处理合适位置
                eligible_positions = player_data.get("eligible_positions", [])
                if eligible_positions:
                    self.write_player_eligible_positions(player_key, eligible_positions)
                
                # 批量提交
                if count % self.batch_size == 0:
                    self.session.commit()
                    
            except Exception as e:
                print(f"写入球员失败 {player_data.get('player_key', 'unknown')}: {e}")
                self.session.rollback()
                continue
        
        # 最终提交
        try:
            if count > 0:
                self.session.commit()
                self.stats['players'] += count
        except Exception as e:
            print(f"批量提交失败: {e}")
            self.session.rollback()
        
        return count
    
    def write_teams_batch(self, teams_data: List[Dict], league_key: str) -> int:
        """批量写入团队数据"""
        count = 0
        
        for team_data in teams_data:
            try:
                team_key = team_data.get("team_key")
                if not team_key:
                    continue
                
                # 检查是否已存在
                existing = self.session.query(Team).filter_by(team_key=team_key).first()
                if existing:
                    continue
                
                # 处理布尔值转换
                clinched_playoffs = team_data.get("clinched_playoffs", False)
                if isinstance(clinched_playoffs, str):
                    clinched_playoffs = bool(int(clinched_playoffs)) if clinched_playoffs.isdigit() else False
                elif clinched_playoffs is None:
                    clinched_playoffs = False
                
                has_draft_grade = team_data.get("has_draft_grade", False)
                if isinstance(has_draft_grade, str):
                    has_draft_grade = bool(int(has_draft_grade)) if has_draft_grade.isdigit() else False
                elif has_draft_grade is None:
                    has_draft_grade = False
                
                team = Team(
                    team_key=team_key,
                    team_id=team_data.get("team_id"),
                    league_key=league_key,
                    name=team_data.get("name"),
                    url=team_data.get("url"),
                    team_logo_url=team_data.get("team_logo_url"),
                    division_id=team_data.get("division_id"),
                    waiver_priority=team_data.get("waiver_priority"),
                    faab_balance=team_data.get("faab_balance"),
                    number_of_moves=team_data.get("number_of_moves", 0),
                    number_of_trades=team_data.get("number_of_trades", 0),
                    roster_adds_week=str(team_data.get("roster_adds_week", "")),
                    roster_adds_value=team_data.get("roster_adds_value"),
                    clinched_playoffs=clinched_playoffs,
                    has_draft_grade=has_draft_grade
                )
                self.session.add(team)
                count += 1
                
                # 处理manager数据
                managers_data = team_data.get("managers", [])
                for manager_data in managers_data:
                    if isinstance(manager_data, dict) and "manager" in manager_data:
                        manager_info = manager_data["manager"]
                    else:
                        manager_info = manager_data
                    
                    if not manager_info.get("manager_id"):
                        continue
                    
                    # 检查manager是否已存在
                    existing_manager = self.session.query(Manager).filter_by(
                        manager_id=manager_info["manager_id"], 
                        team_key=team_key
                    ).first()
                    if existing_manager:
                        continue
                    
                    is_commissioner = manager_info.get("is_commissioner", False)
                    if isinstance(is_commissioner, str):
                        is_commissioner = bool(int(is_commissioner)) if is_commissioner.isdigit() else False
                    elif is_commissioner is None:
                        is_commissioner = False
                    
                    manager = Manager(
                        manager_id=manager_info["manager_id"],
                        team_key=team_key,
                        nickname=manager_info.get("nickname"),
                        guid=manager_info.get("guid"),
                        is_commissioner=is_commissioner,
                        email=manager_info.get("email"),
                        image_url=manager_info.get("image_url"),
                        felo_score=manager_info.get("felo_score"),
                        felo_tier=manager_info.get("felo_tier")
                    )
                    self.session.add(manager)
                    self.stats['managers'] += 1
                
                # 批量提交
                if count % self.batch_size == 0:
                    self.session.commit()
                    
            except Exception as e:
                print(f"写入团队失败 {team_data.get('team_key', 'unknown')}: {e}")
                self.session.rollback()
                continue
        
        # 最终提交
        try:
            if count > 0:
                self.session.commit()
                self.stats['teams'] += count
        except Exception as e:
            print(f"批量提交团队失败: {e}")
            self.session.rollback()
        
        return count
    
    def write_transactions_batch(self, transactions_data: List[Dict], league_key: str) -> int:
        """批量写入交易数据"""
        count = 0
        
        for transaction_data in transactions_data:
            try:
                transaction_key = transaction_data.get("transaction_key")
                if not transaction_key:
                    continue
                
                # 检查是否已存在
                existing = self.session.query(Transaction).filter_by(transaction_key=transaction_key).first()
                if existing:
                    continue
                
                transaction_type = transaction_data.get("type", "unknown")
                
                # 处理交易相关字段
                trader_team_key = None
                trader_team_name = None
                tradee_team_key = None
                tradee_team_name = None
                picks_data = None
                
                if transaction_type == "trade":
                    trader_team_key = transaction_data.get("trader_team_key")
                    trader_team_name = transaction_data.get("trader_team_name")
                    tradee_team_key = transaction_data.get("tradee_team_key")
                    tradee_team_name = transaction_data.get("tradee_team_name")
                    picks_data = transaction_data.get("picks")
                
                transaction = Transaction(
                    transaction_key=transaction_key,
                    transaction_id=transaction_data.get("transaction_id"),
                    league_key=league_key,
                    type=transaction_type,
                    status=transaction_data.get("status"),
                    timestamp=transaction_data.get("timestamp"),
                    trader_team_key=trader_team_key,
                    trader_team_name=trader_team_name,
                    tradee_team_key=tradee_team_key,
                    tradee_team_name=tradee_team_name,
                    picks_data=picks_data,
                    players_data=transaction_data.get("players", {})
                )
                self.session.add(transaction)
                count += 1
                
                # 处理transaction_players数据
                players_data = transaction_data.get("players", {})
                if isinstance(players_data, dict):
                    for key, player_data in players_data.items():
                        if key == "count":
                            continue
                        
                        if not isinstance(player_data, dict) or "player" not in player_data:
                            continue
                        
                        player_info_list = player_data["player"]
                        if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                            continue
                        
                        player_info = player_info_list[0]
                        transaction_info_container = player_info_list[1]
                        
                        if not isinstance(transaction_info_container, dict) or "transaction_data" not in transaction_info_container:
                            continue
                        
                        # 提取player基本信息
                        player_key = None
                        player_id = None
                        player_name = None
                        editorial_team_abbr = None
                        display_position = None
                        position_type = None
                        
                        if isinstance(player_info, list):
                            for item in player_info:
                                if isinstance(item, dict):
                                    if "player_key" in item:
                                        player_key = item["player_key"]
                                    elif "player_id" in item:
                                        player_id = item["player_id"]
                                    elif "name" in item and isinstance(item["name"], dict):
                                        player_name = item["name"].get("full")
                                    elif "editorial_team_abbr" in item:
                                        editorial_team_abbr = item["editorial_team_abbr"]
                                    elif "display_position" in item:
                                        display_position = item["display_position"]
                                    elif "position_type" in item:
                                        position_type = item["position_type"]
                        elif isinstance(player_info, dict):
                            player_key = player_info.get("player_key")
                            player_id = player_info.get("player_id")
                            if "name" in player_info and isinstance(player_info["name"], dict):
                                player_name = player_info["name"].get("full")
                            editorial_team_abbr = player_info.get("editorial_team_abbr")
                            display_position = player_info.get("display_position")
                            position_type = player_info.get("position_type")
                        
                        if not player_key:
                            continue
                        
                        # 提取transaction信息
                        transaction_info_data = transaction_info_container["transaction_data"]
                        if isinstance(transaction_info_data, list) and len(transaction_info_data) > 0:
                            transaction_info = transaction_info_data[0]
                        elif isinstance(transaction_info_data, dict):
                            transaction_info = transaction_info_data
                        else:
                            continue
                        
                        # 检查是否已存在
                        existing_tp = self.session.query(TransactionPlayer).filter_by(
                            transaction_key=transaction_key,
                            player_key=player_key
                        ).first()
                        if existing_tp:
                            continue
                        
                        transaction_player = TransactionPlayer(
                            transaction_key=transaction_key,
                            player_key=player_key,
                            player_id=player_id,
                            player_name=player_name,
                            editorial_team_abbr=editorial_team_abbr,
                            display_position=display_position,
                            position_type=position_type,
                            transaction_type=transaction_info.get("type"),
                            source_type=transaction_info.get("source_type"),
                            source_team_key=transaction_info.get("source_team_key"),
                            source_team_name=transaction_info.get("source_team_name"),
                            destination_type=transaction_info.get("destination_type"),
                            destination_team_key=transaction_info.get("destination_team_key"),
                            destination_team_name=transaction_info.get("destination_team_name")
                        )
                        self.session.add(transaction_player)
                        self.stats['transaction_players'] += 1
                
                # 批量提交
                if count % self.batch_size == 0:
                    self.session.commit()
                    
            except Exception as e:
                print(f"写入交易失败 {transaction_data.get('transaction_key', 'unknown')}: {e}")
                self.session.rollback()
                continue
        
        # 最终提交
        try:
            if count > 0:
                self.session.commit()
                self.stats['transactions'] += count
        except Exception as e:
            print(f"批量提交交易失败: {e}")
            self.session.rollback()
        
        return count
    
    # ===== 便捷方法 =====
    
    def parse_coverage_date(self, date_str: Union[str, None]) -> Optional[date]:
        """解析日期字符串为date对象"""
        if not date_str:
            return None
        try:
            if isinstance(date_str, str):
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            return date_str
        except:
            return None
    
    def parse_week(self, week_str: Union[str, int, None]) -> Optional[int]:
        """解析周数"""
        if week_str is None:
            return None
        try:
            return int(week_str)
        except:
            return None 
    
    def clear_database(self, confirm: bool = False) -> bool:
        """清空数据库 - 重新创建所有表"""
        if not confirm:
            print("⚠️ 此操作将删除所有数据，请确认!")
            response = input("输入 'YES' 来确认清空数据库: ").strip()
            if response != 'YES':
                print("❌ 操作已取消")
                return False
        
        print("🔄 重新创建数据库表...")
        try:
            # 关闭当前连接
            self.close()
            
            # 使用recreate_tables重建所有表
            from model import recreate_tables, create_database_engine
            engine = create_database_engine()
            
            success = recreate_tables(engine)
            if success:
                # 重新初始化连接
                self.__init__(self.batch_size)
                print("✅ 数据库清空并重建成功")
                return True
            else:
                print("❌ 数据库重建失败")
                return False
                
        except Exception as e:
            print(f"❌ 清空数据库失败: {e}")
            return False
    
    def get_database_summary(self) -> Dict[str, int]:
        """获取数据库摘要信息"""
        summary = {}
        
        # 定义所有表和对应的模型类
        tables = {
            'games': Game,
            'leagues': League,
            'league_settings': LeagueSettings,
            'stat_categories': StatCategory,
            'teams': Team,
            'managers': Manager,
            'players': Player,
            'player_eligible_positions': PlayerEligiblePosition,
            'player_season_stats': PlayerSeasonStats,        # 更新为新的混合存储表
            'player_daily_stats': PlayerDailyStats,          # 更新为新的混合存储表
            'team_stats': TeamStats,                         # 更新为新的混合存储表
            'matchup_stat_winners': MatchupStatWinners,
            'roster_daily': RosterDaily,
            'transactions': Transaction,
            'transaction_players': TransactionPlayer,
            'date_dimension': DateDimension
        }
        
        for table_name, model_class in tables.items():
            try:
                count = self.session.query(model_class).count()
                summary[table_name] = count
            except Exception as e:
                print(f"查询 {table_name} 表时出错: {e}")
                summary[table_name] = -1  # 表示查询失败
        
        return summary
    
    # ===== 便捷方法（为向后兼容而保留的方法名） =====
    
    def write_player_season_stats(self, player_key: str, editorial_player_key: str,
                                 league_key: str, stats_data: Dict, season: str) -> bool:
        """写入球员赛季统计（便捷方法，调用混合存储方法）"""
        result = self.write_player_season_stat_values(player_key, editorial_player_key, 
                                                    league_key, season, stats_data)
        return result > 0
    
    def write_player_daily_stats(self, player_key: str, editorial_player_key: str,
                                league_key: str, stats_data: Dict, season: str,
                                stats_date: date, week: Optional[int] = None) -> bool:
        """写入球员日期统计（便捷方法，调用混合存储方法）"""
        result = self.write_player_daily_stat_values(player_key, editorial_player_key,
                                                   league_key, season, stats_date, 
                                                   stats_data, week)
        return result > 0
    
    # ===== 工具方法 =====
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        try:
            if value is None or value == '':
                return None
            return int(float(value))  # 先转float再转int，处理'1.0'格式
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def recreate_database_tables(self) -> bool:
        """强制重新创建数据库表结构"""
        try:
            print("🔄 强制重新创建数据库表...")
            
            # 关闭当前session
            if self.session:
                self.session.close()
            
            # 重新创建所有表
            success = recreate_tables(self.engine)
            if not success:
                return False
            
            # 重新初始化session
            self.session = get_session(self.engine)
            
            print("✅ 数据库表重新创建成功")
            return True
            
        except Exception as e:
            print(f"重新创建数据库表失败: {e}")
            return False 