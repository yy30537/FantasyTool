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
    create_database_engine, create_tables, get_session,
    Game, League, LeagueSettings, Team, Manager, Player, 
    Roster, Transaction, TransactionPlayer, PlayerStatsHistory, 
    TeamStats, RosterHistory
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
        create_tables(self.engine)
        self.session = get_session(self.engine)
        
        # 统计计数器
        self.stats = {
            'games': 0,
            'leagues': 0,
            'teams': 0,
            'managers': 0,
            'players': 0,
            'player_stats_history': 0,
            'team_stats': 0,
            'rosters': 0,
            'roster_history': 0,
            'transactions': 0,
            'transaction_players': 0,
            'league_settings': 0
        }
    
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
    
    def get_stats_summary(self) -> str:
        """获取统计摘要"""
        return (f"统计: 游戏({self.stats['games']}) 联盟({self.stats['leagues']}) "
                f"团队({self.stats['teams']}) 球员({self.stats['players']}) "
                f"交易({self.stats['transactions']}) 交易球员({self.stats['transaction_players']}) "
                f"名单({self.stats['rosters']}) 历史统计({self.stats['player_stats_history']})")
    
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
            return True
            
        except Exception as e:
            print(f"写入联盟设置失败 {league_key}: {e}")
            self.session.rollback()
            return False
    
    # ===== 时间序列数据写入方法 =====
    
    def write_player_stats_history(self, player_key: str, editorial_player_key: str, 
                                 league_key: str, stats_data: Dict, 
                                 coverage_type: str, season: str, 
                                 week: Optional[int] = None, 
                                 coverage_date: Optional[date] = None,
                                 fantasy_points: Optional[str] = None) -> bool:
        """写入球员历史统计数据"""
        try:
            # 检查是否已存在
            query = self.session.query(PlayerStatsHistory).filter_by(
                player_key=player_key,
                league_key=league_key,
                coverage_type=coverage_type,
                season=season
            )
            
            if week is not None:
                query = query.filter_by(week=week)
            if coverage_date is not None:
                query = query.filter_by(coverage_date=coverage_date)
            
            existing = query.first()
            if existing:
                # 更新现有记录
                existing.stats_data = stats_data
                existing.fantasy_points = fantasy_points
                existing.fetched_at = datetime.utcnow()
            else:
                # 创建新记录
                history = PlayerStatsHistory(
                    player_key=player_key,
                    editorial_player_key=editorial_player_key,
                    league_key=league_key,
                    coverage_type=coverage_type,
                    season=season,
                    week=week,
                    coverage_date=coverage_date,
                    stats_data=stats_data,
                    fantasy_points=fantasy_points
                )
                self.session.add(history)
                self.stats['player_stats_history'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入球员统计历史失败 {player_key}: {e}")
            self.session.rollback()
            return False
    
    def write_roster_history(self, team_key: str, player_key: str, league_key: str,
                           coverage_type: str, season: str,
                           week: Optional[int] = None,
                           coverage_date: Optional[date] = None,
                           selected_position: Optional[str] = None,
                           is_starting: bool = False,
                           player_status: Optional[str] = None,
                           injury_note: Optional[str] = None) -> bool:
        """写入名单历史数据"""
        try:
            # 检查是否已存在
            query = self.session.query(RosterHistory).filter_by(
                team_key=team_key,
                player_key=player_key,
                coverage_type=coverage_type,
                season=season
            )
            
            if week is not None:
                query = query.filter_by(week=week)
            if coverage_date is not None:
                query = query.filter_by(coverage_date=coverage_date)
            
            existing = query.first()
            if existing:
                # 更新现有记录
                existing.selected_position = selected_position
                existing.is_starting = is_starting
                existing.player_status = player_status
                existing.injury_note = injury_note
                existing.fetched_at = datetime.utcnow()
            else:
                # 创建新记录
                history = RosterHistory(
                    team_key=team_key,
                    player_key=player_key,
                    league_key=league_key,
                    coverage_type=coverage_type,
                    season=season,
                    week=week,
                    coverage_date=coverage_date,
                    selected_position=selected_position,
                    is_starting=is_starting,
                    is_bench=not is_starting,
                    player_status=player_status,
                    injury_note=injury_note
                )
                self.session.add(history)
                self.stats['roster_history'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入名单历史失败 {team_key}/{player_key}: {e}")
            self.session.rollback()
            return False
    
    def write_team_stats(self, team_key: str, league_key: str, stats_data: Dict,
                        coverage_type: str, season: str,
                        week: Optional[int] = None,
                        coverage_date: Optional[date] = None,
                        total_points: Optional[str] = None,
                        opponent_team_key: Optional[str] = None,
                        is_playoff: bool = False,
                        win: Optional[bool] = None) -> bool:
        """写入团队统计数据"""
        try:
            # 检查是否已存在
            query = self.session.query(TeamStats).filter_by(
                team_key=team_key,
                coverage_type=coverage_type,
                season=season
            )
            
            if week is not None:
                query = query.filter_by(week=week)
            if coverage_date is not None:
                query = query.filter_by(coverage_date=coverage_date)
            
            existing = query.first()
            if existing:
                # 更新现有记录
                existing.stats_data = stats_data
                existing.total_points = total_points
                existing.opponent_team_key = opponent_team_key
                existing.is_playoff = is_playoff
                existing.win = win
                existing.loss = not win if win is not None else None
                existing.updated_at = datetime.utcnow()
            else:
                # 创建新记录
                team_stats = TeamStats(
                    team_key=team_key,
                    league_key=league_key,
                    coverage_type=coverage_type,
                    season=season,
                    week=week,
                    coverage_date=coverage_date,
                    stats_data=stats_data,
                    total_points=total_points,
                    opponent_team_key=opponent_team_key,
                    is_playoff=is_playoff,
                    win=win,
                    loss=not win if win is not None else None
                )
                self.session.add(team_stats)
                self.stats['team_stats'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入团队统计失败 {team_key}: {e}")
            self.session.rollback()
            return False
    
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
                    eligible_positions=player_data.get("eligible_positions"),
                    season=player_data.get("season"),
                    last_updated=datetime.now()
                )
                self.session.add(player)
                count += 1
                
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
        """清空数据库中的所有数据
        
        Args:
            confirm: 确认清空，防止误操作
        """
        if not confirm:
            print("⚠️ 需要确认才能清空数据库，请设置 confirm=True")
            return False
        
        try:
            print("🗑️ 开始清空数据库...")
            
            # 按依赖关系的逆序删除数据
            tables_to_clear = [
                ('transaction_players', TransactionPlayer),
                ('transactions', Transaction),
                ('roster_history', RosterHistory),
                ('rosters', Roster),
                ('player_stats_history', PlayerStatsHistory),
                ('players', Player),
                ('managers', Manager),
                ('teams', Team),
                ('league_settings', LeagueSettings),
                ('leagues', League),
                ('team_stats', TeamStats),
                ('games', Game),
            ]
            
            for table_name, model_class in tables_to_clear:
                try:
                    deleted_count = self.session.query(model_class).delete()
                    self.session.commit()
                    print(f"  ✓ 清空 {table_name}: {deleted_count} 条记录")
                except Exception as e:
                    print(f"  ✗ 清空 {table_name} 失败: {e}")
                    self.session.rollback()
            
            # 重置统计计数器
            for key in self.stats:
                self.stats[key] = 0
            
            print("✅ 数据库清空完成")
            return True
            
        except Exception as e:
            print(f"清空数据库失败: {e}")
            self.session.rollback()
            return False
    
    def get_database_summary(self) -> Dict[str, int]:
        """获取数据库中各表的记录数量"""
        try:
            summary = {}
            tables_to_count = [
                ('games', Game),
                ('leagues', League),
                ('league_settings', LeagueSettings),
                ('teams', Team),
                ('managers', Manager),
                ('players', Player),
                ('player_stats_history', PlayerStatsHistory),
                ('team_stats', TeamStats),
                ('rosters', Roster),
                ('roster_history', RosterHistory),
                ('transactions', Transaction),
                ('transaction_players', TransactionPlayer),
            ]
            
            for table_name, model_class in tables_to_count:
                try:
                    count = self.session.query(model_class).count()
                    summary[table_name] = count
                except Exception as e:
                    print(f"统计 {table_name} 失败: {e}")
                    summary[table_name] = -1
            
            return summary
            
        except Exception as e:
            print(f"获取数据库摘要失败: {e}")
            return {} 