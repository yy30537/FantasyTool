"""
批量数据加载器
包含批量写入函数，从 archive/database_writer.py 迁移
"""

from typing import Dict, List, Optional
from datetime import date, datetime
from sqlalchemy.orm import Session
from fantasy_etl.database.models import (
    Game, League, LeagueSettings, StatCategory, Player, Team, Manager,
    Transaction, TransactionPlayer, DateDimension, PlayerEligiblePosition,
    RosterDaily, LeagueRosterPosition
)


class BatchLoaders:
    """批量数据加载器"""
    
    def __init__(self, session: Session, batch_size: int = 100):
        """
        初始化批量加载器
        
        Args:
            session: SQLAlchemy数据库会话
            batch_size: 批量写入大小，默认100条记录
        """
        self.session = session
        self.batch_size = batch_size
        self.stats = {
            'games': 0,
            'leagues': 0,
            'league_settings': 0,
            'stat_categories': 0,
            'players': 0,
            'teams': 0,
            'transactions': 0
        }
        
    # ============================================================================
    # 基础数据批量写入方法
    # ============================================================================
    
    def write_games_data(self, games_data: Dict) -> int:
        """
        写入游戏数据
        
        迁移自: archive/database_writer.py write_games_data() 第168行
        """
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
        """
        写入联盟数据
        
        迁移自: archive/database_writer.py write_leagues_data() 第213行
        """
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
        """
        写入联盟设置
        
        迁移自: archive/database_writer.py write_league_settings() 第266行
        """
        try:
            fantasy_content = settings_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 提取settings
            settings = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings = item["settings"]
                        break
            
            if not settings:
                return False
            
            # 检查是否已存在
            existing = self.session.query(LeagueSettings).filter_by(league_key=league_key).first()
            if existing:
                # 更新现有记录
                for key, value in self._extract_league_settings(settings).items():
                    setattr(existing, key, value)
            else:
                # 创建新记录
                league_settings = LeagueSettings(
                    league_key=league_key,
                    **self._extract_league_settings(settings)
                )
                self.session.add(league_settings)
            
            # 同时写入stat_categories
            if "stat_categories" in settings:
                self.write_stat_categories(league_key, settings["stat_categories"])
            
            # 写入roster_positions
            if "roster_positions" in settings:
                self.write_league_roster_positions(league_key, settings["roster_positions"])
            
            self.session.commit()
            self.stats['league_settings'] += 1
            return True
            
        except Exception as e:
            self.session.rollback()
            print(f"写入联盟设置失败: {e}")
            return False
    
    def _extract_league_settings(self, settings: Dict) -> Dict:
        """提取联盟设置字段"""
        return {
            'draft_type': settings.get('draft_type'),
            'is_auction_draft': bool(int(settings.get('is_auction_draft', '0'))),
            'scoring_type': settings.get('scoring_type'),
            'uses_playoff': bool(int(settings.get('uses_playoff', '0'))),
            'has_playoff_consolation_games': bool(settings.get('has_playoff_consolation_games', False)),
            'playoff_start_week': int(settings.get('playoff_start_week', 0)) if settings.get('playoff_start_week') else None,
            'uses_playoff_reseeding': bool(int(settings.get('uses_playoff_reseeding', '0'))),
            'uses_lock_eliminated_teams': bool(int(settings.get('uses_lock_eliminated_teams', '0'))),
            'num_playoff_teams': int(settings.get('num_playoff_teams', 0)) if settings.get('num_playoff_teams') else None,
            'num_playoff_consolation_teams': int(settings.get('num_playoff_consolation_teams', 0)) if settings.get('num_playoff_consolation_teams') else None,
            'uses_roster_import': bool(int(settings.get('uses_roster_import', '0'))),
            'roster_import_deadline': settings.get('roster_import_deadline'),
            'waiver_type': settings.get('waiver_type'),
            'waiver_rule': settings.get('waiver_rule'),
            'uses_faab': bool(int(settings.get('uses_faab', '0'))),
            'draft_time': int(settings.get('draft_time', 0)) if settings.get('draft_time') else None,
            'draft_pick_time': int(settings.get('draft_pick_time', 0)) if settings.get('draft_pick_time') else None,
            'post_draft_players': settings.get('post_draft_players'),
            'max_teams': int(settings.get('max_teams', 0)) if settings.get('max_teams') else None,
            'waiver_time': int(settings.get('waiver_time', 0)) if settings.get('waiver_time') else None,
            'trade_end_date': settings.get('trade_end_date'),
            'trade_ratify_type': settings.get('trade_ratify_type'),
            'trade_reject_time': int(settings.get('trade_reject_time', 0)) if settings.get('trade_reject_time') else None,
            'player_pool': settings.get('player_pool'),
            'cant_cut_list': settings.get('cant_cut_list'),
            'is_publicly_viewable': bool(int(settings.get('is_publicly_viewable', '0'))),
            'roster_positions': settings.get('roster_positions', []),
            'stat_categories': settings.get('stat_categories', {}),
            'max_weekly_adds': int(settings.get('max_weekly_adds', 0)) if settings.get('max_weekly_adds') else None
        }
        
    def write_stat_categories(self, league_key: str, stat_categories_data: Dict) -> int:
        """
        写入统计类别定义到数据库
        
        迁移自: archive/database_writer.py write_stat_categories() 第337行
        """
        if not stat_categories_data or not isinstance(stat_categories_data, dict):
            return 0
        
        count = 0
        stats_list = stat_categories_data.get("stats", [])
        
        for stat_item in stats_list:
            if not isinstance(stat_item, dict) or "stat" not in stat_item:
                continue
            
            stat_info = stat_item["stat"]
            stat_id = int(stat_info.get("stat_id", 0))
            
            if stat_id == 0:
                continue
            
            # 检查是否已存在
            existing = self.session.query(StatCategory).filter_by(
                league_key=league_key,
                stat_id=stat_id
            ).first()
            
            if existing:
                # 更新现有记录
                existing.display_name = stat_info.get("display_name", "")
                existing.name = stat_info.get("name", "")
                existing.abbr = stat_info.get("abbr", "")
                existing.sort_order = int(stat_info.get("sort_order", 0))
                existing.position_type = stat_info.get("position_type", "")
                existing.is_only_display_stat = bool(stat_info.get("is_only_display_stat", False))
                existing.is_composite_stat = bool(stat_info.get("is_composite_stat", False))
                existing.is_core_stat = stat_id in [5, 8, 10, 12, 15, 16, 17, 18, 19, 20, 21]  # 11个核心统计
            else:
                # 创建新记录
                stat_category = StatCategory(
                    league_key=league_key,
                    stat_id=stat_id,
                    display_name=stat_info.get("display_name", ""),
                    name=stat_info.get("name", ""),
                    abbr=stat_info.get("abbr", ""),
                    sort_order=int(stat_info.get("sort_order", 0)),
                    position_type=stat_info.get("position_type", ""),
                    is_only_display_stat=bool(stat_info.get("is_only_display_stat", False)),
                    is_composite_stat=bool(stat_info.get("is_composite_stat", False)),
                    is_core_stat=stat_id in [5, 8, 10, 12, 15, 16, 17, 18, 19, 20, 21]  # 11个核心统计
                )
                self.session.add(stat_category)
                count += 1
        
        self.session.commit()
        self.stats['stat_categories'] += count
        return count
    
    # ============================================================================
    # 实体数据批量写入方法
    # ============================================================================
    
    def write_players_batch(self, players_data: List[Dict], league_key: str) -> int:
        """
        批量写入球员数据
        
        迁移自: archive/database_writer.py write_players_batch() 第1359行
        """
        if not players_data:
            return 0
        
        count = 0
        players_to_add = []
        
        for player_info in players_data:
            # 检查是否已存在
            existing = self.session.query(Player).filter_by(
                player_key=player_info["player_key"]
            ).first()
            
            if existing:
                continue
            
            # 创建Player对象
            player = Player(
                player_key=player_info["player_key"],
                player_id=player_info.get("player_id"),
                editorial_player_key=player_info.get("editorial_player_key"),
                editorial_team_key=player_info.get("editorial_team_key"),
                editorial_team_full_name=player_info.get("editorial_team_full_name"),
                editorial_team_abbr=player_info.get("editorial_team_abbr"),
                uniform_number=player_info.get("uniform_number"),
                display_position=player_info.get("display_position"),
                image_url=player_info.get("image_url"),
                is_undroppable=bool(player_info.get("is_undroppable", 0)),
                position_type=player_info.get("position_type"),
                primary_position=player_info.get("primary_position"),
                player_notes_last_timestamp=player_info.get("player_notes_last_timestamp"),
                full_name=player_info.get("full_name"),
                first_name=player_info.get("first_name"),
                last_name=player_info.get("last_name"),
                status=player_info.get("status"),
                status_full=player_info.get("status_full"),
                injury_note=player_info.get("injury_note"),
                editorial_team_name=player_info.get("editorial_team_name"),
                games_played=player_info.get("games_played"),
                games_started=player_info.get("games_started"),
                minutes_played=player_info.get("minutes_played"),
                field_goals_made=player_info.get("field_goals_made"),
                field_goals_attempted=player_info.get("field_goals_attempted"),
                field_goal_percentage=player_info.get("field_goal_percentage"),
                free_throws_made=player_info.get("free_throws_made"),
                free_throws_attempted=player_info.get("free_throws_attempted"),
                free_throw_percentage=player_info.get("free_throw_percentage"),
                three_pointers_made=player_info.get("three_pointers_made"),
                points=player_info.get("points"),
                rebounds=player_info.get("rebounds"),
                assists=player_info.get("assists"),
                steals=player_info.get("steals"),
                blocks=player_info.get("blocks"),
                turnovers=player_info.get("turnovers"),
                last_updated=datetime.now()
            )
            
            players_to_add.append(player)
            
            # 处理eligible_positions
            if "eligible_positions" in player_info:
                self.write_player_eligible_positions(
                    player_info["player_key"],
                    player_info["eligible_positions"]
                )
            
            count += 1
            
            # 批量提交
            if len(players_to_add) >= self.batch_size:
                self.session.bulk_save_objects(players_to_add)
                self.session.commit()
                players_to_add = []
        
        # 提交剩余的
        if players_to_add:
            self.session.bulk_save_objects(players_to_add)
            self.session.commit()
        
        self.stats['players'] += count
        print(f"✓ 成功写入 {count} 个球员")
        return count
        
    def write_teams_batch(self, teams_data: List[Dict], league_key: str) -> int:
        """
        批量写入团队数据
        
        迁移自: archive/database_writer.py write_teams_batch() 第1442行
        """
        if not teams_data:
            return 0
        
        count = 0
        teams_to_add = []
        
        for team_info in teams_data:
            # 检查是否已存在
            existing = self.session.query(Team).filter_by(
                team_key=team_info["team_key"]
            ).first()
            
            if existing:
                continue
            
            # 创建Team对象
            team = Team(
                team_key=team_info["team_key"],
                team_id=team_info.get("team_id"),
                league_key=league_key,
                name=team_info.get("name"),
                is_owned_by_current_login=self._safe_bool(team_info.get("is_owned_by_current_login")),
                url=team_info.get("url"),
                team_logo_url=team_info.get("team_logo_url"),
                auction_budget_total=team_info.get("auction_budget_total"),
                auction_budget_spent=team_info.get("auction_budget_spent"),
                faab_balance=team_info.get("faab_balance"),
                number_of_moves=team_info.get("number_of_moves"),
                number_of_trades=team_info.get("number_of_trades", 0),
                roster_adds_week=team_info.get("roster_adds_week"),
                roster_adds_value=team_info.get("roster_adds_value"),
                roster_adds_season=team_info.get("roster_adds_season"),
                clinched_playoffs=self._safe_bool(team_info.get("clinched_playoffs")),
                has_draft_grade=self._safe_bool(team_info.get("has_draft_grade")),
                draft_grade=team_info.get("draft_grade"),
                draft_recap_url=team_info.get("draft_recap_url"),
                league_scoring_type=team_info.get("league_scoring_type"),
                has_keeper_settings=self._safe_bool(team_info.get("has_keeper_settings")),
                waiver_priority=team_info.get("waiver_priority"),
                last_updated=datetime.now()
            )
            
            teams_to_add.append(team)
            
            # 处理managers
            if "managers" in team_info and team_info["managers"]:
                self._write_managers_for_team(team_info["team_key"], team_info["managers"])
            
            count += 1
            
            # 批量提交
            if len(teams_to_add) >= self.batch_size:
                self.session.bulk_save_objects(teams_to_add)
                self.session.commit()
                teams_to_add = []
        
        # 提交剩余的
        if teams_to_add:
            self.session.bulk_save_objects(teams_to_add)
            self.session.commit()
        
        self.stats['teams'] += count
        print(f"✓ 成功写入 {count} 个团队")
        return count
    
    def _write_managers_for_team(self, team_key: str, managers_data):
        """写入团队管理员数据"""
        if not managers_data:
            return
        
        # 处理managers数据格式
        managers_list = []
        if isinstance(managers_data, dict):
            # 如果是字典格式，提取manager列表
            for key, value in managers_data.items():
                if key != "count" and isinstance(value, dict) and "manager" in value:
                    managers_list.append(value["manager"])
        elif isinstance(managers_data, list):
            managers_list = managers_data
        
        for manager_info in managers_list:
            if isinstance(manager_info, dict):
                # 检查是否已存在
                existing = self.session.query(Manager).filter_by(
                    manager_id=manager_info.get("manager_id"),
                    team_key=team_key
                ).first()
                
                if not existing:
                    manager = Manager(
                        manager_id=manager_info.get("manager_id"),
                        team_key=team_key,
                        nickname=manager_info.get("nickname"),
                        guid=manager_info.get("guid"),
                        email=manager_info.get("email"),
                        image_url=manager_info.get("image_url"),
                        is_commissioner=bool(manager_info.get("is_commissioner", 0)),
                        is_current_login=bool(manager_info.get("is_current_login", 0)),
                        felo_score=manager_info.get("felo_score"),
                        felo_tier=manager_info.get("felo_tier")
                    )
                    self.session.add(manager)
        
    def write_transactions_batch(self, transactions_data: List[Dict], league_key: str) -> int:
        """
        批量写入交易数据
        
        迁移自: archive/database_writer.py write_transactions_batch() 第1560行
        """
        if not transactions_data:
            return 0
        
        count = 0
        transactions_to_add = []
        
        for trans_info in transactions_data:
            # 检查是否已存在
            existing = self.session.query(Transaction).filter_by(
                transaction_key=trans_info["transaction_key"]
            ).first()
            
            if existing:
                continue
            
            # 创建Transaction对象
            transaction = Transaction(
                transaction_key=trans_info["transaction_key"],
                transaction_id=trans_info.get("transaction_id"),
                league_key=league_key,
                type=trans_info.get("type"),
                status=trans_info.get("status"),
                timestamp=int(trans_info.get("timestamp", 0)) if trans_info.get("timestamp") else None,
                trader_team_key=trans_info.get("trader_team_key"),
                tradee_team_key=trans_info.get("tradee_team_key"),
                trade_note=trans_info.get("trade_note"),
                waiver_date=trans_info.get("waiver_date"),
                waiver_priority=trans_info.get("waiver_priority"),
                faab_bid=trans_info.get("faab_bid"),
                created_date=datetime.now()
            )
            
            transactions_to_add.append(transaction)
            
            # 处理transaction players
            if "players" in trans_info:
                self._write_transaction_players(trans_info["transaction_key"], trans_info["players"])
            
            count += 1
            
            # 批量提交
            if len(transactions_to_add) >= self.batch_size:
                self.session.bulk_save_objects(transactions_to_add)
                self.session.commit()
                transactions_to_add = []
        
        # 提交剩余的
        if transactions_to_add:
            self.session.bulk_save_objects(transactions_to_add)
            self.session.commit()
        
        self.stats['transactions'] += count
        print(f"✓ 成功写入 {count} 个交易记录")
        return count
    
    def _write_transaction_players(self, transaction_key: str, players_data):
        """写入交易涉及的球员数据"""
        if not players_data:
            return
        
        # 处理players数据格式
        players_list = []
        if isinstance(players_data, dict):
            # 如果是字典格式，提取player列表
            for key, value in players_data.items():
                if key != "count" and isinstance(value, dict) and "player" in value:
                    players_list.append(value["player"])
        elif isinstance(players_data, list):
            players_list = players_data
        
        for player_item in players_list:
            if isinstance(player_item, list) and len(player_item) >= 2:
                # player_item[0] 是基本信息，player_item[1] 是transaction_data
                player_info = {}
                trans_data = {}
                
                for item in player_item:
                    if isinstance(item, dict):
                        if "transaction_data" in item:
                            trans_data = item["transaction_data"]
                        else:
                            player_info.update(item)
                
                if player_info.get("player_key") and trans_data:
                    # 检查是否已存在
                    existing = self.session.query(TransactionPlayer).filter_by(
                        transaction_key=transaction_key,
                        player_key=player_info["player_key"]
                    ).first()
                    
                    if not existing:
                        trans_player = TransactionPlayer(
                            transaction_key=transaction_key,
                            player_key=player_info["player_key"],
                            transaction_type=trans_data.get("type"),
                            source_type=trans_data.get("source_type"),
                            source_team_key=trans_data.get("source_team_key"),
                            source_team_name=trans_data.get("source_team_name"),
                            destination_type=trans_data.get("destination_type"),
                            destination_team_key=trans_data.get("destination_team_key"),
                            destination_team_name=trans_data.get("destination_team_name")
                        )
                        self.session.add(trans_player)
        
    def write_date_dimensions_batch(self, dates_data: List[Dict]) -> int:
        """
        批量写入赛季日期维度数据
        
        迁移自: archive/database_writer.py write_date_dimensions_batch() 第1295行
        """
        if not dates_data:
            return 0
        
        # 获取已存在的日期记录
        existing_dates = set()
        for date_info in dates_data:
            league_key = date_info.get("league_key")
            if league_key:
                existing_records = self.session.query(DateDimension.date).filter_by(
                    league_key=league_key
                ).all()
                existing_dates.update([(league_key, record.date) for record in existing_records])
        
        # 准备批量插入的数据
        dates_to_add = []
        count = 0
        
        for date_info in dates_data:
            league_key = date_info.get("league_key")
            date_obj = date_info.get("date")
            season = date_info.get("season")
            
            if not all([league_key, date_obj, season]):
                continue
            
            # 检查是否已存在
            if (league_key, date_obj) in existing_dates:
                continue
            
            date_dim = DateDimension(
                date=date_obj,
                league_key=league_key,
                season=season,
                year=date_obj.year,
                month=date_obj.month,
                day=date_obj.day,
                week_of_year=date_obj.isocalendar()[1],
                day_of_week=date_obj.weekday(),
                is_weekend=date_obj.weekday() >= 5,
                quarter=((date_obj.month - 1) // 3) + 1
            )
            
            dates_to_add.append(date_dim)
            count += 1
            
            # 批量提交
            if len(dates_to_add) >= self.batch_size:
                self.session.bulk_save_objects(dates_to_add)
                self.session.commit()
                dates_to_add = []
                print(f"  已写入 {count} 个日期...")
        
        # 提交剩余的
        if dates_to_add:
            self.session.bulk_save_objects(dates_to_add)
            self.session.commit()
        
        print(f"✓ 成功写入 {count} 个日期维度记录")
        return count
    
    # ============================================================================
    # 辅助写入方法
    # ============================================================================
    
    def write_player_eligible_positions(self, player_key: str, positions: List) -> bool:
        """
        写入球员合适位置
        
        迁移自: archive/database_writer.py write_player_eligible_positions() 第1162行
        """
        try:
            # 先删除现有记录
            self.session.query(PlayerEligiblePosition).filter_by(
                player_key=player_key
            ).delete()
            
            # 处理positions数据
            positions_list = []
            if isinstance(positions, dict):
                positions_list = [pos for key, pos in positions.items() if key != "count"]
            elif isinstance(positions, list):
                positions_list = positions
            
            # 写入新记录
            for position in positions_list:
                if position:  # 确保position不为空
                    pos_record = PlayerEligiblePosition(
                        player_key=player_key,
                        position=position
                    )
                    self.session.add(pos_record)
            
            self.session.commit()
            return True
            
        except Exception as e:
            self.session.rollback()
            print(f"写入球员位置失败 {player_key}: {e}")
            return False
        
    def write_roster_daily(self, **kwargs) -> bool:
        """
        写入每日名单数据
        
        迁移自: archive/database_writer.py write_roster_daily() 第1196行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(RosterDaily).filter_by(
                team_key=kwargs.get("team_key"),
                player_key=kwargs.get("player_key"),
                roster_date=kwargs.get("roster_date")
            ).first()
            
            if existing:
                # 更新现有记录
                for key, value in kwargs.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
            else:
                # 创建新记录
                roster = RosterDaily(**kwargs)
                self.session.add(roster)
            
            self.session.commit()
            return True
            
        except Exception as e:
            self.session.rollback()
            print(f"写入roster失败: {e}")
            return False
        
    def write_date_dimension(self, date_obj: date, league_key: str, season: str) -> bool:
        """
        写入日期维度数据
        
        迁移自: archive/database_writer.py write_date_dimension() 第1266行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(DateDimension).filter_by(
                date=date_obj,
                league_key=league_key
            ).first()
            
            if existing:
                return True
            
            date_dim = DateDimension(
                date=date_obj,
                league_key=league_key,
                season=season,
                year=date_obj.year,
                month=date_obj.month,
                day=date_obj.day,
                week_of_year=date_obj.isocalendar()[1],
                day_of_week=date_obj.weekday(),
                is_weekend=date_obj.weekday() >= 5,
                quarter=((date_obj.month - 1) // 3) + 1
            )
            
            self.session.add(date_dim)
            self.session.commit()
            return True
            
        except Exception as e:
            self.session.rollback()
            print(f"写入日期维度失败: {e}")
            return False
        
    def write_league_roster_positions(self, league_key: str, roster_positions_data) -> bool:
        """
        写入联盟roster_positions到新表
        
        迁移自: archive/database_writer.py write_league_roster_positions() 第2087行
        """
        try:
            # 先删除现有记录
            self.session.query(LeagueRosterPosition).filter_by(
                league_key=league_key
            ).delete()
            
            # 处理roster_positions数据
            positions_list = []
            if isinstance(roster_positions_data, str):
                import json
                positions_list = json.loads(roster_positions_data)
            elif isinstance(roster_positions_data, list):
                positions_list = roster_positions_data
            
            # 写入新记录
            for pos_info in positions_list:
                if isinstance(pos_info, dict) and "roster_position" in pos_info:
                    pos_data = pos_info["roster_position"]
                    
                    roster_pos = LeagueRosterPosition(
                        league_key=league_key,
                        position=pos_data.get("position"),
                        position_type=pos_data.get("position_type"),
                        count=int(pos_data.get("count", 0)),
                        is_starting_position=bool(pos_data.get("is_starting_position", False))
                    )
                    self.session.add(roster_pos)
            
            self.session.commit()
            return True
            
        except Exception as e:
            self.session.rollback()
            print(f"写入roster positions失败: {e}")
            return False
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        if isinstance(value, (int, float)):
            return bool(value)
        return False
        
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
    
    def get_stats_summary(self) -> Dict[str, int]:
        """获取写入统计摘要"""
        return self.stats.copy()