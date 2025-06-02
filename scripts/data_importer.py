#!/usr/bin/env python3
"""
数据导入脚本 - 将Yahoo Fantasy数据从JSON文件导入到数据库
"""

import json
import os
from datetime import datetime
from model import (
    create_database_engine, create_tables, get_session,
    Game, League, LeagueSettings, Team, Manager, Player, PlayerStats, 
    Roster, Transaction, TransactionPlayer
)

class FantasyDataImporter:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.engine = create_database_engine()
        create_tables(self.engine)
        self.session = get_session(self.engine)
        
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
    
    def load_json_file(self, file_path):
        """加载JSON文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载文件失败 {file_path}: {e}")
            return None
    
    def import_games_data(self):
        """导入游戏数据"""
        games_file = os.path.join(self.data_dir, "games_data.json")
        data = self.load_json_file(games_file)
        if not data:
            return
        
        games = data["fantasy_content"]["users"]["0"]["user"][1]["games"]
        
        for key, game_data in games.items():
            if key == "count":
                continue
                
            if isinstance(game_data["game"], list):
                game_info = game_data["game"][0]
            else:
                game_info = game_data["game"]
            
            existing_game = self.session.query(Game).filter_by(game_key=game_info["game_key"]).first()
            if existing_game:
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
        
        self.session.commit()
    
    def import_leagues_data(self):
        """导入联盟数据"""
        leagues_file = os.path.join(self.data_dir, "all_leagues_data.json")
        data = self.load_json_file(leagues_file)
        if not data:
            return
        
        for game_key, leagues in data.items():
            for league_info in leagues:
                existing_league = self.session.query(League).filter_by(league_key=league_info["league_key"]).first()
                if existing_league:
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
        
        self.session.commit()
    
    def import_league_details(self, league_key):
        """导入指定联盟的详细数据"""
        league_dir = os.path.join(self.data_dir, f"selected_league_{league_key}")
        if not os.path.exists(league_dir):
            print(f"联盟目录不存在: {league_dir}")
            return
        
        self.import_league_settings(league_key, league_dir)
        self.import_teams_data(league_key, league_dir)
        self.import_players_data(league_key, league_dir)
        self.import_player_stats(league_key, league_dir)
        self.import_rosters_data(league_key, league_dir)
        self.import_transactions_data(league_key, league_dir)
    
    def import_league_settings(self, league_key, league_dir):
        """导入联盟设置"""
        league_info_file = os.path.join(league_dir, "league_info.json")
        data = self.load_json_file(league_info_file)
        
        if not data or "settings" not in data:
            return
        
        settings_data = data["settings"]["fantasy_content"]["league"][1]["settings"][0]
        
        existing = self.session.query(LeagueSettings).filter_by(league_key=league_key).first()
        if existing:
            return
        
        settings = LeagueSettings(
            league_key=league_key,
            draft_type=settings_data.get("draft_type"),
            is_auction_draft=bool(int(settings_data.get("is_auction_draft", "0"))),
            persistent_url=settings_data.get("persistent_url"),
            uses_playoff=bool(int(settings_data.get("uses_playoff", "1"))),
            has_playoff_consolation_games=settings_data.get("has_playoff_consolation_games", False),
            playoff_start_week=settings_data.get("playoff_start_week"),
            uses_playoff_reseeding=bool(settings_data.get("uses_playoff_reseeding", 0)),
            uses_lock_eliminated_teams=bool(settings_data.get("uses_lock_eliminated_teams", 0)),
            num_playoff_teams=int(settings_data.get("num_playoff_teams", 0)) if settings_data.get("num_playoff_teams") else None,
            num_playoff_consolation_teams=settings_data.get("num_playoff_consolation_teams", 0),
            has_multiweek_championship=bool(settings_data.get("has_multiweek_championship", 0)),
            waiver_type=settings_data.get("waiver_type"),
            waiver_rule=settings_data.get("waiver_rule"),
            uses_faab=bool(int(settings_data.get("uses_faab", "0"))),
            draft_time=settings_data.get("draft_time"),
            draft_pick_time=settings_data.get("draft_pick_time"),
            post_draft_players=settings_data.get("post_draft_players"),
            max_teams=int(settings_data.get("max_teams", 0)) if settings_data.get("max_teams") else None,
            waiver_time=settings_data.get("waiver_time"),
            trade_end_date=settings_data.get("trade_end_date"),
            trade_ratify_type=settings_data.get("trade_ratify_type"),
            trade_reject_time=settings_data.get("trade_reject_time"),
            player_pool=settings_data.get("player_pool"),
            cant_cut_list=settings_data.get("cant_cut_list"),
            draft_together=bool(settings_data.get("draft_together", 0)),
            is_publicly_viewable=bool(int(settings_data.get("is_publicly_viewable", "1"))),
            can_trade_draft_picks=bool(int(settings_data.get("can_trade_draft_picks", "0"))),
            sendbird_channel_url=settings_data.get("sendbird_channel_url"),
            roster_positions=settings_data.get("roster_positions"),
            stat_categories=settings_data.get("stat_categories")
        )
        self.session.add(settings)
        self.session.commit()
    
    def import_teams_data(self, league_key, league_dir):
        """导入团队数据"""
        teams_file = os.path.join(league_dir, "teams.json")
        data = self.load_json_file(teams_file)
        
        if not data:
            return
        
        teams_data = data["fantasy_content"]["league"][1]["teams"]
        
        for key, team_data in teams_data.items():
            if key == "count":
                continue
            
            team_info = team_data["team"][0]
            
            team_key = None
            team_id = None
            name = None
            url = None
            team_logo_url = None
            division_id = None
            waiver_priority = None
            faab_balance = None
            number_of_moves = 0
            number_of_trades = 0
            roster_adds_week = None
            roster_adds_value = None
            clinched_playoffs = False
            has_draft_grade = False
            managers_data = []
            
            for item in team_info:
                if isinstance(item, dict):
                    if "team_key" in item:
                        team_key = item["team_key"]
                    elif "team_id" in item:
                        team_id = item["team_id"]
                    elif "name" in item:
                        name = item["name"]
                    elif "url" in item:
                        url = item["url"]
                    elif "team_logos" in item:
                        if item["team_logos"] and len(item["team_logos"]) > 0:
                            team_logo_url = item["team_logos"][0]["team_logo"]["url"]
                    elif "division_id" in item:
                        division_id = item["division_id"]
                    elif "waiver_priority" in item:
                        waiver_priority = item["waiver_priority"]
                    elif "faab_balance" in item:
                        faab_balance = item["faab_balance"]
                    elif "number_of_moves" in item:
                        number_of_moves = item["number_of_moves"]
                    elif "number_of_trades" in item:
                        number_of_trades = int(item["number_of_trades"])
                    elif "roster_adds" in item:
                        roster_adds_week = str(item["roster_adds"]["coverage_value"])
                        roster_adds_value = item["roster_adds"]["value"]
                    elif "clinched_playoffs" in item:
                        clinched_playoffs = bool(item["clinched_playoffs"])
                    elif "has_draft_grade" in item:
                        has_draft_grade = bool(item["has_draft_grade"])
                    elif "managers" in item:
                        managers_data = item["managers"]
            
            if not team_key:
                continue
            
            existing_team = self.session.query(Team).filter_by(team_key=team_key).first()
            if existing_team:
                continue
            
            team = Team(
                team_key=team_key,
                team_id=team_id,
                league_key=league_key,
                name=name,
                url=url,
                team_logo_url=team_logo_url,
                division_id=division_id,
                waiver_priority=waiver_priority,
                faab_balance=faab_balance,
                number_of_moves=number_of_moves,
                number_of_trades=number_of_trades,
                roster_adds_week=roster_adds_week,
                roster_adds_value=roster_adds_value,
                clinched_playoffs=clinched_playoffs,
                has_draft_grade=has_draft_grade
            )
            self.session.add(team)
            
            for manager_data in managers_data:
                manager_info = manager_data["manager"]
                manager = Manager(
                    manager_id=manager_info["manager_id"],
                    team_key=team_key,
                    nickname=manager_info["nickname"],
                    guid=manager_info["guid"],
                    is_commissioner=bool(int(manager_info.get("is_commissioner", "0"))),
                    email=manager_info.get("email"),
                    image_url=manager_info.get("image_url"),
                    felo_score=manager_info.get("felo_score"),
                    felo_tier=manager_info.get("felo_tier")
                )
                self.session.add(manager)
        
        self.session.commit()
    
    def import_players_data(self, league_key, league_dir):
        """导入球员数据"""
        players_dir = os.path.join(league_dir, "players")
        
        static_file = os.path.join(players_dir, "static_players.json")
        dynamic_file = os.path.join(players_dir, "dynamic_players.json")
        
        static_data = self.load_json_file(static_file)
        dynamic_data = self.load_json_file(dynamic_file)
        
        if not static_data or not dynamic_data:
            return
        
        static_players = static_data["static_players"]
        dynamic_players = dynamic_data["dynamic_players"]
        
        for editorial_key, static_info in static_players.items():
            if editorial_key in dynamic_players:
                dynamic_info = dynamic_players[editorial_key]
                
                player_key = dynamic_info["player_key"]
                
                existing_player = self.session.query(Player).filter_by(player_key=player_key).first()
                if existing_player:
                    continue
                
                last_updated = None
                if "last_updated" in dynamic_info:
                    try:
                        last_updated = datetime.fromisoformat(dynamic_info["last_updated"].replace('Z', '+00:00'))
                    except:
                        pass
                
                player = Player(
                    player_key=player_key,
                    player_id=static_info["player_id"],
                    editorial_player_key=editorial_key,
                    league_key=league_key,
                    full_name=static_info["full_name"],
                    first_name=static_info["first_name"],
                    last_name=static_info["last_name"],
                    current_team_key=dynamic_info.get("current_team_key"),
                    current_team_name=dynamic_info.get("current_team_name"),
                    current_team_abbr=dynamic_info.get("current_team_abbr"),
                    display_position=dynamic_info.get("display_position"),
                    primary_position=dynamic_info.get("primary_position"),
                    position_type=dynamic_info.get("position_type"),
                    uniform_number=dynamic_info.get("uniform_number"),
                    status=dynamic_info.get("status"),
                    image_url=dynamic_info.get("image_url"),
                    headshot_url=dynamic_info.get("headshot_url"),
                    is_undroppable=dynamic_info.get("is_undroppable", False),
                    eligible_positions=dynamic_info.get("eligible_positions"),
                    season=dynamic_info.get("season"),
                    last_updated=last_updated
                )
                self.session.add(player)
        
        self.session.commit()
    
    def import_player_stats(self, league_key, league_dir):
        """导入球员统计数据"""
        players_dir = os.path.join(league_dir, "players")
        stats_file = os.path.join(players_dir, "player_stats.json")
        
        data = self.load_json_file(stats_file)
        if not data:
            return
        
        player_stats = data["player_stats"]
        
        for player_key, stats in player_stats.items():
            for stat_id, stat_data in stats.items():
                existing_stat = self.session.query(PlayerStats).filter_by(
                    player_key=player_key, stat_id=stat_id
                ).first()
                if existing_stat:
                    continue
                
                stat = PlayerStats(
                    player_key=player_key,
                    stat_id=stat_id,
                    value=stat_data["value"]
                )
                self.session.add(stat)
        
        self.session.commit()
    
    def import_rosters_data(self, league_key, league_dir):
        """导入名单数据"""
        rosters_dir = os.path.join(league_dir, "rosters")
        
        if not os.path.exists(rosters_dir):
            return
        
        for filename in os.listdir(rosters_dir):
            if not filename.endswith('.json'):
                continue
            
            roster_file = os.path.join(rosters_dir, filename)
            data = self.load_json_file(roster_file)
            
            if not data:
                continue
            
            team_info = data["fantasy_content"]["team"][0]
            team_key = None
            
            for item in team_info:
                if isinstance(item, dict) and "team_key" in item:
                    team_key = item["team_key"]
                    break
            
            if not team_key:
                continue
            
            roster_info = data["fantasy_content"]["team"][1]["roster"]
            coverage_date = roster_info["date"]
            is_prescoring = bool(roster_info["is_prescoring"])
            is_editable = bool(roster_info["is_editable"])
            
            players_data = roster_info["0"]["players"]
            
            for key, player_data in players_data.items():
                if key == "count":
                    continue
                
                player_info = player_data["player"][0]
                position_data = player_data["player"][1] if len(player_data["player"]) > 1 else {}
                
                player_key = None
                status = None
                status_full = None
                injury_note = None
                is_keeper = False
                keeper_cost = None
                kept = False
                selected_position = None
                eligible_positions_to_add = None
                
                for item in player_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_key = item["player_key"]
                        elif "status" in item:
                            status = item["status"]
                        elif "status_full" in item:
                            status_full = item["status_full"]
                        elif "injury_note" in item:
                            injury_note = item["injury_note"]
                        elif "is_keeper" in item:
                            keeper_info = item["is_keeper"]
                            is_keeper = keeper_info.get("status", False)
                            keeper_cost = keeper_info.get("cost")
                            kept = keeper_info.get("kept", False)
                        elif "eligible_positions_to_add" in item:
                            eligible_positions_to_add = item["eligible_positions_to_add"]
                
                if "selected_position" in position_data:
                    selected_position_data = position_data["selected_position"]
                    if isinstance(selected_position_data, list):
                        for item in selected_position_data:
                            if isinstance(item, dict) and "position" in item:
                                selected_position = item["position"]
                                break
                    elif isinstance(selected_position_data, dict) and "position" in selected_position_data:
                        selected_position = selected_position_data["position"]
                
                if not player_key:
                    continue
                
                existing_roster = self.session.query(Roster).filter_by(
                    team_key=team_key, player_key=player_key, coverage_date=coverage_date
                ).first()
                if existing_roster:
                    continue
                
                roster = Roster(
                    team_key=team_key,
                    player_key=player_key,
                    coverage_date=coverage_date,
                    is_prescoring=is_prescoring,
                    is_editable=is_editable,
                    status=status,
                    status_full=status_full,
                    injury_note=injury_note,
                    is_keeper=is_keeper,
                    keeper_cost=str(keeper_cost) if keeper_cost else None,
                    kept=kept,
                    selected_position=selected_position,
                    eligible_positions_to_add=eligible_positions_to_add
                )
                self.session.add(roster)
        
        self.session.commit()
    
    def import_transactions_data(self, league_key, league_dir):
        """导入交易数据"""
        transactions_dir = os.path.join(league_dir, "transactions")
        transactions_file = os.path.join(transactions_dir, "all_transactions.json")
        
        data = self.load_json_file(transactions_file)
        if not data:
            return
        
        transactions = data["all_transactions"]
        
        for transaction_data in transactions:
            transaction_key = transaction_data["transaction_key"]
            
            existing_transaction = self.session.query(Transaction).filter_by(transaction_key=transaction_key).first()
            if existing_transaction:
                continue
            
            if "players" not in transaction_data:
                continue
            
            transaction_type = transaction_data["type"]
            trader_team_key = trader_team_name = tradee_team_key = tradee_team_name = picks_data = None
            
            if transaction_type == "trade":
                trader_team_key = transaction_data.get("trader_team_key")
                trader_team_name = transaction_data.get("trader_team_name")
                tradee_team_key = transaction_data.get("tradee_team_key")
                tradee_team_name = transaction_data.get("tradee_team_name")
                picks_data = transaction_data.get("picks")
            
            transaction = Transaction(
                transaction_key=transaction_key,
                transaction_id=transaction_data["transaction_id"],
                league_key=league_key,
                type=transaction_type,
                status=transaction_data["status"],
                timestamp=transaction_data["timestamp"],
                trader_team_key=trader_team_key,
                trader_team_name=trader_team_name,
                tradee_team_key=tradee_team_key,
                tradee_team_name=tradee_team_name,
                picks_data=picks_data,
                players_data=transaction_data["players"]
            )
            self.session.add(transaction)
            
            players_data = transaction_data["players"]
            for key, player_data in players_data.items():
                if key == "count":
                    continue
                
                player_info = player_data["player"][0]
                
                if len(player_data["player"]) < 2 or "transaction_data" not in player_data["player"][1]:
                    continue
                
                transaction_info_container = player_data["player"][1]["transaction_data"]
                
                transaction_info = None
                if isinstance(transaction_info_container, list):
                    if len(transaction_info_container) > 0:
                        transaction_info = transaction_info_container[0]
                elif isinstance(transaction_info_container, dict):
                    transaction_info = transaction_info_container
                
                if not transaction_info:
                    continue
                
                player_key = player_id = player_name = editorial_team_abbr = display_position = position_type = None
                
                for item in player_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_key = item["player_key"]
                        elif "player_id" in item:
                            player_id = item["player_id"]
                        elif "name" in item:
                            player_name = item["name"]["full"]
                        elif "editorial_team_abbr" in item:
                            editorial_team_abbr = item["editorial_team_abbr"]
                        elif "display_position" in item:
                            display_position = item["display_position"]
                        elif "position_type" in item:
                            position_type = item["position_type"]
                
                if not player_key:
                    continue
                
                existing_tp = self.session.query(TransactionPlayer).filter_by(
                    transaction_key=transaction_key, player_key=player_key
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
                    transaction_type=transaction_info["type"],
                    source_type=transaction_info.get("source_type"),
                    source_team_key=transaction_info.get("source_team_key"),
                    source_team_name=transaction_info.get("source_team_name"),
                    destination_type=transaction_info.get("destination_type"),
                    destination_team_key=transaction_info.get("destination_team_key"),
                    destination_team_name=transaction_info.get("destination_team_name")
                )
                self.session.add(transaction_player)
        
        self.session.commit()
    
    def run_full_import(self):
        print("开始数据导入...")
        start_time = datetime.now()
        
        # 检查data文件夹是否存在
        if not os.path.exists(self.data_dir):
            print(f"❌ 数据文件夹不存在: {self.data_dir}")
            print(f"")
            print(f"💡 提示: 现在推荐使用 single_league_fetcher.py 直接写入数据库")
            print(f"   运行命令: python3 single_league_fetcher.py --complete")
            print(f"")
            print(f"   如果您确实需要使用data_importer，请先:")
            print(f"   1. 创建data文件夹: mkdir {self.data_dir}")
            print(f"   2. 使用旧版本脚本生成JSON文件")
            return
        
        try:
            print("导入游戏数据...")
            self.import_games_data()
            
            print("导入联盟数据...")
            self.import_leagues_data()
            
            league_dirs = [item for item in os.listdir(self.data_dir) if item.startswith("selected_league_")]
            total_leagues = len(league_dirs)
            
            if total_leagues == 0:
                print(f"⚠️ 在 {self.data_dir} 中未找到任何联盟数据文件夹")
                print(f"   期望的文件夹格式: selected_league_*")
                print(f"")
                print(f"💡 推荐使用 single_league_fetcher.py 直接写入数据库:")
                print(f"   python3 single_league_fetcher.py --complete")
                return
            
            print(f"导入 {total_leagues} 个联盟的详细数据...")
            
            for i, item in enumerate(league_dirs, 1):
                league_key = item.replace("selected_league_", "")
                print(f"[{i}/{total_leagues}] 处理联盟: {league_key}")
                
                try:
                    self.import_league_details(league_key)
                except Exception as e:
                    print(f"联盟 {league_key} 导入失败: {e}")
                    continue
            
            total_time = datetime.now() - start_time
            print(f"\n数据导入完成！用时: {total_time}")
            
            games_count = self.session.query(Game).count()
            leagues_count = self.session.query(League).count()
            teams_count = self.session.query(Team).count()
            players_count = self.session.query(Player).count()
            transactions_count = self.session.query(Transaction).count()
            transaction_players_count = self.session.query(TransactionPlayer).count()
            rosters_count = self.session.query(Roster).count()
            stats_count = self.session.query(PlayerStats).count()
            
            print(f"\n统计: 游戏({games_count}) 联盟({leagues_count}) 团队({teams_count}) 球员({players_count})")
            print(f"      交易({transactions_count}) 交易球员({transaction_players_count}) 名单({rosters_count}) 统计({stats_count})")
            
        except Exception as e:
            print(f"导入失败: {e}")
            self.session.rollback()
            raise
        finally:
            self.close()

def main():
    """主函数"""
    importer = FantasyDataImporter()
    importer.run_full_import()

if __name__ == "__main__":
    main() 