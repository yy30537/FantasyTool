#!/usr/bin/env python3
"""
Yahoo Fantasy统一数据获取工具
整合单联盟深度获取和时间序列数据获取功能
"""

import os
import sys
import time
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from flask import Flask, redirect, request, render_template, session, url_for, flash
from requests_oauthlib import OAuth2Session
import os
from dotenv import load_dotenv
import json
import time
import pickle
import pathlib
from datetime import datetime
import json
import os
import pickle
import pathlib
import time
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime, timedelta


import database.database_ops as database_ops
from extract.get_api_data import *
from database_writer import *
from database.model import *

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class YahooFantasyDataPipeline:
    """Yahoo Fantasy统一数据获取器"""
    
    def __init__(self, delay: int = 2, batch_size: int = 100):
        """初始化数据获取器"""
        self.delay = delay
        self.batch_size = batch_size
        self.selected_league = None
        self.db_writer = FantasyDatabaseWriter(batch_size=batch_size)
        # 添加缓存属性
        self._season_dates_cache = None
        self._cache_league_key = None
        
    def wait(self, message: Optional[str] = None) -> None:
        """等待指定时间"""
        time.sleep(self.delay)
    
    def close(self):
        """关闭资源"""
        if self.db_writer:
            self.db_writer.close()
    

    # ===== Pipeline =====
    def pipeline(self) -> bool:
        
        # 确保联盟基本信息存在于数据库中
        if not self.ensure_league_exists_in_db():
            print("❌ 无法获取联盟基本信息，请先选择联盟")
            return False

        if not self.selected_league:
            return False
        
        # 联盟
        league_key = self.selected_league['league_key']
        settings_data = self.fetch_api_league_settings(league_key)
        self.db_writer.write_league_settings(league_key, settings_data)
 
        # 赛季日程
        dates_data = self.extract_season_schedule_from_league()
        self.db_writer.write_date_dimensions_batch(dates_data)
            
        # 球员数据
        all_players = self.fetch_api_players(league_key)
        self.db_writer.write_players_batch(all_players, league_key)
        
        # 团队数据
        teams_data = self.fetch_api_teams(league_key)
        self.db_writer.write_teams_to_db(teams_data, league_key)

        # 获取所有transactions
        all_transactions = self.fetch_api_league_transactions(league_key)
        self.db_writer.write_transactions_to_db(all_transactions, league_key)
            
        
        # League standings数据
        standings_data = self.fetch_api_league_standings(league_key)
        if standings_data:
            teams_standings = self.parse_league_standings_from_api(standings_data)
            for team_info in teams_standings:
                season = self.selected_league.get('season')
                self.db_writer.write_league_standings_from_data(team_info, league_key, season)

        # League matchups数据
        team_keys = self._extract_team_keys_from_data(teams_data)
        for i, team_key in enumerate(team_keys):
            matchups_data = self.fetch_api_team_matchups(team_key)
            if matchups_data:
                self.db_writer.process_team_matchups_to_db(matchups_data, team_key, league_key, season)

        # 获取球员赛季统计数据
        player_season = self.pull_api_player_season(all_players, league_key)
        self.parse_player_season(player_season)
        self.db_writer.process_player_season_stats_data(player_season, league_key, season)



        #self.fetch_team_weekly_stats_from_matchups()

        #self.fetch_team_season_stats_from_standings()


        return True
    

    # ===== 辅助和交互方法 =====
    def run_interactive_menu(self):
        """运行交互式菜单"""
        while True:
            print("\n=== Yahoo NBA Fantasy 数据获取工具 ===")
            if self.selected_league:
                print(f"当前联盟: {self.selected_league['name']} ({self.selected_league['league_key']})")
            else:
                print("当前联盟: 未选择")
            
            print("\n1. Select League")
            print("2. Fetch Data")
            print("3. Fetch Roster History")
            print("4. Fetch Player Daily Stats")
            print("5. Database Summary")
            print("6. Clear Database")
            print("0. Quit")

            
            choice = input("\n请选择操作: ").strip()
            
            if choice == "0":
                print("Exit")
                break
            elif choice == "1":
                self.select_league()

            elif choice == "2":
                if self.ensure_league_selected():

                    self.pipeline()

            elif choice == "3":
                if self.ensure_league_selected():
                    self.run_roster_history_fetch()

            elif choice == "4":
                if self.ensure_league_selected():
                    self.run_player_stats_fetch()

            elif choice == "5":
                DatabaseOps.show_database_summary()

            elif choice == "6":
                DatabaseOps.clear_database(1)

            
            else:
                print("无效选择，请重试")
    
    def run_roster_history_fetch(self) -> bool:
        """运行阵容历史数据获取"""
        print("🚀 阵容历史数据获取")
        
        # 获取时间范围
        date_range = self.get_time_selection_interactive("阵容")
        if not date_range:
            return False
        
        start_date, end_date = date_range
        return self.fetch_roster_history_data(start_date, end_date)
    
    def run_player_stats_fetch(self) -> bool:
        """运行球员日统计数据获取（不包含赛季统计）"""
        print("🚀 球员日统计数据获取")
        
        # 获取时间范围
        date_range = self.get_time_selection_interactive("球员日统计")
        if not date_range:
            return False
        
        start_date, end_date = date_range
        return self.fetch_player_stats_data(start_date, end_date, include_season_stats=False)
    
    
    def select_league(self) -> bool:

        """获取基础数据并选择联盟（直接从数据库或API获取）"""
        print("🚀 获取联盟数据...")
        
        games_data = self.fetch_api_games()
        self.db_writer.write_games_data(games_data)
        
        # 提取游戏键并获取联盟数据
        game_keys = self.parse_game_keys(games_data)
        
        all_leagues = {}

        leagues_data = self.extract_leagues_from_db()

        if not leagues_data:
            for i, game_key in enumerate(game_keys):
                leagues_data = self.fetch_api_leagues(game_key)
                if leagues_data:
                    extracted_leagues = self.extract_leagues_from_YahooAPI(leagues_data, game_key)
                    if extracted_leagues:
                        all_leagues[game_key] = extracted_leagues
                
                if i < len(game_keys) - 1:
                    self.wait()
            
            if all_leagues:
                # 写入联盟数据到数据库
                leagues_count = self.db_writer.write_leagues_data(all_leagues)
                return all_leagues

        if not leagues_data:
            print("✗ 无法获取联盟数据")
            return False
        
        # 选择联盟
        selected_league = None

        """选择联盟"""
        all_leagues = self.print_league_selection_info(leagues_data)
        
        while True:
            try:
                choice = input(f"请选择联盟 (1-{len(all_leagues)}): ").strip()
                
                if not choice:
                    continue
                    
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(all_leagues):
                    selected_league = all_leagues[choice_num - 1]
                    print(f"\n✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                    self.selected_league = selected_league
                    return selected_league
                else:
                    print(f"请输入1到{len(all_leagues)}之间的数字")
                    
            except ValueError:
                print("请输入有效的数字")
            except KeyboardInterrupt:
                print("\n用户取消选择")
                return None 

    
    # ===== Games =====
    def fetch_api_games(self) -> Optional[Dict]:
        
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        if data:
            return data
        return None
    
    def parse_game_keys(self, games_data: Dict) -> List[str]:
        
        game_keys = []
        
        try:
            fantasy_content = games_data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_container = user_data[1]["games"]
            games_count = int(games_container.get("count", 0))
            
            for i in range(games_count):
                str_index = str(i)
                if str_index not in games_container:
                    continue
                    
                game_container = games_container[str_index]
                game_data = game_container["game"]
                
                if isinstance(game_data, list) and len(game_data) > 0:
                    game_info = game_data[0]
                    game_key = game_info.get("game_key")
                    game_type = game_info.get("type")
                    
                    if game_key and game_type == "full":
                        game_keys.append(game_key)
                
        except Exception as e:
            pass
        
        return game_keys


    # ===== Leagues =====
    def fetch_api_leagues(self, game_key: str) -> Optional[Dict]:
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        return get_api_data(url)
    
    def fetch_api_league_settings(self, league_key: str) -> Optional[Dict]:
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return get_api_data(url)
    
    def fetch_api_league_standings(self, league_key: str) -> Optional[Dict]:
        """获取league standings数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        return get_api_data(url)
    
    def parse_league_standings_from_api(self, standings_data: Dict) -> List[Dict]:
        """解析league standings数据并返回团队standings信息列表"""
        teams_standings = []
        
        try:
            if not standings_data:
                return teams_standings
            
            # 解析standings数据
            fantasy_content = standings_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
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
                return teams_standings
            
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
                return teams_standings
            
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # 提取team信息
                team_info = self.parse_team_standings(team_data)
                if team_info:
                    teams_standings.append(team_info)
            
            return teams_standings
            
        except Exception as e:
            print(f"    ✗ 解析league standings时出错: {e}")
            return teams_standings

    def extract_leagues_from_db(self) -> Optional[Dict]:
        
        try:
            
            leagues = self.db_writer.session.query(League).all()
            if not leagues:
                return None
            
            # 按game_key分组
            leagues_data = {}
            for league in leagues:
                game_key = league.game_key
                if game_key not in leagues_data:
                    leagues_data[game_key] = []
                
                # 转换为字典格式
                league_dict = {
                    'league_key': league.league_key,
                    'league_id': league.league_id,
                    'game_key': league.game_key,
                    'name': league.name,
                    'url': league.url,
                    'logo_url': league.logo_url,
                    'password': league.password,
                    'draft_status': league.draft_status,
                    'num_teams': league.num_teams,
                    'edit_key': league.edit_key,
                    'weekly_deadline': league.weekly_deadline,
                    'league_update_timestamp': league.league_update_timestamp,
                    'scoring_type': league.scoring_type,
                    'league_type': league.league_type,
                    'renew': league.renew,
                    'renewed': league.renewed,
                    'felo_tier': league.felo_tier,
                    'iris_group_chat_id': league.iris_group_chat_id,
                    'short_invitation_url': league.short_invitation_url,
                    'allow_add_to_dl_extra_pos': league.allow_add_to_dl_extra_pos,
                    'is_pro_league': league.is_pro_league,
                    'is_cash_league': league.is_cash_league,
                    'current_week': league.current_week,
                    'start_week': league.start_week,
                    'start_date': league.start_date,
                    'end_week': league.end_week,
                    'end_date': league.end_date,
                    'is_finished': league.is_finished,
                    'is_plus_league': league.is_plus_league,
                    'game_code': league.game_code,
                    'season': league.season
                }
                leagues_data[game_key].append(league_dict)
            
            return leagues_data
            
        except Exception as e:
            return None

    def extract_leagues_from_YahooAPI(self, data: Dict, game_key: str) -> List[Dict]:
        
        leagues = []
        
        try:
            if "error" in data:
                return leagues
            
            fantasy_content = data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_container = user_data[1]["games"]
            
            for i in range(int(games_container.get("count", 0))):
                str_index = str(i)
                if str_index not in games_container:
                    continue
                
                game_container = games_container[str_index]
                game_data = game_container["game"]
                
                current_game_key = None
                if isinstance(game_data, list) and len(game_data) > 0:
                    current_game_key = game_data[0].get("game_key")
                
                if current_game_key != game_key:
                    continue
                
                if len(game_data) > 1 and "leagues" in game_data[1]:
                    leagues_container = game_data[1]["leagues"]
                    leagues_count = int(leagues_container.get("count", 0))
                    
                    for j in range(leagues_count):
                        str_league_index = str(j)
                        if str_league_index not in leagues_container:
                            continue
                        
                        league_container = leagues_container[str_league_index]
                        league_data = league_container["league"]
                        
                        league_info = {}
                        if isinstance(league_data, list):
                            for item in league_data:
                                if isinstance(item, dict):
                                    league_info.update(item)
                        
                        # 确保联盟信息包含game_key
                        league_info["game_key"] = game_key
                        leagues.append(league_info)
                break
        
        except Exception as e:
            pass
        
        return leagues

    def extract_season_schedule_from_league(self) -> bool:
        
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 优先从数据库中的League表获取start_date和end_date
        start_date = None
        end_date = None
        
        try:
            from database.model import League
            league_db = self.db_writer.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if league_db:
                start_date = league_db.start_date
                end_date = league_db.end_date
            else:
                # 如果数据库中没有，从selected_league获取
                start_date = self.selected_league.get('start_date')
                end_date = self.selected_league.get('end_date')
        except Exception as e:
            start_date = self.selected_league.get('start_date')
            end_date = self.selected_league.get('end_date')
        
        if not start_date or not end_date:
            # NBA 2024-25赛季默认日期
            if season == '2024':
                start_date = '2024-10-22'
                end_date = '2025-04-13'
            else:
                return False
        
        try:
            # 解析日期
            
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # 生成日期范围
            dates_data = []
            current_date = start_dt
            
            while current_date <= end_dt:
                dates_data.append({
                    'date': current_date,
                    'league_key': league_key,
                    'season': season
                })
                current_date += timedelta(days=1)

            return dates_data
            
            
            
        except Exception as e:
            return False

    def ensure_league_exists_in_db(self) -> bool:
        
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        
        try:
            # 检查联盟是否已存在于数据库中
            existing_league = self.db_writer.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if existing_league:
                return True
            
            # 联盟不存在，说明数据库中缺少完整数据，建议重新获取
            print(f"⚠️ 联盟 {league_key} 不存在于数据库中")
            return False
                
        except Exception as e:
            return False
    
    def ensure_league_selected(self) -> bool:
        """确保已选择联盟"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        return True
    
    
    # ===== Players =====
    def fetch_api_players(self, league_key: str) -> List[Dict]:
        """使用改进的分页逻辑获取所有球员"""
        all_players = []
        start = 0
        page_size = 25
        max_iterations = 100
        iteration = 0
            
        while iteration < max_iterations:
            iteration += 1
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
            if start > 0:
                url += f";start={start}"
            url += "?format=json"
                
            players_data = get_api_data(url)
            if not players_data:
                break
                
            batch_players = self.parse_players_data_from_api(players_data)
            
            if not batch_players:
                break
                
            all_players.extend(batch_players)
                
            if len(batch_players) < page_size:
                break
                
            start += page_size
            time.sleep(0.5)
            
        return all_players
    
    def parse_players_data_from_api(self, players_data: Dict) -> List[Dict]:
        """从联盟球员数据中提取球员信息"""
        players = []
        
        try:
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            players_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                return players
            
            total_count = int(players_container.get("count", 0))
            
            for i in range(total_count):
                player_index = str(i)
                if player_index not in players_container:
                    continue
                    
                player_data = players_container[player_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if isinstance(player_info_list, list) and len(player_info_list) > 0:
                    player_basic_info = player_info_list[0]
                    
                    if isinstance(player_basic_info, list):
                        merged_info = {}
                        for info_item in player_basic_info:
                            if isinstance(info_item, dict):
                                merged_info.update(info_item)
                        if merged_info:
                            # 添加必要的字段处理
                            self._normalize_players_data(merged_info)
                            players.append(merged_info)
                    elif isinstance(player_basic_info, dict):
                        self._normalize_players_data(player_basic_info)
                        players.append(player_basic_info)
        
        except Exception as e:
            pass
        
        return players
    
    def _normalize_players_data(self, player_info: Dict) -> None:
        """标准化球员信息"""
        # 处理姓名信息
        if "name" in player_info:
            name_info = player_info["name"]
            if isinstance(name_info, dict):
                player_info["full_name"] = name_info.get("full")
                player_info["first_name"] = name_info.get("first")
                player_info["last_name"] = name_info.get("last")
        
        # 处理团队信息
        if "editorial_team_key" in player_info:
            player_info["current_team_key"] = player_info["editorial_team_key"]
        if "editorial_team_full_name" in player_info:
            player_info["current_team_name"] = player_info["editorial_team_full_name"]
        if "editorial_team_abbr" in player_info:
            player_info["current_team_abbr"] = player_info["editorial_team_abbr"]
        
        # 处理头像信息
        if "headshot" in player_info:
            headshot_info = player_info["headshot"]
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                player_info["headshot_url"] = headshot_info["url"]
        
        # 添加时间戳
        player_info["season"] = self.selected_league.get('season', 'unknown')
        player_info["last_updated"] = datetime.now()
    
    def pull_api_player_season(self, players: List, league_key: str) -> Optional[Dict]:
        """直接获取所有球员的赛季统计数据"""
        # 支持字典和对象两种格式
        player_keys = [
            player.player_key if hasattr(player, 'player_key') 
            else player['player_key'] 
            for player in players
        ]
        
        print(f"📊 获取 {len(player_keys)} 个球员的赛季统计数据...")
        
        try:
            # 构建API URL - 批量获取所有球员赛季统计
            player_keys_str = ",".join(player_keys)
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
            
            stats_data = get_api_data(url)
            if stats_data:
                print("✓ 成功获取球员赛季统计API数据")
                return stats_data
            else:
                print("❌ 获取球员赛季统计API数据失败")
                return None
                
        except Exception as e:
            print(f"❌ 获取球员赛季统计数据出错: {e}")
            return None
    
    def parse_player_season(self, stats_data: Dict) -> bool:
        """解析球员赛季统计数据（暂时保留为空，实际解析在database_writer中）"""
        # 实际的数据解析在 database_writer.process_player_season_stats_data 中进行
        return stats_data is not None
    
    
    def fetch_player_stats_data(self, start_date: date, end_date: date, include_season_stats: bool = True) -> bool:
        """获取球员统计数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            include_season_stats: 是否包含赛季统计数据
        """
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print(f"📊 获取球员统计数据: {start_date} 到 {end_date}")
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 获取数据库中的球员列表
        try:
            from database.model import Player
            players = self.db_writer.session.query(Player).filter_by(
                league_key=league_key
            ).all()
            
            if not players:
                print("❌ 数据库中没有球员数据，请先获取联盟数据")
                return False
            
            success_count = 0
            
            # 1. 获取赛季统计数据（如果需要）
            if include_season_stats:
                print("📊 获取球员赛季统计...")
                stats_data = self.pull_api_player_season(players, league_key)
                if stats_data:
                    self.parse_player_season(stats_data)
                    success_count_season = self.db_writer.process_player_season_stats_data(stats_data, league_key, season)
                    if success_count_season > 0:
                        success_count += 1
            
            # 2. 获取日统计数据
            print(f"📊 获取球员日统计数据...")
            if self._fetch_player_daily_stats_for_range(players, league_key, season, start_date, end_date):
                success_count += 1
            
            print(f"✓ 球员统计数据获取完成: {success_count}/{2 if include_season_stats else 1} 成功")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 获取球员统计数据失败: {e}")
            return False
    
    def _fetch_player_daily_stats_for_range(self, players: List, league_key: str, season: str, 
                                          start_date: date, end_date: date) -> bool:
        """获取指定日期范围的球员日统计数据"""
        from datetime import timedelta
        
        total_success_count = 0
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 分批处理球员，每批25个
            batch_size = 25
            total_batches = (len(players) + batch_size - 1) // batch_size
            
            day_success_count = 0
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(players))
                batch_players = players[start_idx:end_idx]
                
                # 支持字典和对象两种格式
                player_keys = [
                    player.player_key if hasattr(player, 'player_key') 
                    else player['player_key'] 
                    for player in batch_players
                ]
                
                try:
                    # 构建API URL - 批量获取球员日统计
                    player_keys_str = ",".join(player_keys)
                    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
                    
                    stats_data = get_api_data(url)
                    if stats_data:
                        batch_success_count = self.db_writer._process_player_daily_stats_data(stats_data, league_key, season, current_date)
                        day_success_count += batch_success_count
                        
                except Exception as e:
                    pass
                
                # 批次间等待
                if batch_idx < total_batches - 1:
                    time.sleep(0.5)
            
            total_success_count += day_success_count
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                time.sleep(1)
        
        return total_success_count > 0

    # ===== Teams =====
    def fetch_api_teams(self, league_key) -> Optional[Dict]:
        """获取团队数据并写入数据库"""
        league_key = self.selected_league['league_key']
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        
        teams_data = get_api_data(url)
        return teams_data
        
    def fetch_team_rosters(self, teams_data: Dict) -> bool:
        """获取所有团队的roster数据并写入数据库（获取赛季最后一天的roster，不是系统今天）"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        # 直接从数据库的League表获取赛季结束日期，不使用系统today
        roster_date = None
        try:
            from database.model import League
            from datetime import datetime
            
            league_key = self.selected_league['league_key']
            league_db = self.db_writer.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if league_db and league_db.end_date:
                # 使用赛季结束日期，不管是否已经过去
                roster_date = datetime.strptime(league_db.end_date, '%Y-%m-%d').date()
        except Exception as e:
            pass
        
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
                if roster_date:
                    # 获取指定日期的roster（赛季结束日期）
                    roster_data = self.fetch_api_team_roster_single_date(team_key, roster_date.strftime('%Y-%m-%d'))
                else:
                    # 获取当前roster（API默认）
                    roster_data = self._fetch_team_roster(team_key)
                
                if roster_data:
                    if self.db_writer.process_roster_data_to_db(roster_data, team_key, self.selected_league['league_key'], self.selected_league.get('season', '2024')):
                        success_count += 1
            except Exception as e:
                pass
            
            # 简化等待
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        return success_count > 0
    
    def _fetch_team_roster(self, team_key: str) -> Optional[Dict]:
        """获取单个团队的roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
        return get_api_data(url)
    
    def _extract_team_keys_from_data(self, teams_data: Dict) -> List[str]:
        """从团队数据中提取团队键"""
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
            pass
        
        return team_keys
    
    def fetch_team_rosters_for_date_range(self, teams_data: Dict, start_date: date, end_date: date) -> bool:
        """获取指定日期范围内的团队roster数据"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        success_count = 0
        
        from datetime import timedelta
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            day_success_count = 0
            for i, team_key in enumerate(team_keys):
                try:
                    roster_data = self.fetch_api_team_roster_single_date(team_key, date_str)
                    if roster_data:
                        if self.db_writer.process_roster_data_to_db(roster_data, team_key, self.selected_league['league_key'], self.selected_league.get('season', '2024')):
                            day_success_count += 1
                except Exception as e:
                    pass
                
                # 团队间间隔
                if i < len(team_keys) - 1:
                    time.sleep(0.2)
            
            success_count += day_success_count
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                self.wait()
        
        return success_count > 0
    
    def fetch_api_team_roster_single_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)
    
    def parse_team_standings(self, team_data) -> Optional[Dict]:
        """从team数据中提取standings信息"""
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
            
            standings_info = {
                "team_key": team_key,
                "team_standings": team_standings,
                "team_points": team_points
            }
            
            return standings_info
            
        except Exception as e:
            return None
    
    def fetch_api_team_matchups(self, team_key: str) -> Optional[Dict]:
        """获取团队matchups数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        return get_api_data(url)
    
    def fetch_roster_history_data(self, start_date: date, end_date: date) -> bool:
        """获取历史roster数据"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print(f"📋 获取阵容历史数据: {start_date} 到 {end_date}")
        
        league_key = self.selected_league['league_key']
        
        # 从数据库获取团队数据，避免重复写入
        teams_data = self._get_teams_data_from_db(league_key)
        
        if not teams_data:
            print("❌ 数据库中没有团队数据，请先执行'获取联盟数据'")
            return False
        
        # 获取指定时间范围内的roster数据
        success = self.fetch_team_rosters_for_date_range(teams_data, start_date, end_date)
        
        return success
    
    # ===== 团队每周统计数据获取方法 =====

    
    def fetch_team_weekly_stats_from_matchups(self) -> bool:
        """从 team_matchups 表中提取数据并生成 team_stats_weekly 记录"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        try:
            from database.model import TeamMatchups
            
            # 获取该联盟的所有 team_matchups 记录
            matchups = self.db_writer.session.query(TeamMatchups).filter_by(
                league_key=league_key,
                season=season
            ).all()
            
            if not matchups:
                print("❌ 没有找到团队对战数据，请先获取联盟数据")
                return False
            
            print(f"📊 发现 {len(matchups)} 条对战记录")
            
            success_count = 0
            processed_weeks = set()
            
            # 检查团队每周统计数据是否已存在
            from database.model import TeamStatsWeekly
            stats = self.db_writer.session.query(TeamStatsWeekly).filter_by(
                league_key=league_key,
                season=season
            ).all()
            
            if stats:
                print(f"✅ 团队每周统计数据已存在: {len(stats)} 条记录")
                print("💡 这些数据在获取联盟数据时已自动生成，无需重新处理")
                success_count = len(stats)
                processed_weeks = set(stat.week for stat in stats)
            else:
                print("❌ 未找到团队每周统计数据")
                print("💡 请选择选项2 '获取联盟数据' 来获取完整数据")
            
            print(f"✓ 团队每周统计数据生成完成: {success_count}/{len(matchups)} 成功")
            print(f"  处理周数: {sorted(processed_weeks)}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 获取团队每周统计数据失败: {e}")
            return False
    
    def _process_matchup_to_weekly_stats(self, team_key: str, week: int, 
                                       opponent_team_key: str, is_playoffs: bool,
                                       is_winner: Optional[bool], team_points: int,
                                       matchup_data: Dict, league_key: str, season: str) -> bool:
        """处理单个对战记录并生成团队周统计数据"""
        try:
            # 从 matchup_data 中提取团队统计数据
            team_stats_data = self._extract_team_stats_from_matchup(matchup_data, team_key)
            
            if not team_stats_data:
                return False
            
            # 使用专门的方法写入团队周统计
            return self.db_writer.write_team_weekly_stats_from_matchup(
                team_key=team_key,
                league_key=league_key,
                season=season,
                week=week,
                team_stats_data=team_stats_data
            )
            
        except Exception as e:
            return False
    
    def _extract_team_stats_from_matchup(self, matchup_data: Dict, target_team_key: str) -> Optional[Dict]:
        """从 matchup_data 中提取指定团队的统计数据"""
        try:
            if not matchup_data or "0" not in matchup_data:
                return None
            
            teams_container = matchup_data["0"].get("teams", {})
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # 从复杂的嵌套结构中提取 team_key 和 team_stats
                current_team_key = None
                team_stats = None
                
                if isinstance(team_data, list) and len(team_data) >= 2:
                    # team_data[0] 包含团队基本信息
                    team_info = team_data[0]
                    if isinstance(team_info, list):
                        for info_item in team_info:
                            if isinstance(info_item, dict) and "team_key" in info_item:
                                current_team_key = info_item["team_key"]
                                break
                    
                    # team_data[1] 包含统计数据
                    if len(team_data) > 1 and isinstance(team_data[1], dict):
                        team_stats_container = team_data[1]
                        if "team_stats" in team_stats_container:
                            team_stats = team_stats_container["team_stats"]
                
                # 如果找到了目标团队，返回其统计数据
                if current_team_key == target_team_key and team_stats:
                    return team_stats
            
            return None
            
        except Exception as e:
            return None
    
    def _count_categories_won(self, matchup_data: Dict, team_key: str) -> int:
        """计算团队在多少个统计类别中获胜"""
        try:
            categories_won = 0
            
            # 从 stat_winners 中统计该团队获胜的类别数量
            if "stat_winners" in matchup_data:
                stat_winners = matchup_data["stat_winners"]
                if isinstance(stat_winners, list):
                    for stat_winner in stat_winners:
                        if isinstance(stat_winner, dict) and "stat_winner" in stat_winner:
                            winner_info = stat_winner["stat_winner"]
                            if isinstance(winner_info, dict) and winner_info.get("winner_team_key") == team_key:
                                categories_won += 1
            
            return categories_won
            
        except Exception as e:
            return 0


    def fetch_team_season_stats_from_standings(self) -> bool:
        """从league_standings数据生成团队赛季统计"""
        if not self.selected_league:
            return False
        
        try:
            from database.model import LeagueStandings
            
            league_key = self.selected_league["league_key"]
            season = self.selected_league["season"]
            
            # 从数据库获取league_standings记录
            standings = self.db_writer.session.query(LeagueStandings).filter_by(
                league_key=league_key,
                season=season
            ).all()
            
            if not standings:
                print("❌ 未找到 league_standings 数据，请先获取联盟数据")
                return False
            
            print(f"📊 发现 {len(standings)} 条排名记录")
            
            success_count = 0
            total_count = len(standings)
            
            for standing in standings:
                if self._process_standing_to_season_stats(standing, league_key, season):
                    success_count += 1
            
            print(f"✓ 团队赛季统计数据生成完成: {success_count}/{total_count} 成功")
            return success_count > 0
            
        except Exception as e:
            print(f"获取团队赛季统计失败: {e}")
            return False
    
    def _process_standing_to_season_stats(self, standing, league_key: str, season: str) -> bool:
        """处理单个排名记录并生成团队赛季统计数据"""
        try:
            # 团队赛季数据现在直接存储在 league_standings 表中，无需额外处理
            print(f"✓ 团队 {standing.team_key} 的赛季数据已存储在 league_standings 表中")
            print(f"  排名: {standing.rank}, 胜场: {standing.wins}, 负场: {standing.losses}")
            return True
            
        except Exception as e:
            print(f"处理团队赛季统计失败 {standing.team_key}: {e}")
            return False

    def _get_teams_data_from_db(self, league_key: str) -> Optional[Dict]:
        """从数据库获取团队数据，转换为API格式以供后续方法使用"""
        try:
            from database.model import Team
            
            teams = self.db_writer.session.query(Team).filter_by(
                league_key=league_key
            ).all()
            
            if not teams:
                return None
            
            # 模拟API返回的团队数据格式
            teams_data = {
                "fantasy_content": {
                    "league": [
                        {},  # 其他联盟信息
                        {
                            "teams": {
                                "count": len(teams)
                            }
                        }
                    ]
                }
            }
            
            # 添加团队数据
            teams_container = teams_data["fantasy_content"]["league"][1]["teams"]
            for i, team in enumerate(teams):
                teams_container[str(i)] = {
                    "team": [
                        [
                            {
                                "team_key": team.team_key,
                                "team_id": team.team_id,
                                "name": team.name,
                                "url": team.url,
                                "team_logo_url": team.team_logo_url,
                                "waiver_priority": team.waiver_priority,
                                "number_of_moves": team.number_of_moves,
                                "number_of_trades": team.number_of_trades,
                                "roster_adds": {
                                    "coverage_value": team.roster_adds_week,
                                    "value": team.roster_adds_value
                                },
                                "clinched_playoffs": team.clinched_playoffs,
                                "has_draft_grade": team.has_draft_grade,
                                "managers": []  # 管理员数据需要时可以从Manager表获取
                            }
                        ]
                    ]
                }
            
            return teams_data
            
        except Exception as e:
            return None


    # ===== Transactions =====

    def fetch_api_league_transactions(self, league_key: str, max_count: int = None) -> List[Dict]:
        """获取联盟所有transactions（分页处理）"""
        all_transactions = []
        start = 0
        page_size = 25
        max_iterations = 200
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions"
            params = []
            if start > 0:
                params.append(f"start={start}")
            params.append(f"count={page_size}")
            
            if params:
                url += f";{';'.join(params)}"
            url += "?format=json"
            
            transactions_data = get_api_data(url)
            if not transactions_data:
                break
            
            batch_transactions = self.extract_transactions_from_data(transactions_data)
            
            if not batch_transactions:
                break
            
            all_transactions.extend(batch_transactions)
            
            if len(batch_transactions) < page_size:
                break
            
            start += page_size
            time.sleep(0.5)
        
        return all_transactions
    
    def extract_transactions_from_data(self, transactions_data: Dict) -> List[Dict]:
        """从API返回的数据中提取transaction信息"""
        transactions = []
        
        try:
            fantasy_content = transactions_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            transactions_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            elif isinstance(league_data, dict) and "transactions" in league_data:
                transactions_container = league_data["transactions"]
            
            if not transactions_container:
                return transactions
            
            total_count = int(transactions_container.get("count", 0))
            
            for i in range(total_count):
                transaction_index = str(i)
                if transaction_index not in transactions_container:
                    continue
                
                transaction_data = transactions_container[transaction_index]
                if "transaction" not in transaction_data:
                    continue
                
                transaction_info = transaction_data["transaction"]
                
                if isinstance(transaction_info, list):
                    merged_transaction = {}
                    for info_item in transaction_info:
                        if isinstance(info_item, dict):
                            merged_transaction.update(info_item)
                    if merged_transaction:
                        transactions.append(merged_transaction)
                elif isinstance(transaction_info, dict):
                    transactions.append(transaction_info)
        
        except Exception as e:
            pass
        
        return transactions
    

    # ===== Utilities =====
    def get_season_date_info(self) -> Dict:
        """获取赛季日期信息和状态"""
        if not self.selected_league:
            return {}
        
        league_key = self.selected_league['league_key']
        
        try:
            from database.model import League
            from datetime import datetime, date
            
            league_db = self.db_writer.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if not league_db:
                return {}
            
            start_date_str = league_db.start_date
            end_date_str = league_db.end_date
            is_finished = league_db.is_finished
            
            if not start_date_str or not end_date_str:
                return {}
            
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            today = date.today()
            
            # 判断赛季状态 - 主要基于数据库的is_finished字段
            if is_finished:
                season_status = "finished"
                latest_date = end_date  # 如果赛季已结束，使用结束日期
            elif today > end_date:
                season_status = "finished"  # 根据日期判断已结束
                latest_date = end_date
            elif today < start_date:
                season_status = "not_started"
                latest_date = start_date
            else:
                season_status = "ongoing"
                latest_date = min(today, end_date)  # 进行中的赛季使用今天和结束日期的较小值
            
            return {
                "start_date": start_date,
                "end_date": end_date,
                "latest_date": latest_date,
                "season_status": season_status,
                "is_finished": is_finished,
                "today": today
            }
            
        except Exception as e:
            print(f"获取赛季日期信息失败: {e}")
            return {}
    
    def calculate_date_range(self, mode: str, days_back: int = None, 
                           target_date: str = None) -> Optional[tuple]:
        """计算日期范围
        
        Args:
            mode: 'specific' | 'days_back' | 'full_season'
            days_back: 回溯天数 (mode='days_back'时使用)
            target_date: 目标日期 'YYYY-MM-DD' (mode='specific'时使用)
            
        Returns:
            (start_date, end_date) 或 None
        """
        from datetime import timedelta
        
        season_info = self.get_season_date_info()
        if not season_info:
            print("❌ 无法获取赛季信息")
            return None
        
        if mode == "specific":
            if not target_date:
                print("❌ 指定日期模式需要提供target_date")
                return None
            try:
                target = datetime.strptime(target_date, '%Y-%m-%d').date()
                # 检查日期是否在赛季范围内
                if target < season_info["start_date"] or target > season_info["end_date"]:
                    print(f"⚠️ 指定日期 {target_date} 不在赛季范围内 ({season_info['start_date']} 到 {season_info['end_date']})")
                return (target, target)
            except ValueError:
                print(f"❌ 日期格式错误: {target_date}")
                return None
        
        elif mode == "days_back":
            if days_back is None:
                print("❌ 天数回溯模式需要提供days_back")
                return None
            
            # 从最新日期向前回溯
            end_date = season_info["latest_date"]
            start_date = end_date - timedelta(days=days_back)
            
            # 确保不超出赛季范围
            start_date = max(start_date, season_info["start_date"])
            
            print(f"📅 天数回溯模式: 从 {start_date} 到 {end_date} (回溯{days_back}天，赛季状态: {season_info['season_status']})")
            return (start_date, end_date)
        
        elif mode == "full_season":
            start_date = season_info["start_date"]
            end_date = season_info["latest_date"]
            
            print(f"📅 完整赛季模式: 从 {start_date} 到 {end_date} (赛季状态: {season_info['season_status']})")
            return (start_date, end_date)
        
        else:
            print(f"❌ 不支持的模式: {mode}")
            return None

    def get_time_selection_interactive(self, data_type: str) -> Optional[tuple]:
        """交互式时间选择
        
        Args:
            data_type: 数据类型描述，如"阵容"、"球员统计"
            
        Returns:
            (start_date, end_date) 或 None
        """
        print(f"\n=== {data_type}数据时间选择 ===")
        print("1. 指定日期 (YYYY-MM-DD)")
        print("2. 指定时间段 (start: YYYY-MM-DD, end: YYYY-MM-DD)")
        print("3. 天数回溯")
        print("0. 返回")
        
        choice = input("\n请选择时间模式 (0-3): ").strip()
        
        if choice == "0":
            return None
        elif choice == "1":
            target_date = input("请输入日期 (YYYY-MM-DD): ").strip()
            if not target_date:
                print("❌ 日期不能为空")
                return None
            return self.calculate_date_range("specific", target_date=target_date)
        elif choice == "2":
            start_date = input("请输入开始日期 (YYYY-MM-DD): ").strip()
            end_date = input("请输入结束日期 (YYYY-MM-DD): ").strip()
            if not start_date or not end_date:
                print("❌ 开始和结束日期不能为空")
                return None
            try:
                from datetime import datetime
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                if start_dt > end_dt:
                    print("❌ 开始日期不能晚于结束日期")
                    return None
                return (start_dt, end_dt)
            except ValueError:
                print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                return None
        elif choice == "3":
            days_input = input("请输入回溯天数: ").strip()
            try:
                days_back = int(days_input)
                if days_back <= 0:
                    print("❌ 天数必须大于0")
                    return None
                return self.calculate_date_range("days_back", days_back=days_back)
            except ValueError:
                print("❌ 天数必须是有效数字")
                return None
        else:
            print("❌ 无效选择")
            return None

    def print_league_selection_info(self, leagues_data):
        """打印联盟选择信息"""
        print("\n" + "="*80)
        print("可选择的Fantasy联盟")
        print("="*80)
        
        all_leagues = []
        league_counter = 1
        
        for game_key, leagues in leagues_data.items():
            for league in leagues:
                league_info = {
                    'index': league_counter,
                    'league_key': league.get('league_key'),
                    'name': league.get('name', '未知联盟'),
                    'season': league.get('season', '未知赛季'),
                    'num_teams': league.get('num_teams', 0),
                    'game_code': league.get('game_code', '未知运动'),
                    'scoring_type': league.get('scoring_type', '未知'),
                    'is_finished': league.get('is_finished', 0) == 1
                }
                all_leagues.append(league_info)
                
                # 打印联盟信息
                status = "已结束" if league_info['is_finished'] else "进行中"
                print(f"{league_counter:2d}. {league_info['name']}")
                print(f"    联盟ID: {league_info['league_key']}")
                print(f"    运动类型: {league_info['game_code'].upper()} | 赛季: {league_info['season']} | 状态: {status}")
                print(f"    球队数量: {league_info['num_teams']} | 计分方式: {league_info['scoring_type']}")
                print()
                
                league_counter += 1
        
        print("="*80)
        return all_leagues


def main():
    """主函数 - Yahoo NBA Fantasy数据获取工具"""
    # 创建数据获取器
    fetcher = YahooFantasyDataPipeline(delay=2, batch_size=100)
    
    try:
        # 运行交互式菜单
        fetcher.run_interactive_menu()
    
    finally:
        # 确保清理资源
        fetcher.close()

if __name__ == "__main__":
    main()

