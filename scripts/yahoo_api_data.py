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

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yahoo_api_utils import (
    get_api_data,
    select_league_interactively
)
from database_writer import FantasyDatabaseWriter
from model import RosterDaily, Player, DateDimension, LeagueStandings, TeamMatchups, LeagueSettings, Manager

class YahooFantasyDataFetcher:
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
    
    # ===== 基础数据获取和联盟选择 =====
    
    def fetch_and_select_league(self, use_existing_data: bool = False) -> bool:
        """获取基础数据并选择联盟（直接从数据库或API获取）"""
        print("🚀 获取联盟数据...")
        
        # 优先从数据库获取联盟数据
        leagues_data = self._get_leagues_from_database()
        
        if not leagues_data or not use_existing_data:
            leagues_data = self._fetch_all_leagues_data()
        
        if not leagues_data:
            print("✗ 无法获取联盟数据")
            return False
        
        # 选择联盟
        selected_league = select_league_interactively(leagues_data)
        if not selected_league:
            print("✗ 未选择联盟")
            return False
        
        self.selected_league = selected_league
        
        print(f"✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
    def _get_leagues_from_database(self) -> Optional[Dict]:
        """从数据库获取联盟数据，格式化为选择界面需要的格式"""
        try:
            from model import League
            
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
    
    def _fetch_all_leagues_data(self) -> Optional[Dict]:
        """获取所有联盟数据并直接写入数据库，返回联盟数据用于选择"""
        # 获取games数据
        games_data = self._fetch_games_data()
        if not games_data:
            return None
        
        # 写入games数据到数据库
        games_count = self.db_writer.write_games_data(games_data)
        
        # 提取游戏键并获取联盟数据
        game_keys = self._extract_game_keys(games_data)
        if not game_keys:
            return None
        
        all_leagues = {}
        for i, game_key in enumerate(game_keys):
            leagues_data = self._fetch_leagues_data(game_key)
            if leagues_data:
                extracted_leagues = self._extract_leagues_from_data(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
            
            if i < len(game_keys) - 1:
                self.wait()
        
        if all_leagues:
            # 写入联盟数据到数据库
            leagues_count = self.db_writer.write_leagues_data(all_leagues)
            return all_leagues
        
        return None
    
    def _fetch_games_data(self) -> Optional[Dict]:
        """获取用户的games数据"""
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        if data:
            return data
        return None
    
    def _fetch_leagues_data(self, game_key: str) -> Optional[Dict]:
        """获取指定game下用户的leagues数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        return get_api_data(url)
    
    def _extract_game_keys(self, games_data: Dict) -> List[str]:
        """从游戏数据中提取游戏键（只包含type='full'的游戏）"""
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
    
    def _extract_leagues_from_data(self, data: Dict, game_key: str) -> List[Dict]:
        """从API返回的数据中提取联盟信息"""
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

    # ===== 单联盟深度数据获取 =====
    
    def _ensure_league_exists_in_db(self) -> bool:
        """确保当前选择的联盟基本信息存在于数据库中"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        
        try:
            # 检查联盟是否已存在于数据库中
            from model import League, Game
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
    
    def fetch_complete_league_data(self) -> bool:
        """获取完整的联盟数据并直接写入数据库（不包含roster历史数据）"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取联盟数据: {league_key} ===")
        
        success_count = 0
        total_steps = 6
        
        # 0. 确保联盟基本信息存在于数据库中
        if self._ensure_league_exists_in_db():
            success_count += 1
        
        # 1. 获取联盟详细信息
        if self.fetch_league_details():
            success_count += 1
        
        # 2. 获取赛季日程数据
        if self.fetch_season_schedule_data():
            success_count += 1
        
        # 3. 获取完整球员数据（优先获取，为后续步骤提供依赖）
        if self.fetch_complete_players_data():
            success_count += 1
        
        # 4. 获取团队数据
        teams_data = self.fetch_teams_data()
        if teams_data:
            success_count += 1
        
        # 5. 获取transaction数据
        if self.fetch_complete_transactions_data(teams_data):
            success_count += 1
        
        # 6. 获取团队统计数据（包括联盟排名、团队对战等）
        if self.fetch_team_stats_data(teams_data):
            success_count += 1
            
        print(f"\n✓ 联盟数据获取完成: {success_count}/{total_steps} 成功")
        return success_count > 0
    
    def fetch_league_details(self) -> bool:
        """获取联盟详细信息并写入数据库"""
        league_key = self.selected_league['league_key']
        
        try:
            # 获取联盟设置数据
            settings_data = self._fetch_league_settings(league_key)
            if settings_data:
                # 直接写入数据库
                self.db_writer.write_league_settings(league_key, settings_data)
                return True
            else:
                return False
        except Exception as e:
            return False
    
    def _fetch_league_settings(self, league_key: str) -> Optional[Dict]:
        """获取联盟设置"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return get_api_data(url)
    
    def fetch_teams_data(self) -> Optional[Dict]:
        """获取团队数据并写入数据库"""
        league_key = self.selected_league['league_key']
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        
        teams_data = get_api_data(url)
        
        if teams_data:
            # 提取并写入团队数据
            success_count = self._write_teams_to_db(teams_data, league_key)
            return teams_data
        else:
            return None
    
    def _write_teams_to_db(self, teams_data: Dict, league_key: str) -> int:
        """将团队数据写入数据库"""
        teams_list = []
        
        try:
            fantasy_content = teams_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            teams_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            
            if not teams_container:
                return 0
            
            teams_count = int(teams_container.get("count", 0))
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                team_data = team_container["team"]
                
                # 处理团队数据
                team_dict = self._extract_team_data_from_api(team_data)
                if team_dict:
                    teams_list.append(team_dict)
        
        except Exception as e:
            return 0
        
        # 批量写入数据库
        if teams_list:
            return self.db_writer.write_teams_batch(teams_list, league_key)
        
        return 0
    
    def _extract_team_data_from_api(self, team_data: List) -> Optional[Dict]:
        """从API团队数据中提取团队信息"""
        try:
            # team_data[0] 应该是一个包含多个字典的列表
            if not isinstance(team_data, list) or len(team_data) == 0:
                return None
            
            team_info_list = team_data[0]
            if not isinstance(team_info_list, list):
                return None
            
            # 提取团队基本信息
            team_dict = {}
            managers_data = []
            
            for item in team_info_list:
                if isinstance(item, dict):
                    if "managers" in item:
                        managers_data = item["managers"]
                    elif "team_logos" in item and item["team_logos"]:
                        # 处理team logo
                        if len(item["team_logos"]) > 0 and "team_logo" in item["team_logos"][0]:
                            team_dict["team_logo_url"] = item["team_logos"][0]["team_logo"].get("url")
                    elif "roster_adds" in item:
                        # 处理roster adds
                        roster_adds = item["roster_adds"]
                        team_dict["roster_adds_week"] = roster_adds.get("coverage_value")
                        team_dict["roster_adds_value"] = roster_adds.get("value")
                    elif "clinched_playoffs" in item:
                        team_dict["clinched_playoffs"] = bool(item["clinched_playoffs"])
                    elif "has_draft_grade" in item:
                        team_dict["has_draft_grade"] = bool(item["has_draft_grade"])
                    elif "number_of_trades" in item:
                        # 处理可能是字符串的数字字段
                        try:
                            team_dict["number_of_trades"] = int(item["number_of_trades"])
                        except (ValueError, TypeError):
                            team_dict["number_of_trades"] = 0
                    else:
                        # 直接更新其他字段
                        team_dict.update(item)
            
                        # 添加managers数据
            team_dict["managers"] = managers_data
            
            # 验证必要字段
            if not team_dict.get("team_key"):
                return None
            
            return team_dict
            
        except Exception as e:
            return None

    def fetch_team_rosters(self, teams_data: Dict) -> bool:
        """获取所有团队的roster数据并写入数据库（获取赛季最后一天的roster，不是系统今天）"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        # 直接从数据库的League表获取赛季结束日期，不使用系统today
        roster_date = None
        try:
            from model import League
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
                    roster_data = self._fetch_team_roster_for_date(team_key, roster_date.strftime('%Y-%m-%d'))
                else:
                    # 获取当前roster（API默认）
                    roster_data = self._fetch_team_roster(team_key)
                
                if roster_data:
                    if self._process_roster_data_to_db(roster_data, team_key):
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
    
    def _process_roster_data_to_db(self, roster_data: Dict, team_key: str) -> bool:
        """处理roster数据并写入数据库"""
        try:
            fantasy_content = roster_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 获取roster信息
            roster_info = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "roster" in item:
                        roster_info = item["roster"]
                        break
            
            if not roster_info:
                return False
            
            coverage_date = roster_info.get("date")
            is_prescoring = bool(roster_info.get("is_prescoring", False))
            is_editable = bool(roster_info.get("is_editable", False))
            
            # 获取球员信息
            players_container = None
            if "0" in roster_info and "players" in roster_info["0"]:
                players_container = roster_info["0"]["players"]
            
            if not players_container:
                return False
            
            roster_list = []
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) == 0:
                    continue
                
                # 提取球员基本信息
                player_info = player_info_list[0]
                position_data = player_info_list[1] if len(player_info_list) > 1 else {}
                
                player_dict = {}
                
                # 处理player info
                if isinstance(player_info, list):
                    for item in player_info:
                        if isinstance(item, dict):
                            player_dict.update(item)
                elif isinstance(player_info, dict):
                    player_dict.update(player_info)
                
                # 处理position data
                if isinstance(position_data, dict):
                    player_dict.update(position_data)
                
                # 创建roster记录
                roster_entry = {
                    "team_key": team_key,
                    "player_key": player_dict.get("player_key"),
                    "coverage_date": coverage_date,
                    "is_prescoring": is_prescoring,
                    "is_editable": is_editable,
                    "status": player_dict.get("status"),
                    "status_full": player_dict.get("status_full"),
                    "injury_note": player_dict.get("injury_note"),
                    "selected_position": self._extract_position_string(player_dict.get("selected_position"))
                }
                
                # 处理keeper信息
                if "is_keeper" in player_dict:
                    keeper_info = player_dict["is_keeper"]
                    if isinstance(keeper_info, dict):
                        roster_entry["is_keeper"] = keeper_info.get("status", False)
                        roster_entry["keeper_cost"] = str(keeper_info.get("cost", "")) if keeper_info.get("cost") else None
                        roster_entry["kept"] = keeper_info.get("kept", False)
                
                if roster_entry["player_key"]:
                    roster_list.append(roster_entry)
            
            # 批量写入数据库
            count = 0
            for roster_entry in roster_list:
                try:
                    # 解析日期 - 如果无法解析则跳过该记录，不使用当前日期
                    roster_date_str = roster_entry["coverage_date"]
                    if not roster_date_str:
                        continue
                    
                    try:
                        roster_date = datetime.strptime(roster_date_str, '%Y-%m-%d').date()
                    except Exception as e:
                        continue
                    
                    # 判断是否首发
                    selected_position = roster_entry["selected_position"]
                    is_starting = selected_position not in ['BN', 'IL', 'IR'] if selected_position else False
                    is_bench = selected_position == 'BN' if selected_position else False
                    is_injured_reserve = selected_position in ['IL', 'IR'] if selected_position else False
                    
                    # 使用新的write_roster_daily方法
                    if self.db_writer.write_roster_daily(
                        team_key=roster_entry["team_key"],
                        player_key=roster_entry["player_key"],
                        league_key=self.selected_league['league_key'],
                        roster_date=roster_date,
                        season=self.selected_league.get('season', '2024'),
                        selected_position=selected_position,
                        is_starting=is_starting,
                        is_bench=is_bench,
                        is_injured_reserve=is_injured_reserve,
                        player_status=roster_entry["status"],
                        status_full=roster_entry["status_full"],
                        injury_note=roster_entry["injury_note"],
                        is_keeper=roster_entry.get("is_keeper", False),
                        keeper_cost=roster_entry.get("keeper_cost"),
                        is_prescoring=roster_entry["is_prescoring"],
                        is_editable=roster_entry["is_editable"]
                    ):
                        count += 1
                        
                except Exception as e:
                    continue
            
            return count > 0
            
        except Exception as e:
            return False
    
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
                    roster_data = self._fetch_team_roster_for_date(team_key, date_str)
                    if roster_data:
                        if self._process_roster_data_to_db(roster_data, team_key):
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
    
    def _fetch_team_roster_for_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)

    def fetch_complete_players_data(self) -> bool:
        """获取完整的球员数据并直接写入数据库"""
        league_key = self.selected_league['league_key']
        
        # 1. 获取所有球员的基础信息
        all_players = self._fetch_all_league_players(league_key)
        if not all_players:
            return False
        
        # 2. 批量写入球员数据到数据库
        players_count = self.db_writer.write_players_batch(all_players, league_key)
        
        return players_count > 0
    
    def _fetch_all_league_players(self, league_key: str) -> List[Dict]:
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
                
            batch_players = self._extract_player_info_from_league_data(players_data)
            
            if not batch_players:
                break
                
            all_players.extend(batch_players)
                
            if len(batch_players) < page_size:
                break
                
            start += page_size
            time.sleep(0.5)
            
        return all_players
    
    def _extract_player_info_from_league_data(self, players_data: Dict) -> List[Dict]:
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
                            self._normalize_player_info(merged_info)
                            players.append(merged_info)
                    elif isinstance(player_basic_info, dict):
                        self._normalize_player_info(player_basic_info)
                        players.append(player_basic_info)
        
        except Exception as e:
            pass
        
        return players
    
    def _normalize_player_info(self, player_info: Dict) -> None:
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
    
    def fetch_complete_transactions_data(self, teams_data: Optional[Dict] = None) -> bool:
        """获取完整的transaction数据并直接写入数据库"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        
        # 获取所有transactions
        all_transactions = self._fetch_all_league_transactions(league_key)
        
        if all_transactions:
            # 直接写入数据库
            transactions_count = self._write_transactions_to_db(all_transactions, league_key)
            return transactions_count > 0
        else:
            return False
    
    def _fetch_all_league_transactions(self, league_key: str, max_count: int = None) -> List[Dict]:
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
            
            batch_transactions = self._extract_transactions_from_data(transactions_data)
            
            if not batch_transactions:
                break
            
            all_transactions.extend(batch_transactions)
            
            if len(batch_transactions) < page_size:
                break
            
            start += page_size
            time.sleep(0.5)
        
        return all_transactions
    
    def _extract_transactions_from_data(self, transactions_data: Dict) -> List[Dict]:
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
    
    def _write_transactions_to_db(self, transactions: List[Dict], league_key: str) -> int:
        """将transaction数据写入数据库"""
        if not transactions:
            return 0
        
        return self.db_writer.write_transactions_batch(transactions, league_key)

    def fetch_team_stats_data(self, teams_data: Optional[Dict] = None) -> bool:
        """获取团队统计数据"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 获取团队列表
        if teams_data is None:
            teams_data = self.fetch_teams_data()
            if not teams_data:
                return False
        
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        # 1. 获取league standings数据
        standings_success = self._fetch_and_process_league_standings(league_key, season)
        
        # 2. 获取每个团队的matchups数据
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
                # 获取团队matchups数据
                matchups_data = self._fetch_team_matchups(team_key)
                if matchups_data:
                    if self._process_team_matchups_to_db(matchups_data, team_key, league_key, season):
                        success_count += 1
                        
            except Exception as e:
                pass
            
            # 请求间隔
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        return standings_success and success_count > 0
    
    def _fetch_and_process_league_standings(self, league_key: str, season: str) -> bool:
        """获取并处理league standings数据"""
        try:
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
            standings_data = get_api_data(url)
            
            if not standings_data:
                return False
            
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
                return False
            
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
                return False
            
            teams_count = int(teams_container.get("count", 0))
            success_count = 0
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # 提取team信息
                team_info = self._extract_team_standings_info(team_data)
                if not team_info:
                    continue
                
                # 写入数据库
                if self._write_league_standings_to_db(team_info, league_key, season):
                    success_count += 1
            
            return success_count > 0
        
        except Exception as e:
            print(f"    ✗ 获取league standings时出错: {e}")
            return False
    
    def _extract_team_standings_info(self, team_data) -> Optional[Dict]:
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
    
    def _write_league_standings_to_db(self, team_info: Dict, league_key: str, season: str) -> bool:
        """将league standings数据写入数据库"""
        try:
            team_key = team_info["team_key"]
            team_standings = team_info.get("team_standings", {})
            team_points = team_info.get("team_points", {})
            
            # 提取standings数据
            rank = None
            wins = 0
            losses = 0
            ties = 0
            win_percentage = 0.0
            games_back = "-"
            playoff_seed = None
            
            if isinstance(team_standings, dict):
                rank = team_standings.get("rank")
                wins = int(team_standings.get("outcome_totals", {}).get("wins", 0))
                losses = int(team_standings.get("outcome_totals", {}).get("losses", 0))
                ties = int(team_standings.get("outcome_totals", {}).get("ties", 0))
                win_percentage = float(team_standings.get("outcome_totals", {}).get("percentage", 0))
                games_back = team_standings.get("games_back", "-")
                playoff_seed = team_standings.get("playoff_seed")
                
                # 分区记录
                divisional_outcome = team_standings.get("divisional_outcome_totals", {})
                divisional_wins = int(divisional_outcome.get("wins", 0))
                divisional_losses = int(divisional_outcome.get("losses", 0))
                divisional_ties = int(divisional_outcome.get("ties", 0))
            else:
                divisional_wins = 0
                divisional_losses = 0
                divisional_ties = 0
            
            # 写入数据库（不再需要存储 JSON 数据，所有字段已结构化）
            return self.db_writer.write_league_standings(
                league_key=league_key,
                team_key=team_key,
                season=season,
                rank=rank,
                playoff_seed=playoff_seed,
                wins=wins,
                losses=losses,
                ties=ties,
                win_percentage=win_percentage,
                games_back=games_back,
                divisional_wins=divisional_wins,
                divisional_losses=divisional_losses,
                divisional_ties=divisional_ties
            )
            
        except Exception as e:
            return False

    def handle_database_error(self) -> bool:
        """处理数据库错误，必要时重新创建表结构"""
        try:
            print("🔧 检测到数据库错误，尝试修复...")
            
            from model import recreate_tables, create_database_engine
            
            # 尝试重新创建数据库表
            engine = create_database_engine()
            if recreate_tables(engine):
                print("✅ 数据库表结构修复成功")
                # 重新初始化数据库写入器
                self.db_writer = FantasyDatabaseWriter(batch_size=self.batch_size)
                return True
            else:
                print("❌ 数据库表结构修复失败")
                return False
                
        except Exception as e:
            print(f"处理数据库错误时出错: {e}")
            return False
    
    # ===== 时间范围计算工具方法 =====
    
    def get_season_date_info(self) -> Dict:
        """获取赛季日期信息和状态"""
        if not self.selected_league:
            return {}
        
        league_key = self.selected_league['league_key']
        
        try:
            from model import League
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

    # ===== 辅助和交互方法 =====
    
    def run_interactive_menu(self):
        """运行交互式菜单"""
        while True:
            print("\n=== Yahoo NBA Fantasy 数据获取工具 ===")
            if self.selected_league:
                print(f"当前联盟: {self.selected_league['name']} ({self.selected_league['league_key']})")
            else:
                print("当前联盟: 未选择")
            
            print("\n1. 选择联盟")
            print("2. 获取联盟数据")
            print("3. 获取阵容历史数据")
            print("4. 获取球员日统计数据")
            print("5. 获取球员赛季统计数据")
            print("6. 数据库摘要")
            print("7. 清空数据库")
            print("8. 获取团队每周数据")
            print("9. 获取团队赛季数据")
            print("0. 退出")
            
            choice = input("\n请选择操作 (0-9): ").strip()
            
            if choice == "0":
                print("退出程序")
                break
            elif choice == "1":
                self.select_league_interactive()
            elif choice == "2":
                if self._ensure_league_selected():
                    if self.run_complete_league_fetch():
                        self.show_database_summary()
            elif choice == "3":
                if self._ensure_league_selected():
                    self.run_roster_history_fetch()
            elif choice == "4":
                if self._ensure_league_selected():
                    self.run_player_stats_fetch()
            elif choice == "5":
                if self._ensure_league_selected():
                    self.run_player_season_stats_fetch()
            elif choice == "6":
                self.show_database_summary()
            elif choice == "7":
                confirm = input("确认清空数据库？输入 'YES' 确认: ").strip()
                if confirm == "YES":
                    if self.clear_database(confirm=True):
                        print("✅ 数据库已清空")
                    else:
                        print("❌ 数据库清空失败")
            elif choice == "8":
                if self._ensure_league_selected():
                    self.run_team_weekly_stats_fetch()
            elif choice == "9":
                if self._ensure_league_selected():
                    self.run_team_season_stats_fetch()
            else:
                print("无效选择，请重试")
    
    def _ensure_league_selected(self) -> bool:
        """确保已选择联盟"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        return True
    
    def select_league_interactive(self) -> bool:
        """交互式选择联盟"""
        return self.fetch_and_select_league(use_existing_data=True)
    
    def run_complete_league_fetch(self) -> bool:
        """运行完整联盟数据获取"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print(f"🚀 开始获取联盟数据: {self.selected_league['league_key']}")
        return self.fetch_complete_league_data()
    
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
    
    def run_player_season_stats_fetch(self) -> bool:
        """运行球员赛季统计数据获取"""
        print("🚀 球员赛季统计数据获取")
        
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 获取数据库中的球员列表
        try:
            from model import Player
            players = self.db_writer.session.query(Player).filter_by(
                league_key=league_key
            ).all()
            
            if not players:
                print("❌ 数据库中没有球员数据，请先获取联盟数据")
                return False
            
            print(f"📊 开始获取 {len(players)} 个球员的赛季统计数据...")
            success = self._fetch_player_season_stats(players, league_key, season)
            
            if success:
                print("✓ 球员赛季统计数据获取完成")
                self.show_database_summary()
            else:
                print("❌ 球员赛季统计数据获取失败")
            
            return success
            
        except Exception as e:
            print(f"❌ 获取球员赛季统计数据失败: {e}")
            return False
    
    def show_database_summary(self):
        """显示数据库摘要"""
        try:
            from model import (League, Team, Player, Game, Transaction, 
                             RosterDaily, TeamStatsWeekly,
                             LeagueStandings, TeamMatchups, LeagueSettings, Manager,
                             PlayerSeasonStats, PlayerDailyStats, StatCategory,
                             PlayerEligiblePosition, TransactionPlayer, DateDimension)
            
            print("\n📊 数据库摘要:")
            print("-" * 60)
            
            # 统计各表数据量
            tables = [
                ("游戏", Game),
                ("联盟", League), 
                ("联盟设置", LeagueSettings),
                ("统计类别", StatCategory),
                ("团队", Team),
                ("管理员", Manager),
                ("球员", Player),
                ("球员位置", PlayerEligiblePosition),
                ("交易", Transaction),
                ("交易球员", TransactionPlayer),
                ("每日阵容", RosterDaily),
                ("球员赛季统计", PlayerSeasonStats),
                ("球员日统计", PlayerDailyStats),
                ("团队周统计", TeamStatsWeekly),
                ("联盟排名", LeagueStandings),
                ("团队对战", TeamMatchups),
                ("日期维度", DateDimension)
            ]
            
            for name, model in tables:
                try:
                    count = self.db_writer.session.query(model).count()
                    print(f"{name:12}: {count:6d} 条记录")
                except Exception as e:
                    print(f"{name:12}: 查询失败 ({e})")
            
            print("-" * 60)
            
        except Exception as e:
            print(f"显示数据库摘要失败: {e}")
    
    def clear_database(self, confirm: bool = False) -> bool:
        """清空数据库"""
        if not confirm:
            print("❌ 未确认清空操作")
            return False
        
        return self.db_writer.clear_database(confirm=True)
    
    # ===== 辅助处理方法 =====
    
    def _extract_position_string(self, position_data) -> Optional[str]:
        """从位置数据中提取位置字符串"""
        if not position_data:
            return None
        
        if isinstance(position_data, str):
            return position_data
        
        if isinstance(position_data, dict):
            return position_data.get("position", None)
        
        if isinstance(position_data, list) and len(position_data) > 0:
            if isinstance(position_data[0], str):
                return position_data[0]
            elif isinstance(position_data[0], dict):
                return position_data[0].get("position", None)
        
        return None
    
    def _fetch_team_matchups(self, team_key: str) -> Optional[Dict]:
        """获取团队matchups数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        return get_api_data(url)
    
    def _process_team_matchups_to_db(self, matchups_data: Dict, team_key: str, 
                                   league_key: str, season: str) -> bool:
        """处理团队matchups数据并写入数据库（使用新的结构化字段）"""
        try:
            fantasy_content = matchups_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 查找matchups容器
            matchups_container = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "matchups" in item:
                        matchups_container = item["matchups"]
                        break
            
            if not matchups_container:
                return False
            
            matchups_count = int(matchups_container.get("count", 0))
            success_count = 0
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_data = matchups_container[str_index]
                if "matchup" not in matchup_data:
                    continue
                
                matchup_info = matchup_data["matchup"]
                
                # 使用新的方法直接从matchup数据写入数据库
                if self.db_writer.write_team_matchup_from_data(matchup_info, team_key, league_key, season):
                    success_count += 1
                    
                    # 同时写入TeamStatsWeekly数据
                    self._extract_and_write_team_weekly_stats(matchup_info, team_key, league_key, season)
            
            return success_count > 0
            
        except Exception as e:
            print(f"处理团队matchups失败 {team_key}: {e}")
            return False
    
    def _extract_and_write_team_weekly_stats(self, matchup_info: Dict, team_key: str, 
                                           league_key: str, season: str) -> bool:
        """从matchup数据中提取并写入团队周统计数据"""
        try:
            week = matchup_info.get("week")
            if week is None:
                return False
            
            # 从matchup中的teams数据提取统计数据
            teams_data = matchup_info.get("0", {}).get("teams", {})
            team_stats_data = self._extract_team_stats_from_matchup_data(teams_data, team_key)
            
            if team_stats_data:
                return self.db_writer.write_team_weekly_stats_from_matchup(
                    team_key=team_key,
                    league_key=league_key,
                    season=season,
                    week=week,
                    team_stats_data=team_stats_data
                )
            
            return False
            
        except Exception as e:
            print(f"提取团队周统计失败 {team_key}: {e}")
            return False
    
    def _extract_team_stats_from_matchup_data(self, teams_data: Dict, target_team_key: str) -> Optional[Dict]:
        """从teams数据中提取目标团队的统计数据"""
        try:
            teams_count = int(teams_data.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if "team" not in team_container:
                    continue
                
                team_info = team_container["team"]
                
                # 提取team_key
                current_team_key = None
                if isinstance(team_info, list) and len(team_info) >= 1:
                    team_basic_info = team_info[0]
                    if isinstance(team_basic_info, list):
                        for info_item in team_basic_info:
                            if isinstance(info_item, dict) and "team_key" in info_item:
                                current_team_key = info_item["team_key"]
                                break
                
                # 如果找到目标团队，提取统计数据
                if current_team_key == target_team_key and len(team_info) > 1:
                    team_stats_container = team_info[1]
                    if "team_stats" in team_stats_container:
                        return team_stats_container["team_stats"]
            
            return None
            
        except Exception as e:
            print(f"提取团队统计数据失败: {e}")
            return None
    
    def _extract_matchup_info(self, matchup_info, team_key: str) -> Optional[Dict]:
        """从matchup数据中提取对战信息"""
        
        def convert_to_bool(value) -> bool:
            """将API返回的布尔值（字符串'0'/'1'或数字0/1）转换为真正的布尔值"""
            if value is None:
                return False
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip() == '1'
            if isinstance(value, (int, float)):
                return value == 1
            return False
        
        try:
            week = None
            week_start = None
            week_end = None
            status = None
            is_playoffs = False
            is_consolation = False
            is_matchup_of_week = False
            opponent_team_key = None
            is_winner = None
            is_tied = False
            team_points = 0
            
            # 提取基本信息
            if isinstance(matchup_info, list):
                for item in matchup_info:
                    if isinstance(item, dict):
                        week = item.get("week", week)
                        week_start = item.get("week_start", week_start)
                        week_end = item.get("week_end", week_end)
                        status = item.get("status", status)
                        is_playoffs = convert_to_bool(item.get("is_playoffs", is_playoffs))
                        is_consolation = convert_to_bool(item.get("is_consolation", is_consolation))
                        is_matchup_of_week = convert_to_bool(item.get("is_matchup_of_week", is_matchup_of_week))
                        
                        # 提取is_tied和winner信息
                        is_tied = convert_to_bool(item.get("is_tied", is_tied))
                        winner_team_key = item.get("winner_team_key")
                        if winner_team_key:
                            is_winner = (winner_team_key == team_key)
                        
                        # 查找teams信息
                        if "teams" in item:
                            teams_data = item["teams"]
                            team_info = self._extract_team_matchup_details(teams_data, team_key)
                            if team_info:
                                opponent_team_key = team_info.get("opponent_team_key")
                                if is_winner is None:  # 如果还没有从winner_team_key获取到胜负信息
                                    is_winner = team_info.get("is_winner")
                                if not is_tied:  # 如果还没有获取到平局信息
                                    is_tied = convert_to_bool(team_info.get("is_tied", False))
                                team_points = team_info.get("team_points", 0)
            elif isinstance(matchup_info, dict):
                week = matchup_info.get("week")
                week_start = matchup_info.get("week_start")
                week_end = matchup_info.get("week_end")
                status = matchup_info.get("status")
                is_playoffs = convert_to_bool(matchup_info.get("is_playoffs", False))
                is_consolation = convert_to_bool(matchup_info.get("is_consolation", False))
                is_matchup_of_week = convert_to_bool(matchup_info.get("is_matchup_of_week", False))
                
                # 提取is_tied和winner信息
                is_tied = convert_to_bool(matchup_info.get("is_tied", False))
                winner_team_key = matchup_info.get("winner_team_key")
                if winner_team_key:
                    is_winner = (winner_team_key == team_key)
                
                if "teams" in matchup_info:
                    teams_data = matchup_info["teams"]
                    team_info = self._extract_team_matchup_details(teams_data, team_key)
                    if team_info:
                        opponent_team_key = team_info.get("opponent_team_key")
                        if is_winner is None:  # 如果还没有从winner_team_key获取到胜负信息
                            is_winner = team_info.get("is_winner")
                        if not is_tied:  # 如果还没有获取到平局信息
                            is_tied = convert_to_bool(team_info.get("is_tied", False))
                        team_points = team_info.get("team_points", 0)
            
            if week is None:
                return None
            
            return {
                "week": week,
                "week_start": week_start,
                "week_end": week_end,
                "status": status,
                "opponent_team_key": opponent_team_key,
                "is_winner": is_winner,
                "is_tied": is_tied,
                "team_points": team_points,
                "is_playoffs": is_playoffs,
                "is_consolation": is_consolation,
                "is_matchup_of_week": is_matchup_of_week
            }
            
        except Exception as e:
            return None
    
    def _extract_team_matchup_details(self, teams_data, target_team_key: str) -> Optional[Dict]:
        """从teams数据中提取当前团队的对战详情"""
        try:
            if not isinstance(teams_data, dict):
                return None
            
            # 查找对战中的两个团队，从 teams_data["0"]["teams"] 中
            matchup_container = teams_data.get("0", {})
            teams_container = matchup_container.get("teams", {})
            
            if not teams_container:
                return None
            
            teams_count = int(teams_container.get("count", 0))
            opponent_team_key = None
            is_winner = None
            is_tied = False
            team_points = 0
            
            target_team_data = None
            opponent_team_data = None
            
            # 遍历对战中的所有团队（通常是2个）
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_info = team_container["team"]
                
                # 提取team_key和points
                current_team_key = None
                points = 0
                
                if isinstance(team_info, list) and len(team_info) >= 2:
                    # team_info[0] 包含团队基本信息（包括 team_key）
                    team_basic_info = team_info[0]
                    if isinstance(team_basic_info, list):
                        for info_item in team_basic_info:
                            if isinstance(info_item, dict) and "team_key" in info_item:
                                current_team_key = info_item["team_key"]
                                break
                    
                    # team_info[1] 包含团队统计数据和积分信息
                    if len(team_info) > 1 and isinstance(team_info[1], dict):
                        team_stats_container = team_info[1]
                        if "team_points" in team_stats_container:
                            team_points_data = team_stats_container["team_points"]
                            if isinstance(team_points_data, dict) and "total" in team_points_data:
                                try:
                                    points = int(team_points_data["total"])
                                except (ValueError, TypeError):
                                    points = 0
                
                # 区分目标团队和对手团队
                if current_team_key == target_team_key:
                    target_team_data = {"team_key": current_team_key, "points": points}
                    team_points = points
                elif current_team_key and current_team_key != target_team_key:
                    opponent_team_data = {"team_key": current_team_key, "points": points}
                    opponent_team_key = current_team_key
            
            # 判断胜负关系（基于积分比较）
            if target_team_data and opponent_team_data:
                target_points = target_team_data["points"]
                opponent_points = opponent_team_data["points"]
                
                if target_points > opponent_points:
                    is_winner = True
                    is_tied = False
                elif target_points < opponent_points:
                    is_winner = False
                    is_tied = False
                else:
                    is_winner = None
                    is_tied = True
            
            return {
                "opponent_team_key": opponent_team_key,
                "is_winner": is_winner,
                "is_tied": is_tied,
                "team_points": team_points
            }
            
        except Exception as e:
            return None
    
    # 旧的_write_team_matchup_to_db方法已被移除，现在直接使用 db_writer.write_team_matchup_from_data
    
    def _fetch_player_season_stats_direct(self) -> bool:
        """直接获取球员赛季统计数据（不依赖日期维度）"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 获取数据库中的球员列表
        try:
            from model import Player
            players = self.db_writer.session.query(Player).filter_by(
                league_key=league_key
            ).all()
            
            if not players:
                return False
            
            return self._fetch_player_season_stats(players, league_key, season)
            
        except Exception as e:
            return False

    def _fetch_player_season_stats(self, players: List, league_key: str, season: str) -> bool:
        """获取球员赛季统计数据"""
        print(f"📊 开始获取球员赛季统计数据... 总共 {len(players)} 个球员")
        total_success_count = 0
        
        # 分批处理球员，每批25个（API限制）
        batch_size = 25
        total_batches = (len(players) + batch_size - 1) // batch_size
        
        print(f"📊 将分 {total_batches} 批处理")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(players))
            batch_players = players[start_idx:end_idx]
            
            player_keys = [player.player_key for player in batch_players]
            
            print(f"📊 处理第 {batch_idx + 1}/{total_batches} 批，{len(player_keys)} 个球员")
            
            try:
                # 构建API URL - 批量获取球员赛季统计
                player_keys_str = ",".join(player_keys)
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
                
                print(f"📊 请求URL: {url[:100]}...")
                stats_data = get_api_data(url)
                if stats_data:
                    print(f"📊 成功获取API数据，开始处理...")
                    batch_success_count = self._process_player_season_stats_data(stats_data, league_key, season)
                    total_success_count += batch_success_count
                    print(f"📊 本批处理完成，成功写入 {batch_success_count} 条记录")
                else:
                    print(f"❌ 第 {batch_idx + 1} 批API数据获取失败")
                    
            except Exception as e:
                print(f"❌ 第 {batch_idx + 1} 批处理出错: {e}")
            
            # 批次间等待
            if batch_idx < total_batches - 1:
                time.sleep(1)
        
        print(f"📊 球员赛季统计数据获取完成，总共成功写入 {total_success_count} 条记录")
        return total_success_count > 0
    
    def _process_player_season_stats_data(self, stats_data: Dict, league_key: str, season: str) -> int:
        """处理球员赛季统计数据"""
        success_count = 0
        
        try:
            fantasy_content = stats_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                print("❌ API数据中未找到players容器")
                return 0
            
            players_count = int(players_container.get("count", 0))
            print(f"📊 API返回 {players_count} 个球员的统计数据")
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    print(f"❌ 球员 {str_index} 数据格式不正确")
                    continue
                
                # 提取球员基本信息
                player_basic_info = player_info_list[0]
                player_key = None
                editorial_player_key = None
                
                if isinstance(player_basic_info, list):
                    for item in player_basic_info:
                        if isinstance(item, dict):
                            if "player_key" in item:
                                player_key = item["player_key"]
                            elif "editorial_player_key" in item:
                                editorial_player_key = item["editorial_player_key"]
                elif isinstance(player_basic_info, dict):
                    player_key = player_basic_info.get("player_key")
                    editorial_player_key = player_basic_info.get("editorial_player_key")
                
                if not player_key:
                    print(f"❌ 球员 {str_index} 缺少player_key")
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if not isinstance(stats_container, dict) or "player_stats" not in stats_container:
                    print(f"❌ 球员 {player_key} 缺少统计数据容器")
                    continue
                
                player_stats = stats_container["player_stats"]
                if not isinstance(player_stats, dict) or "stats" not in player_stats:
                    print(f"❌ 球员 {player_key} 缺少stats字段")
                    continue
                
                stats_list = player_stats["stats"]
                if not isinstance(stats_list, list):
                    print(f"❌ 球员 {player_key} stats不是列表格式")
                    continue
                
                # 转换统计数据为字典格式
                stats_dict = {}
                for stat_item in stats_list:
                    if "stat" in stat_item:
                        stat_info = stat_item["stat"]
                        stat_id = stat_info.get("stat_id")
                        value = stat_info.get("value")
                        if stat_id is not None:
                            stats_dict[str(stat_id)] = value
                
                print(f"📊 球员 {player_key} 提取到 {len(stats_dict)} 个统计项")
                
                # 写入数据库
                if stats_dict:
                    write_result = self.db_writer.write_player_season_stat_values(
                        player_key=player_key,
                        editorial_player_key=editorial_player_key or player_key,
                        league_key=league_key,
                        season=season,
                        stats_data=stats_dict
                    )
                    if write_result:
                        success_count += 1
                        print(f"✓ 球员 {player_key} 赛季统计写入成功")
                    else:
                        print(f"❌ 球员 {player_key} 赛季统计写入失败")
                else:
                    print(f"❌ 球员 {player_key} 没有有效的统计数据")
            
        except Exception as e:
            print(f"❌ 处理球员赛季统计数据时出错: {e}")
            import traceback
            traceback.print_exc()
        
        return success_count
    
    def _process_player_daily_stats_data(self, stats_data: Dict, league_key: str, 
                                       season: str, date_obj: date) -> int:
        """处理球员日统计数据"""
        success_count = 0
        
        try:
            fantasy_content = stats_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                return 0
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    continue
                
                # 提取球员基本信息
                player_basic_info = player_info_list[0]
                player_key = None
                editorial_player_key = None
                
                if isinstance(player_basic_info, list):
                    for item in player_basic_info:
                        if isinstance(item, dict):
                            if "player_key" in item:
                                player_key = item["player_key"]
                            elif "editorial_player_key" in item:
                                editorial_player_key = item["editorial_player_key"]
                elif isinstance(player_basic_info, dict):
                    player_key = player_basic_info.get("player_key")
                    editorial_player_key = player_basic_info.get("editorial_player_key")
                
                if not player_key:
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if not isinstance(stats_container, dict) or "player_stats" not in stats_container:
                    continue
                
                player_stats = stats_container["player_stats"]
                if not isinstance(player_stats, dict) or "stats" not in player_stats:
                    continue
                
                stats_list = player_stats["stats"]
                if not isinstance(stats_list, list):
                    continue
                
                # 转换统计数据为字典格式
                stats_dict = {}
                for stat_item in stats_list:
                    if "stat" in stat_item:
                        stat_info = stat_item["stat"]
                        stat_id = stat_info.get("stat_id")
                        value = stat_info.get("value")
                        if stat_id is not None:
                            stats_dict[str(stat_id)] = value
                
                # 写入数据库
                if stats_dict:
                    if self.db_writer.write_player_daily_stat_values(
                        player_key=player_key,
                        editorial_player_key=editorial_player_key or player_key,
                        league_key=league_key,
                        season=season,
                        date_obj=date_obj,
                        stats_data=stats_dict
                    ):
                        success_count += 1
            
        except Exception as e:
            pass
        
        return success_count

    # ===== 赛季日程和时间序列数据获取 =====
    
    def fetch_season_schedule_data(self) -> bool:
        """获取赛季日程数据并写入date_dimension表"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 优先从数据库中的League表获取start_date和end_date
        start_date = None
        end_date = None
        
        try:
            from model import League
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
            from datetime import datetime, timedelta
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
            
            # 批量写入数据库
            if dates_data:
                count = self.db_writer.write_date_dimensions_batch(dates_data)
                return count > 0
            else:
                return False
            
        except Exception as e:
            return False
    
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
    
    def _extract_position_string(self, position_data) -> Optional[str]:
        """从位置数据中提取位置字符串"""
        if not position_data:
            return None
        
        if isinstance(position_data, str):
            return position_data
        
        if isinstance(position_data, dict):
            return position_data.get("position", None)
        
        if isinstance(position_data, list) and len(position_data) > 0:
            if isinstance(position_data[0], str):
                return position_data[0]
            elif isinstance(position_data[0], dict):
                return position_data[0].get("position", None)
        
        return None
    
    def _fetch_player_season_stats(self, players: List, league_key: str, season: str) -> bool:
        """获取球员赛季统计数据"""
        print("获取球员赛季统计数据...")
        total_success_count = 0
        
        # 分批处理球员，每批25个（API限制）
        batch_size = 25
        total_batches = (len(players) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(players))
            batch_players = players[start_idx:end_idx]
            
            player_keys = [player.player_key for player in batch_players]
            
            try:
                # 构建API URL - 批量获取球员赛季统计
                player_keys_str = ",".join(player_keys)
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
                
                stats_data = get_api_data(url)
                if stats_data:
                    batch_success_count = self._process_player_season_stats_data(stats_data, league_key, season)
                    total_success_count += batch_success_count
                    
            except Exception as e:
                pass
            
            # 批次间等待
            if batch_idx < total_batches - 1:
                time.sleep(1)
        return total_success_count > 0
    
    def _process_player_season_stats_data(self, stats_data: Dict, league_key: str, season: str) -> int:
        """处理球员赛季统计数据"""
        success_count = 0
        
        try:
            fantasy_content = stats_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找players容器
            players_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                return 0
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_data = players_container[str_index]
                if "player" not in player_data:
                    continue
                
                player_info_list = player_data["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                    continue
                
                # 提取球员基本信息
                player_basic_info = player_info_list[0]
                player_key = None
                editorial_player_key = None
                
                if isinstance(player_basic_info, list):
                    for item in player_basic_info:
                        if isinstance(item, dict):
                            if "player_key" in item:
                                player_key = item["player_key"]
                            elif "editorial_player_key" in item:
                                editorial_player_key = item["editorial_player_key"]
                elif isinstance(player_basic_info, dict):
                    player_key = player_basic_info.get("player_key")
                    editorial_player_key = player_basic_info.get("editorial_player_key")
                
                if not player_key:
                    continue
                
                # 提取统计数据
                stats_container = player_info_list[1]
                if not isinstance(stats_container, dict) or "player_stats" not in stats_container:
                    continue
                
                player_stats = stats_container["player_stats"]
                if not isinstance(player_stats, dict) or "stats" not in player_stats:
                    continue
                
                stats_list = player_stats["stats"]
                if not isinstance(stats_list, list):
                    continue
                
                # 转换统计数据为字典格式
                stats_dict = {}
                for stat_item in stats_list:
                    if "stat" in stat_item:
                        stat_info = stat_item["stat"]
                        stat_id = stat_info.get("stat_id")
                        value = stat_info.get("value")
                        if stat_id is not None:
                            stats_dict[str(stat_id)] = value
                
                # 写入数据库
                if stats_dict:
                    if self.db_writer.write_player_season_stat_values(
                        player_key=player_key,
                        editorial_player_key=editorial_player_key or player_key,
                        league_key=league_key,
                        season=season,
                        stats_data=stats_dict
                    ):
                        success_count += 1
            
        except Exception as e:
            pass
        
        return success_count

    # ===== 时间选择交互方法 =====
    
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

    # ===== 球员统计数据获取方法 =====
    
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
            from model import Player
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
                if self._fetch_player_season_stats(players, league_key, season):
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
                
                player_keys = [player.player_key for player in batch_players]
                
                try:
                    # 构建API URL - 批量获取球员日统计
                    player_keys_str = ",".join(player_keys)
                    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
                    
                    stats_data = get_api_data(url)
                    if stats_data:
                        batch_success_count = self._process_player_daily_stats_data(stats_data, league_key, season, current_date)
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

    # ===== 团队每周统计数据获取方法 =====

    def run_team_weekly_stats_fetch(self) -> bool:
        """检查团队每周统计数据"""
        print("🚀 团队每周统计数据检查")
        print("团队每周统计数据现在在获取联盟数据时自动生成")
        
        return self.fetch_team_weekly_stats_from_matchups()
    
    def fetch_team_weekly_stats_from_matchups(self) -> bool:
        """从 team_matchups 表中提取数据并生成 team_stats_weekly 记录"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        try:
            from model import TeamMatchups
            
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
            from model import TeamStatsWeekly
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

    def run_team_season_stats_fetch(self) -> bool:
        """运行团队赛季统计数据获取"""
        print("🚀 团队赛季统计数据获取")
        print("基于现有的 league_standings 数据生成团队赛季统计")
        return self.fetch_team_season_stats_from_standings()
    
    def fetch_team_season_stats_from_standings(self) -> bool:
        """从league_standings数据生成团队赛季统计"""
        if not self.selected_league:
            return False
        
        try:
            from model import LeagueStandings
            
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
            from model import Team
            
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


def main():
    """主函数 - Yahoo NBA Fantasy数据获取工具"""
    # 创建数据获取器
    fetcher = YahooFantasyDataFetcher(delay=2, batch_size=100)
    
    try:
        # 运行交互式菜单
        fetcher.run_interactive_menu()
    
    finally:
        # 确保清理资源
        fetcher.close()


if __name__ == "__main__":
    main()


