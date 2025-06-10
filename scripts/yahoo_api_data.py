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
        if message:
            print(f"{message}，等待 {self.delay} 秒...")
        else:
            print(f"等待 {self.delay} 秒...")
        time.sleep(self.delay)
    
    def close(self):
        """关闭资源"""
        if self.db_writer:
            self.db_writer.close()
    
    # ===== 基础数据获取和联盟选择 =====
    
    def fetch_and_select_league(self, use_existing_data: bool = False) -> bool:
        """获取基础数据并选择联盟（直接从数据库或API获取）"""
        print("🚀 开始获取基础数据和联盟选择...")
        
        # 优先从数据库获取联盟数据
        leagues_data = self._get_leagues_from_database()
        
        if not leagues_data or not use_existing_data:
            print("📋 从API获取联盟数据...")
            leagues_data = self._fetch_all_leagues_data()
        else:
            print("📋 使用数据库中的联盟数据...")
        
        if not leagues_data:
            print("✗ 无法获取联盟数据")
            return False
        
        # 选择联盟
        selected_league = select_league_interactively(leagues_data)
        if not selected_league:
            print("✗ 未选择联盟")
            return False
        
        self.selected_league = selected_league
        
        print(f"✓ 联盟选择完成: {selected_league['name']} ({selected_league['league_key']})")
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
            
            print(f"✓ 从数据库获取到 {len(leagues)} 个联盟")
            return leagues_data
            
        except Exception as e:
            print(f"从数据库获取联盟数据失败: {e}")
            return None
    
    def _fetch_all_leagues_data(self) -> Optional[Dict]:
        """获取所有联盟数据并直接写入数据库，返回联盟数据用于选择"""
        # 获取games数据
        games_data = self._fetch_games_data()
        if not games_data:
            return None
        
        # 写入games数据到数据库
        games_count = self.db_writer.write_games_data(games_data)
        print(f"✓ 写入 {games_count} 个游戏数据到数据库")
        
        # 提取游戏键并获取联盟数据
        game_keys = self._extract_game_keys(games_data)
        if not game_keys:
            return None
        
        all_leagues = {}
        for i, game_key in enumerate(game_keys):
            print(f"获取游戏 {i+1}/{len(game_keys)} 的联盟数据: {game_key}")
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
            print(f"✓ 写入 {leagues_count} 个联盟数据到数据库")
            return all_leagues
        
        return None
    
    def _fetch_games_data(self) -> Optional[Dict]:
        """获取用户的games数据"""
        print("获取用户的games数据...")
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
            print(f"提取游戏键时出错: {e}")
        
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
            print(f"提取联盟数据时出错: {e}")
        
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
                print(f"✓ 联盟 {league_key} 已存在于数据库中")
                return True
            
            # 联盟不存在，说明数据库中缺少完整数据，建议重新获取
            print(f"⚠️ 联盟 {league_key} 不存在于数据库中")
            print("建议重新选择联盟（选项1）以获取完整的联盟数据")
            return False
                
        except Exception as e:
            print(f"检查联盟存在时出错: {e}")
            return False
    
    def fetch_complete_league_data(self) -> bool:
        """获取完整的联盟数据并直接写入数据库"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取联盟完整数据: {league_key} ===")
        
        # 0. 确保联盟基本信息存在于数据库中
        print("\n📋 步骤0: 确保联盟基本信息存在")
        if not self._ensure_league_exists_in_db():
            print("⚠️ 联盟基本信息写入失败，但继续其他步骤")
        
        # 1. 获取联盟详细信息
        print("\n📋 步骤1: 获取联盟详细信息")
        if not self.fetch_league_details():
            print("⚠️ 联盟详细信息获取失败，继续其他步骤")
        
        # 2. 获取赛季日程数据
        print("\n📅 步骤2: 获取赛季日程数据")
        if not self.fetch_season_schedule_data():
            print("⚠️ 赛季日程数据获取失败，继续其他步骤")
        
        # 3. 获取完整球员数据（优先获取，为后续步骤提供依赖）
        print("\n📋 步骤3: 获取完整球员数据")
        if not self.fetch_complete_players_data():
            print("⚠️ 球员数据获取失败，但继续其他步骤")
        
        # 4. 获取团队数据
        print("\n📋 步骤4: 获取团队数据")
        teams_data = self.fetch_teams_data()
        
        # 5. 获取团队当前roster数据（不获取历史数据）
        print("\n📋 步骤5: 获取团队当前roster数据")
        if teams_data:
            print("  注意：仅获取当前roster，不获取历史数据")
            self.fetch_team_rosters(teams_data)
        else:
            print("⚠️ 由于团队数据获取失败，跳过roster数据获取")
        
        # 6. 获取transaction数据
        print("\n📋 步骤6: 获取transaction数据")
        self.fetch_complete_transactions_data(teams_data)
        
        # 7. 获取球员赛季统计数据（不依赖日期维度）
        print("\n📊 步骤7: 获取球员赛季统计数据")
        self._fetch_player_season_stats_direct()
        
        # 8. 获取团队统计数据（包括联盟排名、团队对战等）
        print("\n📋 步骤8: 获取团队统计数据")
        self.fetch_team_stats_data(teams_data)
        
        print(f"\n🎯 联盟数据获取完成: {league_key}")
        return True
    
    def fetch_league_details(self) -> bool:
        """获取联盟详细信息并写入数据库"""
        league_key = self.selected_league['league_key']
        
        try:
            # 获取联盟设置数据
            print(f"获取联盟设置: {league_key}")
            settings_data = self._fetch_league_settings(league_key)
            if settings_data:
                # 直接写入数据库
                self.db_writer.write_league_settings(league_key, settings_data)
                print("✓ 联盟设置数据写入数据库")
                return True
            else:
                print("⚠️ 联盟设置数据获取失败，但继续执行")
                return False
        except Exception as e:
            print(f"获取联盟详细信息时出错: {e}")
            return False
    
    def _fetch_league_settings(self, league_key: str) -> Optional[Dict]:
        """获取联盟设置"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return get_api_data(url)
    
    def fetch_teams_data(self) -> Optional[Dict]:
        """获取团队数据并写入数据库"""
        league_key = self.selected_league['league_key']
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        
        print(f"获取团队数据: {league_key}")
        teams_data = get_api_data(url)
        
        if teams_data:
            print("✓ 团队数据API调用成功")
            # 提取并写入团队数据
            success_count = self._write_teams_to_db(teams_data, league_key)
            print(f"✓ 团队数据获取完成，写入数据库 {success_count} 个团队")
            return teams_data
        else:
            print("✗ 团队数据API调用失败")
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
            print(f"提取团队数据失败: {e}")
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
                print(f"团队数据格式错误: 期望列表，实际 {type(team_data)}")
                return None
            
            team_info_list = team_data[0]
            if not isinstance(team_info_list, list):
                print(f"团队信息格式错误: 期望列表，实际 {type(team_info_list)}")
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
                print(f"警告: 团队数据缺少 team_key")
                return None
            
            return team_dict
            
        except Exception as e:
            print(f"提取团队数据失败: {e}")
            print(f"调试信息 - team_data 类型: {type(team_data)}")
            if isinstance(team_data, list) and len(team_data) > 0:
                print(f"调试信息 - team_data[0] 类型: {type(team_data[0])}")
            return None

    def fetch_team_rosters(self, teams_data: Dict) -> bool:
        """获取所有团队的roster数据并写入数据库（获取赛季最后一天的roster，不是系统今天）"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 未找到任何团队键")
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
                print(f"📅 使用赛季结束日期获取roster: {roster_date} (赛季结束: {league_db.end_date}, 是否结束: {league_db.is_finished})")
            else:
                print("⚠️ 无法从数据库获取赛季结束日期，使用API默认roster")
        except Exception as e:
            print(f"⚠️ 获取赛季结束日期失败: {e}，使用API默认roster")
        
        print(f"获取 {len(team_keys)} 个团队的rosters...")
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
                print(f"  ✗ 团队 {team_key} 处理出错: {e}")
            
            # 简化等待
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        print(f"✓ Rosters获取完成: {success_count}/{len(team_keys)}")
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
                print(f"    roster信息未找到在 {team_key}")
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
                        print(f"    跳过无日期的roster记录: {roster_entry.get('player_key', 'unknown')}")
                        continue
                    
                    try:
                        roster_date = datetime.strptime(roster_date_str, '%Y-%m-%d').date()
                    except Exception as e:
                        print(f"    跳过日期解析失败的记录: {roster_date_str} - {e}")
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
                    print(f"    写入roster记录失败: {e}")
                    continue
            
            return count > 0
            
        except Exception as e:
            print(f"    处理roster数据失败 {team_key}: {e}")
            # 添加调试信息
            if "fantasy_content" in roster_data:
                fantasy_content = roster_data["fantasy_content"]
                if "team" in fantasy_content:
                    team_data = fantasy_content["team"]
                    print(f"    调试信息 - team_data 类型: {type(team_data)}, 长度: {len(team_data) if isinstance(team_data, list) else 'N/A'}")
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
            print(f"提取团队键时出错: {e}")
            # 添加调试信息
            print(f"调试信息 - teams_data 结构: {type(teams_data)}")
            if "fantasy_content" in teams_data:
                print(f"调试信息 - fantasy_content 存在")
                if "league" in teams_data["fantasy_content"]:
                    league_data = teams_data["fantasy_content"]["league"]
                    print(f"调试信息 - league_data 类型: {type(league_data)}")
                    if isinstance(league_data, list):
                        print(f"调试信息 - league_data 长度: {len(league_data)}")
        
        print(f"提取到 {len(team_keys)} 个团队键: {team_keys}")
        return team_keys
    
    def fetch_team_rosters_for_date_range(self, teams_data: Dict, start_date: date, end_date: date) -> bool:
        """获取指定日期范围内的团队roster数据"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 未找到任何团队键")
            return False
        
        print(f"获取 {len(team_keys)} 个团队在 {start_date} 到 {end_date} 期间的rosters...")
        success_count = 0
        
        from datetime import timedelta
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"  获取 {date_str} 的roster数据...")
            
            day_success_count = 0
            for i, team_key in enumerate(team_keys):
                try:
                    roster_data = self._fetch_team_roster_for_date(team_key, date_str)
                    if roster_data:
                        if self._process_roster_data_to_db(roster_data, team_key):
                            day_success_count += 1
                except Exception as e:
                    print(f"    ✗ 团队 {team_key} 处理出错: {e}")
                
                # 团队间间隔
                if i < len(team_keys) - 1:
                    time.sleep(0.2)
            
            print(f"    ✓ {date_str}: {day_success_count}/{len(team_keys)} 个团队")
            success_count += day_success_count
            
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                self.wait("处理下一天数据")
        
        print(f"✓ Roster历史数据获取完成: 总计 {success_count} 个团队日")
        return success_count > 0
    
    def _fetch_team_roster_for_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)

    def fetch_complete_players_data(self) -> bool:
        """获取完整的球员数据并直接写入数据库"""
        league_key = self.selected_league['league_key']
        
        print("获取联盟完整球员数据...")
        
        # 1. 获取所有球员的基础信息
        all_players = self._fetch_all_league_players(league_key)
        if not all_players:
            print("✗ 获取球员基础信息失败")
            return False
    
        print(f"✓ 获取了 {len(all_players)} 个球员的基础信息")
        
        # 2. 批量写入球员数据到数据库
        players_count = self.db_writer.write_players_batch(all_players, league_key)
        print(f"✓ 完整球员数据写入数据库: {players_count} 个球员")
        
        return True
    
    def _fetch_all_league_players(self, league_key: str) -> List[Dict]:
        """使用改进的分页逻辑获取所有球员"""
        all_players = []
        start = 0
        page_size = 25
        max_iterations = 100
        iteration = 0
        
        print(f"分页获取球员数据...")
            
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
            
        print(f"球员数据获取完成: 总计 {len(all_players)} 个球员")
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
            print(f"    从联盟数据提取球员信息时出错: {e}")
        
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
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"获取联盟transaction数据: {league_key}")
        
        # 获取所有transactions
        print("获取联盟所有transactions...")
        all_transactions = self._fetch_all_league_transactions(league_key)
        
        if all_transactions:
            # 直接写入数据库
            transactions_count = self._write_transactions_to_db(all_transactions, league_key)
            print(f"✓ Transaction数据获取完成，写入数据库: {transactions_count} 个")
        else:
            print("✗ 未获取到transaction数据")
            return False
        
        return True
    
    def _fetch_all_league_transactions(self, league_key: str, max_count: int = None) -> List[Dict]:
        """获取联盟所有transactions（分页处理）"""
        all_transactions = []
        start = 0
        page_size = 25
        max_iterations = 200
        iteration = 0
        
        print(f"分页获取transaction数据...")
        
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
        
        print(f"Transaction数据获取完成: 总计 {len(all_transactions)} 个")
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
            print(f"    从数据提取transaction信息时出错: {e}")
        
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
        print(f"\n📊 获取联盟排名数据...")
        standings_success = self._fetch_and_process_league_standings(league_key, season)
        
        if standings_success:
            print("✓ 联盟排名数据获取成功")
        else:
            print("⚠️ 联盟排名数据获取失败")
        
        # 2. 获取每个团队的matchups数据
        print(f"\n🏆 获取团队对战数据...")
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
                print(f"  获取团队 {i+1}/{len(team_keys)}: {team_key}")
                
                # 获取团队matchups数据
                matchups_data = self._fetch_team_matchups(team_key)
                if matchups_data:
                    if self._process_team_matchups_to_db(matchups_data, team_key, league_key, season):
                        success_count += 1
                        print(f"    ✓ 团队对战数据处理成功")
                    else:
                        print(f"    ⚠️ 团队对战数据处理失败")
                else:
                    print(f"    ⚠️ 团队对战数据获取失败")
                    
            except Exception as e:
                print(f"    ✗ 团队 {team_key} 处理出错: {e}")
            
            # 请求间隔
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        print(f"✓ 团队对战数据获取完成: {success_count}/{len(team_keys)}")
        
        return standings_success and success_count > 0
    
    def _fetch_and_process_league_standings(self, league_key: str, season: str) -> bool:
        """获取并处理league standings数据"""
        try:
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
            standings_data = get_api_data(url)
            
            if not standings_data:
                print("    ✗ 无法获取standings数据")
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
                print("    ✗ 在数据中未找到standings容器")
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
                print("    ✗ 在standings中未找到teams容器")
                return False
            
            teams_count = int(teams_container.get("count", 0))
            success_count = 0
            
            print(f"    处理 {teams_count} 个团队的standings数据...")
            
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
            
            print(f"    ✓ 成功处理 {success_count}/{teams_count} 个团队的standings数据")
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
            print(f"    提取team standings信息失败: {e}")
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
            
            # 构建赛季统计数据
            season_stats_data = {
                "team_points": team_points,
                "team_standings": team_standings
            }
            
            # 写入数据库
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
                divisional_ties=divisional_ties,
                season_stats_data=season_stats_data
            )
            
        except Exception as e:
            print(f"    写入league standings失败: {e}")
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
            print("1. 选择联盟并获取完整数据")
            print("2. 获取历史roster数据")
            print("3. 获取球员统计数据")
            print("4. 显示数据库摘要")
            print("5. 清空数据库（危险操作！）")
            print("0. 退出")
            
            choice = input("\n请选择操作 (0-5): ").strip()
            
            if choice == "0":
                print("退出程序")
                break
            elif choice == "1":
                if self.select_league_interactive():
                    self.show_database_summary()
                    if self.run_complete_league_fetch():
                        self.show_database_summary()
            elif choice == "2":
                if self.select_league_interactive():
                    self.show_database_summary()
                    if self.fetch_roster_history_data():
                        self.show_database_summary()
            elif choice == "3":
                if self.select_league_interactive():
                    self.show_database_summary()
                    if self.fetch_player_stats_data():
                        self.show_database_summary()
            elif choice == "4":
                self.show_database_summary()
            elif choice == "5":
                confirm = input("确认清空数据库？输入 'YES' 确认: ").strip()
                if confirm == "YES":
                    if self.clear_database(confirm=True):
                        print("✅ 数据库已清空")
                    else:
                        print("❌ 数据库清空失败")
            else:
                print("无效选择，请重试")
    
    def select_league_interactive(self) -> bool:
        """交互式选择联盟"""
        return self.fetch_and_select_league(use_existing_data=True)
    
    def run_complete_league_fetch(self) -> bool:
        """运行完整联盟数据获取"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print(f"🚀 开始获取联盟完整数据: {self.selected_league['league_key']}")
        return self.fetch_complete_league_data()
    
    def run_historical_data_fetch(self, weeks_back: int = 5, days_back: int = 30) -> bool:
        """运行历史数据获取（时间序列）"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print(f"🚀 开始获取历史数据: {self.selected_league['league_key']}")
        print(f"回溯周数: {weeks_back}, 回溯天数: {days_back}")
        
        # 这里可以添加具体的时间序列数据获取逻辑
        # 目前暂时返回当前的完整数据获取
        return self.fetch_complete_league_data()
    
    def show_database_summary(self):
        """显示数据库摘要"""
        try:
            from model import (League, Team, Player, Game, Transaction, 
                             RosterDaily, TeamStatsWeekly, TeamStatsSeason,
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
                ("团队赛季统计", TeamStatsSeason),
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
        """处理团队matchups数据并写入数据库"""
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
                print(f"      ⚠️ 未找到matchups数据")
                return False
            
            matchups_count = int(matchups_container.get("count", 0))
            success_count = 0
            
            print(f"      处理 {matchups_count} 个对战记录...")
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_data = matchups_container[str_index]
                if "matchup" not in matchup_data:
                    continue
                
                matchup_info = matchup_data["matchup"]
                
                # 提取matchup信息
                matchup_details = self._extract_matchup_info(matchup_info, team_key)
                if not matchup_details:
                    continue
                
                # 写入数据库
                if self._write_team_matchup_to_db(matchup_details, team_key, league_key, season):
                    success_count += 1
            
            print(f"      ✓ 成功处理 {success_count}/{matchups_count} 个对战记录")
            return success_count > 0
            
        except Exception as e:
            print(f"      ✗ 处理团队matchups数据失败: {e}")
            return False
    
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
                "is_matchup_of_week": is_matchup_of_week,
                "matchup_data": matchup_info  # json存储完整数据
            }
            
        except Exception as e:
            print(f"        提取matchup信息失败: {e}")
            return None
    
    def _extract_team_matchup_details(self, teams_data, target_team_key: str) -> Optional[Dict]:
        """从teams数据中提取当前团队的对战详情"""
        try:
            if not isinstance(teams_data, dict):
                return None
            
            teams_count = int(teams_data.get("count", 0))
            opponent_team_key = None
            is_winner = None
            is_tied = False
            team_points = 0
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_data:
                    continue
                
                team_container = teams_data[str_index]
                if "team" not in team_container:
                    continue
                
                team_info = team_container["team"]
                
                # 提取team_key和points
                current_team_key = None
                points = 0
                winner_result = None
                
                if isinstance(team_info, list):
                    for item in team_info:
                        if isinstance(item, dict):
                            if "team_key" in item:
                                current_team_key = item["team_key"]
                            elif "team_points" in item:
                                points = int(item["team_points"].get("total", 0))
                            elif "win_probability" in item:
                                # 有时胜负信息在这里
                                pass
                        elif isinstance(item, list):
                            for sub_item in item:
                                if isinstance(sub_item, dict):
                                    if "team_key" in sub_item:
                                        current_team_key = sub_item["team_key"]
                                    elif "team_points" in sub_item:
                                        points = int(sub_item["team_points"].get("total", 0))
                
                if current_team_key == target_team_key:
                    team_points = points
                else:
                    opponent_team_key = current_team_key
            
            # 简单的胜负判断 - 可能需要基于具体的API数据结构调整
            # 这里暂时返回基本信息
            return {
                "opponent_team_key": opponent_team_key,
                "is_winner": is_winner,
                "is_tied": is_tied,
                "team_points": team_points
            }
            
        except Exception as e:
            print(f"        提取team matchup详情失败: {e}")
            return None
    
    def _write_team_matchup_to_db(self, matchup_details: Dict, team_key: str, 
                                league_key: str, season: str) -> bool:
            """将team matchup数据写入数据库"""
            try:
                return self.db_writer.write_team_matchup(
                    league_key=league_key,
                    team_key=team_key,
                    season=season,
                    week=matchup_details["week"],
                    week_start=matchup_details.get("week_start"),
                    week_end=matchup_details.get("week_end"),
                    status=matchup_details.get("status"),
                    opponent_team_key=matchup_details.get("opponent_team_key"),
                    is_winner=matchup_details.get("is_winner"),
                    is_tied=matchup_details.get("is_tied", False),
                    team_points=matchup_details.get("team_points", 0),
                    is_playoffs=matchup_details.get("is_playoffs", False),
                    is_consolation=matchup_details.get("is_consolation", False),
                    is_matchup_of_week=matchup_details.get("is_matchup_of_week", False),
                    matchup_data=matchup_details.get("matchup_data", {})
                )
            
            except Exception as e:
                print(f"        写入team matchup失败: {e}")
                return False
    
    def _fetch_player_season_stats_direct(self) -> bool:
        """直接获取球员赛季统计数据（不依赖日期维度）"""
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print("获取球员赛季统计数据...")
        
        # 获取数据库中的球员列表
        try:
            from model import Player
            players = self.db_writer.session.query(Player).filter_by(
                league_key=league_key
            ).all()
            
            if not players:
                print("  ⚠️ 数据库中没有球员数据，跳过球员统计获取")
                return False
            
            print(f"  找到 {len(players)} 个球员")
            return self._fetch_player_season_stats(players, league_key, season)
            
        except Exception as e:
            print(f"  ✗ 获取球员赛季统计失败: {e}")
            return False

    # ===== 赛季日程和时间序列数据获取 =====
    
    def fetch_season_schedule_data(self) -> bool:
        """获取赛季日程数据并写入date_dimension表"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print(f"📅 获取赛季日程数据: {season}")
        
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
                print(f"✓ 从数据库获取到日期范围: {start_date} 到 {end_date}")
            else:
                # 如果数据库中没有，从selected_league获取
                start_date = self.selected_league.get('start_date')
                end_date = self.selected_league.get('end_date')
                print(f"⚠️ 从selected_league获取日期范围: {start_date} 到 {end_date}")
        except Exception as e:
            print(f"⚠️ 从数据库获取日期失败: {e}")
            start_date = self.selected_league.get('start_date')
            end_date = self.selected_league.get('end_date')
        
        if not start_date or not end_date:
            print("⚠️ 联盟缺少开始/结束日期信息，使用默认NBA赛季日期")
            # NBA 2024-25赛季默认日期
            if season == '2024':
                start_date = '2024-10-22'
                end_date = '2025-04-13'
            else:
                print("❌ 无法确定赛季日期范围")
                return False
        
        try:
            # 解析日期
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            print(f"准备生成日期范围: {start_dt} 到 {end_dt}")
            
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
            
            print(f"生成了 {len(dates_data)} 个日期记录")
            
            # 批量写入数据库
            if dates_data:
                count = self.db_writer.write_date_dimensions_batch(dates_data)
                print(f"✓ 赛季日程数据写入完成: {count} 天")
                return count > 0
            else:
                print("❌ 没有生成日期数据")
                return False
            
        except Exception as e:
            print(f"获取赛季日程数据失败: {e}")
            return False
        
        return False
    
    def fetch_roster_history_data(self, mode: str = "days_back", days_back: int = 30, 
                                 target_date: str = None) -> bool:
        """获取历史roster数据
        
        Args:
            mode: 'specific' | 'days_back' | 'full_season'
            days_back: 回溯天数 (mode='days_back'时使用)
            target_date: 目标日期 'YYYY-MM-DD' (mode='specific'时使用)
        """
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        print("📋 获取历史roster数据...")
        
        # 首先确保有赛季日程数据
        if not self.fetch_season_schedule_data():
            print("⚠️ 赛季日程数据获取失败，但继续roster数据获取")
        
        # 获取团队数据
        teams_data = self.fetch_teams_data()
        if not teams_data:
            print("❌ 获取团队数据失败")
            return False
        
        # 计算日期范围
        date_range = self.calculate_date_range(mode, days_back, target_date)
        if not date_range:
            return False
        
        start_date, end_date = date_range
        
        # 获取指定时间范围内的roster数据
        success = self.fetch_team_rosters_for_date_range(teams_data, start_date, end_date)
        
        return success
    
    def fetch_player_stats_data(self, mode: str = "days_back", days_back: int = 30, 
                               target_date: str = None, include_season_stats: bool = True) -> bool:
        """获取球员统计数据
        
        Args:
            mode: 'specific' | 'days_back' | 'full_season' (仅用于日统计)
            days_back: 回溯天数 (mode='days_back'时使用)
            target_date: 目标日期 'YYYY-MM-DD' (mode='specific'时使用)
            include_season_stats: 是否包含赛季统计数据
        """
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print("📊 获取球员统计数据...")
        
        # 获取球员列表
        from model import Player
        players = self.db_writer.session.query(Player).filter_by(
            league_key=league_key
        ).all()
        
        if not players:
            print("❌ 数据库中没有球员数据，请先运行完整数据获取")
            return False
        
        print(f"找到 {len(players)} 个球员，开始获取统计数据...")
        
        success_results = []
        
        # 获取球员赛季统计（不依赖时间范围）
        if include_season_stats:
            season_stats_success = self._fetch_player_season_stats(players, league_key, season)
            success_results.append(season_stats_success)
        
        # 获取球员日统计（使用指定的时间范围）
        daily_stats_success = self._fetch_player_daily_stats(players, league_key, season, mode, days_back, target_date)
        success_results.append(daily_stats_success)
        
        return any(success_results)
    
    def _fetch_player_season_stats(self, players: List, league_key: str, season: str) -> bool:
        """获取球员赛季统计数据"""
        print("获取球员赛季统计数据...")
        total_success_count = 0
        
        # 分批处理球员，每批25个（API限制）
        batch_size = 25
        total_batches = (len(players) + batch_size - 1) // batch_size
        
        print(f"处理 {len(players)} 个球员，分 {total_batches} 批...")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(players))
            batch_players = players[start_idx:end_idx]
            
            player_keys = [player.player_key for player in batch_players]
            
            try:
                print(f"  批次 {batch_idx + 1}/{total_batches}: {len(player_keys)} 个球员")
                
                # 构建API URL - 批量获取球员赛季统计
                player_keys_str = ",".join(player_keys)
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
                
                stats_data = get_api_data(url)
                if stats_data:
                    batch_success_count = self._process_player_season_stats_data(stats_data, league_key, season)
                    total_success_count += batch_success_count
                    print(f"    ✓ 批次 {batch_idx + 1} 完成: {batch_success_count} 个球员")
                else:
                    print(f"    ⚠️ 批次 {batch_idx + 1} 数据获取失败")
                    
            except Exception as e:
                print(f"    ✗ 批次 {batch_idx + 1} 处理失败: {e}")
            
            # 批次间等待
            if batch_idx < total_batches - 1:
                time.sleep(1)
        
        print(f"✓ 球员赛季统计数据处理完成: 总计 {total_success_count} 个球员")
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
            print(f"处理球员赛季统计数据失败: {e}")
        
        return success_count
    
    def _fetch_player_daily_stats(self, players: List, league_key: str, season: str,
                                 mode: str = "days_back", days_back: int = 7, 
                                 target_date: str = None) -> bool:
        """获取球员日统计数据
        
        Args:
            players: 球员列表
            league_key: 联盟键
            season: 赛季
            mode: 'specific' | 'days_back' | 'full_season'
            days_back: 回溯天数 (mode='days_back'时使用)
            target_date: 目标日期 'YYYY-MM-DD' (mode='specific'时使用)
        """
        print("获取球员日统计数据...")
        
        # 计算日期范围
        date_range = self.calculate_date_range(mode, days_back, target_date)
        if not date_range:
            return False
        
        start_date, end_date = date_range
        success_count = 0
        
        try:
            from datetime import timedelta
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                print(f"  获取 {date_str} 的球员统计...")
                
                # 分批获取该日期的球员统计
                batch_size = 20  # 日统计API限制更小
                total_players = len(players)
                
                day_success_count = 0
                for batch_start in range(0, total_players, batch_size):
                    batch_end = min(batch_start + batch_size, total_players)
                    batch_players = players[batch_start:batch_end]
                    player_keys = [player.player_key for player in batch_players]
                    player_keys_str = ",".join(player_keys)
                    
                    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
                    
                    stats_data = get_api_data(url)
                    if stats_data:
                        batch_daily_count = self._process_player_daily_stats_data(stats_data, league_key, season, current_date)
                        day_success_count += batch_daily_count
                    
                    # 批次间短暂等待
                    if batch_end < total_players:
                        time.sleep(0.3)
                
                success_count += day_success_count
                print(f"    ✓ {date_str}: {day_success_count} 个球员 (共 {total_players} 个)")
                
                current_date += timedelta(days=1)
                
                # 只有在处理多天数据时才等待
                if current_date <= end_date:
                    self.wait("处理下一天数据")
                
        except Exception as e:
            print(f"获取球员日统计数据失败: {e}")
        
        print(f"✓ 球员日统计数据处理完成: 总计 {success_count} 条记录")
        return success_count > 0
    
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
            print(f"处理球员日统计数据失败: {e}")
        
        return success_count


def main():
    """主函数 - 专注于NBA Fantasy数据获取"""
    parser = argparse.ArgumentParser(description="Yahoo NBA Fantasy数据获取工具")
    
    parser.add_argument("--complete", action="store_true", help="执行完整数据获取（推荐）")
    parser.add_argument("--roster-history", action="store_true", help="获取历史roster数据")
    parser.add_argument("--player-stats", action="store_true", help="获取球员统计数据")
    parser.add_argument("--clear-db", action="store_true", help="清空数据库（慎用！）")
    parser.add_argument("--show-summary", action="store_true", help="显示数据库摘要")
    
    # 时间范围控制参数
    parser.add_argument("--mode", choices=["specific", "days_back", "full_season"], 
                       default="days_back", help="时间获取模式 (默认: days_back)")
    parser.add_argument("--days-back", type=int, default=30, 
                       help="回溯天数，从赛季结束日期或今天算起 (默认: 30)")
    parser.add_argument("--target-date", type=str, 
                       help="指定日期 YYYY-MM-DD (mode=specific时使用)")
    
    # 其他参数
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    parser.add_argument("--batch-size", type=int, default=100, help="数据库批量写入大小，默认100")
    
    args = parser.parse_args()
    
    # 创建数据获取器
    fetcher = YahooFantasyDataFetcher(delay=args.delay, batch_size=args.batch_size)
    
    try:
        # 检查是否有命令行参数
        has_args = any([args.complete, args.roster_history, args.player_stats, 
                       args.clear_db, args.show_summary])
        
        if not has_args:
            # 没有参数，运行交互式菜单
            fetcher.run_interactive_menu()
        else:
            # 有参数，执行对应的功能
            if args.clear_db:
                # 清空数据库
                print("⚠️ 即将清空数据库，所有数据将被删除！")
                confirm = input("请输入 'YES' 确认清空数据库: ").strip()
                if confirm == "YES":
                    if fetcher.clear_database(confirm=True):
                        print("✅ 数据库已清空")
                    else:
                        print("❌ 数据库清空失败")
                else:
                    print("❌ 操作已取消")
            
            elif args.show_summary:
                # 显示数据库摘要
                fetcher.show_database_summary()
            
            elif args.complete:
                # 执行完整数据获取流程
                print("🚀 开始NBA Fantasy完整数据获取流程")
                
                # 首先选择联盟
                if not fetcher.select_league_interactive():
                    print("\n❌ 联盟选择失败")
                    return
                
                fetcher.show_database_summary()  # 显示开始前的状态
                
                if fetcher.run_complete_league_fetch():
                    fetcher.show_database_summary()  # 显示结束后的状态
                else:
                    print("\n❌ 完整数据获取失败")
            
            elif args.roster_history:
                # 执行roster历史数据获取
                print("🚀 开始roster历史数据获取流程")
                print(f"时间模式: {args.mode}, 天数回溯: {args.days_back}, 目标日期: {args.target_date}")
                
                if not fetcher.select_league_interactive():
                    print("\n❌ 联盟选择失败")
                    return
                
                fetcher.show_database_summary()
                
                if fetcher.fetch_roster_history_data(
                    mode=args.mode, 
                    days_back=args.days_back, 
                    target_date=args.target_date
                ):
                    fetcher.show_database_summary()
                else:
                    print("\n❌ roster历史数据获取失败")
                    
            elif args.player_stats:
                # 执行球员统计数据获取
                print("🚀 开始球员统计数据获取流程")
                print(f"时间模式: {args.mode}, 天数回溯: {args.days_back}, 目标日期: {args.target_date}")
                
                if not fetcher.select_league_interactive():
                    print("\n❌ 联盟选择失败")
                    return
                
                fetcher.show_database_summary()
                
                if fetcher.fetch_player_stats_data(
                    mode=args.mode, 
                    days_back=args.days_back, 
                    target_date=args.target_date,
                    include_season_stats=True
                ):
                    fetcher.show_database_summary()
                else:
                    print("\n❌ 球员统计数据获取失败")
    
    finally:
        # 确保清理资源
        fetcher.close()


if __name__ == "__main__":
    main()


