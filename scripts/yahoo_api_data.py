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
from model import RosterDaily, Player, DateDimension

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
        
        # 2. 获取完整球员数据（优先获取，为后续步骤提供依赖）
        print("\n📋 步骤2: 获取完整球员数据")
        if not self.fetch_complete_players_data():
            print("⚠️ 球员数据获取失败，但继续其他步骤")
        
        # 3. 获取团队数据
        print("\n📋 步骤3: 获取团队数据")
        teams_data = self.fetch_teams_data()
        
        # 4. 获取transaction数据
        print("\n📋 步骤4: 获取transaction数据")
        self.fetch_complete_transactions_data(teams_data)
        
        # 5. 获取团队统计数据
        print("\n📋 步骤5: 获取团队统计数据")
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
        """获取所有团队的roster数据并写入数据库"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 未找到任何团队键")
            return False
        
        print(f"获取 {len(team_keys)} 个团队的rosters...")
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
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
        """获取团队统计数据并写入数据库"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        # 如果没有teams_data，先获取
        if not teams_data:
            teams_data = self.fetch_teams_data()
            if not teams_data:
                print("✗ 获取团队数据失败")
                return False
        
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 未找到团队键")
            return False
        
        print(f"获取 {len(team_keys)} 个团队的统计数据...")
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
                # 获取团队赛季统计
                stats_data = self._fetch_team_stats(team_key)
                if stats_data:
                    if self._process_team_stats_to_db(stats_data, team_key, league_key, season):
                        success_count += 1
                    
                # 获取团队matchups数据
                matchups_data = self._fetch_team_matchups(team_key)
                if matchups_data:
                    self._process_team_matchups_to_db(matchups_data, team_key, league_key, season)
                    
            except Exception as e:
                print(f"  ✗ 团队 {team_key} 处理出错: {e}")
            
            # 简化等待
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        print(f"✓ 团队统计获取完成: {success_count}/{len(team_keys)}")
        return success_count > 0
    
    def _fetch_team_stats(self, team_key: str) -> Optional[Dict]:
        """获取团队统计数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/stats?format=json"
        return get_api_data(url)
    
    def _fetch_team_matchups(self, team_key: str) -> Optional[Dict]:
        """获取团队matchups数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        return get_api_data(url)
    
    def _process_team_stats_to_db(self, stats_data: Dict, team_key: str, league_key: str, season: str) -> bool:
        """处理团队统计数据并写入数据库"""
        try:
            fantasy_content = stats_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 获取统计信息
            stats_info = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "team_stats" in item:
                        stats_info = item["team_stats"]
                        break
            
            if not stats_info:
                return False
            
            # 提取统计数据
            stats_container = stats_info.get("stats", [])
            normalized_stats = {}
            
            for stat_item in stats_container:
                if "stat" in stat_item:
                    stat_info = stat_item["stat"]
                    stat_id = str(stat_info.get("stat_id"))
                    if stat_id:
                        normalized_stats[stat_id] = stat_info.get("value")
            
            coverage_type = stats_info.get("coverage_type", "season")
            
            # 写入标准化统计值表
            count = self.db_writer.write_team_stat_values(
                team_key=team_key,
                league_key=league_key,
                season=season,
                coverage_type=coverage_type,
                stats_data=normalized_stats
            )
            

            
            return count > 0
            
        except Exception as e:
            print(f"    处理团队统计失败 {team_key}: {e}")
            return False
    
    def _process_team_matchups_to_db(self, matchups_data: Dict, team_key: str, league_key: str, season: str) -> int:
        """处理团队matchups数据并写入数据库（支持Category和Points联盟）"""
        try:
            fantasy_content = matchups_data["fantasy_content"]
            team_data = fantasy_content["team"]
            
            # 获取matchups信息
            matchups_info = None
            if isinstance(team_data, list) and len(team_data) > 1:
                for item in team_data:
                    if isinstance(item, dict) and "matchups" in item:
                        matchups_info = item["matchups"]
                        break
            
            if not matchups_info:
                return 0
            
            matchups_count = int(matchups_info.get("count", 0))
            processed_count = 0
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_info:
                    continue
                
                matchup_container = matchups_info[str_index]
                if "matchup" not in matchup_container:
                    continue
                
                matchup_data = matchup_container["matchup"]
                if not isinstance(matchup_data, dict):
                    continue
                
                # 提取matchup基本信息
                week = matchup_data.get("week")
                is_playoff = bool(matchup_data.get("is_playoffs", 0))
                winner_team_key = matchup_data.get("winner_team_key")
                
                # 处理Category联盟的stat_winners
                stat_winners = matchup_data.get("stat_winners", [])
                categories_won = 0
                categories_total = len(stat_winners)
                
                # 计算该团队赢得的类别数量
                for stat_winner in stat_winners:
                    if isinstance(stat_winner, dict) and "stat_winner" in stat_winner:
                        if stat_winner["stat_winner"].get("winner_team_key") == team_key:
                            categories_won += 1
                
                # 提取团队统计数据
                teams_data = matchup_data.get("0", {}).get("teams", {})
                opponent_team_key = None
                total_points = None
                team_stats_data = {}
                
                teams_count = int(teams_data.get("count", 0))
                for j in range(teams_count):
                    team_index = str(j)
                    if team_index not in teams_data:
                        continue
                    
                    team_info = teams_data[team_index]["team"]
                    current_team_key = None
                    points = None
                    
                    # 处理team_info（可能是列表）
                    if isinstance(team_info, list) and len(team_info) > 1:
                        # 提取team_key
                        basic_info = team_info[0]
                        if isinstance(basic_info, list):
                            for info_item in basic_info:
                                if isinstance(info_item, dict) and "team_key" in info_item:
                                    current_team_key = info_item["team_key"]
                                    break
                        
                        # 提取统计信息
                        stats_info = team_info[1]
                        if isinstance(stats_info, dict):
                            # 提取team_points
                            if "team_points" in stats_info:
                                team_points = stats_info["team_points"]
                                if isinstance(team_points, dict):
                                    points = team_points.get("total")
                            
                            # 提取team_stats（当前团队的）
                            if current_team_key == team_key and "team_stats" in stats_info:
                                team_stats = stats_info["team_stats"]
                                if isinstance(team_stats, dict) and "stats" in team_stats:
                                    stats_list = team_stats["stats"]
                                    for stat_item in stats_list:
                                        if "stat" in stat_item:
                                            stat = stat_item["stat"]
                                            stat_id = str(stat.get("stat_id"))
                                            team_stats_data[stat_id] = stat.get("value")
                    
                    if current_team_key == team_key:
                        total_points = points
                    else:
                        opponent_team_key = current_team_key
                
                # 判断输赢
                win = None
                if winner_team_key:
                    win = (winner_team_key == team_key)
                elif categories_total > 0:
                    # Category联盟：比较获胜类别数量
                    win = (categories_won > categories_total // 2)
                
                # 1. 写入团队统计值表
                if team_stats_data:
                    count = self.db_writer.write_team_stat_values(
                        team_key=team_key,
                        league_key=league_key,
                        season=season,
                        coverage_type="week",
                        stats_data=team_stats_data,
                        week=int(week) if week else None,
                        date_obj=None,
                        opponent_team_key=opponent_team_key,
                        is_playoff=is_playoff,
                        win=win
                    )
                    if count > 0:
                        processed_count += 1
                
                # 2. 写入matchup stat winners表
                if stat_winners:
                    self.db_writer.write_matchup_stat_winners(
                        league_key=league_key,
                        season=season,
                        week=int(week) if week else None,
                        stat_winners=stat_winners
                    )
                

            
            return processed_count
            
        except Exception as e:
            print(f"    处理团队matchups失败 {team_key}: {e}")
            return 0

    # ===== 时间序列数据获取功能 =====
    
    def fetch_historical_rosters(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """获取NBA历史名单数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print(f"🔄 开始获取NBA历史名单数据: {league_key}")
        
        # 首先获取团队列表
        teams_data = self._fetch_teams_data_for_history(league_key)
        if not teams_data:
            print("✗ 获取团队数据失败")
            return False
        
        team_keys = self._extract_team_keys_from_teams_data(teams_data)
        if not team_keys:
            print("✗ 提取团队键失败")
            return False
        
        print(f"找到 {len(team_keys)} 个团队")
        
        # NBA按日期获取数据
        return self._fetch_rosters_by_dates(team_keys, league_key, season, start_date, end_date)
    
    def _fetch_teams_data_for_history(self, league_key: str) -> Optional[Dict]:
        """获取团队数据（为历史数据获取使用）"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        return get_api_data(url)
    
    def _extract_team_keys_from_teams_data(self, teams_data: Dict) -> List[str]:
        """从团队数据中提取团队键（为历史数据功能使用）"""
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
        
        return team_keys

    def _fetch_rosters_by_dates(self, team_keys: List[str], league_key: str, season: str,
                               start_date: Optional[date], end_date: Optional[date]) -> bool:
        """按日期获取名单数据（MLB/NBA/NHL）"""
        if start_date is None:
            start_date = date.today() - timedelta(days=30)
        if end_date is None:
            end_date = date.today()
        
        # 生成日期列表（每天一次）
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        
        print(f"  获取 {start_date} 至 {end_date} 名单数据...")
        
        for target_date in dates:
            date_str = target_date.strftime('%Y-%m-%d')
            for team_key in team_keys:
                roster_data = self._fetch_team_roster_by_date(team_key, date_str)
                if roster_data:
                    self._process_roster_data_to_history_db(roster_data, team_key, league_key,
                                                          'date', season, coverage_date=target_date)
                
                time.sleep(0.5)  # 简化等待
        
        print(f"✓ 名单数据获取完成: {start_date} 至 {end_date}")
        return True
    
    def _fetch_team_roster_by_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队名单"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)
    
    def _process_roster_data_to_history_db(self, roster_data: Dict, team_key: str, league_key: str,
                                         coverage_type: str, season: str,
                                         week: Optional[int] = None,
                                         coverage_date: Optional[date] = None) -> None:
        """处理名单数据并写入历史数据库"""
        try:
            roster_info = roster_data["fantasy_content"]["team"][1]["roster"]
            players_data = roster_info["0"]["players"]
            
            for key, player_data in players_data.items():
                if key == "count":
                    continue
                
                player_info = player_data["player"][0]
                position_data = player_data["player"][1] if len(player_data["player"]) > 1 else {}
                
                # 提取球员基本信息
                player_key = None
                for item in player_info:
                    if isinstance(item, dict) and "player_key" in item:
                        player_key = item["player_key"]
                        break
                
                if not player_key:
                    continue
                
                # 提取位置信息
                selected_position = None
                if "selected_position" in position_data:
                    selected_position_data = position_data["selected_position"]
                    if isinstance(selected_position_data, list):
                        for item in selected_position_data:
                            if isinstance(item, dict) and "position" in item:
                                selected_position = item["position"]
                                break
                    elif isinstance(selected_position_data, dict) and "position" in selected_position_data:
                        selected_position = selected_position_data["position"]
                
                # 判断是否首发
                is_starting = selected_position not in ['BN', 'IL', 'IR'] if selected_position else False
                
                # 提取球员状态
                player_status = None
                injury_note = None
                for item in player_info:
                    if isinstance(item, dict):
                        if "status" in item:
                            player_status = item["status"]
                        elif "injury_note" in item:
                            injury_note = item["injury_note"]
                
                # 判断位置状态
                is_bench = selected_position == 'BN' if selected_position else False
                is_injured_reserve = selected_position in ['IL', 'IR'] if selected_position else False
                
                # 写入数据库
                self.db_writer.write_roster_daily(
                    team_key=team_key,
                    player_key=player_key,
                    league_key=league_key,
                    roster_date=coverage_date,
                    season=season,
                    week=week,
                    selected_position=selected_position,
                    is_starting=is_starting,
                    is_bench=is_bench,
                    is_injured_reserve=is_injured_reserve,
                    player_status=player_status,
                    injury_note=injury_note
                )
                
        except Exception as e:
            print(f"处理名单数据失败 {team_key}: {e}")

    def fetch_historical_player_stats(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """获取NBA历史球员统计数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print(f"🔄 开始获取NBA历史球员统计数据: {league_key}")
        
        # 检查数据库中是否已有球员数据，避免重复API调用
        print("📋 检查球员数据...")
        player_keys, all_players = self._get_or_fetch_player_data(league_key)
        if not player_keys:
            print("✗ 获取球员数据失败")
            return False
        
        print(f"找到 {len(player_keys)} 个球员键")
        
        # 获取不同类型的统计数据
        success = True
        
        # 1. 获取赛季总计统计
        print("📊 获取赛季统计...")
        if not self._fetch_player_stats_by_season(player_keys, all_players, league_key, season):
            success = False
        
        # 2. 获取NBA日期统计
        print("📊 获取日期统计...")
        if not self._fetch_player_stats_by_dates(player_keys, all_players, league_key, season, start_date, end_date):
            success = False
        
        return success
    
    def _get_or_fetch_player_data(self, league_key: str) -> tuple[List[str], List[Dict]]:
        """获取或从数据库/API获取球员数据，避免重复调用"""
        try:
            # 先检查数据库中是否已有该联盟的球员数据
            existing_players = self.db_writer.session.query(Player).filter_by(
                league_key=league_key
            ).all()
            
            if existing_players and len(existing_players) > 0:
                print(f"✓ 从数据库获取 {len(existing_players)} 个球员数据")
                
                # 从数据库记录创建player_keys和all_players数据
                player_keys = [player.player_key for player in existing_players]
                all_players = []
                
                for player in existing_players:
                    player_dict = {
                        'player_key': player.player_key,
                        'editorial_player_key': player.editorial_player_key,
                        'player_id': player.player_id,
                        'full_name': player.full_name,
                        'first_name': player.first_name,
                        'last_name': player.last_name,
                        'current_team_key': player.current_team_key,
                        'current_team_name': player.current_team_name,
                        'current_team_abbr': player.current_team_abbr,
                        'display_position': player.display_position,
                        'primary_position': player.primary_position,
                        'position_type': player.position_type,
                        'uniform_number': player.uniform_number,
                        'status': player.status,
                        'headshot_url': player.headshot_url,
                        'is_undroppable': player.is_undroppable,
                        'season': player.season
                    }
                    all_players.append(player_dict)
                
                return player_keys, all_players
                
            else:
                # 数据库中没有数据，需要从API获取
                print("📋 数据库中无球员数据，从API获取...")
                all_players = self._fetch_all_league_players(league_key)
                if not all_players:
                    return [], []
                
                print(f"✓ 从API获取了 {len(all_players)} 个球员的基础信息")
                
                # 确保球员数据存在于数据库中
                print("📋 确保球员数据存在于数据库中...")
                self._ensure_players_exist_in_db(all_players, league_key)
                
                # 提取球员键
                player_keys = [player.get('player_key') for player in all_players if player.get('player_key')]
                return player_keys, all_players
                
        except Exception as e:
            print(f"获取球员数据时出错: {e}")
            return [], []
    
    def _fetch_player_stats_by_season(self, player_keys: List[str], players_data: List[Dict],
                                    league_key: str, season: str) -> bool:
        """获取球员赛季总计统计数据"""
        try:
            # 创建player_key到editorial_player_key的映射
            key_mapping = {}
            for player in players_data:
                if player.get("player_key") and player.get("editorial_player_key"):
                    key_mapping[player["player_key"]] = player["editorial_player_key"]
            
            print(f"  获取赛季统计...")
            
            stats_data = self._fetch_player_stats_batch_by_season(player_keys, league_key)
            
            stats_count = 0
            for player_key, stats in stats_data.items():
                editorial_key = key_mapping.get(player_key, player_key)
                
                if self.db_writer.write_player_season_stats(
                    player_key=player_key,
                    editorial_player_key=editorial_key,
                    league_key=league_key,
                    stats_data=stats,
                    season=season
                ):
                    stats_count += 1
            
            print(f"✓ 赛季统计完成: {stats_count} 个球员")
            return True
            
        except Exception as e:
            print(f"获取赛季统计失败: {e}")
            return False
    
    def _fetch_player_stats_batch_by_season(self, player_keys: List[str], league_key: str) -> Dict[str, Dict]:
        """批量获取赛季总计统计数据"""
        batch_size = 25
        all_stats = {}
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats;type=season?format=json"
            
            data = get_api_data(url)
            if data:
                batch_stats = self._extract_player_stats_from_data(data)
                all_stats.update(batch_stats)
            
            time.sleep(0.5)
        
        return all_stats
    
    def _ensure_players_exist_in_db(self, players_data: List[Dict], league_key: str) -> None:
        """确保球员数据存在于数据库中"""
        try:
            existing_count = 0
            created_count = 0
            
            for player_data in players_data:
                player_key = player_data.get('player_key')
                if not player_key:
                    continue
                
                # 检查球员是否已存在
                existing_player = self.db_writer.session.query(Player).filter_by(
                    player_key=player_key
                ).first()
                
                if existing_player:
                    existing_count += 1
                    continue
                
                # 创建新球员记录
                try:
                    player = Player(
                        player_key=player_key,
                        player_id=player_data.get('player_id', ''),
                        editorial_player_key=player_data.get('editorial_player_key', ''),
                        league_key=league_key,
                        full_name=player_data.get('full_name', player_data.get('name', {}).get('full', '')),
                        first_name=player_data.get('first_name', player_data.get('name', {}).get('first', '')),
                        last_name=player_data.get('last_name', player_data.get('name', {}).get('last', '')),
                        current_team_key=player_data.get('current_team_key', player_data.get('editorial_team_key', '')),
                        current_team_name=player_data.get('current_team_name', player_data.get('editorial_team_full_name', '')),
                        current_team_abbr=player_data.get('current_team_abbr', player_data.get('editorial_team_abbr', '')),
                        display_position=player_data.get('display_position', ''),
                        primary_position=player_data.get('primary_position', ''),
                        position_type=player_data.get('position_type', ''),
                        uniform_number=player_data.get('uniform_number', ''),
                        status=player_data.get('status', ''),
                        headshot_url=player_data.get('headshot_url', ''),
                        is_undroppable=player_data.get('is_undroppable', False),
                        season=self.selected_league.get('season', '2024'),
                        last_updated=datetime.now()
                    )
                    
                    self.db_writer.session.add(player)
                    created_count += 1
                    
                    # 每50个球员提交一次
                    if created_count % 50 == 0:
                        self.db_writer.session.commit()
                        
                except Exception as e:
                    print(f"创建球员 {player_key} 失败: {e}")
                    self.db_writer.session.rollback()
                    continue
            
            # 提交剩余的球员
            if created_count > 0:
                try:
                    self.db_writer.session.commit()
                except Exception as e:
                    print(f"提交球员数据失败: {e}")
                    self.db_writer.session.rollback()
            
            if created_count > 0:
                print(f"✓ 球员数据更新: 新创建 {created_count} 个")
            
        except Exception as e:
            print(f"确保球员数据存在时出错: {e}")
            self.db_writer.session.rollback()
    
    def _fetch_player_stats_by_dates(self, player_keys: List[str], players_data: List[Dict],
                                   league_key: str, season: str, start_date: Optional[date], end_date: Optional[date]) -> bool:
        """按日期获取球员统计数据（MLB/NBA/NHL）"""
        if start_date is None:
            start_date = date.today() - timedelta(days=30)
        if end_date is None:
            end_date = date.today()
        
        # 创建player_key到editorial_player_key的映射
        key_mapping = {}
        for player in players_data:
            if player.get("player_key") and player.get("editorial_player_key"):
                key_mapping[player["player_key"]] = player["editorial_player_key"]
        
        # 生成日期列表（每天一次）
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        
        print(f"  获取 {start_date} 至 {end_date} 球员统计...")
        
        for target_date in dates:
            date_str = target_date.strftime('%Y-%m-%d')
            stats_data = self._fetch_player_stats_batch_by_date(player_keys, league_key, date_str)
            
            for player_key, stats in stats_data.items():
                editorial_key = key_mapping.get(player_key, player_key)
                
                self.db_writer.write_player_daily_stats(
                    player_key=player_key,
                    editorial_player_key=editorial_key,
                    league_key=league_key,
                    stats_data=stats,
                    season=season,
                    stats_date=target_date
                )
        
        print(f"✓ 日期统计完成: {start_date} 至 {end_date}")
        return True

    def _fetch_player_stats_batch_by_date(self, player_keys: List[str], league_key: str, date_str: str) -> Dict[str, Dict]:
        """批量获取指定日期的球员统计数据"""
        batch_size = 25
        all_stats = {}
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats;type=date;date={date_str}?format=json"
            
            data = get_api_data(url)
            if data:
                batch_stats = self._extract_player_stats_from_data(data)
                all_stats.update(batch_stats)
            
            time.sleep(0.5)
        
        return all_stats
    
    def _extract_player_stats_from_data(self, stats_data: Dict) -> Dict[str, Dict]:
        """从统计数据中提取球员统计信息"""
        player_stats = {}
        
        try:
            fantasy_content = stats_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            if isinstance(league_data, list) and len(league_data) > 1:
                players_data = league_data[1].get("players", {})
                
                for player_index, player_data in players_data.items():
                    if not player_index.isdigit():
                        continue
                    
                    if "player" in player_data and len(player_data["player"]) > 1:
                        player_basic_info = player_data["player"][0]
                        player_key = None
                        
                        if isinstance(player_basic_info, list):
                            for info_item in player_basic_info:
                                if isinstance(info_item, dict) and "player_key" in info_item:
                                    player_key = info_item["player_key"]
                                    break
                        
                        if player_key:
                            stats = player_data["player"][1].get("player_stats", {}).get("stats", [])
                            normalized_stats = {}
                            for stat_item in stats:
                                if "stat" in stat_item:
                                    stat_info = stat_item["stat"]
                                    stat_id = str(stat_info.get("stat_id"))
                                    if stat_id:
                                        normalized_stats[stat_id] = stat_info.get("value")
                            
                            player_stats[player_key] = normalized_stats
        
        except Exception as e:
            print(f"提取球员统计数据时出错: {e}")
        
        return player_stats
    
    # ===== 辅助方法 =====
    
    def _extract_position_string(self, selected_position_data) -> Optional[str]:
        """从selected_position数据中提取position字符串"""
        if not selected_position_data:
            return None
        
        # 如果是字符串，直接返回
        if isinstance(selected_position_data, str):
            return selected_position_data
        
        # 如果是字典，尝试提取position
        if isinstance(selected_position_data, dict):
            return selected_position_data.get("position")
        
        # 如果是列表，查找包含position的字典
        if isinstance(selected_position_data, list):
            for item in selected_position_data:
                if isinstance(item, dict) and "position" in item:
                    return item["position"]
        
        return None
    
    def clear_database(self, confirm: bool = False) -> bool:
        """清空数据库"""
        result = self.db_writer.clear_database(confirm=confirm)
        if result:
            # 清空缓存
            self._season_dates_cache = None
            self._cache_league_key = None
        return result
    
    def show_database_summary(self) -> None:
        """显示数据库摘要"""
        try:
            summary = self.db_writer.get_database_summary()
            print("\n📊 数据库摘要:")
            
            has_error = False
            for table_name, count in summary.items():
                if count >= 0:
                    print(f"  {table_name}: {count} 条记录")
                else:
                    print(f"  {table_name}: 查询失败")
                    has_error = True
            
            # 如果有错误，提供修复选项
            if has_error:
                print("\n⚠️ 检测到数据库查询错误，可能是表结构问题")
                response = input("是否尝试修复数据库表结构? (y/N): ").strip().lower()
                if response == 'y':
                    if self.handle_database_error():
                        print("🔄 修复完成，重新显示数据库摘要:")
                        self.show_database_summary()
                    else:
                        print("❌ 数据库修复失败")
                        
        except Exception as e:
            print(f"\n❌ 显示数据库摘要时出错: {e}")
            print("⚠️ 可能是数据库连接或表结构问题")
            response = input("是否尝试修复数据库表结构? (y/N): ").strip().lower()
            if response == 'y':
                self.handle_database_error()
    
    # ===== 主要流程 =====
    
    def run_complete_league_fetch(self) -> bool:
        """执行完整的单联盟数据获取流程"""
        print("🚀 开始Yahoo Fantasy单联盟完整数据获取...")
        
        # 检查是否已选择联盟
        if not self.selected_league:
            print("✗ 尚未选择联盟，请先选择联盟")
            return False
        
        # 获取完整联盟数据
        if not self.fetch_complete_league_data():
            print("✗ 联盟数据获取失败")
            return False
        
        # 显示数据统计
        print(f"\n📊 数据获取统计:")
        print(self.db_writer.get_stats_summary())
        
        print("🎉 单联盟数据获取成功！")
        return True
    
    def run_historical_data_fetch(self, start_date: Optional[date] = None, 
                                  end_date: Optional[date] = None,
                                  days_back: Optional[int] = None) -> bool:
        """执行NBA历史数据获取流程"""
        print("🚀 开始NBA历史数据获取...")
        
        # 检查是否已选择联盟
        if not self.selected_league:
            print("✗ 尚未选择联盟，请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        
        # 智能计算日期范围
        if start_date is None and end_date is None and days_back is None:
            # 默认情况：获取最近30天的数据（从赛季结束日期开始）
            days_back = 30
        
        if start_date is None or end_date is None:
            start_date, end_date = self._calculate_smart_date_range(days_back, start_date, end_date)
        
        # 获取历史名单数据
        print(f"\n📋 步骤1: 获取历史名单数据")
        self.fetch_historical_rosters(start_date=start_date, end_date=end_date)
        
        # 获取历史球员统计数据
        print(f"\n📋 步骤2: 获取历史球员统计数据")
        self.fetch_historical_player_stats(start_date=start_date, end_date=end_date)
        
        # 显示统计摘要
        print(f"\n📊 数据获取统计:")
        print(self.db_writer.get_stats_summary())
        
        print("🎉 NBA历史数据获取成功！")
        return True
    
    # ===== 交互式菜单系统 =====
    
    def select_league_interactive(self) -> bool:
        """交互式选择联盟"""
        print("🔍 开始获取联盟信息和选择联盟...")
        
        # 获取基础数据并选择联盟
        if not self.fetch_and_select_league(use_existing_data=True):
            print("✗ 联盟获取或选择失败")
            return False
        
        print(f"✅ 联盟选择成功!")
        print(f"   联盟名称: {self.selected_league.get('name', 'Unknown')}")
        print(f"   联盟键: {self.selected_league.get('league_key', 'Unknown')}")
        print(f"   赛季: {self.selected_league.get('season', 'Unknown')}")
        print(f"   游戏类型: {self.selected_league.get('game_code', 'Unknown')}")
        
        return True
    
    def show_main_menu(self) -> None:
        """显示主菜单"""
        print("\n" + "="*60)
        print("🏈 Yahoo Fantasy 统一数据获取工具")
        print("="*60)
        
        # 显示当前选择的联盟信息
        if self.selected_league:
            league_name = self.selected_league.get('name', 'Unknown')
            league_key = self.selected_league.get('league_key', 'Unknown')
            season = self.selected_league.get('season', 'Unknown')
            print(f"📍 当前选择的联盟: {league_name} ({season})")
            print(f"   联盟键: {league_key}")
        else:
            print("📍 当前选择的联盟: 未选择")
        
        print("="*60)
        print("1. 选择联盟")
        print("2. 获取历史数据（指定时间段的roster和统计数据）")
        print("3. 获取完整历史数据（整个赛季的统计数据）")
        print("4. 查看数据库")
        print("5. 清空数据库")
        print("6. 退出")
        print("="*60)
    
    def run_interactive_menu(self) -> None:
        """运行交互式菜单"""
        while True:
            try:
                self.show_main_menu()
                choice = input("请选择操作 (1-6): ").strip()
                
                if choice == "1":
                    # 选择联盟并自动获取基础数据
                    print("\n🔍 联盟选择...")
                    if self.select_league_interactive():
                        print("✅ 联盟选择成功")
                        
                        # 自动执行基础数据获取
                        print("\n🚀 自动获取基础数据...")
                        if self.run_complete_league_fetch():
                            print("✅ 基础数据获取成功")
                            print("ℹ️  注意：roster数据将在步骤2或3的历史数据获取中获取")
                        else:
                            print("⚠️ 基础数据获取失败，但联盟已选择")
                            
                        # 自动发现赛季日程
                        print("\n🔍 自动发现赛季日程...")
                        if self.discover_season_schedule_smart():
                            print("✅ 赛季日程发现成功")
                        else:
                            print("⚠️ 赛季日程发现失败")
                            
                        self.show_database_summary()
                    else:
                        print("❌ 联盟选择失败")
                
                elif choice == "2":
                    # 获取历史数据（智能日期选择）
                    if not self.selected_league:
                        print("\n❌ 请先选择联盟 (选项1)")
                        continue
                    
                    print("\n🚀 配置历史数据获取...")
                    start_date, end_date = self._interactive_date_selection()
                    
                    if start_date and end_date:
                        print(f"\n🚀 开始历史数据获取 ({start_date} 至 {end_date})...")
                        self.show_database_summary()  # 显示开始前的状态
                        
                        if self.run_historical_data_fetch(start_date=start_date, end_date=end_date):
                            self.show_database_summary()  # 显示结束后的状态
                        else:
                            print("\n❌ 历史数据获取失败")
                    else:
                        print("\n❌ 日期选择失败")
                
                elif choice == "3":
                    # 获取完整历史数据（全赛季）
                    if not self.selected_league:
                        print("\n❌ 请先选择联盟 (选项1)")
                        continue
                    
                    print("\n🏀 获取完整历史数据...")
                    force_refresh = input("是否强制刷新已有数据? (y/N): ").strip().lower() == 'y'
                    
                    self.show_database_summary()  # 显示开始前的状态
                    
                    if self.fetch_complete_season_data(force_refresh=force_refresh):
                        print("✅ 完整历史数据获取成功")
                        self.show_database_summary()  # 显示结束后的状态
                    else:
                        print("❌ 完整历史数据获取失败")
                
                elif choice == "4":
                    # 查看数据库
                    self.show_database_summary()
                
                elif choice == "5":
                    # 清空数据库
                    print("\n⚠️ 即将清空数据库并重建表结构，所有数据将被删除！")
                    if self.clear_database(confirm=False):
                        print("✅ 数据库已清空并重建")
                        self.show_database_summary()
                    else:
                        print("❌ 数据库清空失败")
                
                elif choice == "6":
                    # 退出
                    print("\n👋 感谢使用！再见！")
                    break
                
                else:
                    print("❌ 无效选择，请输入1-6之间的数字")
                
                # 等待用户确认后继续
                if choice in ["1", "2", "3", "4", "5"] and choice != "6":
                    input("\n按回车键继续...")
                    
            except KeyboardInterrupt:
                print("\n\n👋 用户取消操作，再见！")
                break
            except Exception as e:
                print(f"\n❌ 发生错误: {e}")
                input("按回车键继续...")
    
    def _get_int_input(self, prompt: str, default: int) -> int:
        """获取整数输入"""
        try:
            value = input(prompt).strip()
            if not value:
                return default
            return int(value)
        except ValueError:
            print(f"输入无效，使用默认值: {default}")
            return default
    
    def _calculate_smart_date_range(self, days_back: Optional[int] = None, 
                                   start_date: Optional[date] = None, 
                                   end_date: Optional[date] = None) -> tuple[date, date]:
        """智能计算日期范围 - 考虑赛季开始和结束日期"""
        season_start, season_end = self._get_league_season_dates()
        
        if not season_start or not season_end:
            print("⚠️ 无法获取赛季日期，使用默认日期范围")
            if start_date is None:
                start_date = date.today() - timedelta(days=days_back or 30)
            if end_date is None:
                end_date = date.today()
            return start_date, end_date
        
        # 如果只指定了days_back
        if days_back is not None and start_date is None and end_date is None:
            today = date.today()
            
            # 如果今天在赛季结束后，从赛季结束日期开始往回算
            if today > season_end:
                end_date = season_end
                start_date = max(season_start, season_end - timedelta(days=days_back))
            # 如果今天在赛季期间，从今天开始往回算
            elif season_start <= today <= season_end:
                end_date = today
                start_date = max(season_start, today - timedelta(days=days_back))
            # 如果今天在赛季开始前（不太可能），使用赛季开始的一段时间
            else:
                start_date = season_start
                end_date = min(season_end, season_start + timedelta(days=days_back))
        
        # 如果只指定了start_date
        elif start_date is not None and end_date is None:
            end_date = min(season_end, date.today())
        
        # 如果只指定了end_date
        elif end_date is not None and start_date is None:
            if days_back:
                start_date = max(season_start, end_date - timedelta(days=days_back))
            else:
                start_date = season_start
        
        # 确保日期在赛季范围内
        start_date = max(start_date, season_start)
        end_date = min(end_date, season_end)
        
        # 确保start_date <= end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        return start_date, end_date
    
    def _get_date_input(self, prompt: str, default: Optional[date] = None) -> Optional[date]:
        """获取日期输入"""
        try:
            value = input(prompt).strip()
            if not value:
                return default
            
            # 尝试多种日期格式
            for fmt in ['%Y-%m-%d', '%m-%d', '%Y/%m/%d', '%m/%d']:
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    # 如果只提供了月-日，假设是当前年份
                    if fmt in ['%m-%d', '%m/%d']:
                        current_year = date.today().year
                        parsed_date = parsed_date.replace(year=current_year)
                    return parsed_date
                except ValueError:
                    continue
            
            print(f"日期格式无效: {value}，请使用 YYYY-MM-DD 格式")
            return default
        except Exception as e:
            print(f"日期解析失败: {e}")
            return default
    
    def _interactive_date_selection(self) -> tuple[Optional[date], Optional[date]]:
        """交互式日期选择"""
        season_start, season_end = self._get_league_season_dates()
        
        print(f"\n📅 当前赛季日期范围: {season_start} 至 {season_end}")
        print("请选择历史数据获取方式:")
        print("1. 指定天数回溯（从赛季结束日期开始）")
        print("2. 指定具体日期")
        print("3. 指定时间段")
        print("4. 使用默认（最近30天）")
        
        choice = input("请选择 (1-4): ").strip()
        
        if choice == "1":
            days_back = self._get_int_input("请输入回溯天数: ", 30)
            start_date, end_date = self._calculate_smart_date_range(days_back=days_back)
            print(f"计算得到日期范围: {start_date} 至 {end_date}")
            return start_date, end_date
            
        elif choice == "2":
            target_date = self._get_date_input("请输入目标日期 (YYYY-MM-DD): ")
            if target_date:
                print(f"将获取 {target_date} 的数据")
                return target_date, target_date
            else:
                print("日期输入无效，使用默认")
                return self._calculate_smart_date_range(days_back=30)
                
        elif choice == "3":
            print("请输入时间段:")
            start_date = self._get_date_input("开始日期 (YYYY-MM-DD): ")
            end_date = self._get_date_input("结束日期 (YYYY-MM-DD): ")
            
            if start_date and end_date:
                # 验证日期范围
                start_date, end_date = self._calculate_smart_date_range(start_date=start_date, end_date=end_date)
                print(f"将获取 {start_date} 至 {end_date} 的数据")
                return start_date, end_date
            else:
                print("日期输入不完整，使用默认")
                return self._calculate_smart_date_range(days_back=30)
                
        else:  # choice == "4" 或其他
            start_date, end_date = self._calculate_smart_date_range(days_back=30)
            print(f"使用默认日期范围: {start_date} 至 {end_date}")
            return start_date, end_date

    # ===== 赛季日程和完整数据获取功能 =====
    
    def discover_season_schedule_smart(self) -> bool:
        """简化的赛季日程发现 - 直接使用联盟日期范围"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        game_code = self.selected_league.get('game_code', 'nba')
        
        print(f"🔍 发现赛季日程: {league_key} ({game_code.upper()} {season})")
        
        # 从数据库获取联盟的确切日期范围
        start_date, end_date = self._get_league_season_dates()
        
        if not start_date or not end_date:
            print("✗ 无法确定赛季日期范围")
            return False
        
        print(f"📅 联盟赛季日期范围: {start_date} 至 {end_date}")
        
        # 生成所有日期（每天一个）
        available_dates = []
        current_date = start_date
        while current_date <= end_date:
            available_dates.append(current_date)
            current_date += timedelta(days=1)
        
        print(f"📊 生成了 {len(available_dates)} 个赛季日期")
        
        # 保存到DateDimension表
        if available_dates:
            saved_count = self._save_date_dimension(available_dates, league_key, season)
            print(f"✓ 赛季日程发现完成: {saved_count} 个日期")
            return saved_count > 0
        
        return False
    
    def discover_season_schedule(self) -> bool:
        """向后兼容方法 - 调用简化版本"""
        return self.discover_season_schedule_smart()
    
    def _save_date_dimension(self, dates: List[date], league_key: str, season: str) -> int:
        """保存日期维度数据"""
        try:
            # 准备批量数据
            dates_data = []
            
            for target_date in dates:
                date_dict = {
                    'date': target_date,
                    'league_key': league_key,
                    'season': season
                }
                dates_data.append(date_dict)
            
            # 使用database_writer的批量写入方法
            count = self.db_writer.write_date_dimensions_batch(dates_data)
            
            print(f"✓ 成功保存 {count} 个日期到日程表")
            return count
            
        except Exception as e:
            print(f"保存日期维度失败: {e}")
            return 0
    
    def _get_available_dates_from_db(self, league_key: str, season: str) -> List[date]:
        """从数据库获取可用日期列表"""
        try:
            dates = self.db_writer.session.query(DateDimension.date).filter_by(
                league_key=league_key,
                season=season
            ).order_by(DateDimension.date).all()
            
            result = [d[0] for d in dates]
            
            if result:
                print(f"✓ 从数据库获取到 {len(result)} 个可用日期")
                print(f"  日期范围: {result[0]} 至 {result[-1]}")
            else:
                print("✗ 数据库中未找到可用日期")
            
            return result
            
        except Exception as e:
            print(f"获取可用日期失败: {e}")
            return []
    
    def _has_date_data(self, league_key: str, target_date: date) -> bool:
        """检查是否已有指定日期的数据"""
        try:
            from model import PlayerDailyStats
            
            existing = self.db_writer.session.query(PlayerDailyStats).filter_by(
                league_key=league_key,
                date=target_date
            ).first()
            
            return existing is not None
            
        except Exception as e:
            return False

    def fetch_complete_season_data(self, force_refresh: bool = False) -> bool:
        """获取完整NBA赛季的所有可用日期数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print(f"🏀 开始获取完整NBA赛季数据: {league_key} ({season})")
        
        # 1. 获取或生成赛季日程
        available_dates = self._get_available_dates_from_db(league_key, season)
        
        if not available_dates or force_refresh:
            print("📅 未找到赛季日程或强制刷新，开始智能发现...")
            if self.discover_season_schedule_smart():
                available_dates = self._get_available_dates_from_db(league_key, season)
            else:
                print("⚠️ 智能发现失败，尝试详细采样方法...")
                if self.discover_season_schedule():
                    available_dates = self._get_available_dates_from_db(league_key, season)
        
        if not available_dates:
            print("✗ 无法获取赛季日程")
            return False
        
        print(f"📊 将获取 {len(available_dates)} 个日期的数据")
        print(f"   日期范围: {available_dates[0]} 至 {available_dates[-1]}")
        
        # 2. 检查球员数据
        print("📋 检查球员数据...")
        player_keys, all_players = self._get_or_fetch_player_data(league_key)
        if not player_keys:
            print("✗ 获取球员数据失败")
            return False
        
        print(f"✓ 找到 {len(player_keys)} 个球员")
        
        # 3. 创建player_key到editorial_player_key的映射
        key_mapping = {}
        for player in all_players:
            if player.get("player_key") and player.get("editorial_player_key"):
                key_mapping[player["player_key"]] = player["editorial_player_key"]
        
        # 4. 批量获取所有日期的数据
        success_count = 0
        total_dates = len(available_dates)
        skipped_count = 0
        
        print(f"🔄 开始获取 {total_dates} 个日期的球员统计数据...")
        
        # 分组显示进度，每20个日期为一组
        batch_size = 20
        
        for i, target_date in enumerate(available_dates):
            date_str = target_date.strftime('%Y-%m-%d')
            
            # 只在每个批次开始时显示详细信息
            if i % batch_size == 0 or i == 0:
                print(f"  处理日期批次: {i+1}-{min(i+batch_size, total_dates)}/{total_dates}")
            
            # 检查是否已有数据
            if not force_refresh and self._has_date_data(league_key, target_date):
                skipped_count += 1
                continue
            
            # 获取该日期的球员统计
            stats_data = self._fetch_player_stats_batch_by_date(player_keys, league_key, date_str)
            
            if stats_data:
                # 写入数据库
                date_success_count = 0
                for player_key, stats in stats_data.items():
                    editorial_key = key_mapping.get(player_key, player_key)
                    
                    if self.db_writer.write_player_daily_stats(
                        player_key=player_key,
                        editorial_player_key=editorial_key,
                        league_key=league_key,
                        stats_data=stats,
                        season=season,
                        stats_date=target_date
                    ):
                        date_success_count += 1
                
                success_count += 1
                
                # 简化的单行输出
                if i % 5 == 0 or success_count % 10 == 0:  # 每5个日期或每10个成功日期显示一次
                    print(f"    {date_str}: ✓ {len(stats_data)}球员 {date_success_count}记录")
            
            # 控制请求频率，避免过快请求
            if i < total_dates - 1:
                time.sleep(0.8)
        
        print(f"\n🎉 完整赛季数据获取完成!")
        print(f"   成功: {success_count}/{total_dates} 个日期")
        print(f"   跳过: {skipped_count} 个日期（已存在）")
        print(f"   总统计记录: {self.db_writer.stats.get('player_daily_stats', 0)}")
        
        return success_count > 0
    
    def _get_league_season_dates(self) -> tuple[Optional[date], Optional[date]]:
        """从数据库中的联盟信息获取赛季开始和结束日期（带缓存）"""
        try:
            if not self.selected_league:
                return None, None
            
            league_key = self.selected_league['league_key']
            
            # 检查缓存
            if (self._season_dates_cache is not None and 
                self._cache_league_key == league_key):
                return self._season_dates_cache
            
            # 首先尝试从数据库中的League表获取
            from model import League
            league_record = self.db_writer.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            start_date = None
            end_date = None
            
            if league_record:
                # 只在首次获取时显示详细信息
                if self._season_dates_cache is None:
                    print(f"📅 获取联盟日期信息: {league_record.name}")
                
                # 从数据库记录解析日期
                if league_record.start_date:
                    try:
                        start_date = datetime.strptime(league_record.start_date, '%Y-%m-%d').date()
                    except:
                        if self._season_dates_cache is None:
                            print(f"  ⚠️ 解析开始日期失败: {league_record.start_date}")
                
                if league_record.end_date:
                    try:
                        end_date = datetime.strptime(league_record.end_date, '%Y-%m-%d').date()
                    except:
                        if self._season_dates_cache is None:
                            print(f"  ⚠️ 解析结束日期失败: {league_record.end_date}")
                
                # 如果数据库中的日期不完整，使用NBA默认值
                if not start_date or not end_date:
                    season = league_record.season or '2024'
                    
                    if self._season_dates_cache is None:
                        print(f"  使用 {season} 赛季默认日期范围")
                    
                    year = int(season)
                    
                    # NBA赛季通常从10月开始到次年4月结束  
                    if not start_date:
                        start_date = date(year - 1, 10, 1)
                    if not end_date:
                        end_date = date(year, 4, 30)
            
            else:
                # 数据库中没有找到联盟记录，从selected_league获取
                if self._season_dates_cache is None:
                    print("  从内存中的联盟信息获取")
                
                start_date_str = self.selected_league.get('start_date')
                end_date_str = self.selected_league.get('end_date')
                
                if start_date_str:
                    try:
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                    except:
                        if self._season_dates_cache is None:
                            print(f"  ⚠️ 解析开始日期失败: {start_date_str}")
                
                if end_date_str:
                    try:
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                    except:
                        if self._season_dates_cache is None:
                            print(f"  ⚠️ 解析结束日期失败: {end_date_str}")
                
                # 如果内存中的日期也不完整，使用NBA默认值
                if not start_date or not end_date:
                    season = self.selected_league.get('season', '2024')
                    
                    if self._season_dates_cache is None:
                        print(f"  使用 {season} 赛季默认日期范围")
                    
                    year = int(season)
                    
                    # NBA赛季通常从10月开始到次年4月结束
                    if not start_date:
                        start_date = date(year - 1, 10, 1)
                    if not end_date:
                        end_date = date(year, 4, 30)
            
            # 缓存结果并显示最终日期（仅首次）
            result = (start_date, end_date)
            if self._season_dates_cache is None and start_date and end_date:
                print(f"  日期范围: {start_date} 至 {end_date}")
            
            self._season_dates_cache = result
            self._cache_league_key = league_key
            
            return result
            
        except Exception as e:
            if self._season_dates_cache is None:
                print(f"获取赛季日期失败: {e}")
            return None, None
    
    def handle_database_error(self) -> bool:
        """处理数据库错误，必要时重新创建表结构"""
        try:
            print("🔧 检测到数据库错误，尝试修复...")
            
            # 尝试重新创建数据库表
            if self.db_writer.recreate_database_tables():
                print("✅ 数据库表结构修复成功")
                return True
            else:
                print("❌ 数据库表结构修复失败")
                return False
                
        except Exception as e:
            print(f"处理数据库错误时出错: {e}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy统一数据获取工具")
    
    parser.add_argument("--single-league", action="store_true", help="执行单联盟完整数据获取")
    parser.add_argument("--time-series", action="store_true", help="执行时间序列历史数据获取")
    parser.add_argument("--historical", action="store_true", help="执行历史数据获取流程（与--time-series相同）")
    parser.add_argument("--weeks-back", type=int, default=5, help="回溯周数（NFL），默认5周")
    parser.add_argument("--days-back", type=int, default=30, help="回溯天数（其他运动），默认30天")
    parser.add_argument("--clear-db", action="store_true", help="清空数据库（慎用！）")
    parser.add_argument("--show-summary", action="store_true", help="显示数据库摘要")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    parser.add_argument("--batch-size", type=int, default=100, help="数据库批量写入大小，默认100")
    
    args = parser.parse_args()
    
    # 创建数据获取器
    fetcher = YahooFantasyDataFetcher(delay=args.delay, batch_size=args.batch_size)
    
    try:
        # 检查是否有命令行参数
        has_args = any([args.single_league, args.time_series, args.historical, 
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
            
            elif args.single_league:
                # 执行单联盟完整流程
                print("🚀 开始单联盟完整数据获取流程")
                
                # 首先选择联盟
                if not fetcher.select_league_interactive():
                    print("\n❌ 联盟选择失败")
                    return
                
                fetcher.show_database_summary()  # 显示开始前的状态
                
                if fetcher.run_complete_league_fetch():
                    fetcher.show_database_summary()  # 显示结束后的状态
                else:
                    print("\n❌ 单联盟完整数据获取失败")
            
            elif args.time_series or args.historical:
                # 执行时间序列流程
                print("🚀 开始时间序列历史数据获取流程")
                
                # 首先选择联盟
                if not fetcher.select_league_interactive():
                    print("\n❌ 联盟选择失败")
                    return
                
                fetcher.show_database_summary()  # 显示开始前的状态
                
                if fetcher.run_historical_data_fetch(weeks_back=args.weeks_back, days_back=args.days_back):
                    fetcher.show_database_summary()  # 显示结束后的状态
                else:
                    print("\n❌ 时间序列历史数据获取失败")
    
    finally:
        # 确保清理资源
        fetcher.close()


if __name__ == "__main__":
    main()


