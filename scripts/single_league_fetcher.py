#!/usr/bin/env python3
"""
Yahoo Fantasy数据获取工具 - 单联盟深度模式（直接数据库写入版）
专注于获取单个联盟的完整深度数据，直接写入数据库
"""
import os
import sys
import time
import argparse
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yahoo_api_utils import (
    get_api_data, save_json_data, load_json_data, 
    GAMES_DIR, LEAGUES_DIR,
    select_league_interactively
)
from database_writer import FantasyDatabaseWriter
from model import Roster

class SingleLeagueDataFetcher:
    """Yahoo Fantasy单联盟数据获取器（直接数据库写入版）"""
    
    def __init__(self, delay: int = 2, batch_size: int = 100):
        """初始化数据获取器"""
        self.delay = delay
        self.selected_league: Optional[Dict] = None
        self.db_writer = FantasyDatabaseWriter(batch_size=batch_size)
        
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
    
    def fetch_and_select_league(self) -> bool:
        """获取基础数据并选择联盟（直接写入数据库）"""
        print("🚀 开始获取基础数据和联盟选择...")
        
        # 获取或加载联盟数据
        leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not leagues_file.exists():
            print("📋 获取联盟数据...")
            if not self._fetch_all_leagues_data():
                return False
        
        # 选择联盟
        leagues_data = load_json_data(leagues_file)
        if not leagues_data:
            print("✗ 无法加载联盟数据")
            return False
        
        selected_league = select_league_interactively(leagues_data)
        if not selected_league:
            print("✗ 未选择联盟")
            return False
        
        self.selected_league = selected_league
        
        print(f"✓ 联盟选择完成: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
    def _fetch_all_leagues_data(self) -> bool:
        """获取所有联盟数据并直接写入数据库"""
        # 获取games数据
        games_data = self._fetch_games_data()
        if not games_data:
            return False
        
        # 写入games数据到数据库
        games_count = self.db_writer.write_games_data(games_data)
        print(f"✓ 写入 {games_count} 个游戏数据到数据库")
        
        # 提取游戏键并获取联盟数据
        game_keys = self._extract_game_keys(games_data)
        if not game_keys:
            return False
        
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
            
            # 同时保存JSON文件以便选择联盟
            leagues_file = LEAGUES_DIR / "all_leagues_data.json"
            save_json_data(all_leagues, leagues_file)
            print(f"✓ 联盟数据获取完成")
            return True
        
        return False
    
    def _fetch_games_data(self) -> Optional[Dict]:
        """获取用户的games数据"""
        print("获取用户的games数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        if data:
            # 同时保存JSON文件以便选择联盟
            save_json_data(data, GAMES_DIR / "games_data.json")
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
                        
                        leagues.append(league_info)
                break
        
        except Exception as e:
            print(f"提取联盟数据时出错: {e}")
        
        return leagues
    
    # ===== 联盟深度数据获取 =====
    
    def fetch_complete_league_data(self) -> bool:
        """获取完整的联盟数据并直接写入数据库"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取联盟完整数据: {league_key} ===")
        
        # 1. 获取联盟详细信息
        print("\n📋 步骤1: 获取联盟详细信息")
        if not self.fetch_league_details():
            print("⚠️ 联盟详细信息获取失败，继续其他步骤")
        
        # 2. 获取团队和rosters数据
        print("\n📋 步骤2: 获取团队和rosters数据")
        teams_data = self.fetch_teams_data()
        if teams_data:
            self.fetch_team_rosters(teams_data)
        
        # 3. 获取完整球员数据
        print("\n📋 步骤3: 获取完整球员数据")
        self.fetch_complete_players_data()
        
        # 4. 获取transaction数据
        print("\n📋 步骤4: 获取transaction数据")
        self.fetch_complete_transactions_data(teams_data)
        
        print(f"\n🎯 联盟数据获取完成: {league_key}")
        return True
    
    def fetch_league_details(self) -> bool:
        """获取联盟详细信息并写入数据库"""
        league_key = self.selected_league['league_key']
        
        # 获取联盟设置数据
        settings_data = self._fetch_league_settings(league_key)
        if settings_data:
            # 直接写入数据库
            self.db_writer.write_league_settings(league_key, settings_data)
            print("✓ 联盟设置数据写入数据库")
        
        return True
    
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
            print(f"✓ 团队数据获取完成，写入数据库 {success_count} 个团队")
            return teams_data
        
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
            team_info = team_data[0]
            
            # 提取团队基本信息
            team_dict = {}
            managers_data = []
            
            for item in team_info:
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
                        team_dict["clinched_playoffs"] = item["clinched_playoffs"]
                    elif "has_draft_grade" in item:
                        team_dict["has_draft_grade"] = item["has_draft_grade"]
                    else:
                        team_dict.update(item)
            
            # 添加managers数据
            team_dict["managers"] = managers_data
            
            return team_dict if team_dict.get("team_key") else None
            
        except Exception as e:
            print(f"提取团队数据失败: {e}")
            return None
    
    def _process_team_data_to_db(self, team_data: List, league_key: str) -> bool:
        """处理单个团队数据并写入数据库"""
        team_dict = self._extract_team_data_from_api(team_data)
        if team_dict:
            count = self.db_writer.write_teams_batch([team_dict], league_key)
            return count > 0
        return False
    
    def fetch_team_rosters(self, teams_data: Dict) -> bool:
        """获取所有团队的roster数据并写入数据库"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        print(f"获取 {len(team_keys)} 个团队的rosters...")
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            print(f"  获取团队roster {i+1}/{len(team_keys)}: {team_key}")
            
            roster_data = self._fetch_team_roster(team_key)
            if roster_data:
                # 直接处理roster数据并写入数据库
                if self._process_roster_data_to_db(roster_data, team_key):
                    success_count += 1
            
            if i < len(team_keys) - 1:
                self.wait()
        
        print(f"✓ Team rosters获取完成: {success_count}/{len(team_keys)}")
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
            
            # 这里需要实现roster数据的写入逻辑
            # 由于Roster表的结构，我们可以直接写入
            count = 0
            for roster_entry in roster_list:
                try:
                    # 检查是否已存在
                    existing = self.db_writer.session.query(Roster).filter_by(
                        team_key=roster_entry["team_key"],
                        player_key=roster_entry["player_key"],
                        coverage_date=roster_entry["coverage_date"]
                    ).first()
                    
                    if existing:
                        continue
                    
                    roster = Roster(
                        team_key=roster_entry["team_key"],
                        player_key=roster_entry["player_key"],
                        coverage_date=roster_entry["coverage_date"],
                        is_prescoring=roster_entry["is_prescoring"],
                        is_editable=roster_entry["is_editable"],
                        status=roster_entry["status"],
                        status_full=roster_entry["status_full"],
                        injury_note=roster_entry["injury_note"],
                        is_keeper=roster_entry.get("is_keeper", False),
                        keeper_cost=roster_entry.get("keeper_cost"),
                        kept=roster_entry.get("kept", False),
                        selected_position=roster_entry["selected_position"],
                        eligible_positions_to_add=roster_entry.get("eligible_positions_to_add")
                    )
                    self.db_writer.session.add(roster)
                    count += 1
                    
                    # 每10条记录提交一次，减少内存使用
                    if count % 10 == 0:
                        self.db_writer.session.commit()
                        
                except Exception as e:
                    print(f"写入roster记录失败: {e}")
                    # 回滚当前事务
                    self.db_writer.session.rollback()
                    continue
            
            # 最终提交剩余的记录
            if count > 0:
                try:
                    self.db_writer.session.commit()
                    self.db_writer.stats['rosters'] += count
                except Exception as e:
                    print(f"最终提交roster记录失败: {e}")
                    self.db_writer.session.rollback()
            
            return count > 0
            
        except Exception as e:
            print(f"处理roster数据失败 {team_key}: {e}")
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
                            if (isinstance(team_data, list) and 
                                len(team_data) > 0 and 
                                isinstance(team_data[0], list) and 
                                len(team_data[0]) > 0):
                                
                                team_key = team_data[0][0].get("team_key")
                                if team_key:
                                    team_keys.append(team_key)
        
        except Exception as e:
            print(f"提取团队键时出错: {e}")
        
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
        
        print(f"开始分页获取球员数据 (每页约 {page_size} 个)")
            
        while iteration < max_iterations:
            iteration += 1
            print(f"  正在获取第 {iteration} 批数据 (起始位置: {start})")
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
            if start > 0:
                url += f";start={start}"
            url += "?format=json"
                
            players_data = get_api_data(url)
            if not players_data:
                print(f"    ✗ 第 {iteration} 批数据获取失败")
                break
                
            batch_players = self._extract_player_info_from_league_data(players_data)
            
            if not batch_players:
                print(f"    ✓ 第 {iteration} 批：无新球员，获取完成")
                break
                
            all_players.extend(batch_players)
            print(f"    ✓ 第 {iteration} 批：获取了 {len(batch_players)} 个球员")
                
            if len(batch_players) < page_size:
                break
                
            start += page_size
            time.sleep(0.5)
            
        print(f"分页获取完成: 总计 {len(all_players)} 个球员，用了 {iteration} 批次")
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
        
        print(f"开始分页获取transaction数据 (每页 {page_size} 个)")
        
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
            
            print(f"  正在获取第 {iteration} 批transaction数据 (起始位置: {start})")
            
            transactions_data = get_api_data(url)
            if not transactions_data:
                print(f"    ✗ 第 {iteration} 批数据获取失败")
                break
            
            batch_transactions = self._extract_transactions_from_data(transactions_data)
            
            if not batch_transactions:
                print(f"    ✓ 第 {iteration} 批：无新transaction，获取完成")
                break
            
            all_transactions.extend(batch_transactions)
            print(f"    ✓ 第 {iteration} 批：获取了 {len(batch_transactions)} 个transaction")
            
            if len(batch_transactions) < page_size:
                print(f"    ✓ 数据获取完成：最后一批只有 {len(batch_transactions)} 个transaction")
                break
            
            start += page_size
            time.sleep(0.5)
        
        print(f"分页获取完成: 总计 {len(all_transactions)} 个transaction，用了 {iteration} 批次")
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
    
    # ===== 主要流程 =====
    
    def run_complete_data_fetch(self) -> bool:
        """执行完整的数据获取流程"""
        print("🚀 开始Yahoo Fantasy单联盟完整数据获取（直接数据库写入版）...")
        
        try:
            # 1. 基础数据获取和联盟选择
            if not self.fetch_and_select_league():
                print("✗ 基础数据获取或联盟选择失败")
                return False
            
            # 2. 获取完整联盟数据
            if not self.fetch_complete_league_data():
                print("✗ 联盟数据获取失败")
                return False
            
            # 3. 显示数据统计
            print(f"\n📊 数据获取统计:")
            print(self.db_writer.get_stats_summary())
            
            print("🎉 单联盟数据获取成功！")
            return True
            
        finally:
            self.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy单联盟数据获取工具（直接数据库写入版）")
    
    parser.add_argument("--complete", action="store_true", help="执行完整的数据获取流程")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    parser.add_argument("--batch-size", type=int, default=100, help="数据库批量写入大小，默认100")
    
    args = parser.parse_args()
    
    # 创建数据获取器
    fetcher = SingleLeagueDataFetcher(delay=args.delay, batch_size=args.batch_size)
    
    if args.complete:
        fetcher.run_complete_data_fetch()
    else:
        # 默认执行完整流程
        fetcher.run_complete_data_fetch()


if __name__ == "__main__":
    main() 