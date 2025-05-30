#!/usr/bin/env python3
"""
Yahoo Fantasy数据获取工具 - 单联盟深度模式
专注于获取单个联盟的完整深度数据
"""
import os
import sys
import time
import argparse
from datetime import datetime

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import (
    get_api_data, save_json_data, load_json_data, 
    GAMES_DIR, LEAGUES_DIR,
    create_league_directory, get_league_directory,
    select_league_interactively, print_data_overview
)

class SingleLeagueFantasyDataFetcher:
    """Yahoo Fantasy单联盟数据获取器"""
    
    def __init__(self, delay=2):
        """初始化数据获取器"""
        self.delay = delay
        self.selected_league = None
        self.league_dirs = None
        
    def wait(self, message=None):
        """等待指定时间"""
        if message:
            print(f"{message}，等待 {self.delay} 秒...")
        else:
            print(f"等待 {self.delay} 秒...")
        time.sleep(self.delay)
    
    # ===== 基础数据获取 =====
    
    def fetch_games_data(self):
        """获取用户的games数据"""
        print("获取用户的games数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        if data:
            print("成功获取用户的games数据")
            save_json_data(data, GAMES_DIR / "games_data.json")
            return data
        return None
    
    def fetch_all_leagues_data(self):
        """获取所有联盟数据"""
        print("获取所有联盟数据...")
        
        # 先获取games数据
        games_data = self.fetch_games_data()
        if not games_data:
            print("✗ 获取games数据失败")
            return None
        
        # 提取游戏键
        game_keys = self.extract_game_keys(games_data)
        if not game_keys:
            print("✗ 未找到游戏键")
            return None
        
        # 获取所有联盟数据
        all_leagues = {}
        success_count = 0
        
        for i, game_key in enumerate(game_keys):
            print(f"获取游戏 {i+1}/{len(game_keys)} 的联盟数据: {game_key}")
            leagues_data = self.fetch_leagues_data(game_key)
            if leagues_data:
                extracted_leagues = self.extract_leagues_from_data(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
                    success_count += 1
            
            if i < len(game_keys) - 1:
                self.wait()
        
        if all_leagues:
            # 保存所有联盟数据
            leagues_file = LEAGUES_DIR / "all_leagues_data.json"
            save_json_data(all_leagues, leagues_file)
            print(f"✓ 成功获取 {success_count}/{len(game_keys)} 个游戏的联盟数据")
            return all_leagues
        
        return None
    
    def fetch_leagues_data(self, game_key):
        """获取指定game下用户的leagues数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"  ✓ 成功获取game {game_key}的leagues数据")
            return data
        return None
    
    def extract_game_keys(self, games_data):
        """从游戏数据中提取所有游戏键，只包含type为'full'的游戏"""
        game_keys = []
        
        try:
            if not games_data or "fantasy_content" not in games_data:
                print("游戏数据格式不正确")
                return game_keys
                
            fantasy_content = games_data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_container = user_data[1]["games"]
            games_count = int(games_container.get("count", 0))
            
            print(f"找到 {games_count} 个游戏")
            
            for i in range(games_count):
                str_index = str(i)
                
                if str_index not in games_container:
                    continue
                    
                game_container = games_container[str_index]
                game_data = game_container["game"]
                
                if isinstance(game_data, list) and len(game_data) > 0 and isinstance(game_data[0], dict):
                    game_info = game_data[0]
                    game_key = game_info.get("game_key")
                    game_type = game_info.get("type")
                    game_name = game_info.get("name", "Unknown")
                    
                    if game_key and game_type == "full":
                        game_keys.append(game_key)
                        print(f"  提取游戏键: {game_key} (类型: {game_type}, 名称: {game_name})")
                
        except Exception as e:
            print(f"提取游戏键时出错: {str(e)}")
        
        print(f"总共提取了 {len(game_keys)} 个full类型的游戏键")
        return game_keys
    
    def extract_leagues_from_data(self, data, game_key):
        """从API返回的数据中提取联盟信息"""
        leagues = []
        
        try:
            if not data or "fantasy_content" not in data:
                return leagues
            
            if "error" in data:
                error_msg = data.get("error", {}).get("description", "未知错误")
                if "Invalid subresource leagues requested" in error_msg:
                    print("  跳过不支持leagues查询的游戏类型")
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
            print(f"  提取游戏 {game_key} 的联盟数据时出错: {str(e)}")
        
        print(f"  提取了 {len(leagues)} 个联盟")
        return leagues
    
    # ===== 联盟选择 =====
    
    def select_league(self):
        """选择要处理的联盟"""
        # 加载联盟数据
        leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not leagues_file.exists():
            print("联盟数据不存在，请先获取基础数据")
            return False
        
        leagues_data = load_json_data(leagues_file)
        if not leagues_data:
            print("加载联盟数据失败")
            return False
        
        # 交互式选择联盟
        selected_league = select_league_interactively(leagues_data)
        if not selected_league:
            print("未选择联盟")
            return False
        
        self.selected_league = selected_league
        self.league_dirs = create_league_directory(selected_league['league_key'])
        
        print(f"✓ 联盟选择完成，开始获取深度数据...")
        return True
    
    # ===== 单联盟深度数据获取 =====
    
    def fetch_league_details(self):
        """获取联盟详细信息"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取联盟详细信息: {league_key} ===")
        
        # 获取联盟settings
        print("获取联盟设置...")
        settings_data = self.fetch_league_settings(league_key)
        
        # 获取联盟standings
        print("获取联盟排名...")
        standings_data = self.fetch_league_standings(league_key)
        
        # 获取联盟scoreboard
        print("获取联盟记分板...")
        scoreboard_data = self.fetch_league_scoreboard(league_key)
        
        # 整合联盟信息
        league_info = {
            "basic_info": self.selected_league,
            "settings": settings_data,
            "standings": standings_data,
            "scoreboard": scoreboard_data,
            "metadata": {
                "fetched_at": datetime.now().isoformat(),
                "league_key": league_key
            }
        }
        
        # 保存联盟信息
        league_info_file = self.league_dirs['base'] / "league_info.json"
        success = save_json_data(league_info, league_info_file)
        
        if success:
            print("✓ 联盟详细信息获取完成")
            return True
        else:
            print("✗ 联盟详细信息保存失败")
            return False
    
    def fetch_league_settings(self, league_key):
        """获取联盟设置"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return get_api_data(url)
    
    def fetch_league_standings(self, league_key):
        """获取联盟排名"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        return get_api_data(url)
    
    def fetch_league_scoreboard(self, league_key):
        """获取联盟记分板"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard?format=json"
        return get_api_data(url)
    
    def fetch_teams_data(self):
        """获取联盟的所有团队数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取团队数据: {league_key} ===")
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        teams_data = get_api_data(url)
        
        if not teams_data:
            print("✗ 获取团队数据失败")
            return False
        
        # 保存团队数据
        teams_file = self.league_dirs['base'] / "teams.json"
        success = save_json_data(teams_data, teams_file)
        
        if success:
            print("✓ 团队数据获取完成")
            return teams_data
        else:
            print("✗ 团队数据保存失败")
            return False
    
    def fetch_team_rosters(self, teams_data):
        """获取所有团队的roster数据"""
        if not teams_data:
            print("✗ 团队数据不存在")
            return False
        
        print(f"\n=== 获取团队rosters数据 ===")
        
        # 提取团队键
        team_keys = self.extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 未找到团队键")
            return False
        
        print(f"找到 {len(team_keys)} 个团队")
        
        success_count = 0
        for i, team_key in enumerate(team_keys):
            print(f"获取团队roster {i+1}/{len(team_keys)}: {team_key}")
            
            roster_data = self.fetch_team_roster(team_key)
            if roster_data:
                roster_file = self.league_dirs['rosters'] / f"team_roster_{team_key}.json"
                if save_json_data(roster_data, roster_file):
                    success_count += 1
            
            if i < len(team_keys) - 1:
                self.wait()
        
        print(f"✓ Team rosters获取完成: {success_count}/{len(team_keys)}")
        return success_count > 0
    
    def fetch_team_roster(self, team_key):
        """获取单个团队的roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
        return get_api_data(url)
    
    def extract_team_keys_from_data(self, teams_data):
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
            
            if not teams_container:
                return team_keys
            
            teams_count = int(teams_container.get("count", 0))
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                if (isinstance(team_data, list) and 
                    len(team_data) > 0 and 
                    isinstance(team_data[0], list) and 
                    len(team_data[0]) > 0 and 
                    isinstance(team_data[0][0], dict)):
                    
                    team_key = team_data[0][0].get("team_key")
                    if team_key:
                        team_keys.append(team_key)
        
        except Exception as e:
            print(f"提取团队键时出错: {str(e)}")
        
        return team_keys
    
    def extract_players_from_rosters(self):
        """从rosters中提取球员静态数据"""
        print(f"\n=== 从rosters提取球员数据 ===")
        
        roster_files = list(self.league_dirs['rosters'].glob("*.json"))
        if not roster_files:
            print("✗ 未找到roster文件")
            return False
        
        print(f"处理 {len(roster_files)} 个roster文件")
        
        all_static_players = {}
        roster_assignments = []
        
        for i, roster_file in enumerate(roster_files):
            print(f"处理文件 {i+1}/{len(roster_files)}: {roster_file.name}")
            roster_data = load_json_data(roster_file)
            if roster_data:
                team_key = self.extract_team_key_from_roster(roster_data)
                if team_key:
                    static_players, assignments = self.extract_players_and_assignments_from_roster(roster_data, team_key)
                    
                    # 合并静态球员数据
                    for player in static_players:
                        global_key = player.get("editorial_player_key")
                        if global_key and global_key not in all_static_players:
                            all_static_players[global_key] = player
                    
                    roster_assignments.extend(assignments)
        
        # 保存静态球员数据
        static_data = {
            "static_players": all_static_players,
            "roster_assignments": roster_assignments,
            "metadata": {
                "league_key": self.selected_league['league_key'],
                "total_unique_players": len(all_static_players),
                "total_assignments": len(roster_assignments),
                "created_at": datetime.now().isoformat(),
                "description": "从团队rosters中提取的球员静态信息 (仅包含Taken players)"
            }
        }
        
        static_players_file = self.league_dirs['players'] / "static_players.json"
        if save_json_data(static_data, static_players_file):
            print(f"✓ 球员静态数据提取完成: {len(all_static_players)} 个唯一球员，{len(roster_assignments)} 个分配关系")
            print("⚠️  注意：这只包含了Taken球员，不包含FA/W状态的球员")
            return all_static_players
        else:
            print("✗ 球员静态数据保存失败")
            return False
    
    def fetch_all_status_players(self):
        """获取所有状态的球员静态数据 (A, FA, W, T, K)"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取所有状态球员数据: {league_key} ===")
        
        # 定义所有状态
        all_statuses = ["A", "FA", "W", "T", "K"]
        all_players_by_status = {}
        all_static_players = {}
        
        for status in all_statuses:
            print(f"获取状态 '{status}' 的球员...")
            status_players = self.fetch_players_by_status(league_key, status)
            
            if status_players:
                # 提取静态信息
                static_players = self.extract_static_from_league_players(status_players, status)
                all_players_by_status[status] = {
                    "count": len(static_players),
                    "players": static_players
                }
                
                # 合并到总的静态球员数据中
                for editorial_key, player_info in static_players.items():
                    if editorial_key not in all_static_players:
                        all_static_players[editorial_key] = player_info
                        all_static_players[editorial_key]["first_seen_status"] = status
                    
                    # 更新状态列表
                    if "statuses" not in all_static_players[editorial_key]:
                        all_static_players[editorial_key]["statuses"] = []
                    if status not in all_static_players[editorial_key]["statuses"]:
                        all_static_players[editorial_key]["statuses"].append(status)
                
                print(f"  ✓ 状态 '{status}': {len(static_players)} 个球员")
                self.wait()
            else:
                print(f"  ✗ 状态 '{status}': 获取失败或无球员")
                all_players_by_status[status] = {"count": 0, "players": {}}
        
        # 保存完整的静态球员数据
        complete_static_data = {
            "static_players": all_static_players,
            "players_by_status": all_players_by_status,
            "metadata": {
                "league_key": league_key,
                "total_unique_players": len(all_static_players),
                "statuses_fetched": all_statuses,
                "status_breakdown": {status: data["count"] for status, data in all_players_by_status.items()},
                "created_at": datetime.now().isoformat(),
                "description": "包含所有状态(A, FA, W, T, K)的完整球员静态信息"
            }
        }
        
        complete_static_file = self.league_dirs['players'] / "complete_static_players.json"
        if save_json_data(complete_static_data, complete_static_file):
            print(f"\n✓ 完整球员静态数据获取完成!")
            print(f"  总计: {len(all_static_players)} 个唯一球员")
            for status, data in all_players_by_status.items():
                print(f"  状态 {status}: {data['count']} 个球员")
            return all_static_players
        else:
            print("✗ 完整球员静态数据保存失败")
            return False
    
    def fetch_players_by_status(self, league_key, status):
        """获取指定状态的球员"""
        try:
            all_players = []
            start = 0
            count = 200
            max_attempts = 10
            attempts = 0
            
            while attempts < max_attempts:
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
                
                params = [f"status={status}"]
                if start > 0:
                    params.append(f"start={start}")
                if count != 25:
                    params.append(f"count={count}")
                
                url += ";" + ";".join(params) + "?format=json"
                
                players_data = get_api_data(url)
                if not players_data:
                    break
                
                page_players = self.extract_player_info_from_league_data(players_data)
                if not page_players:
                    break
                
                all_players.extend(page_players)
                print(f"    第 {attempts + 1} 页: 获取了 {len(page_players)} 个球员")
                
                if len(page_players) < count:
                    break
                
                start += count
                attempts += 1
                
                if attempts < max_attempts:
                    time.sleep(0.5)
            
            return all_players
            
        except Exception as e:
            print(f"    获取状态 {status} 的球员时出错: {str(e)}")
            return []
    
    def extract_player_info_from_league_data(self, players_data):
        """从联盟球员数据中提取球员信息"""
        players = []
        
        try:
            if "fantasy_content" not in players_data:
                return players
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            if isinstance(league_data, list) and len(league_data) > 1:
                players_container = league_data[1].get("players", {})
                
                for player_index, player_data in players_container.items():
                    if not player_index.isdigit():
                        continue
                    
                    if "player" in player_data:
                        player_basic_info = player_data["player"][0]
                        if isinstance(player_basic_info, list):
                            # 合并球员基本信息
                            merged_info = {}
                            for info_item in player_basic_info:
                                if isinstance(info_item, dict):
                                    merged_info.update(info_item)
                            
                            if merged_info:
                                players.append(merged_info)
        
        except Exception as e:
            print(f"    从联盟数据提取球员信息时出错: {e}")
        
        return players
    
    def extract_static_from_league_players(self, players_list, status):
        """从联盟球员列表中提取静态信息"""
        static_players = {}
        
        for player_info in players_list:
            # 使用现有的静态信息提取方法
            static_info = self.extract_static_player_info(player_info)
            
            if static_info and static_info.get("editorial_player_key"):
                editorial_key = static_info["editorial_player_key"]
                static_info["discovered_via_status"] = status
                static_players[editorial_key] = static_info
        
        return static_players
    
    def extract_team_key_from_roster(self, roster_data):
        """从roster数据中提取团队键"""
        try:
            return roster_data["fantasy_content"]["team"][0][0]["team_key"]
        except (KeyError, IndexError):
            return None
    
    def extract_players_and_assignments_from_roster(self, roster_data, team_key):
        """从roster数据中提取球员信息和分配关系"""
        static_players = []
        assignments = []
        
        try:
            team_data = roster_data["fantasy_content"]["team"]
            roster_date = team_data[1]["roster"]["date"]
            
            roster = team_data[1]["roster"]
            if "0" in roster and "players" in roster["0"]:
                players_container = roster["0"]["players"]
                
                for player_index in players_container:
                    if player_index.isdigit():
                        player_container = players_container[player_index]
                        if "player" in player_container:
                            player_data = player_container["player"]
                            
                            if isinstance(player_data, list) and len(player_data) > 0:
                                player_basic_info = player_data[0]
                                static_info = self.extract_static_player_info(player_basic_info)
                                
                                if static_info and static_info.get("editorial_player_key"):
                                    static_players.append(static_info)
                                
                                # 提取分配关系
                                player_key = None
                                editorial_key = static_info.get("editorial_player_key")
                                
                                if isinstance(player_basic_info, list):
                                    for info_item in player_basic_info:
                                        if isinstance(info_item, dict) and "player_key" in info_item:
                                            player_key = info_item["player_key"]
                                            break
                                
                                selected_position = None
                                if len(player_data) > 1 and isinstance(player_data[1], dict):
                                    selected_pos_data = player_data[1].get("selected_position")
                                    if isinstance(selected_pos_data, list) and len(selected_pos_data) > 1:
                                        if isinstance(selected_pos_data[1], dict):
                                            selected_position = selected_pos_data[1].get("position")
                                
                                if player_key and editorial_key:
                                    assignments.append({
                                        "team_key": team_key,
                                        "player_key": player_key,
                                        "editorial_player_key": editorial_key,
                                        "selected_position": selected_position,
                                        "roster_date": roster_date
                                    })
        
        except Exception as e:
            print(f"从roster数据提取信息时出错: {e}")
        
        return static_players, assignments
    
    def extract_static_player_info(self, player_data):
        """提取球员静态信息"""
        try:
            if isinstance(player_data, list):
                merged_info = {}
                for item in player_data:
                    if isinstance(item, dict):
                        merged_info.update(item)
                player_info = merged_info
            elif isinstance(player_data, dict):
                player_info = player_data.copy()
            else:
                return {}
            
            static_info = {}
            
            # 基本标识信息
            if "player_id" in player_info:
                static_info["player_id"] = player_info["player_id"]
            if "editorial_player_key" in player_info:
                static_info["editorial_player_key"] = player_info["editorial_player_key"]
            
            # 姓名信息
            name_info = player_info.get("name")
            if isinstance(name_info, dict):
                if "full" in name_info:
                    static_info["full_name"] = name_info["full"]
                if "first" in name_info:
                    static_info["first_name"] = name_info["first"]
                if "last" in name_info:
                    static_info["last_name"] = name_info["last"]
            
            return static_info
            
        except Exception as e:
            print(f"提取球员静态信息时出错: {e}")
            return {}
    
    def fetch_league_dynamic_players(self):
        """获取联盟的球员动态数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', 'unknown')
        
        print(f"\n=== 获取球员动态数据: {league_key} ===")
        
        total_dynamic_data = {}
        start = 0
        count = 200
        page = 1
        
        while True:
            print(f"获取第 {page} 页动态数据 (起始: {start})")
            players_data = self.fetch_league_players(league_key, status=None, start=start, count=count)
            
            if not players_data:
                break
            
            page_data = self.extract_dynamic_players_from_league_data(players_data, season, league_key)
            if not page_data:
                break
            
            total_dynamic_data.update(page_data)
            print(f"  获取了 {len(page_data)} 个球员的动态数据")
            
            if len(page_data) < count:
                break
            
            start += count
            page += 1
            self.wait("获取下一页数据")
        
        if total_dynamic_data:
            dynamic_data = {
                "dynamic_players": total_dynamic_data,
                "metadata": {
                    "league_key": league_key,
                    "season": season,
                    "total_players": len(total_dynamic_data),
                    "created_at": datetime.now().isoformat(),
                    "description": f"联盟 {league_key} 的球员动态信息"
                }
            }
            
            dynamic_players_file = self.league_dirs['players'] / "dynamic_players.json"
            if save_json_data(dynamic_data, dynamic_players_file):
                print(f"✓ 球员动态数据获取完成: {len(total_dynamic_data)} 个球员")
                return total_dynamic_data
        
        print("✗ 球员动态数据获取失败")
        return False
    
    def fetch_league_players(self, league_key, status=None, start=0, count=200):
        """获取联盟球员信息"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
        
        params = []
        if status:
            params.append(f"status={status}")
        if start > 0:
            params.append(f"start={start}")
        if count != 25:
            params.append(f"count={count}")
        
        if params:
            url += ";" + ";".join(params)
        
        url += "?format=json"
        
        return get_api_data(url)
    
    def extract_dynamic_players_from_league_data(self, players_data, season, league_key):
        """从联盟球员数据中提取动态信息"""
        dynamic_data = {}
        
        try:
            if "fantasy_content" not in players_data:
                return dynamic_data
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            if isinstance(league_data, list) and len(league_data) > 1:
                players_container = league_data[1].get("players", {})
                
                for player_index, player_data in players_container.items():
                    if not player_index.isdigit():
                        continue
                    
                    if "player" in player_data:
                        player_basic_info = player_data["player"][0]
                        if isinstance(player_basic_info, list):
                            player_key = None
                            for info_item in player_basic_info:
                                if isinstance(info_item, dict) and "player_key" in info_item:
                                    player_key = info_item["player_key"]
                                    break
                            
                            if player_key:
                                dynamic_info = self.extract_dynamic_player_info(
                                    player_basic_info, season, league_key
                                )
                                if dynamic_info:
                                    dynamic_data[player_key] = dynamic_info
        
        except Exception as e:
            print(f"从联盟数据提取动态球员信息时出错: {e}")
        
        return dynamic_data
    
    def extract_dynamic_player_info(self, player_data, season, league_key):
        """提取球员动态信息"""
        try:
            if isinstance(player_data, list):
                merged_info = {}
                for item in player_data:
                    if isinstance(item, dict):
                        merged_info.update(item)
                player_info = merged_info
            elif isinstance(player_data, dict):
                player_info = player_data.copy()
            else:
                return {}
            
            dynamic_info = {}
            
            # 团队信息
            if "editorial_team_key" in player_info:
                dynamic_info["current_team_key"] = player_info["editorial_team_key"]
            if "editorial_team_full_name" in player_info:
                dynamic_info["current_team_name"] = player_info["editorial_team_full_name"]
            if "editorial_team_abbr" in player_info:
                dynamic_info["current_team_abbr"] = player_info["editorial_team_abbr"]
            
            # 位置信息
            if "display_position" in player_info:
                dynamic_info["display_position"] = player_info["display_position"]
            if "primary_position" in player_info:
                dynamic_info["primary_position"] = player_info["primary_position"]
            if "position_type" in player_info:
                dynamic_info["position_type"] = player_info["position_type"]
            
            # 其他信息
            if "uniform_number" in player_info:
                dynamic_info["uniform_number"] = player_info["uniform_number"]
            if "status" in player_info:
                dynamic_info["status"] = player_info["status"]
            
            # 头像信息
            headshot_info = player_info.get("headshot")
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                dynamic_info["headshot_url"] = headshot_info["url"]
            
            # 是否不可丢弃
            is_undroppable = player_info.get("is_undroppable")
            if is_undroppable is not None:
                dynamic_info["is_undroppable"] = str(is_undroppable) == "1"
            
            # 位置资格
            eligible_positions = player_info.get("eligible_positions")
            if isinstance(eligible_positions, list):
                dynamic_info["eligible_positions"] = eligible_positions
            
            # Yahoo URL
            if "url" in player_info:
                dynamic_info["yahoo_url"] = player_info["url"]
            
            # 添加元数据
            dynamic_info["season"] = season
            dynamic_info["league_key"] = league_key
            dynamic_info["last_updated"] = datetime.now().isoformat()
            
            return dynamic_info
            
        except Exception as e:
            print(f"提取球员动态信息时出错: {e}")
            return {}
    
    def fetch_league_player_stats(self):
        """获取联盟的球员统计数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n=== 获取球员统计数据: {league_key} ===")
        
        # 先获取联盟中实际存在的球员列表
        print("获取联盟中的球员列表...")
        league_players = self.get_players_in_league(league_key)
        
        if not league_players:
            print("该联盟没有找到任何球员")
            return False
        
        print(f"找到 {len(league_players)} 个球员")
        
        # 获取统计数据
        stats_batches = self.fetch_player_stats(league_key, league_players)
        
        if not stats_batches:
            print("✗ 获取球员统计数据失败")
            return False
        
        # 整合统计数据
        all_stats = {}
        for batch_data in stats_batches:
            batch_stats = self.extract_player_stats_from_data(batch_data)
            all_stats.update(batch_stats)
        
        if all_stats:
            stats_data = {
                "player_stats": all_stats,
                "metadata": {
                    "league_key": league_key,
                    "total_players": len(all_stats),
                    "created_at": datetime.now().isoformat(),
                    "description": f"联盟 {league_key} 的球员统计数据"
                }
            }
            
            stats_file = self.league_dirs['players'] / "player_stats.json"
            if save_json_data(stats_data, stats_file):
                print(f"✓ 球员统计数据获取完成: {len(all_stats)} 个球员")
                return all_stats
        
        print("✗ 球员统计数据保存失败")
        return False
    
    def get_players_in_league(self, league_key):
        """获取联盟中实际存在的球员列表"""
        league_players = []
        
        try:
            start = 0
            count = 200
            max_attempts = 20
            attempts = 0
            
            while attempts < max_attempts:
                players_data = self.fetch_league_players(league_key, status=None, start=start, count=count)
                
                if not players_data:
                    break
                
                page_players = self.extract_player_keys_from_league_data(players_data)
                
                if not page_players:
                    break
                
                league_players.extend(page_players)
                print(f"    第 {attempts + 1} 页: 获取了 {len(page_players)} 个球员")
                
                if len(page_players) < count:
                    break
                
                start += count
                attempts += 1
                
                if attempts < max_attempts:
                    time.sleep(0.5)
        
        except Exception as e:
            print(f"    获取联盟 {league_key} 的球员列表时出错: {str(e)}")
        
        return league_players
    
    def extract_player_keys_from_league_data(self, players_data):
        """从联盟球员数据中提取球员键"""
        player_keys = []
        
        try:
            if "fantasy_content" not in players_data:
                return player_keys
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            if isinstance(league_data, list) and len(league_data) > 1:
                players_container = league_data[1].get("players", {})
                
                for player_index, player_data in players_container.items():
                    if not player_index.isdigit():
                        continue
                    
                    if "player" in player_data:
                        player_basic_info = player_data["player"][0]
                        if isinstance(player_basic_info, list):
                            for info_item in player_basic_info:
                                if isinstance(info_item, dict) and "player_key" in info_item:
                                    player_key = info_item["player_key"]
                                    if player_key:
                                        player_keys.append(player_key)
                                    break
        
        except Exception as e:
            print(f"    从联盟数据提取球员键时出错: {e}")
        
        return player_keys
    
    def fetch_player_stats(self, league_key, player_keys, stats_type="season"):
        """获取指定球员的统计数据"""
        if not player_keys:
            return None
        
        batch_size = 25
        all_stats_data = []
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats"
            
            if stats_type != "season":
                url += f";type={stats_type}"
            
            url += "?format=json"
            
            print(f"    获取球员统计数据 ({i+1}-{min(i+batch_size, len(player_keys))}/{len(player_keys)}): {len(batch_keys)} 个球员")
            
            data = get_api_data(url)
            if data:
                all_stats_data.append(data)
                print(f"    ✓ 成功获取批次统计数据")
            else:
                print(f"    ✗ 获取批次统计数据失败")
        
        return all_stats_data
    
    def extract_player_stats_from_data(self, stats_data):
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
                            player_stats[player_key] = self.normalize_player_stats(stats)
        
        except Exception as e:
            print(f"提取球员统计数据时出错: {e}")
        
        return player_stats
    
    def normalize_player_stats(self, stats_list):
        """标准化球员统计数据"""
        normalized_stats = {}
        
        for stat_item in stats_list:
            if "stat" in stat_item:
                stat_info = stat_item["stat"]
                stat_id = stat_info.get("stat_id")
                if stat_id:
                    normalized_stats[stat_id] = {
                        "value": stat_info.get("value")
                    }
        
        return normalized_stats
    
    # ===== 完整流程 =====
    
    def fetch_complete_league_data(self):
        """执行完整的单联盟数据获取流程"""
        print("🚀 开始Yahoo Fantasy单联盟完整数据获取...")
        
        # 步骤1: 获取基础数据和选择联盟
        print("\n📋 步骤1: 获取基础数据和联盟选择")
        leagues_data = self.fetch_all_leagues_data()
        if not leagues_data:
            print("✗ 获取基础数据失败")
            return False
        
        if not self.select_league():
            print("✗ 联盟选择失败")
            return False
        
        # 步骤2: 获取联盟详细信息
        print("\n📋 步骤2: 获取联盟详细信息")
        if not self.fetch_league_details():
            print("✗ 获取联盟详细信息失败")
            return False
        
        # 步骤3: 获取团队和rosters数据
        print("\n📋 步骤3: 获取团队和rosters数据")
        teams_data = self.fetch_teams_data()
        if not teams_data:
            print("✗ 获取团队数据失败")
            return False
        
        if not self.fetch_team_rosters(teams_data):
            print("✗ 获取team rosters失败")
            return False
        
        # 步骤4: 提取球员静态数据
        print("\n📋 步骤4: 提取球员静态数据")
        static_players = self.extract_players_from_rosters()
        if not static_players:
            print("✗ 提取球员静态数据失败")
            return False
        
        # 步骤4.5: 获取所有状态的完整球员数据
        print("\n📋 步骤4.5: 获取所有状态球员数据 (A, FA, W, T, K)")
        complete_static_players = self.fetch_all_status_players()
        if not complete_static_players:
            print("⚠️ 获取完整球员状态数据失败，继续使用roster数据")
            complete_static_players = static_players
        
        # 步骤5: 获取球员动态数据
        print("\n📋 步骤5: 获取球员动态数据")
        dynamic_players = self.fetch_league_dynamic_players()
        
        # 步骤6: 获取球员统计数据
        print("\n📋 步骤6: 获取球员统计数据")
        player_stats = self.fetch_league_player_stats()
        
        # 总结
        print(f"\n🎯 === 数据获取完成总结 ===")
        print(f"联盟: {self.selected_league['name']} ({self.selected_league['league_key']})")
        print(f"Roster球员数据: {len(static_players) if static_players else 0} 个")
        print(f"完整球员数据: {len(complete_static_players) if complete_static_players else 0} 个")
        print(f"动态球员数据: {len(dynamic_players) if dynamic_players else 0} 个")
        print(f"统计数据: {len(player_stats) if player_stats else 0} 个")
        
        # 显示数据概览
        print_data_overview(self.selected_league['league_key'])
        
        print("🎉 单联盟数据获取成功！")
        return True

def main():
    """主函数，处理命令行参数并执行相应操作"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy单联盟数据获取工具")
    
    # 主要功能选项
    parser.add_argument("--complete", action="store_true", help="执行完整的单联盟数据获取流程")
    parser.add_argument("--basic", action="store_true", help="只获取基础数据和联盟选择")
    parser.add_argument("--select", action="store_true", help="选择联盟（需要先有基础数据）")
    
    # 工具选项
    parser.add_argument("--overview", action="store_true", help="显示数据概览")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    
    args = parser.parse_args()
    
    # 创建数据获取器
    fetcher = SingleLeagueFantasyDataFetcher(delay=args.delay)
    
    # 处理命令行参数
    if args.complete:
        # 执行完整的数据获取流程
        fetcher.fetch_complete_league_data()
    elif args.basic:
        # 只获取基础数据
        fetcher.fetch_all_leagues_data()
    elif args.select:
        # 选择联盟
        fetcher.select_league()
    elif args.overview:
        # 显示数据概览
        print_data_overview()
    else:
        # 默认交互式模式
        print("Yahoo Fantasy单联盟数据获取工具")
        print("1. 执行完整的数据获取流程")
        print("2. 获取基础数据和联盟列表")
        print("3. 选择特定联盟")
        print("4. 显示数据概览")
        
        choice = input("\n请选择操作 (1-4): ").strip()
        
        if choice == "1":
            fetcher.fetch_complete_league_data()
        elif choice == "2":
            fetcher.fetch_all_leagues_data()
        elif choice == "3":
            fetcher.select_league()
        elif choice == "4":
            print_data_overview()
        else:
            print("无效选择，执行完整数据获取流程...")
            fetcher.fetch_complete_league_data()
    
    print("数据获取完成!")


if __name__ == "__main__":
    main() 