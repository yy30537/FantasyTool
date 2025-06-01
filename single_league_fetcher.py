#!/usr/bin/env python3
"""
Yahoo Fantasy数据获取工具 - 单联盟深度模式（优化版）
专注于获取单个联盟的完整深度数据，支持关系数据库设计
"""
import os
import sys
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import (
    get_api_data, save_json_data, load_json_data, 
    GAMES_DIR, LEAGUES_DIR,
    create_league_directory, get_league_directory,
    select_league_interactively, print_data_overview
)

class SingleLeagueDataFetcher:
    """优化的Yahoo Fantasy单联盟数据获取器"""
    
    def __init__(self, delay: int = 2):
        """初始化数据获取器"""
        self.delay = delay
        self.selected_league: Optional[Dict] = None
        self.league_dirs: Optional[Dict] = None
        
    def wait(self, message: Optional[str] = None) -> None:
        """等待指定时间"""
        if message:
            print(f"{message}，等待 {self.delay} 秒...")
        else:
            print(f"等待 {self.delay} 秒...")
        time.sleep(self.delay)
    
    # ===== 基础数据获取和联盟选择 =====
    
    def fetch_and_select_league(self) -> bool:
        """获取基础数据并选择联盟"""
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
        self.league_dirs = create_league_directory(selected_league['league_key'])
        
        print(f"✓ 联盟选择完成: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
    def _fetch_all_leagues_data(self) -> bool:
        """内部方法：获取所有联盟数据"""
        # 获取games数据
        games_data = self._fetch_games_data()
        if not games_data:
            return False
        
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
        """获取完整的联盟数据"""
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
        """获取联盟详细信息"""
        league_key = self.selected_league['league_key']
        
        # 获取联盟各类详细数据
        settings_data = self._fetch_league_settings(league_key)
        standings_data = self._fetch_league_standings(league_key)
        scoreboard_data = self._fetch_league_scoreboard(league_key)
        
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
        
        league_info_file = self.league_dirs['base'] / "league_info.json"
        return save_json_data(league_info, league_info_file)
    
    def _fetch_league_settings(self, league_key: str) -> Optional[Dict]:
        """获取联盟设置"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return get_api_data(url)
    
    def _fetch_league_standings(self, league_key: str) -> Optional[Dict]:
        """获取联盟排名"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        return get_api_data(url)
    
    def _fetch_league_scoreboard(self, league_key: str) -> Optional[Dict]:
        """获取联盟记分板"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard?format=json"
        return get_api_data(url)
    
    def fetch_teams_data(self) -> Optional[Dict]:
        """获取团队数据"""
        league_key = self.selected_league['league_key']
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        teams_data = get_api_data(url)
        
        if teams_data:
            teams_file = self.league_dirs['base'] / "teams.json"
            save_json_data(teams_data, teams_file)
            print("✓ 团队数据获取完成")
            return teams_data
        
        return None
    
    def fetch_team_rosters(self, teams_data: Dict) -> bool:
        """获取所有团队的roster数据"""
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            return False
        
        print(f"获取 {len(team_keys)} 个团队的rosters...")
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            print(f"  获取团队roster {i+1}/{len(team_keys)}: {team_key}")
            
            roster_data = self._fetch_team_roster(team_key)
            if roster_data:
                roster_file = self.league_dirs['rosters'] / f"team_roster_{team_key}.json"
                if save_json_data(roster_data, roster_file):
                    success_count += 1
            
            if i < len(team_keys) - 1:
                self.wait()
        
        print(f"✓ Team rosters获取完成: {success_count}/{len(team_keys)}")
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
    
    # ===== 完整球员数据获取（优化版） =====
    
    def fetch_complete_players_data(self) -> bool:
        """获取完整的球员数据（静态、动态、统计）"""
        league_key = self.selected_league['league_key']
        
        print("获取联盟完整球员数据...")
        
        # 1. 获取所有球员的基础信息
        all_players = self._fetch_all_league_players(league_key)
        if not all_players:
            print("✗ 获取球员基础信息失败")
            return False
    
        print(f"✓ 获取了 {len(all_players)} 个球员的基础信息")
        
        # 2. 处理静态和动态数据
        static_players, dynamic_players = self._process_players_data(all_players)
        
        # 3. 获取统计数据（简化版）
        player_stats = self._fetch_player_stats(all_players)
        
        # 4. 保存数据
        self._save_players_data(static_players, dynamic_players, player_stats)
        
        print(f"✓ 完整球员数据获取完成:")
        print(f"  静态数据: {len(static_players)} 个球员")
        print(f"  动态数据: {len(dynamic_players)} 个球员")
        print(f"  统计数据: {len(player_stats)} 个球员")
        
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
                # 验证是否真的结束
                next_start = start + page_size
                test_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;start={next_start}?format=json"
                test_data = get_api_data(test_url)
                
                if test_data:
                    test_players = self._extract_player_info_from_league_data(test_data)
                    if not test_players:
                        break
                else:
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
                            players.append(merged_info)
                    elif isinstance(player_basic_info, dict):
                        players.append(player_basic_info)
        
        except Exception as e:
            print(f"    从联盟数据提取球员信息时出错: {e}")
        
        return players
    
    def _process_players_data(self, all_players: List[Dict]) -> tuple[Dict[str, Dict], Dict[str, Dict]]:
        """处理球员数据，分离静态和动态信息"""
        static_players = {}
        dynamic_players = {}
        
        for player_info in all_players:
            editorial_key = player_info.get("editorial_player_key")
            if not editorial_key:
                continue
            
            # 提取静态信息
            static_info = self._extract_static_player_info(player_info)
            if static_info:
                static_players[editorial_key] = static_info
        
            # 提取动态信息
            dynamic_info = self._extract_dynamic_player_info(player_info)
            if dynamic_info:
                dynamic_players[editorial_key] = dynamic_info
        
        return static_players, dynamic_players
    
    def _extract_static_player_info(self, player_data: Dict) -> Dict:
        """提取球员静态信息"""
        try:
            static_info = {}
            
            # 基本标识信息
            for key in ["player_id", "editorial_player_key", "player_key"]:
                if key in player_data:
                    static_info[key] = player_data[key]
            
            # 姓名信息
            name_info = player_data.get("name", {})
            if isinstance(name_info, dict):
                for name_key in ["full", "first", "last"]:
                    if name_key in name_info:
                        static_info[f"{name_key}_name"] = name_info[name_key]
            
            return static_info
        except Exception as e:
            print(f"提取球员静态信息时出错: {e}")
            return {}
    
    def _extract_dynamic_player_info(self, player_data: Dict) -> Dict:
        """提取球员动态信息"""
        try:
            dynamic_info = {}
            
            # 基本标识信息（与静态数据保持一致性）
            for key in ["editorial_player_key", "player_key", "player_id"]:
                if key in player_data:
                    dynamic_info[key] = player_data[key]
            
            # 姓名信息
            name_info = player_data.get("name", {})
            if isinstance(name_info, dict) and "full" in name_info:
                dynamic_info["full_name"] = name_info["full"]
            
            # 团队信息
            team_fields = {
                "editorial_team_key": "current_team_key",
                "editorial_team_full_name": "current_team_name", 
                "editorial_team_abbr": "current_team_abbr"
            }
            for source_key, target_key in team_fields.items():
                if source_key in player_data:
                    dynamic_info[target_key] = player_data[source_key]
            
            # 位置信息
            position_fields = ["display_position", "primary_position", "position_type"]
            for field in position_fields:
                if field in player_data:
                    dynamic_info[field] = player_data[field]
            
            # 其他动态信息
            other_fields = ["uniform_number", "status", "image_url"]
            for field in other_fields:
                if field in player_data:
                    dynamic_info[field] = player_data[field]
            
            # 头像信息
            if "headshot" in player_data:
                headshot_info = player_data["headshot"]
                if isinstance(headshot_info, dict) and "url" in headshot_info:
                    dynamic_info["headshot_url"] = headshot_info["url"]
            
            # 是否不可丢弃
            if "is_undroppable" in player_data:
                dynamic_info["is_undroppable"] = str(player_data["is_undroppable"]) == "1"
            
            # 位置资格
            if "eligible_positions" in player_data:
                dynamic_info["eligible_positions"] = player_data["eligible_positions"]
            
            # 添加元数据
            dynamic_info.update({
                "season": self.selected_league.get('season', 'unknown'),
                "league_key": self.selected_league['league_key'],
                "last_updated": datetime.now().isoformat()
            })
            
            return dynamic_info
        except Exception as e:
            print(f"提取球员动态信息时出错: {e}")
            return {}
    
    def _fetch_player_stats(self, all_players: List[Dict]) -> Dict[str, Dict]:
        """获取球员统计数据（简化版，不合并stat_categories）"""
        # 提取player_keys
        player_keys = []
        for player_info in all_players:
            player_key = player_info.get("player_key")
            if player_key:
                player_keys.append(player_key)
        
        if not player_keys:
            print("✗ 没有找到有效的player_keys")
            return {}
        
        print(f"获取 {len(player_keys)} 个球员的统计数据...")
        
        # 获取原始统计数据
        player_stats = self._fetch_player_stats_batches(player_keys)
        
        return player_stats
    
    def _fetch_player_stats_batches(self, player_keys: List[str]) -> Dict[str, Dict]:
        """批量获取球员统计数据"""
        league_key = self.selected_league['league_key']
        batch_size = 25
        all_stats = {}
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats?format=json"
            
            print(f"    获取统计数据批次 ({i+1}-{min(i+batch_size, len(player_keys))}/{len(player_keys)})")
            
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
                            player_stats[player_key] = self._normalize_player_stats(stats)
        
        except Exception as e:
            print(f"提取球员统计数据时出错: {e}")
        
        return player_stats
    
    def _normalize_player_stats(self, stats_list: List[Dict]) -> Dict[str, Dict]:
        """标准化球员统计数据（简化版，只保留stat_id和value）"""
        normalized_stats = {}
        
        for stat_item in stats_list:
            if "stat" in stat_item:
                stat_info = stat_item["stat"]
                stat_id = str(stat_info.get("stat_id"))
                if stat_id:
                    normalized_stats[stat_id] = {
                        "value": stat_info.get("value")
                    }
        
        return normalized_stats
    
    def _save_players_data(self, static_players: Dict, dynamic_players: Dict, player_stats: Dict) -> None:
        """保存球员数据（优化版）"""
        league_key = self.selected_league['league_key']
        
        # 保存静态数据
        static_data = {
            "static_players": static_players,
            "metadata": {
                "league_key": league_key,
                "total_players": len(static_players),
                "created_at": datetime.now().isoformat(),
                "description": "球员静态信息（基本标识和姓名）"
            }
        }
        save_json_data(static_data, self.league_dirs['players'] / "static_players.json")
        
        # 保存动态数据
        dynamic_data = {
            "dynamic_players": dynamic_players,
            "metadata": {
                "league_key": league_key,
                "total_players": len(dynamic_players),
                "created_at": datetime.now().isoformat(),
                "description": "球员动态信息（团队、位置、状态等）"
            }
        }
        save_json_data(dynamic_data, self.league_dirs['players'] / "dynamic_players.json")
        
        # 保存简化的统计数据（不包含stat_categories冗余信息）
        stats_data = {
            "player_stats": player_stats,
            "metadata": {
                "league_key": league_key,
                "total_players": len(player_stats),
                "created_at": datetime.now().isoformat(),
                "description": "球员统计数据（仅包含stat_id和value，stat_categories信息请参考league_info.json）"
            }
        }
        save_json_data(stats_data, self.league_dirs['players'] / "player_stats.json")
    
    # ===== Transaction数据获取 =====
    
    def fetch_complete_transactions_data(self, teams_data: Optional[Dict] = None) -> bool:
        """获取完整的transaction数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"获取联盟transaction数据: {league_key}")
        
        # 获取所有transactions（增加数量限制）
        print("获取联盟所有transactions...")
        all_transactions = self._fetch_all_league_transactions(league_key)
        
        if all_transactions:
            # 保存完整的transactions数据
            transactions_summary = {
                "all_transactions": all_transactions,
                "metadata": {
                    "league_key": league_key,
                    "total_transactions": len(all_transactions),
                    "created_at": datetime.now().isoformat(),
                    "description": "联盟所有transaction数据（完整版）"
                }
            }
            save_json_data(transactions_summary, self.league_dirs['transactions'] / "all_transactions.json")
            print(f"✓ Transaction数据获取完成: {len(all_transactions)} 个")
        else:
            print("✗ 未获取到transaction数据")
            return False
        
        return True
    
    def _fetch_all_league_transactions(self, league_key: str, max_count: int = None) -> List[Dict]:
        """获取联盟所有transactions（分页处理，完整版）"""
        all_transactions = []
        start = 0
        page_size = 25
        max_iterations = 200  # 增加最大迭代次数，确保获取完整数据
        iteration = 0
        
        print(f"开始分页获取transaction数据 (每页 {page_size} 个)")
        
        while iteration < max_iterations:
            iteration += 1
            
            # 构建URL
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions"
            params = []
            if start > 0:
                params.append(f"start={start}")
            params.append(f"count={page_size}")  # 移除max_count限制
            
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
            
            # 如果返回的数量少于页大小，说明没有更多数据
            if len(batch_transactions) < page_size:
                print(f"    ✓ 数据获取完成：最后一批只有 {len(batch_transactions)} 个transaction")
                break
            
            start += page_size
            time.sleep(0.5)
        
        if iteration >= max_iterations:
            print(f"⚠️ 达到最大迭代次数限制 ({max_iterations})，可能还有更多数据")
        
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
                
                # 处理transaction数据结构
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
    
    # ===== 主要流程 =====
    
    def run_complete_data_fetch(self) -> bool:
        """执行完整的数据获取流程"""
        print("🚀 开始Yahoo Fantasy单联盟完整数据获取（优化版）...")
        
        # 1. 基础数据获取和联盟选择
        if not self.fetch_and_select_league():
            print("✗ 基础数据获取或联盟选择失败")
            return False
        
        # 2. 获取完整联盟数据
        if not self.fetch_complete_league_data():
            print("✗ 联盟数据获取失败")
            return False
        
        # 3. 显示数据概览
        print_data_overview(self.selected_league['league_key'])
        
        print("🎉 单联盟数据获取成功！")
        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy单联盟数据获取工具（优化版）")
    
    parser.add_argument("--complete", action="store_true", help="执行完整的数据获取流程")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    
    args = parser.parse_args()
    
    # 创建优化的数据获取器
    fetcher = SingleLeagueDataFetcher(delay=args.delay)
    
    if args.complete:
        fetcher.run_complete_data_fetch()
    else:
        # 默认执行完整流程
        fetcher.run_complete_data_fetch()


if __name__ == "__main__":
    main() 