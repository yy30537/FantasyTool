#!/usr/bin/env python3
"""
Yahoo Fantasy API 样本数据获取脚本
获取各种类型的API数据并保存为JSON文件，便于调试和分析
"""
import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yahoo_api_utils import (
    get_api_data,
    select_league_interactively
)

class YahooFantasySampleFetcher:
    """Yahoo Fantasy API样本数据获取器"""
    
    def __init__(self):
        """初始化样本数据获取器"""
        # 设置样本数据目录
        self.base_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.sample_dir = self.base_dir / "sample_data"
        self.sample_dir.mkdir(exist_ok=True)
        
        print(f"📁 样本数据目录: {self.sample_dir}")
        
        self.selected_league = None
    
    def save_json_data(self, data: Dict, filename: str, description: str = "") -> bool:
        """保存数据到JSON文件"""
        if not data:
            print(f"⚠️ 跳过空数据: {filename}")
            return False
        
        try:
            filepath = self.sample_dir / f"{filename}.json"
            
            # 添加元数据
            output_data = {
                "metadata": {
                    "fetched_at": datetime.now().isoformat(),
                    "description": description,
                    "filename": filename
                },
                "data": data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"✅ 已保存: {filepath} ({description})")
            return True
            
        except Exception as e:
            print(f"❌ 保存失败 {filename}: {e}")
            return False
    
    # ===== 基础数据获取 =====
    
    def fetch_games_sample(self) -> bool:
        """获取用户游戏数据样本"""
        print("\n🎮 获取用户游戏数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            "user_games", 
            "用户参与的所有Fantasy游戏"
        )
    
    def fetch_leagues_sample(self, game_key: str = None) -> bool:
        """获取联盟数据样本"""
        if not game_key:
            # 如果没有指定game_key，获取所有game的leagues
            print("\n🏆 获取所有游戏的联盟数据...")
            
            # 先获取games数据找到game_keys
            games_data = get_api_data("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json")
            if not games_data:
                return False
            
            game_keys = self._extract_game_keys_from_games_data(games_data)
            success_count = 0
            
            for i, gk in enumerate(game_keys[:2]):  # 限制获取前2个游戏以节省时间
                print(f"获取游戏 {i+1}/{min(2, len(game_keys))} 的联盟数据: {gk}")
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={gk}/leagues?format=json"
                data = get_api_data(url)
                
                if self.save_json_data(
                    data, 
                    f"user_leagues_{gk}", 
                    f"游戏 {gk} 的用户联盟数据"
                ):
                    success_count += 1
            
            return success_count > 0
        else:
            print(f"\n🏆 获取游戏 {game_key} 的联盟数据...")
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
            data = get_api_data(url)
            
            return self.save_json_data(
                data, 
                f"user_leagues_{game_key}", 
                f"游戏 {game_key} 的用户联盟数据"
            )
    
    def _extract_game_keys_from_games_data(self, games_data: Dict) -> List[str]:
        """从游戏数据中提取游戏键"""
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
    
    # ===== 联盟相关数据 =====
    
    def fetch_league_info_sample(self, league_key: str) -> bool:
        """获取联盟基本信息样本"""
        print(f"\n📊 获取联盟基本信息: {league_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_info_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 基本信息"
        )
    
    def fetch_league_settings_sample(self, league_key: str) -> bool:
        """获取联盟设置样本"""
        print(f"\n⚙️ 获取联盟设置: {league_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_settings_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 设置信息"
        )
    
    def fetch_league_standings_sample(self, league_key: str) -> bool:
        """获取联盟排名样本"""
        print(f"\n🏆 获取联盟排名: {league_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_standings_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 排名信息"
        )
    
    # ===== 团队相关数据 =====
    
    def fetch_teams_sample(self, league_key: str) -> bool:
        """获取团队数据样本"""
        print(f"\n👥 获取团队数据: {league_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_teams_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 团队数据"
        )
    
    def fetch_team_roster_sample(self, team_key: str) -> bool:
        """获取单个团队阵容样本"""
        print(f"\n📋 获取团队阵容: {team_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"team_roster_{team_key.replace('.', '_')}", 
            f"团队 {team_key} 阵容数据"
        )
    
    def fetch_team_matchups_sample(self, team_key: str) -> bool:
        """获取团队对战数据样本"""
        print(f"\n⚔️ 获取团队对战数据: {team_key}")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"team_matchups_{team_key.replace('.', '_')}", 
            f"团队 {team_key} 对战数据"
        )
    
    # ===== 球员相关数据 =====
    
    def fetch_players_sample(self, league_key: str, count: int = 25) -> bool:
        """获取球员基本数据样本"""
        print(f"\n🏃 获取球员基本数据: {league_key} (前{count}个)")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
        if count != 25:
            url += f";count={count}"
        url += "?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_players_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 球员基本数据 (前{count}个)"
        )
    
    def fetch_player_season_stats_sample(self, league_key: str, player_keys: List[str] = None, count: int = 5) -> bool:
        """获取球员赛季统计数据样本"""
        print(f"\n📊 获取球员赛季统计数据: {league_key}")
        
        # 如果没有提供player_keys，先获取一些球员
        if not player_keys:
            player_keys = self._get_sample_player_keys(league_key, count)
        
        if not player_keys:
            print("⚠️ 未找到球员键，跳过球员统计数据获取")
            return False
        
        # 限制获取的球员数量
        player_keys = player_keys[:count]
        player_keys_str = ",".join(player_keys)
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"player_season_stats_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 球员赛季统计数据 ({len(player_keys)}个球员)"
        )
    
    def fetch_player_weekly_stats_sample(self, league_key: str, player_keys: List[str] = None, week: int = None, count: int = 3) -> bool:
        """获取球员周统计数据样本（适用于NFL）"""
        print(f"\n📈 获取球员周统计数据: {league_key}")
        
        # 检查是否为NFL联盟
        if not self._is_nfl_league(league_key):
            print("⚠️ 周统计数据仅适用于NFL联盟，跳过")
            return False
        
        # 如果没有提供player_keys，先获取一些球员
        if not player_keys:
            player_keys = self._get_sample_player_keys(league_key, count)
        
        if not player_keys:
            print("⚠️ 未找到球员键，跳过球员周统计数据获取")
            return False
        
        # 限制获取的球员数量
        player_keys = player_keys[:count]
        player_keys_str = ",".join(player_keys)
        
        # 如果没有指定周数，使用当前周或默认周
        if week is None:
            week = self._get_current_week(league_key) or 1
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=week;week={week}?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"player_weekly_stats_w{week}_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 球员第{week}周统计数据 ({len(player_keys)}个球员)"
        )
    
    def fetch_player_daily_stats_sample(self, league_key: str, player_keys: List[str] = None, date: str = None, count: int = 3) -> bool:
        """获取球员日期统计数据样本（适用于MLB, NBA, NHL）"""
        print(f"\n📅 获取球员日期统计数据: {league_key}")
        
        # 检查是否为支持日期统计的联盟
        if self._is_nfl_league(league_key):
            print("⚠️ 日期统计数据不适用于NFL联盟，跳过")
            return False
        
        # 如果没有提供player_keys，先获取一些球员
        if not player_keys:
            player_keys = self._get_sample_player_keys(league_key, count)
        
        if not player_keys:
            print("⚠️ 未找到球员键，跳过球员日期统计数据获取")
            return False
        
        # 限制获取的球员数量
        player_keys = player_keys[:count]
        player_keys_str = ",".join(player_keys)
        
        # 如果没有指定日期，使用默认日期
        if date is None:
            date = self._get_sample_date(league_key)
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date}?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"player_daily_stats_{date.replace('-', '')}_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 球员{date}日期统计数据 ({len(player_keys)}个球员)"
        )
    
    def fetch_all_player_stats_samples(self, league_key: str, count: int = 3) -> bool:
        """获取所有类型的球员统计数据样本"""
        print(f"\n🎯 获取所有球员统计数据样本: {league_key}")
        
        # 先获取球员键
        player_keys = self._get_sample_player_keys(league_key, count)
        if not player_keys:
            print("⚠️ 未找到球员键，无法获取统计数据")
            return False
        
        success_count = 0
        
        # 1. 赛季统计数据
        if self.fetch_player_season_stats_sample(league_key, player_keys, count):
            success_count += 1
        
        # 2. 周统计数据（仅NFL）
        if self._is_nfl_league(league_key):
            if self.fetch_player_weekly_stats_sample(league_key, player_keys, count=count):
                success_count += 1
        else:
            # 3. 日期统计数据（非NFL）
            if self.fetch_player_daily_stats_sample(league_key, player_keys, count=count):
                success_count += 1
        
        return success_count > 0
    
    def _get_sample_player_keys(self, league_key: str, count: int = 5) -> List[str]:
        """获取样本球员键"""
        try:
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;count={count}?format=json"
            data = get_api_data(url)
            
            if not data:
                return []
            
            player_keys = []
            fantasy_content = data.get("fantasy_content", {})
            league_data = fantasy_content.get("league", {})
            
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            elif isinstance(league_data, dict) and "players" in league_data:
                players_container = league_data["players"]
            
            if not players_container:
                return []
            
            players_count = int(players_container.get("count", 0))
            for i in range(players_count):
                player_index = str(i)
                if player_index in players_container:
                    player_data = players_container[player_index]
                    if "player" in player_data:
                        player_info = player_data["player"]
                        if isinstance(player_info, list) and len(player_info) > 0:
                            player_basic = player_info[0]
                            if isinstance(player_basic, list):
                                for item in player_basic:
                                    if isinstance(item, dict) and "player_key" in item:
                                        player_keys.append(item["player_key"])
                                        break
                            elif isinstance(player_basic, dict) and "player_key" in player_basic:
                                player_keys.append(player_basic["player_key"])
            
            return player_keys
            
        except Exception as e:
            print(f"获取球员键时出错: {e}")
            return []
    
    def _is_nfl_league(self, league_key: str) -> bool:
        """检查是否为NFL联盟"""
        return "nfl" in league_key.lower() or league_key.startswith("414.") or league_key.startswith("423.")
    
    def _get_current_week(self, league_key: str) -> Optional[int]:
        """获取联盟当前周（仅用于示例）"""
        # 这里简化处理，返回一个示例周数
        return 1
    
    def _get_sample_date(self, league_key: str) -> str:
        """获取样本日期（仅用于示例）"""
        # 这里简化处理，返回一个示例日期
        if "mlb" in league_key.lower() or league_key.startswith("410."):
            return "2024-06-01"  # MLB赛季日期
        elif "nba" in league_key.lower() or league_key.startswith("418."):
            return "2024-03-01"  # NBA赛季日期
        elif "nhl" in league_key.lower() or league_key.startswith("419."):
            return "2024-03-01"  # NHL赛季日期
        else:
            return "2024-06-01"  # 默认日期
    
    # ===== 交易数据 =====
    
    def fetch_transactions_sample(self, league_key: str, count: int = 25) -> bool:
        """获取交易数据样本"""
        print(f"\n💰 获取交易数据: {league_key} (前{count}个)")
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions"
        if count != 25:
            url += f";count={count}"
        url += "?format=json"
        data = get_api_data(url)
        
        return self.save_json_data(
            data, 
            f"league_transactions_{league_key.replace('.', '_')}", 
            f"联盟 {league_key} 交易数据 (前{count}个)"
        )
    
    # ===== 主要流程 =====
    
    def select_league_for_samples(self) -> bool:
        """选择联盟用于获取样本数据"""
        print("🔍 获取用户联盟列表...")
        
        # 获取games数据
        games_data = get_api_data("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json")
        if not games_data:
            print("❌ 无法获取游戏数据")
            return False
        
        # 提取游戏键
        game_keys = self._extract_game_keys_from_games_data(games_data)
        if not game_keys:
            print("❌ 未找到有效的游戏")
            return False
        
        # 获取所有联盟数据
        all_leagues = {}
        for game_key in game_keys:
            leagues_data = get_api_data(f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json")
            if leagues_data:
                extracted_leagues = self._extract_leagues_from_data(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
        
        if not all_leagues:
            print("❌ 未找到任何联盟")
            return False
        
        # 交互式选择联盟
        selected_league = select_league_interactively(all_leagues)
        if not selected_league:
            print("❌ 未选择联盟")
            return False
        
        self.selected_league = selected_league
        print(f"✅ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
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
                        
                        league_info["game_key"] = game_key
                        leagues.append(league_info)
                break
        
        except Exception as e:
            print(f"提取联盟数据时出错: {e}")
        
        return leagues
    
    def fetch_basic_samples(self) -> bool:
        """获取基础样本数据（无需选择联盟）"""
        print("\n🚀 开始获取基础样本数据...")
        success_count = 0
        
        # 1. 获取用户游戏数据
        if self.fetch_games_sample():
            success_count += 1
        
        # 2. 获取用户联盟数据
        if self.fetch_leagues_sample():
            success_count += 1
        
        print(f"\n✅ 基础样本数据获取完成: {success_count}/2 成功")
        return success_count > 0
    
    def fetch_league_samples(self) -> bool:
        """获取特定联盟的样本数据"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n🚀 开始获取联盟样本数据: {league_key}")
        
        success_count = 0
        total_tasks = 8  # 增加任务数量
        
        # 1. 联盟基本信息
        if self.fetch_league_info_sample(league_key):
            success_count += 1
        
        # 2. 联盟设置
        if self.fetch_league_settings_sample(league_key):
            success_count += 1
        
        # 3. 联盟排名
        if self.fetch_league_standings_sample(league_key):
            success_count += 1
        
        # 4. 团队数据
        if self.fetch_teams_sample(league_key):
            success_count += 1
        
        # 5. 球员基本数据
        if self.fetch_players_sample(league_key, count=50):
            success_count += 1
        
        # 6. 球员统计数据
        if self.fetch_all_player_stats_samples(league_key, count=5):
            success_count += 1
        
        # 7. 交易数据
        if self.fetch_transactions_sample(league_key, count=50):
            success_count += 1
        
        print(f"\n✅ 联盟样本数据获取完成: {success_count}/{total_tasks} 成功")
        return success_count > 0
    
    def fetch_player_stats_only(self) -> bool:
        """仅获取球员统计数据样本"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"\n📊 专门获取球员统计数据: {league_key}")
        
        success_count = 0
        
        # 1. 球员基本数据
        if self.fetch_players_sample(league_key, count=20):
            success_count += 1
        
        # 2. 球员赛季统计
        if self.fetch_player_season_stats_sample(league_key, count=5):
            success_count += 1
        
        # 3. 球员周/日期统计
        if self._is_nfl_league(league_key):
            # NFL: 获取多个周的数据
            for week in [1, 2, 3]:
                if self.fetch_player_weekly_stats_sample(league_key, week=week, count=3):
                    success_count += 1
        else:
            # 其他运动: 获取不同日期的数据
            dates = ["2024-06-01", "2024-06-15", "2024-07-01"]
            for date in dates:
                if self.fetch_player_daily_stats_sample(league_key, date=date, count=3):
                    success_count += 1
        
        print(f"\n✅ 球员统计数据获取完成: {success_count} 个样本")
        return success_count > 0
    
    def fetch_team_samples(self) -> bool:
        """获取团队相关样本数据"""
        if not self.selected_league:
            print("❌ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        
        # 先获取团队列表
        teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        teams_data = get_api_data(teams_url)
        
        if not teams_data:
            print("❌ 无法获取团队数据")
            return False
        
        # 提取团队键
        team_keys = self._extract_team_keys_from_teams_data(teams_data)
        if not team_keys:
            print("❌ 未找到团队数据")
            return False
        
        print(f"\n🚀 开始获取团队样本数据: 找到 {len(team_keys)} 个团队")
        
        success_count = 0
        # 限制获取前2个团队的详细数据以节省时间
        for i, team_key in enumerate(team_keys[:2]):
            print(f"\n📋 处理团队 {i+1}/2: {team_key}")
            
            # 获取团队阵容
            if self.fetch_team_roster_sample(team_key):
                success_count += 1
            
            # 获取团队对战数据
            if self.fetch_team_matchups_sample(team_key):
                success_count += 1
        
        print(f"\n✅ 团队样本数据获取完成: {success_count} 个样本")
        return success_count > 0
    
    def _extract_team_keys_from_teams_data(self, teams_data: Dict) -> List[str]:
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
                                isinstance(team_data[0], list)):
                                for team_item in team_data[0]:
                                    if isinstance(team_item, dict) and "team_key" in team_item:
                                        team_key = team_item["team_key"]
                                        if team_key:
                                            team_keys.append(team_key)
                                        break
        
        except Exception as e:
            print(f"提取团队键时出错: {e}")
        
        return team_keys
    
    def run_interactive_menu(self):
        """运行交互式菜单"""
        while True:
            print("\n" + "="*60)
            print("🔬 Yahoo Fantasy API 样本数据获取工具")
            print("="*60)
            print("1. 获取基础样本数据（游戏、联盟列表）")
            print("2. 选择联盟并获取联盟样本数据")
            print("3. 获取团队相关样本数据")
            print("4. 获取球员统计数据样本（包含赛季、周/日期数据）")
            print("5. 获取所有样本数据（推荐）")
            print("6. 查看已保存的样本文件")
            print("0. 退出")
            
            choice = input("\n请选择操作 (0-6): ").strip()
            
            if choice == "0":
                print("👋 退出程序")
                break
            elif choice == "1":
                self.fetch_basic_samples()
            elif choice == "2":
                if self.select_league_for_samples():
                    self.fetch_league_samples()
            elif choice == "3":
                if not self.selected_league and not self.select_league_for_samples():
                    continue
                self.fetch_team_samples()
            elif choice == "4":
                if not self.selected_league and not self.select_league_for_samples():
                    continue
                self.fetch_player_stats_only()
            elif choice == "5":
                print("\n🚀 开始获取所有样本数据...")
                self.fetch_basic_samples()
                if self.select_league_for_samples():
                    self.fetch_league_samples()
                    self.fetch_team_samples()
            elif choice == "6":
                self.list_sample_files()
            else:
                print("❌ 无效选择，请重试")
    
    def list_sample_files(self):
        """列出已保存的样本文件"""
        json_files = list(self.sample_dir.glob("*.json"))
        
        if not json_files:
            print("\n📂 样本数据目录为空")
            return
        
        print(f"\n📂 已保存的样本文件 ({len(json_files)} 个):")
        print("-" * 60)
        
        for file in sorted(json_files):
            try:
                # 读取文件信息
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    description = data.get("metadata", {}).get("description", "无描述")
                    fetched_at = data.get("metadata", {}).get("fetched_at", "未知时间")
                
                file_size = file.stat().st_size
                size_kb = file_size / 1024
                
                print(f"📄 {file.name}")
                print(f"   描述: {description}")
                print(f"   时间: {fetched_at[:19] if len(fetched_at) > 19 else fetched_at}")
                print(f"   大小: {size_kb:.1f} KB")
                print()
                
            except Exception as e:
                print(f"❌ 读取文件信息失败 {file.name}: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy API 样本数据获取工具")
    
    parser.add_argument("--basic", action="store_true", help="仅获取基础样本数据")
    parser.add_argument("--league", action="store_true", help="获取联盟样本数据")
    parser.add_argument("--teams", action="store_true", help="获取团队样本数据")
    parser.add_argument("--players", action="store_true", help="获取球员统计数据样本")
    parser.add_argument("--all", action="store_true", help="获取所有样本数据")
    parser.add_argument("--list", action="store_true", help="列出已保存的样本文件")
    
    args = parser.parse_args()
    
    # 创建样本数据获取器
    fetcher = YahooFantasySampleFetcher()
    
    # 检查是否有命令行参数
    has_args = any([args.basic, args.league, args.teams, args.players, args.all, args.list])
    
    if not has_args:
        # 没有参数，运行交互式菜单
        fetcher.run_interactive_menu()
    else:
        # 有参数，执行对应的功能
        if args.list:
            fetcher.list_sample_files()
        
        if args.basic:
            fetcher.fetch_basic_samples()
        
        if args.league or args.all:
            if fetcher.select_league_for_samples():
                fetcher.fetch_league_samples()
        
        if args.teams or args.all:
            if not fetcher.selected_league and not fetcher.select_league_for_samples():
                print("❌ 需要选择联盟才能获取团队数据")
            else:
                fetcher.fetch_team_samples()
        
        if args.players or args.all:
            if not fetcher.selected_league and not fetcher.select_league_for_samples():
                print("❌ 需要选择联盟才能获取球员统计数据")
            else:
                fetcher.fetch_player_stats_only()


if __name__ == "__main__":
    main()
