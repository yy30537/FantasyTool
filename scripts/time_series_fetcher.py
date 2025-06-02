#!/usr/bin/env python3
"""
Yahoo Fantasy时间序列数据获取工具
支持历史数据获取、直接数据库写入和增量更新
"""
import os
import sys
import time
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yahoo_api_utils import (
    get_api_data, load_json_data, 
    GAMES_DIR, LEAGUES_DIR,
    select_league_interactively
)

from database_writer import FantasyDatabaseWriter

class TimeSeriesFantasyFetcher:
    """时间序列Yahoo Fantasy数据获取器"""
    
    def __init__(self, delay: int = 2, batch_size: int = 100):
        """初始化时间序列数据获取器
        
        Args:
            delay: API请求间隔时间（秒）
            batch_size: 数据库批量写入大小
        """
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
            from yahoo_api_utils import save_json_data
            leagues_file = LEAGUES_DIR / "all_leagues_data.json"
            save_json_data(all_leagues, leagues_file)
            print(f"✓ 联盟数据获取完成")
            return True
        
        return False
    
    def _fetch_games_data(self) -> Optional[Dict]:
        """获取用户的games数据"""
        print("获取用户的games数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        return get_api_data(url)
    
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
    
    # ===== 时间序列数据获取方法 =====
    
    def fetch_historical_rosters(self, start_week: int = 1, end_week: Optional[int] = None,
                                start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """获取历史名单数据
        
        Args:
            start_week: 开始周（NFL）
            end_week: 结束周（NFL），None表示当前周
            start_date: 开始日期（MLB/NBA/NHL）
            end_date: 结束日期（MLB/NBA/NHL），None表示今天
        """
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        game_code = self.selected_league.get('game_code', 'nfl')
        
        print(f"🔄 开始获取历史名单数据: {league_key}")
        
        # 首先获取团队列表
        teams_data = self._fetch_teams_data(league_key)
        if not teams_data:
            print("✗ 获取团队数据失败")
            return False
        
        team_keys = self._extract_team_keys_from_data(teams_data)
        if not team_keys:
            print("✗ 提取团队键失败")
            return False
        
        print(f"找到 {len(team_keys)} 个团队")
        
        # 根据游戏类型选择时间范围
        if game_code.lower() == 'nfl':
            return self._fetch_rosters_by_weeks(team_keys, league_key, season, start_week, end_week)
        else:
            return self._fetch_rosters_by_dates(team_keys, league_key, season, start_date, end_date)
    
    def _fetch_rosters_by_weeks(self, team_keys: List[str], league_key: str, season: str,
                               start_week: int, end_week: Optional[int]) -> bool:
        """按周获取名单数据（NFL）"""
        if end_week is None:
            end_week = int(self.selected_league.get('current_week', start_week))
        
        total_requests = len(team_keys) * (end_week - start_week + 1)
        current_request = 0
        
        for week in range(start_week, end_week + 1):
            print(f"  获取第 {week} 周名单数据...")
            
            for team_key in team_keys:
                current_request += 1
                print(f"    [{current_request}/{total_requests}] 团队 {team_key} 第 {week} 周")
                
                roster_data = self._fetch_team_roster_by_week(team_key, week)
                if roster_data:
                    self._process_roster_data_to_db(roster_data, team_key, league_key, 
                                                  'week', season, week=week)
                
                if current_request < total_requests:
                    self.wait()
        
        print(f"✓ 历史名单数据获取完成: 第 {start_week}-{end_week} 周")
        return True
    
    def _fetch_rosters_by_dates(self, team_keys: List[str], league_key: str, season: str,
                               start_date: Optional[date], end_date: Optional[date]) -> bool:
        """按日期获取名单数据（MLB/NBA/NHL）"""
        if start_date is None:
            start_date = date.today() - timedelta(days=30)  # 默认过去30天
        if end_date is None:
            end_date = date.today()
        
        # 生成日期列表（每周一次，减少API调用）
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=7)  # 每周一次
        
        total_requests = len(team_keys) * len(dates)
        current_request = 0
        
        for target_date in dates:
            date_str = target_date.strftime('%Y-%m-%d')
            print(f"  获取 {date_str} 名单数据...")
            
            for team_key in team_keys:
                current_request += 1
                print(f"    [{current_request}/{total_requests}] 团队 {team_key} {date_str}")
                
                roster_data = self._fetch_team_roster_by_date(team_key, date_str)
                if roster_data:
                    self._process_roster_data_to_db(roster_data, team_key, league_key,
                                                  'date', season, coverage_date=target_date)
                
                if current_request < total_requests:
                    self.wait()
        
        print(f"✓ 历史名单数据获取完成: {start_date} 至 {end_date}")
        return True
    
    def _fetch_team_roster_by_week(self, team_key: str, week: int) -> Optional[Dict]:
        """获取指定周的团队名单"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;week={week}?format=json"
        return get_api_data(url)
    
    def _fetch_team_roster_by_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队名单"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)
    
    def _process_roster_data_to_db(self, roster_data: Dict, team_key: str, league_key: str,
                                 coverage_type: str, season: str,
                                 week: Optional[int] = None,
                                 coverage_date: Optional[date] = None) -> None:
        """处理名单数据并写入数据库"""
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
                
                # 写入数据库
                self.db_writer.write_roster_history(
                    team_key=team_key,
                    player_key=player_key,
                    league_key=league_key,
                    coverage_type=coverage_type,
                    season=season,
                    week=week,
                    coverage_date=coverage_date,
                    selected_position=selected_position,
                    is_starting=is_starting,
                    player_status=player_status,
                    injury_note=injury_note
                )
                
        except Exception as e:
            print(f"处理名单数据失败 {team_key}: {e}")
    
    def fetch_historical_player_stats(self, start_week: int = 1, end_week: Optional[int] = None,
                                    start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """获取历史球员统计数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        game_code = self.selected_league.get('game_code', 'nfl')
        
        print(f"🔄 开始获取历史球员统计数据: {league_key}")
        
        # 获取球员列表
        players_data = self._fetch_all_league_players(league_key)
        if not players_data:
            print("✗ 获取球员数据失败")
            return False
        
        player_keys = [p.get("player_key") for p in players_data if p.get("player_key")]
        if not player_keys:
            print("✗ 提取球员键失败")
            return False
        
        print(f"找到 {len(player_keys)} 个球员")
        
        # 根据游戏类型选择时间范围
        if game_code.lower() == 'nfl':
            return self._fetch_player_stats_by_weeks(player_keys, players_data, league_key, season, start_week, end_week)
        else:
            return self._fetch_player_stats_by_dates(player_keys, players_data, league_key, season, start_date, end_date)
    
    def _fetch_player_stats_by_weeks(self, player_keys: List[str], players_data: List[Dict],
                                   league_key: str, season: str, start_week: int, end_week: Optional[int]) -> bool:
        """按周获取球员统计数据（NFL）"""
        if end_week is None:
            end_week = int(self.selected_league.get('current_week', start_week))
        
        # 创建player_key到editorial_player_key的映射
        key_mapping = {}
        for player in players_data:
            if player.get("player_key") and player.get("editorial_player_key"):
                key_mapping[player["player_key"]] = player["editorial_player_key"]
        
        for week in range(start_week, end_week + 1):
            print(f"  获取第 {week} 周球员统计...")
            
            # 批量获取统计数据
            stats_data = self._fetch_player_stats_batch_by_week(player_keys, league_key, week)
            
            for player_key, stats in stats_data.items():
                editorial_key = key_mapping.get(player_key, player_key)
                
                self.db_writer.write_player_stats_history(
                    player_key=player_key,
                    editorial_player_key=editorial_key,
                    league_key=league_key,
                    stats_data=stats,
                    coverage_type='week',
                    season=season,
                    week=week
                )
        
        print(f"✓ 历史球员统计获取完成: 第 {start_week}-{end_week} 周")
        return True
    
    def _fetch_player_stats_by_dates(self, player_keys: List[str], players_data: List[Dict],
                                   league_key: str, season: str, start_date: Optional[date], end_date: Optional[date]) -> bool:
        """按日期获取球员统计数据（MLB/NBA/NHL）"""
        if start_date is None:
            start_date = date.today() - timedelta(days=30)  # 默认过去30天
        if end_date is None:
            end_date = date.today()
        
        # 创建player_key到editorial_player_key的映射
        key_mapping = {}
        for player in players_data:
            if player.get("player_key") and player.get("editorial_player_key"):
                key_mapping[player["player_key"]] = player["editorial_player_key"]
        
        # 生成日期列表（每周一次，减少API调用）
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=7)  # 每周一次
        
        for target_date in dates:
            date_str = target_date.strftime('%Y-%m-%d')
            print(f"  获取 {date_str} 球员统计...")
            
            # 批量获取统计数据
            stats_data = self._fetch_player_stats_batch_by_date(player_keys, league_key, date_str)
            
            for player_key, stats in stats_data.items():
                editorial_key = key_mapping.get(player_key, player_key)
                
                self.db_writer.write_player_stats_history(
                    player_key=player_key,
                    editorial_player_key=editorial_key,
                    league_key=league_key,
                    stats_data=stats,
                    coverage_type='date',
                    season=season,
                    coverage_date=target_date
                )
        
        print(f"✓ 历史球员统计获取完成: {start_date} 至 {end_date}")
        return True

    def _fetch_player_stats_batch_by_week(self, player_keys: List[str], league_key: str, week: int) -> Dict[str, Dict]:
        """批量获取指定周的球员统计数据"""
        batch_size = 25
        all_stats = {}
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats;type=week;week={week}?format=json"
            
            print(f"    获取统计数据批次 ({i+1}-{min(i+batch_size, len(player_keys))}/{len(player_keys)})")
            
            data = get_api_data(url)
            if data:
                batch_stats = self._extract_player_stats_from_data(data)
                all_stats.update(batch_stats)
            
            time.sleep(0.5)
        
        return all_stats
    
    def _fetch_player_stats_batch_by_date(self, player_keys: List[str], league_key: str, date_str: str) -> Dict[str, Dict]:
        """批量获取指定日期的球员统计数据"""
        batch_size = 25
        all_stats = {}
        
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            player_keys_param = ",".join(batch_keys)
            
            # 根据Yahoo API文档，按日期获取统计数据的URL格式
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_param}/stats;type=date;date={date_str}?format=json"
            
            print(f"    获取统计数据批次 ({i+1}-{min(i+batch_size, len(player_keys))}/{len(player_keys)})")
            
            data = get_api_data(url)
            if data:
                batch_stats = self._extract_player_stats_from_data(data)
                all_stats.update(batch_stats)
            
            time.sleep(0.5)
        
        return all_stats
    
    # ===== 辅助方法 =====
    
    def _fetch_teams_data(self, league_key: str) -> Optional[Dict]:
        """获取团队数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
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
    
    def _fetch_all_league_players(self, league_key: str) -> List[Dict]:
        """获取联盟所有球员"""
        all_players = []
        start = 0
        page_size = 25
        max_iterations = 100
        iteration = 0
        
        print(f"获取联盟球员列表...")
            
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
        
        print(f"获取完成: 总计 {len(all_players)} 个球员")
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
    
    # ===== 主要流程 =====
    
    def run_historical_data_fetch(self, weeks_back: int = 5, days_back: int = 30) -> bool:
        """执行历史数据获取流程
        
        Args:
            weeks_back: 回溯周数（NFL）
            days_back: 回溯天数（其他运动）
        """
        print("🚀 开始Yahoo Fantasy历史数据获取...")
        
        # 1. 基础数据获取和联盟选择
        if not self.fetch_and_select_league():
            print("✗ 基础数据获取或联盟选择失败")
            return False
        
        league_key = self.selected_league['league_key']
        game_code = self.selected_league.get('game_code', 'nfl')
        current_week = int(self.selected_league.get('current_week', 1))
        
        # 2. 获取历史名单数据
        print(f"\n📋 步骤1: 获取历史名单数据")
        if game_code.lower() == 'nfl':
            start_week = max(1, current_week - weeks_back)
            self.fetch_historical_rosters(start_week=start_week, end_week=current_week)
        else:
            start_date = date.today() - timedelta(days=days_back)
            self.fetch_historical_rosters(start_date=start_date)
        
        # 3. 获取历史球员统计数据
        print(f"\n📋 步骤2: 获取历史球员统计数据")
        if game_code.lower() == 'nfl':
            start_week = max(1, current_week - weeks_back)
            self.fetch_historical_player_stats(start_week=start_week, end_week=current_week)
        else:
            start_date = date.today() - timedelta(days=days_back)
            self.fetch_historical_player_stats(start_date=start_date)
        
        # 4. 显示统计摘要
        print(f"\n📊 数据获取统计:")
        print(self.db_writer.get_stats_summary())
        
        print("🎉 历史数据获取成功！")
        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy时间序列数据获取工具")
    
    parser.add_argument("--historical", action="store_true", help="执行历史数据获取流程")
    parser.add_argument("--weeks-back", type=int, default=5, help="回溯周数（NFL），默认5周")
    parser.add_argument("--days-back", type=int, default=30, help="回溯天数（其他运动），默认30天")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    parser.add_argument("--batch-size", type=int, default=100, help="数据库批量写入大小，默认100")
    
    args = parser.parse_args()
    
    # 创建时间序列数据获取器
    fetcher = TimeSeriesFantasyFetcher(delay=args.delay, batch_size=args.batch_size)
    
    try:
        if args.historical:
            fetcher.run_historical_data_fetch(weeks_back=args.weeks_back, days_back=args.days_back)
        else:
            # 默认执行历史流程
            fetcher.run_historical_data_fetch(weeks_back=args.weeks_back, days_back=args.days_back)
    finally:
        fetcher.close()


if __name__ == "__main__":
    main() 