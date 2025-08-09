#!/usr/bin/env python3
"""
Yahoo Fantasy Data Pipeline
Unified data fetching tool for single league deep fetch and time series data
"""

import os
import sys
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from extract.get_api_data import *
from database_writer import *
from database.model import *

# Ensure proper module import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class YahooFantasyDataPipeline:
    """Yahoo Fantasy unified data pipeline"""
    
    # Configuration constants
    DEFAULT_PAGE_SIZE = 25
    MAX_ITERATIONS = 100
    TRANSACTION_MAX_ITERATIONS = 200
    BATCH_DELAY = 0.5
    TEAM_DELAY = 0.2
    DAY_DELAY = 1.0
    
    def __init__(self, delay: int = 2, batch_size: int = 100):
        """Initialize data pipeline"""
        self.delay = delay
        self.batch_size = batch_size
        self.selected_league = None
        self.db_writer = FantasyDatabaseWriter(batch_size=batch_size)
        
    def wait(self, message: Optional[str] = None) -> None:
        """Wait for specified time"""
        time.sleep(self.delay)
    
    def close(self):
        """Close resources"""
        if self.db_writer:
            self.db_writer.close()
    

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
    
    def parse_league_standings(self, standings_data: Dict) -> List[Dict]:
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
            
            # Group by game_key and convert to dict
            leagues_data = {}
            for league in leagues:
                game_key = league.game_key
                if game_key not in leagues_data:
                    leagues_data[game_key] = []
                
                # Convert league object to dict using __dict__ if available,
                # or use essential fields only
                if hasattr(league, '__dict__'):
                    league_dict = {k: v for k, v in league.__dict__.items() 
                                  if not k.startswith('_')}
                else:
                    # Fallback to essential fields
                    league_dict = {
                        'league_key': league.league_key,
                        'league_id': league.league_id,
                        'game_key': league.game_key,
                        'name': league.name,
                        'season': league.season,
                        'num_teams': league.num_teams,
                        'start_date': league.start_date,
                        'end_date': league.end_date,
                        'is_finished': league.is_finished,
                        'game_code': league.game_code,
                        'scoring_type': league.scoring_type
                    }
                leagues_data[game_key].append(league_dict)
            
            return leagues_data
            
        except Exception as e:
            return None

    def parse_leagues(self, data: Dict, game_key: str) -> List[Dict]:
        
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
                        
                        # Ensure league info contains game_key
                        league_info["game_key"] = game_key
                        leagues.append(league_info)
                break
        
        except Exception as e:
            pass
        
        return leagues

    def parse_season_dates_from_league(self) -> bool:
        
        if not self.selected_league:
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season')
        
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
        """Fetch all players using improved pagination logic"""
        players_data = []
        start = 0
        page_size = self.DEFAULT_PAGE_SIZE
        max_iterations = self.MAX_ITERATIONS
        iteration = 0
            
        while iteration < max_iterations:
            iteration += 1
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
            if start > 0:
                url += f";start={start}"
            url += "?format=json"
                
            api_response = get_api_data(url)
            if not api_response:
                break
                
            batch_players = self.parse_players(api_response)
            
            if not batch_players:
                break
                
            players_data.extend(batch_players)
                
            if len(batch_players) < page_size:
                break
                
            start += page_size
            
        return players_data
    
    def parse_players(self, players_data: Dict) -> List[Dict]:
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
                            self.normalize_players(merged_info)
                            players.append(merged_info)
                    elif isinstance(player_basic_info, dict):
                        self.normalize_players(player_basic_info)
                        players.append(player_basic_info)
        
        except Exception as e:
            pass
        
        return players
    
    def normalize_players(self, player_info: Dict) -> None:
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
    
    def fetch_api_players_stats_season(self, players: List, league_key: str) -> Optional[Dict]:
        """直接获取所有球员的赛季统计数据"""
        # Support both dict and object formats
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
    
    def fetch_api_players_stats_time_range(self, players: List, league_key: str, 
                                          start_date: date, end_date: date) -> List[Dict]:
        """获取指定日期范围的球员日统计数据"""
        
        all_stats_data = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Batch process players
            batch_size = self.DEFAULT_PAGE_SIZE
            total_batches = (len(players) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(players))
                batch_players = players[start_idx:end_idx]
                
                # Support both dict and object formats
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
                        all_stats_data.append({
                            'stats_data': stats_data,
                            'date': current_date
                        })
                        
                except Exception as e:
                    pass
                
                # Wait between batches
                if batch_idx < total_batches - 1:
                    time.sleep(self.BATCH_DELAY)
            
            current_date += timedelta(days=1)
            
            # Wait only when processing multiple days
            if current_date <= end_date:
                time.sleep(self.DAY_DELAY)
        
        return all_stats_data


    # ===== Teams =====
    def fetch_api_teams(self, league_key: str) -> Optional[Dict]:
        """Fetch team data from API"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        return get_api_data(url)
    
    def parse_team_keys_from_teams(self, teams_data: Dict) -> List[str]:
        """Extract team keys from teams data"""
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
                            # Fix: team_data[0] is a dict list, not nested list
                            if (isinstance(team_data, list) and 
                                len(team_data) > 0 and 
                                isinstance(team_data[0], list)):
                                # Find dict containing team_key from team_data[0] list
                                for team_item in team_data[0]:
                                    if isinstance(team_item, dict) and "team_key" in team_item:
                                        team_key = team_item["team_key"]
                                        if team_key:
                                            team_keys.append(team_key)
                                        break
        
        except (KeyError, TypeError, ValueError):
            pass
        
        return team_keys
    
    def _extract_from_nested_data(self, data, target_key):
        """Utility to recursively extract data from nested structures"""
        if isinstance(data, dict):
            if target_key in data:
                return data[target_key]
            for value in data.values():
                result = self._extract_from_nested_data(value, target_key)
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self._extract_from_nested_data(item, target_key)
                if result is not None:
                    return result
        return None
    
    def parse_team_standings(self, team_data) -> Optional[Dict]:
        """Extract standings info from team data"""
        try:
            team_key = self._extract_from_nested_data(team_data, "team_key")
            team_standings = self._extract_from_nested_data(team_data, "team_standings")
            team_points = self._extract_from_nested_data(team_data, "team_points")
            
            if not team_key:
                return None
            
            standings_info = {
                "team_key": team_key,
                "team_standings": team_standings,
                "team_points": team_points
            }
            
            return standings_info
            
        except (KeyError, TypeError):
            return None
    
    def fetch_api_team_matchups(self, team_key: str) -> Optional[Dict]:
        """获取团队matchups数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        return get_api_data(url)
    
    def fetch_team_rosters_time_range(self, teams_data: Dict, start_date: date, end_date: date) -> List[Dict]:
        """获取指定日期范围内的团队roster数据"""
        team_keys = self.parse_team_keys_from_teams(teams_data)
        if not team_keys:
            return []
        
        all_roster_data = []

        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            for i, team_key in enumerate(team_keys):
                try:
                    single_roster_data = self.fetch_api_team_roster_single_day(team_key, date_str)
                    if single_roster_data:
                        all_roster_data.append({
                            'roster_data': single_roster_data,
                            'team_key': team_key,
                            'date': date_str
                        })
                except Exception:
                    pass
                
                # Delay between teams
                if i < len(team_keys) - 1:
                    time.sleep(self.TEAM_DELAY)
            
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                self.wait()
        
        return all_roster_data
    
    def fetch_api_team_roster_single_day(self, team_key: str, date_str: str) -> Optional[Dict]:
        """获取指定日期的团队roster"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return get_api_data(url)
    
    
    # ===== Transactions =====
    def fetch_api_league_transactions(self, league_key: str) -> List[Dict]:
        """Fetch all league transactions with pagination"""
        all_transactions = []
        start = 0
        page_size = self.DEFAULT_PAGE_SIZE
        max_iterations = self.TRANSACTION_MAX_ITERATIONS
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
            
            batch_transactions = self.parse_transactions(transactions_data)
            
            if not batch_transactions:
                break
            
            all_transactions.extend(batch_transactions)
            
            if len(batch_transactions) < page_size:
                break
            
            start += page_size
            time.sleep(self.BATCH_DELAY)
        
        return all_transactions
    
    def parse_transactions(self, transactions_data: Dict) -> List[Dict]:
        """Extract transaction info from API response data"""
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
        """Calculate date range
        
        Args:
            mode: 'specific' | 'days_back' | 'full_season'
            days_back: Number of days back (used when mode='days_back')
            target_date: Target date 'YYYY-MM-DD' (used when mode='specific')
            
        Returns:
            (start_date, end_date) or None
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

