"""
Fantasy数据服务
重构自archive/yahoo_api_data.py，提供统一的数据获取和处理服务
"""
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from ..data.extract import YahooAPIClient, select_league_interactively
from ..data.load import DatabaseLoader, LoadResult
from ..core.database.connection_manager import db_manager

class FantasyDataService:
    """Yahoo Fantasy数据服务"""
    
    def __init__(self, delay: int = 2, batch_size: int = 100):
        """初始化数据服务"""
        self.delay = delay
        self.batch_size = batch_size
        self.api_client = YahooAPIClient(delay=delay)
        self.loader = DatabaseLoader(batch_size=batch_size)
        self.selected_league = None
    
    def wait(self, message: Optional[str] = None) -> None:
        """等待指定时间"""
        if message:
            print(f"⏳ {message}")
        time.sleep(self.delay)
    
    def authenticate_user(self) -> bool:
        """检查用户认证状态"""
        return self.api_client.oauth_manager.is_authenticated()
    
    def fetch_and_select_league(self, use_existing_data: bool = False) -> bool:
        """获取基础数据并选择联盟"""
        print("🚀 获取联盟数据...")
        
        # 优先从数据库获取联盟数据
        if use_existing_data:
            leagues_data = self._get_leagues_from_database()
            if leagues_data:
                selected_league = select_league_interactively(leagues_data)
                if selected_league:
                    self.selected_league = selected_league
                    print(f"✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                    return True
        
        # 从API获取联盟数据
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
            from ..core.database.models import League
            
            with db_manager.session_scope() as session:
                leagues = session.query(League).all()
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
                        'season': league.season,
                        'num_teams': league.num_teams,
                        'game_code': league.game_code,
                        'scoring_type': league.scoring_type,
                        'is_finished': league.is_finished
                    }
                    leagues_data[game_key].append(league_dict)
                
                return leagues_data
                
        except Exception as e:
            print(f"从数据库获取联盟数据失败: {e}")
            return None
    
    def _fetch_all_leagues_data(self) -> Optional[Dict]:
        """从API获取所有联盟数据"""
        try:
            # 首先获取游戏数据
            print("📡 获取游戏数据...")
            games_response = self.api_client.get_user_games()
            if not games_response.success:
                print(f"获取游戏数据失败: {games_response.error_message}")
                return None
            
            # 保存游戏数据到数据库
            games_result = self.loader.load_games_data(games_response.data)
            if games_result.success:
                print(f"✓ 保存游戏数据: {games_result.inserted_count} 条新记录")
            
            self.wait("等待API限制...")
            
            # 获取联盟数据
            print("📡 获取联盟数据...")
            leagues_response = self.api_client.get_user_leagues()
            if not leagues_response.success:
                print(f"获取联盟数据失败: {leagues_response.error_message}")
                return None
            
            # 保存联盟数据到数据库
            leagues_result = self.loader.load_leagues_data(leagues_response.data)
            if leagues_result.success:
                print(f"✓ 保存联盟数据: {leagues_result.inserted_count} 条新记录")
            
            # 格式化联盟数据
            return self._format_leagues_data(leagues_response.data)
            
        except Exception as e:
            print(f"获取联盟数据时出错: {e}")
            return None
    
    def _format_leagues_data(self, leagues_data: Dict) -> Dict:
        """格式化联盟数据为选择界面所需格式"""
        formatted_data = {}
        
        try:
            games = leagues_data["fantasy_content"]["users"]["0"]["user"][1]["games"]
            
            for game_key, game_data in games.items():
                if game_key == "count":
                    continue
                    
                if isinstance(game_data["game"], list):
                    game_info = game_data["game"][0]
                    leagues_list = game_data["game"][1].get("leagues", {})
                else:
                    game_info = game_data["game"]
                    leagues_list = game_info.get("leagues", {})
                
                if not leagues_list:
                    continue
                
                formatted_leagues = []
                for league_key, league_data in leagues_list.items():
                    if league_key == "count":
                        continue
                    
                    if isinstance(league_data["league"], list):
                        league_info = league_data["league"][0]
                    else:
                        league_info = league_data["league"]
                    
                    formatted_league = {
                        'league_key': league_info["league_key"],
                        'league_id': league_info["league_id"],
                        'game_key': game_info["game_key"],
                        'name': league_info["name"],
                        'season': game_info["season"],
                        'num_teams': int(league_info.get("num_teams", 0)),
                        'game_code': game_info["code"],
                        'scoring_type': league_info.get("scoring_type", "未知"),
                        'is_finished': league_info.get("is_finished", 0)
                    }
                    formatted_leagues.append(formatted_league)
                
                if formatted_leagues:
                    formatted_data[game_info["game_key"]] = formatted_leagues
            
            return formatted_data
            
        except Exception as e:
            print(f"格式化联盟数据失败: {e}")
            return {}
    
    def fetch_league_complete_data(self) -> bool:
        """获取联盟完整数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        print(f"🔄 开始获取联盟 {self.selected_league['name']} 的完整数据...")
        
        success_count = 0
        total_operations = 6
        
        try:
            # 1. 获取联盟设置
            print("📡 1/6 获取联盟设置...")
            settings_response = self.api_client.get_league_settings(league_key)
            if settings_response.success:
                print("✓ 联盟设置获取成功")
                success_count += 1
            else:
                print(f"✗ 联盟设置获取失败: {settings_response.error_message}")
            
            self.wait()
            
            # 2. 获取球队信息
            print("📡 2/6 获取球队信息...")
            teams_response = self.api_client.get_league_teams(league_key)
            if teams_response.success:
                print("✓ 球队信息获取成功")
                success_count += 1
            else:
                print(f"✗ 球队信息获取失败: {teams_response.error_message}")
            
            self.wait()
            
            # 3. 获取球员信息
            print("📡 3/6 获取球员信息...")
            players_response = self.api_client.get_league_players(league_key, count=300)
            if players_response.success:
                print("✓ 球员信息获取成功")
                success_count += 1
            else:
                print(f"✗ 球员信息获取失败: {players_response.error_message}")
            
            self.wait()
            
            # 4. 获取联盟排名
            print("📡 4/6 获取联盟排名...")
            standings_response = self.api_client.get_league_standings(league_key)
            if standings_response.success:
                print("✓ 联盟排名获取成功")
                success_count += 1
            else:
                print(f"✗ 联盟排名获取失败: {standings_response.error_message}")
            
            self.wait()
            
            # 5. 获取交易记录
            print("📡 5/6 获取交易记录...")
            transactions_response = self.api_client.get_league_transactions(league_key)
            if transactions_response.success:
                print("✓ 交易记录获取成功")
                success_count += 1
            else:
                print(f"✗ 交易记录获取失败: {transactions_response.error_message}")
            
            self.wait()
            
            # 6. 获取联盟记分板
            print("📡 6/6 获取联盟记分板...")
            scoreboard_response = self.api_client.get_league_scoreboard(league_key)
            if scoreboard_response.success:
                print("✓ 联盟记分板获取成功")
                success_count += 1
            else:
                print(f"✗ 联盟记分板获取失败: {scoreboard_response.error_message}")
            
            print(f"\\n📊 数据获取完成: {success_count}/{total_operations} 个操作成功")
            print(f"📈 {self.loader.get_stats_summary()}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 获取联盟数据时发生错误: {e}")
            return False
    
    def fetch_roster_history_data(self, start_date: date, end_date: date) -> bool:
        """获取阵容历史数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        print(f"📅 获取 {start_date} 到 {end_date} 的阵容历史数据...")
        
        # 1. 获取球队列表
        teams_response = self.api_client.get_league_teams(self.selected_league['league_key'])
        if not teams_response.success:
            print(f"❌ 获取球队列表失败: {teams_response.error_message}")
            return False
        
        # 2. 提取球队keys
        team_keys = self._extract_team_keys_from_response(teams_response.data)
        if not team_keys:
            print("❌ 未找到球队数据")
            return False
        
        print(f"🏀 找到 {len(team_keys)} 支球队")
        
        # 3. 按日期获取每支球队的阵容
        total_days = (end_date - start_date).days + 1
        success_count = 0
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"📅 处理日期: {date_str}")
            
            day_success = 0
            for i, team_key in enumerate(team_keys):
                try:
                    roster_response = self.api_client.get_team_roster(team_key, date_str)
                    if roster_response.success:
                        # 处理并保存阵容数据
                        if self._process_roster_data(roster_response.data, team_key, current_date):
                            day_success += 1
                    else:
                        print(f"  ⚠️ 获取 {team_key} 阵容失败: {roster_response.error_message}")
                    
                    # 团队间等待
                    if i < len(team_keys) - 1:
                        time.sleep(0.3)
                        
                except Exception as e:
                    print(f"  ❌ 处理 {team_key} 时出错: {e}")
            
            if day_success > 0:
                success_count += 1
                print(f"  ✓ {date_str}: {day_success}/{len(team_keys)} 球队成功")
            
            current_date += timedelta(days=1)
            
            # 日期间等待
            if current_date <= end_date:
                self.wait("API限制等待...")
        
        print(f"🎯 阵容历史数据获取完成: {success_count}/{total_days} 天成功")
        return success_count > 0
    
    def _extract_team_keys_from_response(self, teams_data: Dict) -> List[str]:
        """从球队API响应中提取球队keys"""
        team_keys = []
        
        try:
            fantasy_content = teams_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找teams容器
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
                if str_index in teams_container:
                    team_container = teams_container[str_index]
                    if "team" in team_container:
                        team_data = team_container["team"]
                        # 提取team_key
                        if isinstance(team_data, list) and len(team_data) > 0:
                            if isinstance(team_data[0], list):
                                for team_item in team_data[0]:
                                    if isinstance(team_item, dict) and "team_key" in team_item:
                                        team_keys.append(team_item["team_key"])
                                        break
            
            return team_keys
            
        except Exception as e:
            print(f"提取球队keys失败: {e}")
            return []
    
    def _process_roster_data(self, roster_data: Dict, team_key: str, roster_date: date) -> bool:
        """处理并保存单个阵容数据"""
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
            
            # 获取球员信息
            players_container = None
            if "0" in roster_info and "players" in roster_info["0"]:
                players_container = roster_info["0"]["players"]
            
            if not players_container:
                return False
            
            roster_entries = []
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
                player_dict = {}
                
                # 处理player info
                if isinstance(player_info_list[0], list):
                    for item in player_info_list[0]:
                        if isinstance(item, dict):
                            player_dict.update(item)
                elif isinstance(player_info_list[0], dict):
                    player_dict.update(player_info_list[0])
                
                # 处理position data
                if len(player_info_list) > 1 and isinstance(player_info_list[1], dict):
                    player_dict.update(player_info_list[1])
                
                # 创建roster记录
                if player_dict.get("player_key"):
                    roster_entry = {
                        "team_key": team_key,
                        "player_key": player_dict["player_key"],
                        "league_key": self.selected_league['league_key'],
                        "roster_date": roster_date,
                        "season": self.selected_league.get('season', '2024'),
                        "selected_position": self._extract_position_string(player_dict.get("selected_position")),
                        "player_status": player_dict.get("status"),
                        "status_full": player_dict.get("status_full"),
                        "injury_note": player_dict.get("injury_note")
                    }
                    roster_entries.append(roster_entry)
            
            # 批量保存到数据库
            if roster_entries:
                return self.loader.load_roster_daily_data(roster_entries)
            
            return True
            
        except Exception as e:
            print(f"处理阵容数据失败 {team_key}: {e}")
            return False
    
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
    
    def get_season_date_info(self) -> Dict:
        """获取赛季日期信息和状态"""
        if not self.selected_league:
            return {}
        
        league_key = self.selected_league['league_key']
        
        try:
            from ..core.database.models import League
            
            with db_manager.session_scope() as session:
                league_db = session.query(League).filter_by(
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
                
                # 判断赛季状态
                if is_finished:
                    season_status = "已结束"
                    latest_date = end_date
                elif today > end_date:
                    season_status = "已结束"
                    latest_date = end_date
                elif today < start_date:
                    season_status = "未开始"
                    latest_date = start_date
                else:
                    season_status = "进行中"
                    latest_date = min(today, end_date)
                
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
    
    def fetch_player_daily_stats_data(self, start_date: date, end_date: date) -> bool:
        """获取球员日统计数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print(f"📈 获取 {start_date} 到 {end_date} 的球员日统计数据...")
        
        # 1. 从数据库获取球员列表
        player_keys = self._get_league_players_from_db(league_key)
        if not player_keys:
            print("❌ 数据库中没有球员数据，请先获取联盟数据")
            return False
        
        print(f"🏃 找到 {len(player_keys)} 个球员")
        
        # 2. 按日期获取统计数据
        total_days = (end_date - start_date).days + 1
        success_count = 0
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"📅 处理日期: {date_str}")
            
            day_success = self._fetch_players_stats_for_date(player_keys, league_key, season, current_date, date_str)
            if day_success:
                success_count += 1
                print(f"  ✓ {date_str}: 球员统计数据获取成功")
            else:
                print(f"  ⚠️ {date_str}: 球员统计数据获取失败")
            
            current_date += timedelta(days=1)
            
            # 日期间等待
            if current_date <= end_date:
                self.wait("API限制等待...")
        
        print(f"🎯 球员日统计数据获取完成: {success_count}/{total_days} 天成功")
        return success_count > 0
    
    def _get_league_players_from_db(self, league_key: str) -> List[str]:
        """从数据库获取联盟球员列表"""
        try:
            from ..core.database.models import Player
            
            with db_manager.session_scope() as session:
                players = session.query(Player).filter_by(league_key=league_key).all()
                return [player.player_key for player in players if player.player_key]
                
        except Exception as e:
            print(f"从数据库获取球员列表失败: {e}")
            return []
    
    def _fetch_players_stats_for_date(self, player_keys: List[str], league_key: str, 
                                     season: str, date_obj: date, date_str: str) -> bool:
        """获取指定日期的所有球员统计数据"""
        try:
            # 分批处理球员，每批25个
            batch_size = 25
            total_batches = (len(player_keys) + batch_size - 1) // batch_size
            
            processed_players = 0
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(player_keys))
                batch_player_keys = player_keys[start_idx:end_idx]
                
                # 批量获取统计数据
                stats_response = self.api_client.get_players_stats_batch(
                    league_key, batch_player_keys, date_str
                )
                
                if stats_response.success:
                    # 处理并保存统计数据
                    processed = self._process_players_daily_stats(
                        stats_response.data, league_key, season, date_obj
                    )
                    processed_players += processed
                else:
                    print(f"    ⚠️ 批次 {batch_idx + 1}/{total_batches} 获取失败: {stats_response.error_message}")
                
                # 批次间等待
                if batch_idx < total_batches - 1:
                    time.sleep(0.5)
            
            print(f"    📊 处理了 {processed_players} 个球员的统计数据")
            return processed_players > 0
            
        except Exception as e:
            print(f"获取日期 {date_str} 球员统计失败: {e}")
            return False
    
    def _process_players_daily_stats(self, stats_data: Dict, league_key: str, 
                                   season: str, date_obj: date) -> int:
        """处理并保存球员日统计数据"""
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
            processed_count = 0
            
            stats_entries = []
            
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
                
                if stats_dict:
                    stats_entry = {
                        "player_key": player_key,
                        "editorial_player_key": editorial_player_key,
                        "league_key": league_key,
                        "season": season,
                        "date": date_obj,
                        "stats_data": stats_dict
                    }
                    stats_entries.append(stats_entry)
                    processed_count += 1
            
            # 批量保存到数据库
            if stats_entries:
                self.loader.load_players_daily_stats_batch(stats_entries)
            
            return processed_count
            
        except Exception as e:
            print(f"处理球员日统计数据失败: {e}")
            return 0
    
    def fetch_player_season_stats_data(self) -> bool:
        """获取球员赛季统计数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print("📊 获取球员赛季统计数据...")
        
        # 1. 从数据库获取球员列表
        player_keys = self._get_league_players_from_db(league_key)
        if not player_keys:
            print("❌ 数据库中没有球员数据，请先获取联盟数据")
            return False
        
        print(f"🏃 找到 {len(player_keys)} 个球员")
        
        # 2. 分批获取赛季统计数据
        total_batches = (len(player_keys) + 24) // 25  # 每批25个
        success_count = 0
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * 25
            end_idx = min(start_idx + 25, len(player_keys))
            batch_player_keys = player_keys[start_idx:end_idx]
            
            print(f"📊 处理第 {batch_idx + 1}/{total_batches} 批，{len(batch_player_keys)} 个球员")
            
            try:
                # 批量获取赛季统计数据
                stats_response = self.api_client.get_players_season_stats_batch(
                    league_key, batch_player_keys
                )
                
                if stats_response.success:
                    # 处理并保存统计数据
                    processed = self._process_players_season_stats(
                        stats_response.data, league_key, season
                    )
                    if processed > 0:
                        success_count += 1
                        print(f"  ✓ 成功处理 {processed} 个球员的赛季统计")
                    else:
                        print(f"  ⚠️ 批次数据处理失败")
                else:
                    print(f"  ⚠️ 批次 {batch_idx + 1} 获取失败: {stats_response.error_message}")
                
            except Exception as e:
                print(f"  ❌ 处理批次 {batch_idx + 1} 时出错: {e}")
            
            # 批次间等待
            if batch_idx < total_batches - 1:
                self.wait("批次间等待...")
        
        print(f"🎯 球员赛季统计数据获取完成: {success_count}/{total_batches} 批次成功")
        return success_count > 0
    
    def _process_players_season_stats(self, stats_data: Dict, league_key: str, season: str) -> int:
        """处理并保存球员赛季统计数据"""
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
            processed_count = 0
            
            stats_entries = []
            
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
                
                if stats_dict:
                    stats_entry = {
                        "player_key": player_key,
                        "editorial_player_key": editorial_player_key,
                        "league_key": league_key,
                        "season": season,
                        "stats_data": stats_dict
                    }
                    stats_entries.append(stats_entry)
                    processed_count += 1
            
            # 批量保存到数据库
            if stats_entries:
                self.loader.load_players_season_stats_batch(stats_entries)
            
            return processed_count
            
        except Exception as e:
            print(f"处理球员赛季统计数据失败: {e}")
            return 0
    
    def fetch_team_weekly_data(self) -> bool:
        """获取团队每周对战数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        
        print("📅 获取团队每周对战数据...")
        
        # 1. 获取球队列表
        teams_response = self.api_client.get_league_teams(league_key)
        if not teams_response.success:
            print(f"❌ 获取球队列表失败: {teams_response.error_message}")
            return False
        
        # 2. 提取球队keys
        team_keys = self._extract_team_keys_from_response(teams_response.data)
        if not team_keys:
            print("❌ 未找到球队数据")
            return False
        
        print(f"🏀 找到 {len(team_keys)} 支球队")
        
        # 3. 获取每支球队的对战数据
        success_count = 0
        
        for i, team_key in enumerate(team_keys):
            try:
                print(f"📊 处理球队 {i+1}/{len(team_keys)}: {team_key}")
                
                # 获取球队对战数据
                matchups_response = self.api_client.get_team_matchups(team_key)
                if matchups_response.success:
                    # 处理并保存对战数据
                    if self._process_team_matchups_data(matchups_response.data, team_key, league_key, season):
                        success_count += 1
                        print(f"  ✓ 球队对战数据处理成功")
                    else:
                        print(f"  ⚠️ 球队对战数据处理失败")
                else:
                    print(f"  ⚠️ 获取球队对战失败: {matchups_response.error_message}")
                
                # 球队间等待
                if i < len(team_keys) - 1:
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  ❌ 处理球队 {team_key} 时出错: {e}")
        
        print(f"🎯 团队每周数据获取完成: {success_count}/{len(team_keys)} 球队成功")
        return success_count > 0
    
    def _process_team_matchups_data(self, matchups_data: Dict, team_key: str, 
                                   league_key: str, season: str) -> bool:
        """处理并保存团队对战数据"""
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
            
            matchup_entries = []
            team_stats_entries = []
            
            for i in range(matchups_count):
                str_index = str(i)
                if str_index not in matchups_container:
                    continue
                
                matchup_data = matchups_container[str_index]
                if "matchup" not in matchup_data:
                    continue
                
                matchup_info = matchup_data["matchup"]
                
                # 提取对战信息
                matchup_details = self._extract_matchup_details(matchup_info, team_key)
                if matchup_details:
                    matchup_entry = {
                        "league_key": league_key,
                        "team_key": team_key,
                        "season": season,
                        **matchup_details
                    }
                    matchup_entries.append(matchup_entry)
                    
                    # 提取团队周统计数据
                    team_stats = self._extract_team_weekly_stats_from_matchup(matchup_info, team_key)
                    if team_stats:
                        team_stats_entry = {
                            "league_key": league_key,
                            "team_key": team_key,
                            "season": season,
                            "week": matchup_details.get("week"),
                            **team_stats
                        }
                        team_stats_entries.append(team_stats_entry)
                    
                    success_count += 1
            
            # 批量保存到数据库
            if matchup_entries:
                self.loader.load_team_matchups_batch(matchup_entries)
            
            if team_stats_entries:
                self.loader.load_team_weekly_stats_batch(team_stats_entries)
            
            return success_count > 0
            
        except Exception as e:
            print(f"处理团队对战数据失败 {team_key}: {e}")
            return False
    
    def _extract_matchup_details(self, matchup_info, team_key: str) -> Optional[Dict]:
        """从matchup数据中提取对战详情"""
        try:
            matchup_data = {}
            
            # 处理matchup_info可能是列表或字典的情况
            if isinstance(matchup_info, list):
                # 合并列表中的所有字典
                for item in matchup_info:
                    if isinstance(item, dict):
                        matchup_data.update(item)
            elif isinstance(matchup_info, dict):
                matchup_data = matchup_info
            else:
                return None
            
            week = matchup_data.get("week")
            if week is None:
                return None
            
            # 提取基本信息
            details = {
                "week": week,
                "week_start": matchup_data.get("week_start"),
                "week_end": matchup_data.get("week_end"),
                "status": matchup_data.get("status"),
                "is_playoffs": self._safe_bool(matchup_data.get("is_playoffs", False)),
                "is_consolation": self._safe_bool(matchup_data.get("is_consolation", False)),
                "is_matchup_of_week": self._safe_bool(matchup_data.get("is_matchup_of_week", False)),
                "is_tied": self._safe_bool(matchup_data.get("is_tied", False)),
                "winner_team_key": matchup_data.get("winner_team_key")
            }
            
            # 从teams数据中提取对手信息和积分
            teams_data = matchup_data.get("0", {}).get("teams", {})
            if teams_data:
                team_details = self._extract_team_details_from_matchup(teams_data, team_key)
                if team_details:
                    details.update(team_details)
            
            return details
            
        except Exception as e:
            print(f"提取对战详情失败: {e}")
            return None
    
    def _extract_team_details_from_matchup(self, teams_data: Dict, target_team_key: str) -> Optional[Dict]:
        """从teams数据中提取团队详情"""
        try:
            teams_count = int(teams_data.get("count", 0))
            
            team_points = 0
            opponent_points = 0
            opponent_team_key = None
            
            # 遍历对战中的所有团队
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
                
                if isinstance(team_info, list) and len(team_info) >= 2:
                    # team_info[0] 包含团队基本信息
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
                    team_points = points
                elif current_team_key:
                    opponent_team_key = current_team_key
                    opponent_points = points
            
            # 判断胜负关系
            is_winner = None
            if team_points > opponent_points:
                is_winner = True
            elif team_points < opponent_points:
                is_winner = False
            # 平局时 is_winner 保持 None
            
            return {
                "opponent_team_key": opponent_team_key,
                "team_points": team_points,
                "opponent_points": opponent_points,
                "is_winner": is_winner
            }
            
        except Exception as e:
            print(f"提取团队详情失败: {e}")
            return None
    
    def _extract_team_weekly_stats_from_matchup(self, matchup_info, team_key: str) -> Optional[Dict]:
        """从matchup数据中提取团队周统计数据"""
        try:
            # 处理matchup_info可能是列表或字典的情况
            matchup_data = {}
            if isinstance(matchup_info, list):
                for item in matchup_info:
                    if isinstance(item, dict):
                        matchup_data.update(item)
            elif isinstance(matchup_info, dict):
                matchup_data = matchup_info
            else:
                return None
            
            # 从teams数据中提取统计数据
            teams_data = matchup_data.get("0", {}).get("teams", {})
            if not teams_data:
                return None
            
            teams_count = int(teams_data.get("count", 0))
            
            # 查找目标团队的统计数据
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
                if current_team_key == team_key and len(team_info) > 1:
                    team_stats_container = team_info[1]
                    if "team_stats" in team_stats_container:
                        team_stats_data = team_stats_container["team_stats"]
                        if "stats" in team_stats_data:
                            return self._parse_team_stats_from_data(team_stats_data["stats"])
            
            return None
            
        except Exception as e:
            print(f"提取团队周统计数据失败: {e}")
            return None
    
    def _parse_team_stats_from_data(self, stats_list: List) -> Dict:
        """解析团队统计数据"""
        try:
            parsed = {}
            
            # 构建 stat_id 到 value 的映射
            stats_dict = {}
            for stat_item in stats_list:
                if isinstance(stat_item, dict) and 'stat' in stat_item:
                    stat_info = stat_item['stat']
                    stat_id = stat_info.get('stat_id')
                    value = stat_info.get('value')
                    if stat_id is not None:
                        stats_dict[str(stat_id)] = value
            
            # 解析统计项（与球员统计类似的11个项目）
            
            # 1. Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    parsed['field_goals_made'] = self._safe_int(made.strip())
                    parsed['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['field_goals_made'] = None
                    parsed['field_goals_attempted'] = None
            
            # 2. Field Goal Percentage (FG%)
            fg_pct_str = stats_dict.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                parsed['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            
            # 3. Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    parsed['free_throws_made'] = self._safe_int(made.strip())
                    parsed['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    parsed['free_throws_made'] = None
                    parsed['free_throws_attempted'] = None
            
            # 4. Free Throw Percentage (FT%)
            ft_pct_str = stats_dict.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                parsed['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            
            # 5-11. 其他统计项
            parsed['three_pointers_made'] = self._safe_int(stats_dict.get('10'))
            parsed['points'] = self._safe_int(stats_dict.get('12'))
            parsed['rebounds'] = self._safe_int(stats_dict.get('15'))
            parsed['assists'] = self._safe_int(stats_dict.get('16'))
            parsed['steals'] = self._safe_int(stats_dict.get('17'))
            parsed['blocks'] = self._safe_int(stats_dict.get('18'))
            parsed['turnovers'] = self._safe_int(stats_dict.get('19'))
            
            return parsed
            
        except Exception as e:
            print(f"解析团队统计数据失败: {e}")
            return {}
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes')
        if isinstance(value, (int, float)):
            return value != 0
        return False
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        try:
            if value is None or value == '' or value == '-':
                return None
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _parse_percentage(self, pct_str) -> Optional[float]:
        """解析百分比字符串"""
        try:
            if not pct_str or pct_str == '-':
                return None
            
            pct_str = str(pct_str).strip()
            
            # 移除%符号
            if '%' in pct_str:
                clean_value = pct_str.replace('%', '')
                val = float(clean_value)
                return round(val, 3) if val is not None else None
            
            # 处理小数形式
            clean_value = float(pct_str)
            if 0 <= clean_value <= 1:
                return round(clean_value * 100, 3)
            elif 0 <= clean_value <= 100:
                return round(clean_value, 3)
            
            return None
        except (ValueError, TypeError):
            return None
    
    def get_database_summary(self) -> Dict[str, int]:
        """获取数据库摘要"""
        try:
            from ..core.database.models import (
                Game, League, Team, Manager, Player, 
                PlayerSeasonStats, PlayerDailyStats, Transaction, RosterDaily
            )
            
            summary = {}
            with db_manager.session_scope() as session:
                summary['games'] = session.query(Game).count()
                summary['leagues'] = session.query(League).count()
                summary['teams'] = session.query(Team).count()
                summary['managers'] = session.query(Manager).count()
                summary['players'] = session.query(Player).count()
                summary['player_season_stats'] = session.query(PlayerSeasonStats).count()
                summary['player_daily_stats'] = session.query(PlayerDailyStats).count()
                summary['transactions'] = session.query(Transaction).count()
                summary['roster_daily'] = session.query(RosterDaily).count()
            
            return summary
            
        except Exception as e:
            print(f"获取数据库摘要失败: {e}")
            return {}
    
    def fetch_team_season_data(self) -> bool:
        """获取团队赛季数据"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return False
        
        print("🏆 开始获取团队赛季数据...")
        
        try:
            league_key = self.selected_league["league_key"]
            season = self.selected_league["season"]
            
            # 获取联盟排名 (包含赛季统计)
            standings_response = self.api_client.get_league_standings(league_key)
            if not standings_response.success:
                print(f"❌ 获取联盟排名失败: {standings_response.error_message}")
                return False
            
            # 处理并保存联盟排名数据
            if self._process_league_standings_data(standings_response.data, league_key, season):
                print("✅ 团队赛季数据获取完成")
            else:
                print("❌ 团队赛季数据处理失败")
                return False
            
            return True
            
        except Exception as e:
            print(f"获取团队赛季数据失败: {e}")
            return False
    
    def _process_league_standings_data(self, standings_data: Dict, league_key: str, season: str) -> bool:
        """处理联盟排名数据"""
        try:
            fantasy_content = standings_data["fantasy_content"]
            league_info = fantasy_content["league"]
            
            # 查找standings容器
            standings_container = None
            if isinstance(league_info, list) and len(league_info) > 1:
                for item in league_info:
                    if isinstance(item, dict) and "standings" in item:
                        standings_container = item["standings"]
                        break
            
            if not standings_container:
                print("❌ 无法找到standings数据")
                return False
            
            teams = standings_container.get("teams", {})
            if not teams or "count" not in teams:
                print("❌ 无团队排名数据")
                return False
            
            teams_count = int(teams.get("count", 0))
            standings_entries = []
            
            print(f"📊 处理 {teams_count} 支团队的赛季数据...")
            
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams:
                    continue
                
                team_data = teams[str_index]
                if "team" not in team_data:
                    continue
                
                team_info = team_data["team"]
                if isinstance(team_info, list):
                    team_basic = team_info[0]
                    # 查找团队赛季统计
                    team_stats = None
                    for item in team_info[1:]:
                        if isinstance(item, dict) and "team_stats" in item:
                            team_stats = item["team_stats"]
                            break
                else:
                    team_basic = team_info
                    team_stats = None
                
                # 提取团队基本信息
                team_key = team_basic.get("team_key")
                if not team_key:
                    continue
                
                # 排名信息
                standings_entry = {
                    "league_key": league_key,
                    "team_key": team_key,
                    "season": season,
                    "rank": team_basic.get("team_standings", {}).get("rank", 0),
                    "wins": team_basic.get("team_standings", {}).get("outcome_totals", {}).get("wins", 0),
                    "losses": team_basic.get("team_standings", {}).get("outcome_totals", {}).get("losses", 0),
                    "ties": team_basic.get("team_standings", {}).get("outcome_totals", {}).get("ties", 0),
                    "win_percentage": team_basic.get("team_standings", {}).get("outcome_totals", {}).get("percentage", 0.0),
                    "points_for": team_basic.get("team_standings", {}).get("points_for", 0.0),
                    "points_against": team_basic.get("team_standings", {}).get("points_against", 0.0)
                }
                
                standings_entries.append(standings_entry)
            
            # 批量保存到数据库
            if standings_entries:
                success = self.loader.load_league_standings_batch(standings_entries)
                if success:
                    print(f"✅ 成功保存 {len(standings_entries)} 支团队的赛季数据")
                    return True
                else:
                    print("❌ 保存团队赛季数据失败")
                    return False
            else:
                print("❌ 没有找到有效的团队赛季数据")
                return False
            
        except Exception as e:
            print(f"处理联盟排名数据失败: {e}")
            return False