"""
Yahoo Fantasy API数据获取器
包含所有 fetch_* 函数，从 archive/yahoo_api_data.py 迁移
"""

import time
from typing import Optional, Dict, List
from datetime import date, datetime, timedelta
from .client import APIClient

"""
fetch_* 函数 - API 数据获取
✅ 网络请求：直接调用 Yahoo API
✅ 外部接口：公开方法，供其他模块调用
✅ 原始数据：返回 API 的原始响应数据
✅ IO 操作：涉及网络 I/O，可能失败

_extract_* 函数 - 数据解析提取
❌ 无网络请求：纯数据处理函数
🔒 内部辅助：私有方法（下划线前缀），不对外暴露
🎯 结构化数据：从原始 API 数据中提取特定信息
⚡ 高效稳定：无 I/O 操作，只有数据处理

"""

class YahooFantasyFetcher:
    """Yahoo Fantasy API数据获取器"""
    
    def __init__(self, delay: int = 2):
        """初始化获取器"""
        self.api_client = APIClient(delay)
        self.delay = delay
        # 这些属性需要由调用者设置
        self.selected_league = None
        self.db_writer = None
        
    def wait(self, message: Optional[str] = None) -> None:
        """等待指定时间"""
        if message:
            print(f"⏳ {message}")
        time.sleep(self.delay)
        
    # ============================================================================
    # 基础API获取函数
    # ============================================================================
    
    def fetch_games_data(self) -> Optional[Dict]:
        """
        获取用户的games数据
        
        迁移自: archive/yahoo_api_data.py fetch_games_data() 第168行
        """
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = self.api_client.get_api_data(url)
        
        if data:
            return data
        return None
        
    def fetch_leagues_data(self, game_key: str) -> Optional[Dict]:
        """
        获取指定游戏的联盟数据
        
        迁移自: archive/yahoo_api_data.py fetch_leagues_data() 第177行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_league_settings(self, league_key: str) -> Optional[Dict]:
        """
        获取联盟设置
        
        迁移自: archive/yahoo_api_data.py _fetch_league_settings() 第354行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_all_leagues_data(self) -> Optional[Dict]:
        """
        获取所有联盟数据
        
        迁移自: archive/yahoo_api_data.py _fetch_all_leagues_data() 第135行
        注意：这个函数原本包含数据库写入逻辑，现在只负责获取数据
        """
        # 获取games数据
        games_data = self.fetch_games_data()
        if not games_data:
            return None
        
        # 提取游戏键并获取联盟数据
        game_keys = self._extract_game_keys(games_data)
        if not game_keys:
            return None
        
        all_leagues = {}
        for i, game_key in enumerate(game_keys):
            leagues_data = self.fetch_leagues_data(game_key)
            if leagues_data:
                extracted_leagues = self._extract_leagues_from_data(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
            
            if i < len(game_keys) - 1:
                self.wait()
        
        # 返回包含games_data和leagues_data的完整数据
        return {
            'games_data': games_data,
            'leagues_data': all_leagues
        }
    
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
            fantasy_content = data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_data = user_data[1]["games"]
            
            # 找到对应的游戏
            games_count = int(games_data.get("count", 0))
            target_game = None
            
            for i in range(games_count):
                str_index = str(i)
                if str_index in games_data:
                    game_container = games_data[str_index]
                    if "game" in game_container:
                        game_data = game_container["game"]
                        if isinstance(game_data, list) and len(game_data) > 1:
                            game_info = game_data[0]
                            if game_info.get("game_key") == game_key:
                                target_game = game_data[1]
                                break
            
            if not target_game or "leagues" not in target_game:
                return leagues
            
            leagues_container = target_game["leagues"]
            leagues_count = int(leagues_container.get("count", 0))
            
            for i in range(leagues_count):
                str_index = str(i)
                if str_index in leagues_container:
                    league_container = leagues_container[str_index]
                    if "league" in league_container:
                        league_data = league_container["league"]
                        if isinstance(league_data, list) and len(league_data) > 0:
                            league_info = league_data[0]
                            # 添加game_key到联盟信息中
                            league_info["game_key"] = game_key
                            leagues.append(league_info)
                            
        except Exception as e:
            pass
        
        return leagues
    
    # ============================================================================
    # 团队数据获取函数
    # ============================================================================
    
    def fetch_teams_data(self, league_key: str) -> Optional[Dict]:
        """
        获取团队数据
        
        迁移自: archive/yahoo_api_data.py fetch_teams_data() 第359行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_team_roster(self, team_key: str) -> Optional[Dict]:
        """
        获取单个团队的阵容
        
        迁移自: archive/yahoo_api_data.py _fetch_team_roster() 第514行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_team_roster_for_date(self, team_key: str, date_str: str) -> Optional[Dict]:
        """
        获取指定日期的团队阵容
        
        迁移自: archive/yahoo_api_data.py _fetch_team_roster_for_date() 第745行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date={date_str}?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_team_matchups(self, team_key: str) -> Optional[Dict]:
        """
        获取团队对战数据
        
        迁移自: archive/yahoo_api_data.py _fetch_team_matchups() 第1552行
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_team_rosters(self, teams_data: Dict, league_key: str, season_end_date: Optional[str] = None) -> List[Dict]:
        """
        获取所有团队的阵容数据
        
        迁移自: archive/yahoo_api_data.py fetch_team_rosters() 第468行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        from ..transformers.team import TeamTransformer
        
        team_transformer = TeamTransformer()
        team_keys = team_transformer.transform_team_keys_from_data(teams_data)
        
        if not team_keys:
            return []
        
        all_rosters = []
        
        for i, team_key in enumerate(team_keys):
            try:
                if season_end_date:
                    # 获取指定日期的roster（赛季结束日期）
                    roster_data = self.fetch_team_roster_for_date(team_key, season_end_date)
                else:
                    # 获取当前roster（API默认）
                    roster_data = self.fetch_team_roster(team_key)
                
                if roster_data:
                    all_rosters.append({
                        'team_key': team_key,
                        'roster_data': roster_data
                    })
            except Exception as e:
                pass
            
            # 简化等待
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        return all_rosters
        
    def fetch_team_rosters_for_date_range(self, teams_data: Dict, start_date: date, end_date: date) -> List[Dict]:
        """
        获取指定日期范围内的团队阵容数据
        
        迁移自: archive/yahoo_api_data.py fetch_team_rosters_for_date_range() 第708行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        from ..transformers.team import TeamTransformer
        
        team_transformer = TeamTransformer()
        team_keys = team_transformer.transform_team_keys_from_data(teams_data)
        
        if not team_keys:
            return []
        
        all_rosters = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            for i, team_key in enumerate(team_keys):
                try:
                    roster_data = self.fetch_team_roster_for_date(team_key, date_str)
                    if roster_data:
                        all_rosters.append({
                            'team_key': team_key,
                            'date': date_str,
                            'roster_data': roster_data
                        })
                except Exception as e:
                    pass
                
                # 团队间间隔
                if i < len(team_keys) - 1:
                    time.sleep(0.2)
            
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                self.wait()
        
        return all_rosters
    
    # ============================================================================
    # 球员数据获取函数
    # ============================================================================
    
    def fetch_all_league_players(self, league_key: str) -> List[Dict]:
        """
        使用分页逻辑获取所有球员
        
        迁移自: archive/yahoo_api_data.py _fetch_all_league_players() 第764行
        """
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
                
            players_data = self.api_client.get_api_data(url)
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
            
            # 找到players容器
            players_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "players" in item:
                        players_container = item["players"]
                        break
            
            if not players_container:
                return players
            
            players_count = int(players_container.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                if "player" not in player_container:
                    continue
                
                player_info_list = player_container["player"]
                if not isinstance(player_info_list, list) or len(player_info_list) == 0:
                    continue
                
                # 合并player信息
                player_info = {}
                for item in player_info_list:
                    if isinstance(item, dict):
                        player_info.update(item)
                
                if player_info:
                    players.append(player_info)
                        
        except Exception as e:
            pass
        
        return players
        
    def fetch_complete_players_data(self, league_key: str) -> List[Dict]:
        """
        获取完整的球员数据
        
        迁移自: archive/yahoo_api_data.py fetch_complete_players_data() 第750行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        # 获取所有球员的基础信息
        all_players = self.fetch_all_league_players(league_key)
        return all_players
        
    def fetch_player_season_stats(self, players: List, league_key: str, season: str) -> List[Dict]:
        """
        获取球员赛季统计数据
        
        迁移自: archive/yahoo_api_data.py _fetch_player_season_stats() 第1930行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        all_stats = []
        batch_size = 25
        
        # 分批处理球员
        for i in range(0, len(players), batch_size):
            batch_players = players[i:i + batch_size]
            
            # 构建批量查询URL
            player_keys = [player.player_key for player in batch_players]
            players_param = ",".join(player_keys)
            
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={players_param}/stats;type=season?format=json"
            
            stats_data = self.api_client.get_api_data(url)
            if stats_data:
                all_stats.append({
                    'batch_index': i // batch_size,
                    'stats_data': stats_data
                })
            
            # 批次间延迟
            if i + batch_size < len(players):
                time.sleep(1)
        
        return all_stats
        
    def fetch_player_season_stats_direct(self, league_key: str) -> List[Dict]:
        """
        直接获取球员赛季统计数据
        
        迁移自: archive/yahoo_api_data.py _fetch_player_season_stats_direct() 第1907行
        注意：这个函数需要配合数据库查询球员列表，暂时返回空列表
        """
        # TODO: 这个函数需要从数据库获取球员列表，需要重新设计
        return []
        
    def fetch_player_daily_stats_for_range(self, players: List, league_key: str, season: str, 
                                         start_date: date, end_date: date) -> List[Dict]:
        """
        获取指定范围的球员日统计
        
        迁移自: archive/yahoo_api_data.py _fetch_player_daily_stats_for_range() 第2380行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        all_daily_stats = []
        batch_size = 25
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 分批处理球员
            for i in range(0, len(players), batch_size):
                batch_players = players[i:i + batch_size]
                
                # 构建批量查询URL
                player_keys = [player.player_key for player in batch_players]
                players_param = ",".join(player_keys)
                
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={players_param}/stats;type=date;date={date_str}?format=json"
                
                stats_data = self.api_client.get_api_data(url)
                if stats_data:
                    all_daily_stats.append({
                        'date': date_str,
                        'batch_index': i // batch_size,
                        'stats_data': stats_data
                    })
                
                # 批次间延迟
                if i + batch_size < len(players):
                    time.sleep(0.5)
            
            current_date += timedelta(days=1)
            
            # 只有在处理多天数据时才等待
            if current_date <= end_date:
                time.sleep(1)
        
        return all_daily_stats
        
    def fetch_player_stats_data(self, league_key: str, season: str, start_date: date, end_date: date, 
                               include_season_stats: bool = True) -> Dict:
        """
        获取球员统计数据（主要函数）
        
        迁移自: archive/yahoo_api_data.py fetch_player_stats_data() 第2332行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        result = {
            'season_stats': [],
            'daily_stats': []
        }
        
        # 首先获取球员列表
        players = self.fetch_all_league_players(league_key)
        if not players:
            return result
        
        # 获取赛季统计
        if include_season_stats:
            result['season_stats'] = self.fetch_player_season_stats(players, league_key, season)
        
        # 获取日统计
        result['daily_stats'] = self.fetch_player_daily_stats_for_range(players, league_key, season, start_date, end_date)
        
        return result
    
    # ============================================================================
    # 交易数据获取函数
    # ============================================================================
    
    def fetch_all_league_transactions(self, league_key: str, max_count: int = None) -> List[Dict]:
        """
        获取联盟所有交易记录（分页处理）
        
        迁移自: archive/yahoo_api_data.py _fetch_all_league_transactions() 第897行
        """
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
            
            transactions_data = self.api_client.get_api_data(url)
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
            
            # 找到transactions容器
            transactions_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            
            if not transactions_container:
                return transactions
            
            transactions_count = int(transactions_container.get("count", 0))
            
            for i in range(transactions_count):
                str_index = str(i)
                if str_index not in transactions_container:
                    continue
                
                transaction_container = transactions_container[str_index]
                if "transaction" not in transaction_container:
                    continue
                
                transaction_info = transaction_container["transaction"]
                
                # 处理不同格式的transaction数据
                if isinstance(transaction_info, list):
                    # 合并列表中的所有字典
                    merged_transaction = {}
                    for item in transaction_info:
                        if isinstance(item, dict):
                            merged_transaction.update(item)
                    if merged_transaction:
                        transactions.append(merged_transaction)
                elif isinstance(transaction_info, dict):
                    transactions.append(transaction_info)
        
        except Exception as e:
            pass
        
        return transactions
        
    def fetch_complete_transactions_data(self, league_key: str, teams_data: Optional[Dict] = None) -> List[Dict]:
        """
        获取完整的交易数据
        
        迁移自: archive/yahoo_api_data.py fetch_complete_transactions_data() 第880行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        # 获取所有transactions
        all_transactions = self.fetch_all_league_transactions(league_key)
        return all_transactions
    
    # ============================================================================
    # 团队统计获取函数
    # ============================================================================
    
    def fetch_and_process_league_standings(self, league_key: str, season: str) -> Optional[Dict]:
        """
        获取联盟排名数据
        
        迁移自: archive/yahoo_api_data.py _fetch_and_process_league_standings() 第1033行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        return self.api_client.get_api_data(url)
        
    def fetch_team_stats_data(self, league_key: str, season: str, teams_data: Optional[Dict] = None) -> Dict:
        """
        获取团队统计数据
        
        迁移自: archive/yahoo_api_data.py fetch_team_stats_data() 第992行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        result = {
            'standings_data': None,
            'matchups_data': []
        }
        
        # 获取团队列表
        if teams_data is None:
            teams_data = self.fetch_teams_data(league_key)
            if not teams_data:
                return result
        
        from ..transformers.team import TeamTransformer
        team_transformer = TeamTransformer()
        team_keys = team_transformer.transform_team_keys_from_data(teams_data)
        
        if not team_keys:
            return result
        
        # 1. 获取league standings数据
        result['standings_data'] = self.fetch_and_process_league_standings(league_key, season)
        
        # 2. 获取每个团队的matchups数据
        for i, team_key in enumerate(team_keys):
            try:
                # 获取团队matchups数据
                matchups_data = self.fetch_team_matchups(team_key)
                if matchups_data:
                    result['matchups_data'].append({
                        'team_key': team_key,
                        'matchups_data': matchups_data
                    })
                        
            except Exception as e:
                pass
            
            # 请求间隔
            if i < len(team_keys) - 1:
                time.sleep(0.5)
        
        return result
        
    def fetch_team_weekly_stats_from_matchups(self, league_key: str, season: str) -> List[Dict]:
        """
        从对战数据生成团队周统计
        
        迁移自: archive/yahoo_api_data.py fetch_team_weekly_stats_from_matchups() 第2439行
        注意：这个函数主要是从数据库处理数据，这里返回空列表，实际逻辑需要在处理层实现
        """
        # TODO: 这个函数主要是从数据库的matchups表生成周统计，不涉及API获取
        # 实际逻辑应该在处理层实现
        return []
        
    def fetch_team_season_stats_from_standings(self, league_key: str, season: str) -> List[Dict]:
        """
        从联盟排名生成团队赛季统计
        
        迁移自: archive/yahoo_api_data.py fetch_team_season_stats_from_standings() 第2589行
        注意：这个函数主要是从数据库处理数据，这里返回空列表，实际逻辑需要在处理层实现
        """
        # TODO: 这个函数主要是从数据库的standings表生成赛季统计，不涉及API获取
        # 实际逻辑应该在处理层实现
        return []
    
    # ============================================================================
    # 综合数据获取函数 (协调器)
    # ============================================================================
    
    def fetch_complete_league_data(self, league_key: str, season: str) -> Dict:
        """
        获取完整的联盟数据（主要协调器）
        
        迁移自: archive/yahoo_api_data.py fetch_complete_league_data() 第294行
        注意：原函数包含数据库写入逻辑和用户交互，现在只负责获取数据
        """
        result = {
            'league_details': None,
            'teams_data': None,
            'players_data': [],
            'transactions_data': [],
            'team_stats_data': {},
            'rosters_data': []
        }
        
        print(f"\n=== 获取联盟数据: {league_key} ===")
        
        # 1. 获取联盟详细信息
        result['league_details'] = self.fetch_league_settings(league_key)
        
        # 2. 获取完整球员数据（优先获取，为后续步骤提供依赖）
        result['players_data'] = self.fetch_complete_players_data(league_key)
        
        # 3. 获取团队数据
        result['teams_data'] = self.fetch_teams_data(league_key)
        
        # 4. 获取团队阵容数据
        if result['teams_data']:
            result['rosters_data'] = self.fetch_team_rosters(result['teams_data'], league_key)
        
        # 5. 获取transaction数据
        result['transactions_data'] = self.fetch_complete_transactions_data(league_key, result['teams_data'])
        
        # 6. 获取团队统计数据（包括联盟排名、团队对战等）
        result['team_stats_data'] = self.fetch_team_stats_data(league_key, season, result['teams_data'])
            
        print(f"\n✓ 联盟数据获取完成")
        return result
        
    def fetch_league_details(self, league_key: str) -> Optional[Dict]:
        """
        获取联盟详细信息
        
        迁移自: archive/yahoo_api_data.py fetch_league_details() 第338行
        注意：原函数包含数据库写入逻辑，现在只负责获取数据
        """
        # 获取联盟设置数据
        settings_data = self.fetch_league_settings(league_key)
        return settings_data
        
    def fetch_season_schedule_data(self, league_key: str, start_date: str, end_date: str) -> List[Dict]:
        """
        生成赛季日程数据
        
        迁移自: archive/yahoo_api_data.py fetch_season_schedule_data() 第2184行
        注意：这个函数主要是生成日期序列，不涉及API调用
        """
        # TODO: 这个函数主要是生成日期维度数据，不涉及API获取
        # 实际逻辑应该在处理层实现
        return []
        
    def fetch_roster_history_data(self, teams_data: Dict, start_date: date, end_date: date) -> List[Dict]:
        """
        获取指定日期范围的阵容历史数据
        
        迁移自: archive/yahoo_api_data.py fetch_roster_history_data() 第2249行
        """
        return self.fetch_team_rosters_for_date_range(teams_data, start_date, end_date)
        
    def fetch_and_select_league(self, use_existing_data: bool = False) -> Optional[Dict]:
        """
        获取基础数据并选择联盟
        
        迁移自: archive/yahoo_api_data.py fetch_and_select_league() 第46行
        注意：这个函数包含用户交互逻辑，这里只返回联盟数据，不处理选择逻辑
        """
        print("🚀 获取联盟数据...")
        
        # 获取所有联盟数据
        leagues_data = self.fetch_all_leagues_data()
        
        return leagues_data