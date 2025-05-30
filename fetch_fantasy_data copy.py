#!/usr/bin/env python3
"""
Yahoo Fantasy数据获取工具
统一的数据获取脚本，整合游戏和联盟数据获取功能
"""
import os
import sys
import time
import argparse
from datetime import datetime
import copy

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import (
    get_api_data, save_json_data, load_json_data, 
    GAMES_DIR, LEAGUES_DIR, LEAGUE_SETTINGS_DIR, 
    LEAGUE_STANDINGS_DIR, LEAGUE_SCOREBOARDS_DIR, TEAMS_DIR,
    TEAM_ROSTERS_DIR, PLAYERS_DIR, PLAYER_STATS_DIR,
    ensure_league_roster_directory, get_league_key_from_team_key,
    ensure_league_player_stats_directory, ensure_league_players_directory,
    print_data_overview
)

class FantasyDataFetcher:
    """Yahoo Fantasy数据获取器"""
    
    def __init__(self, delay=2):
        """初始化数据获取器
        
        Args:
            delay: 请求间隔时间（秒）
        """
        self.delay = delay
        
    def fetch_games_data(self):
        """获取用户的games数据"""
        print("获取用户的games数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        data = get_api_data(url)
        
        if data:
            print("成功获取用户的games数据")
            # 保存到games子文件夹
            save_json_data(data, GAMES_DIR / "games_data.json")
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
            
            if "users" not in fantasy_content or "0" not in fantasy_content["users"]:
                print("游戏数据格式不正确，缺少users字段")
                return game_keys
                
            user_data = fantasy_content["users"]["0"]["user"]
            
            if len(user_data) < 2 or "games" not in user_data[1]:
                print("游戏数据格式不正确，缺少games字段")
                return game_keys
                
            games_container = user_data[1]["games"]
            games_count = int(games_container.get("count", 0))
            
            print(f"找到 {games_count} 个游戏")
            
            if games_count == 0:
                print("用户没有参与任何游戏")
                return game_keys
            
            # 提取每个游戏的game_key，只包含type为"full"的游戏
            for i in range(games_count):
                str_index = str(i)
                
                if str_index not in games_container:
                    print(f"找不到游戏索引 {str_index}")
                    continue
                    
                game_container = games_container[str_index]
                
                if "game" not in game_container:
                    print(f"游戏 {str_index} 数据不包含game字段")
                    continue
                
                game_data = game_container["game"]
                
                # 提取game_key和type
                if isinstance(game_data, list) and len(game_data) > 0 and isinstance(game_data[0], dict):
                    game_info = game_data[0]
                    game_key = game_info.get("game_key")
                    game_type = game_info.get("type")
                    game_name = game_info.get("name", "Unknown")
                    
                    if game_key:
                        if game_type == "full":
                            game_keys.append(game_key)
                            print(f"提取到游戏键: {game_key} (类型: {game_type}, 名称: {game_name})")
                        else:
                            print(f"跳过游戏: {game_key} (类型: {game_type}, 名称: {game_name}) - 不是full类型")
                
        except Exception as e:
            print(f"提取游戏键时出错: {str(e)}")
        
        print(f"总共提取了 {len(game_keys)} 个full类型的游戏键")
        return game_keys
    
    def fetch_leagues_data(self, game_key):
        """获取指定game下用户的leagues数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"成功获取game {game_key}的leagues数据")
            return data
        return None
    
    def fetch_league_settings(self, league_key):
        """获取指定联盟的设置数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"成功获取联盟 {league_key} 的设置数据")
            return data
        return None
    
    def fetch_league_standings(self, league_key):
        """获取指定联盟的排名数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"成功获取联盟 {league_key} 的排名数据")
            return data
        return None
    
    def fetch_league_scoreboard(self, league_key):
        """获取指定联盟的记分板数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"成功获取联盟 {league_key} 的记分板数据")
            return data
        return None
    
    def fetch_teams_data(self, league_key):
        """获取指定league下的teams数据"""
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        data = get_api_data(url)
        
        if data:
            print(f"成功获取league {league_key}的teams数据")
            return data
        return None
    
    def fetch_team_roster(self, team_key, week=None, date=None):
        """获取指定团队的球员名单
        
        Args:
            team_key: 团队键
            week: NFL游戏的周次（可选）
            date: MLB/NBA/NHL游戏的日期，格式YYYY-MM-DD（可选）
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster"
        
        # 添加参数
        params = []
        if week is not None:
            params.append(f"week={week}")
        if date is not None:
            params.append(f"date={date}")
        
        if params:
            url += ";" + ";".join(params)
        
        url += "?format=json"
        
        data = get_api_data(url)
        
        if data:
            print(f"成功获取团队 {team_key} 的球员名单")
            return data
        return None
    
    def fetch_league_players(self, league_key, position=None, status=None, search=None, 
                           sort=None, sort_type=None, start=0, count=25):
        """获取联盟中的球员信息
        
        Args:
            league_key: 联盟键
            position: 位置过滤（如QB, RB等）
            status: 状态过滤（A=所有, FA=自由球员, W=豁免, T=已被选中, K=保留球员）
            search: 球员姓名搜索
            sort: 排序方式
            sort_type: 排序类型
            start: 起始位置
            count: 返回数量
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players"
        
        # 添加过滤参数
        params = []
        if position:
            params.append(f"position={position}")
        if status:
            params.append(f"status={status}")
        if search:
            params.append(f"search={search}")
        if sort:
            params.append(f"sort={sort}")
        if sort_type:
            params.append(f"sort_type={sort_type}")
        if start > 0:
            params.append(f"start={start}")
        if count != 25:
            params.append(f"count={count}")
        
        if params:
            url += ";" + ";".join(params)
        
        url += "?format=json"
        
        data = get_api_data(url)
        
        if data:
            print(f"成功获取联盟 {league_key} 的球员信息")
            return data
        return None
    
    def fetch_player_stats(self, league_key, player_keys, stats_type="season", week=None, date=None):
        """获取球员统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表或单个球员键
            stats_type: 统计类型（season, week, date等）
            week: 周次（NFL）
            date: 日期（MLB/NBA/NHL）
        """
        # 处理球员键
        if isinstance(player_keys, list):
            player_keys_str = ",".join(player_keys)
        else:
            player_keys_str = player_keys
        
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/players;player_keys={player_keys_str}/stats"
        
        # 添加统计参数
        params = [f"type={stats_type}"]
        if week is not None:
            params.append(f"week={week}")
        if date is not None:
            params.append(f"date={date}")
        
        url += ";" + ";".join(params)
        url += "?format=json"
        
        data = get_api_data(url)
        
        if data:
            print(f"成功获取球员统计数据")
            return data
        return None
    
    def save_league_data(self, game_key, leagues_data):
        """保存联盟数据到文件"""
        try:
            file_path = LEAGUES_DIR / f"league_data_{game_key}.json"
            return save_json_data(leagues_data, file_path)
        except Exception as e:
            print(f"保存游戏 {game_key} 的联盟数据时出错: {str(e)}")
            return False
    
    def save_league_details(self, league_key, settings_data=None, standings_data=None, scoreboard_data=None):
        """保存联盟详细数据到文件"""
        try:
            success = True
            
            if settings_data:
                settings_path = LEAGUE_SETTINGS_DIR / f"league_settings_{league_key}.json"
                if not save_json_data(settings_data, settings_path):
                    success = False
            
            if standings_data:
                standings_path = LEAGUE_STANDINGS_DIR / f"league_standings_{league_key}.json"
                if not save_json_data(standings_data, standings_path):
                    success = False
            
            if scoreboard_data:
                scoreboard_path = LEAGUE_SCOREBOARDS_DIR / f"league_scoreboard_{league_key}.json"
                if not save_json_data(scoreboard_data, scoreboard_path):
                    success = False
            
            return success
        except Exception as e:
            print(f"保存联盟 {league_key} 的详细数据时出错: {str(e)}")
            return False
    
    def save_teams_data(self, league_key, teams_data):
        """保存团队数据到文件"""
        try:
            teams_file_path = TEAMS_DIR / f"teams_data_{league_key}.json"
            return save_json_data(teams_data, teams_file_path)
        except Exception as e:
            print(f"保存联盟 {league_key} 的团队数据时出错: {str(e)}")
            return False
    
    def save_team_roster(self, team_key, roster_data, week=None, date=None):
        """保存团队球员名单数据到文件"""
        try:
            # 从团队键中提取联盟键
            league_key = get_league_key_from_team_key(team_key)
            if not league_key:
                print(f"无法从团队键 {team_key} 中提取联盟键")
                return False
            
            # 确保联盟roster目录存在
            league_roster_dir = ensure_league_roster_directory(league_key)
            
            # 构建文件名
            filename = f"team_roster_{team_key}"
            if week is not None:
                filename += f"_week{week}"
            if date is not None:
                filename += f"_{date}"
            filename += ".json"
            
            # 保存到联盟子文件夹中
            roster_file_path = league_roster_dir / filename
            return save_json_data(roster_data, roster_file_path)
        except Exception as e:
            print(f"保存团队 {team_key} 的球员名单数据时出错: {str(e)}")
            return False
    
    def save_league_players(self, league_key, players_data, suffix=""):
        """保存联盟球员数据到文件（使用新的分离存储结构）"""
        try:
            # 从联盟数据中提取season信息
            season = self.extract_season_from_league_data(players_data)
            if not season:
                print(f"⚠️  无法从联盟数据中提取season信息，使用默认值")
                season = "unknown"
            
            # 使用新的分离保存方法
            return self.save_separated_player_data(league_key, players_data, season)
        except Exception as e:
            print(f"保存联盟 {league_key} 的球员数据时出错: {str(e)}")
            return False
    
    def extract_season_from_league_data(self, players_data):
        """从联盟球员数据中提取season信息"""
        try:
            if "fantasy_content" not in players_data:
                return None
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 提取联盟基本信息中的season
            if isinstance(league_data, list) and len(league_data) > 0:
                league_basic = league_data[0]
                return league_basic.get("season")
            
            return None
        except Exception as e:
            print(f"提取season信息时出错: {e}")
            return None
    
    def save_player_stats(self, league_key, stats_data, stats_type="season", week=None, date=None):
        """保存球员统计数据到文件"""
        try:
            # 构建文件名
            filename = f"player_stats_{league_key}_{stats_type}"
            if week is not None:
                filename += f"_week{week}"
            if date is not None:
                filename += f"_{date}"
            filename += ".json"
            
            stats_file_path = PLAYER_STATS_DIR / filename
            return save_json_data(stats_data, stats_file_path)
        except Exception as e:
            print(f"保存球员统计数据时出错: {str(e)}")
            return False
    
    def extract_leagues_from_data(self, data, game_key):
        """从API返回的数据中提取联盟信息"""
        leagues = []
        
        try:
            # 检查数据格式
            if not data or "fantasy_content" not in data:
                print(f"游戏 {game_key} 的数据格式不正确")
                return leagues
            
            # 检查是否有错误信息
            if "error" in data:
                error_msg = data.get("error", {}).get("description", "未知错误")
                print(f"文件 league_data_{game_key}.json 包含错误: {error_msg}")
                # 检查是否是不支持leagues查询的错误
                if "Invalid subresource leagues requested" in error_msg:
                    print("跳过不支持leagues查询的游戏类型")
                return leagues
            
            fantasy_content = data["fantasy_content"]
            
            if "users" not in fantasy_content or "0" not in fantasy_content["users"]:
                print(f"游戏 {game_key} 的数据格式不正确，缺少users字段")
                return leagues
            
            user_data = fantasy_content["users"]["0"]["user"]
            
            if len(user_data) < 2 or "games" not in user_data[1]:
                print(f"游戏 {game_key} 的数据格式不正确，缺少games字段")
                return leagues
            
            games_container = user_data[1]["games"]
            
            # 查找包含当前game_key的游戏
            for i in range(int(games_container.get("count", 0))):
                str_index = str(i)
                
                if str_index not in games_container:
                    continue
                
                game_container = games_container[str_index]
                
                if "game" not in game_container:
                    continue
                
                game_data = game_container["game"]
                
                # 检查是否为我们要找的游戏
                current_game_key = None
                if isinstance(game_data, list) and len(game_data) > 0 and isinstance(game_data[0], dict):
                    current_game_key = game_data[0].get("game_key")
                
                if current_game_key != game_key:
                    continue
                
                # 找到了匹配的游戏，提取联盟数据
                if len(game_data) > 1 and "leagues" in game_data[1]:
                    leagues_container = game_data[1]["leagues"]
                    leagues_count = int(leagues_container.get("count", 0))
                    
                    print(f"游戏 {game_key} 有 {leagues_count} 个联盟")
                    
                    # 提取每个联盟的数据
                    for j in range(leagues_count):
                        str_league_index = str(j)
                        
                        if str_league_index not in leagues_container:
                            continue
                        
                        league_container = leagues_container[str_league_index]
                        
                        if "league" not in league_container:
                            continue
                        
                        league_data = league_container["league"]
                        
                        # 提取联盟的基本信息
                        league_info = {}
                        if isinstance(league_data, list):
                            for item in league_data:
                                if isinstance(item, dict):
                                    league_info.update(item)
                        
                        leagues.append(league_info)
                else:
                    print(f"游戏 {game_key} 没有联盟数据")
                
                # 已经找到并处理了匹配的游戏，可以退出循环
                break
        
        except Exception as e:
            print(f"提取游戏 {game_key} 的联盟数据时出错: {str(e)}")
        
        return leagues
    
    def wait(self, message=None):
        """等待指定时间"""
        if message:
            print(f"{message}，等待 {self.delay} 秒...")
        else:
            print(f"等待 {self.delay} 秒...")
        time.sleep(self.delay)
    
    def fetch_all_games(self):
        """获取所有游戏数据"""
        print("开始获取所有游戏数据...")
        return self.fetch_games_data()
    
    def fetch_all_leagues(self):
        """获取所有游戏的联盟数据"""
        print("开始获取所有联盟数据...")
        
        # 获取游戏数据
        games_data = self.fetch_games_data()
        
        if not games_data:
            print("无法获取游戏数据")
            return False
        
        # 提取游戏键
        game_keys = self.extract_game_keys(games_data)
        
        if not game_keys:
            print("未找到任何游戏键")
            return False
        
        print(f"找到 {len(game_keys)} 个游戏键")
        
        # 获取每个游戏的联盟数据
        success_count = 0
        for i, game_key in enumerate(game_keys):
            print(f"\n处理游戏 {i+1}/{len(game_keys)}: {game_key}")
            
            leagues_data = self.fetch_leagues_data(game_key)
            if leagues_data and self.save_league_data(game_key, leagues_data):
                success_count += 1
            
            # 避免请求过于频繁，添加延迟
            if i < len(game_keys) - 1:
                self.wait()
        
        print(f"\n所有联盟数据获取完成! 成功: {success_count}/{len(game_keys)}")
        
        # 生成包含所有联盟数据的汇总文件
        self.consolidate_league_data()
        return success_count > 0
    
    def consolidate_league_data(self):
        """整合所有联盟数据到一个文件"""
        all_leagues = {}
        
        try:
            # 从leagues子文件夹获取所有联盟数据文件
            league_files = list(LEAGUES_DIR.glob("league_data_*.json"))
            
            if not league_files:
                print("未找到任何联盟数据文件")
                return
            
            print(f"找到 {len(league_files)} 个联盟数据文件，开始整合...")
            
            # 处理每个文件
            for file_path in league_files:
                try:
                    data = load_json_data(file_path)
                    if not data:
                        continue
                    
                    game_key = file_path.stem.replace("league_data_", "")
                    
                    # 提取联盟数据
                    extracted_leagues = self.extract_leagues_from_data(data, game_key)
                    
                    if extracted_leagues:
                        all_leagues[game_key] = extracted_leagues
                except Exception as e:
                    print(f"处理文件 {file_path} 时出错: {str(e)}")
            
            # 保存整合的数据到leagues子文件夹
            output_file = LEAGUES_DIR / "all_leagues_data.json"
            save_json_data(all_leagues, output_file)
        
        except Exception as e:
            print(f"整合联盟数据时出错: {str(e)}")
    
    def fetch_all_league_details(self):
        """获取所有联盟的详细数据"""
        # 首先加载联盟汇总数据
        all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not all_leagues_file.exists():
            print("找不到联盟汇总数据文件，请先运行获取联盟基本数据")
            return False
        
        all_leagues = load_json_data(all_leagues_file)
        
        league_keys = []
        for game_key, leagues in all_leagues.items():
            for league in leagues:
                if "league_key" in league:
                    league_keys.append(league["league_key"])
        
        print(f"找到 {len(league_keys)} 个联盟，开始获取详细数据")
        
        success_count = 0
        for i, league_key in enumerate(league_keys):
            print(f"\n处理联盟 {i+1}/{len(league_keys)}: {league_key}")
            
            # 获取联盟详细数据
            settings_data = self.fetch_league_settings(league_key)
            standings_data = self.fetch_league_standings(league_key)
            scoreboard_data = self.fetch_league_scoreboard(league_key)
            
            # 保存数据
            if self.save_league_details(league_key, settings_data, standings_data, scoreboard_data):
                success_count += 1
            
            # 避免请求过于频繁，添加延迟
            if i < len(league_keys) - 1:
                self.wait()
        
        print(f"\n所有联盟详细数据获取完成! 成功: {success_count}/{len(league_keys)}")
        return success_count > 0
    
    def fetch_all_teams_data(self):
        """获取所有联盟的团队数据"""
        # 首先加载联盟汇总数据
        all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not all_leagues_file.exists():
            print("找不到联盟汇总数据文件，请先运行获取联盟基本数据")
            return False
        
        all_leagues = load_json_data(all_leagues_file)
        
        league_keys = []
        for game_key, leagues in all_leagues.items():
            for league in leagues:
                if "league_key" in league:
                    league_keys.append(league["league_key"])
        
        print(f"找到 {len(league_keys)} 个联盟，开始获取团队数据")
        
        success_count = 0
        for i, league_key in enumerate(league_keys):
            print(f"\n处理联盟 {i+1}/{len(league_keys)}: {league_key}")
            
            teams_data = self.fetch_teams_data(league_key)
            if teams_data and self.save_teams_data(league_key, teams_data):
                success_count += 1
            
            # 避免请求过于频繁，添加延迟
            if i < len(league_keys) - 1:
                self.wait()
        
        print(f"\n所有团队数据获取完成! 成功: {success_count}/{len(league_keys)}")
        return success_count > 0
    
    def fetch_all_team_rosters(self, sample_only=False, max_teams=5):
        """获取团队球员名单数据
        
        Args:
            sample_only: 是否只获取样本数据，默认False（获取所有）
            max_teams: 最大获取团队数量（当sample_only=True时）
        """
        print("开始获取团队球员名单数据...")
        
        # 从团队数据文件中提取团队键
        team_keys = []
        teams_files = list(TEAMS_DIR.glob("teams_data_*.json"))
        
        if not teams_files:
            print("未找到任何团队数据文件，请先获取团队数据")
            return False
        
        print(f"找到 {len(teams_files)} 个团队数据文件")
        
        for teams_file in teams_files:
            teams_data = load_json_data(teams_file)
            if teams_data:
                # 提取团队键
                extracted_teams = self.extract_team_keys_from_data(teams_data)
                team_keys.extend(extracted_teams)
                print(f"从 {teams_file.name} 提取了 {len(extracted_teams)} 个团队键")
        
        if not team_keys:
            print("未找到任何团队键，请检查团队数据文件格式")
            return False
        
        print(f"总共找到 {len(team_keys)} 个团队")
        
        # 如果只获取样本，限制数量
        if sample_only:
            team_keys = team_keys[:max_teams]
            print(f"样本模式：只获取前 {len(team_keys)} 个团队的球员名单")
        else:
            print(f"完整模式：获取所有 {len(team_keys)} 个团队的球员名单")
        
        success_count = 0
        failed_teams = []
        
        for i, team_key in enumerate(team_keys):
            print(f"\n处理团队 {i+1}/{len(team_keys)}: {team_key}")
            
            try:
                roster_data = self.fetch_team_roster(team_key)
                if roster_data and self.save_team_roster(team_key, roster_data):
                    success_count += 1
                    print(f"✓ 成功获取并保存团队 {team_key} 的球员名单")
                else:
                    failed_teams.append(team_key)
                    print(f"✗ 获取团队 {team_key} 的球员名单失败")
            except Exception as e:
                failed_teams.append(team_key)
                print(f"✗ 获取团队 {team_key} 的球员名单时出错: {str(e)}")
            
            # 避免请求过于频繁，添加延迟
            if i < len(team_keys) - 1:
                self.wait()
        
        print(f"\n=== 团队球员名单数据获取完成 ===")
        print(f"成功: {success_count}/{len(team_keys)}")
        print(f"失败: {len(failed_teams)}/{len(team_keys)}")
        
        if failed_teams:
            print(f"失败的团队: {', '.join(failed_teams[:10])}")  # 只显示前10个
            if len(failed_teams) > 10:
                print(f"... 还有 {len(failed_teams) - 10} 个失败的团队")
        
        return success_count > 0
    
    def fetch_all_league_players(self, sample_only=True, max_leagues=3):
        """获取联盟球员数据
        
        Args:
            sample_only: 是否只获取样本数据
            max_leagues: 最大获取联盟数量（当sample_only=True时）
        """
        print("开始获取联盟球员数据...")
        
        # 首先加载联盟汇总数据
        all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not all_leagues_file.exists():
            print("找不到联盟汇总数据文件，请先运行获取联盟基本数据")
            return False
        
        all_leagues = load_json_data(all_leagues_file)
        
        league_keys = []
        for game_key, leagues in all_leagues.items():
            for league in leagues:
                if "league_key" in league:
                    league_keys.append(league["league_key"])
        
        # 如果只获取样本，限制数量
        if sample_only:
            league_keys = league_keys[:max_leagues]
            print(f"样本模式：只获取前 {len(league_keys)} 个联盟的球员数据")
        
        success_count = 0
        for i, league_key in enumerate(league_keys):
            print(f"\n处理联盟 {i+1}/{len(league_keys)}: {league_key}")
            
            # 获取所有球员（分页获取）
            all_players_data = []
            start = 0
            count = 25
            
            while True:
                players_data = self.fetch_league_players(
                    league_key, 
                    status="A",  # 获取所有可用球员
                    start=start, 
                    count=count
                )
                
                if not players_data:
                    break
                
                all_players_data.append(players_data)
                
                # 检查是否还有更多数据
                # 这里需要根据实际返回的数据结构来判断
                # 暂时只获取第一页作为样本
                break
            
            # 保存数据
            if all_players_data:
                # 如果有多页数据，可以合并或分别保存
                for j, data in enumerate(all_players_data):
                    suffix = f"page{j+1}" if len(all_players_data) > 1 else ""
                    if self.save_league_players(league_key, data, suffix):
                        success_count += 1
            
            # 避免请求过于频繁，添加延迟
            if i < len(league_keys) - 1:
                self.wait()
        
        print(f"\n联盟球员数据获取完成! 成功: {success_count}")
        return success_count > 0
    
    def extract_team_keys_from_data(self, teams_data):
        """从团队数据中提取团队键"""
        team_keys = []
        
        try:
            if not teams_data or "fantasy_content" not in teams_data:
                return team_keys
            
            fantasy_content = teams_data["fantasy_content"]
            
            if "league" not in fantasy_content:
                return team_keys
            
            league_data = fantasy_content["league"]
            
            # league是一个数组，团队数据在索引1的位置
            teams_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                # 查找包含teams的元素
                for item in league_data:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(league_data, dict) and "teams" in league_data:
                teams_container = league_data["teams"]
            
            if not teams_container:
                return team_keys
            
            teams_count = int(teams_container.get("count", 0))
            
            # 提取每个团队的team_key
            for i in range(teams_count):
                str_index = str(i)
                
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                
                if "team" not in team_container:
                    continue
                
                team_data = team_container["team"]
                
                # 团队数据结构: team: [[{team_key: ...}, ...]]
                # team_key在team[0][0]位置
                if (isinstance(team_data, list) and 
                    len(team_data) > 0 and 
                    isinstance(team_data[0], list) and 
                    len(team_data[0]) > 0 and 
                    isinstance(team_data[0][0], dict)):
                    
                    team_key = team_data[0][0].get("team_key")
                    if team_key:
                        team_keys.append(team_key)
                        print(f"  提取到团队键: {team_key}")
        
        except Exception as e:
            print(f"提取团队键时出错: {str(e)}")
        
        return team_keys
    
    def fetch_single_game_leagues(self, game_key):
        """获取指定游戏的联盟数据"""
        print(f"获取游戏 {game_key} 的联盟数据...")
        
        leagues_data = self.fetch_leagues_data(game_key)
        if leagues_data:
            return self.save_league_data(game_key, leagues_data)
        return False
    
    def fetch_single_league_details(self, league_key):
        """获取指定联盟的详细数据"""
        print(f"获取联盟 {league_key} 的详细数据...")
        
        settings_data = self.fetch_league_settings(league_key)
        standings_data = self.fetch_league_standings(league_key)
        scoreboard_data = self.fetch_league_scoreboard(league_key)
        
        return self.save_league_details(league_key, settings_data, standings_data, scoreboard_data)
    
    def fetch_single_league_teams(self, league_key):
        """获取指定联盟的团队数据"""
        print(f"获取联盟 {league_key} 的团队数据...")
        
        teams_data = self.fetch_teams_data(league_key)
        if teams_data:
            return self.save_teams_data(league_key, teams_data)
        return False
    
    def fetch_single_team_roster(self, team_key, week=None, date=None):
        """获取指定团队的球员名单"""
        print(f"获取团队 {team_key} 的球员名单...")
        
        roster_data = self.fetch_team_roster(team_key, week, date)
        if roster_data:
            return self.save_team_roster(team_key, roster_data, week, date)
        return False
    
    def fetch_single_league_players(self, league_key, **kwargs):
        """获取指定联盟的球员数据"""
        print(f"获取联盟 {league_key} 的球员数据...")
        
        players_data = self.fetch_league_players(league_key, **kwargs)
        if players_data:
            suffix = kwargs.get('position', '') or kwargs.get('status', '') or 'all'
            return self.save_league_players(league_key, players_data, suffix)
        return False
    
    def fetch_complete_data(self):
        """获取完整的Fantasy数据"""
        print("开始获取完整的Yahoo Fantasy数据...")
        
        # 1. 获取游戏数据
        print("\n=== 第1步: 获取游戏数据 ===")
        if not self.fetch_all_games():
            print("获取游戏数据失败")
            return False
        
        # 2. 获取联盟基本数据
        print("\n=== 第2步: 获取联盟基本数据 ===")
        if not self.fetch_all_leagues():
            print("获取联盟基本数据失败")
            return False
        
        # 3. 获取联盟详细数据
        print("\n=== 第3步: 获取联盟详细数据 ===")
        if not self.fetch_all_league_details():
            print("获取联盟详细数据失败")
            return False
        
        # 4. 获取团队数据
        print("\n=== 第4步: 获取团队数据 ===")
        if not self.fetch_all_teams_data():
            print("获取团队数据失败")
            return False
        
        # 5. 获取团队球员名单数据
        print("\n=== 第5步: 获取团队球员名单数据 ===")
        if not self.fetch_all_team_rosters():
            print("获取团队球员名单数据失败")
            return False
        
        print("\n=== 完整数据获取完成! ===")
        return True
    
    def extract_player_keys_from_roster_data(self, roster_data):
        """从团队roster数据中提取球员键"""
        player_keys = []
        
        try:
            if not roster_data or "fantasy_content" not in roster_data:
                return player_keys
            
            fantasy_content = roster_data["fantasy_content"]
            
            if "team" not in fantasy_content:
                return player_keys
            
            team_data = fantasy_content["team"]
            
            # 查找roster数据
            roster_container = None
            if isinstance(team_data, list) and len(team_data) > 1:
                # roster数据在team[1]位置
                if isinstance(team_data[1], dict) and "roster" in team_data[1]:
                    roster_container = team_data[1]["roster"]
            
            if not roster_container:
                return player_keys
            
            # 遍历roster中的球员
            # roster结构: roster -> "0" -> players -> "0", "1", ... -> player
            for key in roster_container:
                if key.isdigit():  # 跳过非数字键
                    position_data = roster_container[key]
                    if isinstance(position_data, dict) and "players" in position_data:
                        players_container = position_data["players"]
                        
                        # 获取球员数量
                        players_count = int(players_container.get("count", 0))
                        
                        # 提取每个球员的player_key
                        for i in range(players_count):
                            str_index = str(i)
                            
                            if str_index not in players_container:
                                continue
                            
                            player_container = players_container[str_index]
                            
                            if "player" not in player_container:
                                continue
                            
                            player_data = player_container["player"]
                            
                            # 球员数据结构: player: [[{player_key: ...}, ...], {...}]
                            # player_key在player[0][0]位置
                            if (isinstance(player_data, list) and 
                                len(player_data) > 0 and 
                                isinstance(player_data[0], list) and 
                                len(player_data[0]) > 0 and 
                                isinstance(player_data[0][0], dict)):
                                
                                player_key = player_data[0][0].get("player_key")
                                if player_key:
                                    player_keys.append(player_key)
        
        except Exception as e:
            print(f"提取球员键时出错: {str(e)}")
        
        return player_keys
    
    def extract_all_player_keys_from_rosters(self):
        """从所有团队roster数据中提取球员键"""
        print("从团队roster数据中提取球员键...")
        
        all_player_keys = set()  # 使用set避免重复
        roster_files = list(TEAM_ROSTERS_DIR.glob("**/*.json"))
        
        if not roster_files:
            print("未找到任何团队roster文件")
            return []
        
        print(f"找到 {len(roster_files)} 个roster文件")
        
        for roster_file in roster_files:
            try:
                roster_data = load_json_data(roster_file)
                if roster_data:
                    player_keys = self.extract_player_keys_from_roster_data(roster_data)
                    all_player_keys.update(player_keys)
                    print(f"从 {roster_file.name} 提取了 {len(player_keys)} 个球员键")
            except Exception as e:
                print(f"处理文件 {roster_file} 时出错: {str(e)}")
        
        player_keys_list = list(all_player_keys)
        print(f"总共提取了 {len(player_keys_list)} 个唯一球员键")
        return player_keys_list
    
    def fetch_players_stats_batch(self, league_key, player_keys, batch_size=10, stats_type="season"):
        """批量获取球员统计数据
        
        Args:
            league_key: 联盟键
            player_keys: 球员键列表
            batch_size: 每批处理的球员数量
            stats_type: 统计类型
        """
        print(f"批量获取 {len(player_keys)} 个球员的统计数据...")
        
        all_stats_data = []
        
        # 分批处理球员
        for i in range(0, len(player_keys), batch_size):
            batch_keys = player_keys[i:i + batch_size]
            print(f"处理第 {i//batch_size + 1} 批，球员 {i+1}-{min(i+batch_size, len(player_keys))}")
            
            try:
                stats_data = self.fetch_player_stats(league_key, batch_keys, stats_type)
                if stats_data:
                    all_stats_data.append(stats_data)
                    print(f"✓ 成功获取 {len(batch_keys)} 个球员的统计数据")
                else:
                    print(f"✗ 获取球员统计数据失败")
            except Exception as e:
                print(f"✗ 获取球员统计数据时出错: {str(e)}")
            
            # 避免请求过于频繁
            if i + batch_size < len(player_keys):
                self.wait()
        
        return all_stats_data
    
    def save_players_stats_batch(self, league_key, all_stats_data, stats_type="season"):
        """保存批量球员统计数据（使用优化存储）"""
        try:
            success_count = 0
            
            # 首先保存优化的批次数据
            for i, stats_data in enumerate(all_stats_data):
                batch_number = i + 1
                if self.save_optimized_player_stats(league_key, stats_data, batch_number, stats_type):
                    success_count += 1
            
            # 创建批次索引文件
            league_stats_dir = ensure_league_player_stats_directory(league_key)
            index_file = league_stats_dir / "batches_index.json"
            
            index_data = {
                "league_key": league_key,
                "stats_type": stats_type,
                "total_batches": len(all_stats_data),
                "batch_files": [f"batch_{i+1:03d}_{stats_type}.json" for i in range(len(all_stats_data))],
                "created_at": datetime.now().isoformat(),
                "metadata": {
                    "optimization": "球员基本信息已分离，只保留统计数据",
                    "compression_ratio": "约70-80%数据压缩"
                }
            }
            
            if save_json_data(index_data, index_file):
                print(f"✓ 创建批次索引文件: {league_key}/batches_index.json")
            
            print(f"✓ 优化存储完成: {success_count}/{len(all_stats_data)} 个批次")
            return success_count
            
        except Exception as e:
            print(f"保存优化球员统计数据时出错: {e}")
            return 0
    
    def fetch_all_players_data(self, sample_only=True, max_leagues=3):
        """获取所有球员数据（包括基本信息和统计数据）
        
        Args:
            sample_only: 是否只获取样本数据
            max_leagues: 最大获取联盟数量（当sample_only=True时）
        """
        print("开始获取完整的球员数据...")
        
        # 1. 获取联盟球员基本信息
        print("\n=== 第1步: 获取联盟球员基本信息 ===")
        if not self.fetch_all_league_players(sample_only, max_leagues):
            print("获取联盟球员基本信息失败")
            return False
        
        # 2. 从roster数据中提取球员键
        print("\n=== 第2步: 从roster数据中提取球员键 ===")
        all_player_keys = self.extract_all_player_keys_from_rosters()
        
        if not all_player_keys:
            print("未找到任何球员键")
            return False
        
        # 3. 按联盟组织球员键并获取统计数据
        print("\n=== 第3步: 获取球员统计数据 ===")
        
        # 首先加载联盟汇总数据
        all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not all_leagues_file.exists():
            print("找不到联盟汇总数据文件")
            return False
        
        all_leagues = load_json_data(all_leagues_file)
        
        league_keys = []
        for game_key, leagues in all_leagues.items():
            for league in leagues:
                if "league_key" in league:
                    league_keys.append(league["league_key"])
        
        # 如果只获取样本，限制数量
        if sample_only:
            league_keys = league_keys[:max_leagues]
            print(f"样本模式：只获取前 {len(league_keys)} 个联盟的球员统计数据")
        
        success_count = 0
        
        for i, league_key in enumerate(league_keys):
            print(f"\n处理联盟 {i+1}/{len(league_keys)}: {league_key}")
            
            # 过滤出属于当前联盟的球员键
            league_player_keys = [pk for pk in all_player_keys if pk.startswith(league_key.split('.')[0] + '.p.')]
            
            if not league_player_keys:
                print(f"联盟 {league_key} 没有找到相关球员键")
                continue
            
            print(f"找到 {len(league_player_keys)} 个属于联盟 {league_key} 的球员")
            
            # 批量获取球员统计数据
            all_stats_data = self.fetch_players_stats_batch(
                league_key, 
                league_player_keys, 
                batch_size=10,
                stats_type="season"
            )
            
            # 保存统计数据
            if all_stats_data:
                saved_count = self.save_players_stats_batch(league_key, all_stats_data, "season")
                if saved_count > 0:
                    success_count += 1
            
            # 避免请求过于频繁
            if i < len(league_keys) - 1:
                self.wait()
        
        print(f"\n=== 球员数据获取完成 ===")
        print(f"成功处理的联盟: {success_count}/{len(league_keys)}")
        print(f"总球员键数量: {len(all_player_keys)}")
        
        return success_count > 0

    def enrich_player_stats_with_metadata(self, league_key, stats_data):
        """为球员统计数据添加统计项元数据信息
        
        Args:
            league_key: 联盟键
            stats_data: 球员统计数据
            
        Returns:
            enriched_data: 包含完整统计项信息的数据
        """
        # 加载联盟设置数据
        settings_file = LEAGUE_SETTINGS_DIR / f"league_settings_{league_key}.json"
        if not settings_file.exists():
            print(f"警告：找不到联盟 {league_key} 的设置文件")
            return stats_data
        
        settings_data = load_json_data(settings_file)
        
        # 构建统计项ID到元数据的映射
        stat_metadata = {}
        try:
            stat_categories = settings_data["fantasy_content"]["league"][1]["settings"][0]["stat_categories"]["stats"]
            for stat_item in stat_categories:
                stat_info = stat_item["stat"]
                stat_id = str(stat_info["stat_id"])  # 确保是字符串类型
                stat_metadata[stat_id] = {
                    "name": stat_info.get("name", ""),
                    "display_name": stat_info.get("display_name", ""),
                    "group": stat_info.get("group", ""),
                    "abbr": stat_info.get("abbr", ""),
                    "sort_order": stat_info.get("sort_order", ""),
                    "position_type": stat_info.get("position_type", ""),
                    "enabled": stat_info.get("enabled", ""),
                    "is_only_display_stat": stat_info.get("is_only_display_stat", "")
                }
        except (KeyError, IndexError) as e:
            print(f"解析联盟设置数据时出错: {e}")
            return stats_data
        
        # 为统计数据添加元数据
        enriched_data = copy.deepcopy(stats_data)
        
        try:
            # 检查数据结构 - 处理合并文件格式
            if "batches" in enriched_data:
                # 这是合并的文件格式
                for batch in enriched_data["batches"]:
                    if "fantasy_content" in batch and "league" in batch["fantasy_content"]:
                        players_data = batch["fantasy_content"]["league"][1]["players"]
                        self._enrich_players_data(players_data, stat_metadata)
            elif "fantasy_content" in enriched_data:
                # 这是单个批次的格式
                players_data = enriched_data["fantasy_content"]["league"][1]["players"]
                self._enrich_players_data(players_data, stat_metadata)
            else:
                print("未识别的数据格式")
                return stats_data
                                
        except (KeyError, IndexError) as e:
            print(f"处理球员统计数据时出错: {e}")
            
        return enriched_data
    
    def _enrich_players_data(self, players_data, stat_metadata):
        """为球员数据添加统计项元数据的辅助方法"""
        for player_key, player_data in players_data.items():
            if "player" in player_data and len(player_data["player"]) > 1:
                player_stats = player_data["player"][1].get("player_stats", {})
                if "stats" in player_stats:
                    for stat_item in player_stats["stats"]:
                        stat_info = stat_item["stat"]
                        stat_id = str(stat_info["stat_id"])
                        
                        # 添加元数据
                        if stat_id in stat_metadata:
                            stat_info.update(stat_metadata[stat_id])
                        else:
                            print(f"警告：找不到统计项 {stat_id} 的元数据")
        
    def create_unified_player_database(self, sample_only=True, max_leagues=3):
        """创建统一的球员数据库，使用新的分离数据结构
        
        Args:
            sample_only: 是否只处理样本数据
            max_leagues: 最大处理联盟数量
        """
        print("开始创建统一的球员数据库（新的分离结构）...")
        
        # 1. 收集所有静态球员信息
        all_static_players = {}
        
        # 2. 收集所有季节性信息
        all_seasonal_info = {}  # {league_key: {season: {player_key: seasonal_info}}}
        
        # 3. 收集roster分配信息
        roster_assignments = []  # [{team_key, player_key, position, date, season, league_key}]
        
        # 4. 收集球员统计数据
        player_stats = {}  # {league_key: {player_key: stats}}
        
        # 从roster文件中提取信息
        print(f"\n=== 第1步: 从roster数据中提取球员信息 ===")
        roster_files = list(TEAM_ROSTERS_DIR.glob("**/*.json"))
        
        print(f"找到 {len(roster_files)} 个roster文件")
        
        if sample_only:
            roster_files = roster_files[:max_leagues * 16]  # 假设每联盟16队
            print(f"样本模式：处理前 {len(roster_files)} 个roster文件")
        
        processed_rosters = 0
        
        for roster_file in roster_files:
            try:
                roster_data = load_json_data(roster_file)
                if not roster_data:
                    continue
                
                # 提取团队键和联盟键
                team_key = self.extract_team_key_from_roster(roster_data)
                league_key = get_league_key_from_team_key(team_key) if team_key else None
                
                if not league_key:
                    continue
                
                # 使用新的分离提取方法
                separated_data = self.extract_players_from_roster_with_season(roster_data, league_key)
                
                # 合并静态球员信息
                all_static_players.update(separated_data["static_players"])
                
                # 组织季节性信息
                season = separated_data["season"]
                if league_key not in all_seasonal_info:
                    all_seasonal_info[league_key] = {}
                if season not in all_seasonal_info[league_key]:
                    all_seasonal_info[league_key][season] = {}
                
                all_seasonal_info[league_key][season].update(separated_data["seasonal_info"])
                
                # 记录roster分配
                roster_date = self.extract_roster_date(roster_data)
                for player_key, seasonal_info in separated_data["seasonal_info"].items():
                    roster_assignments.append({
                        'team_key': team_key,
                        'player_key': player_key,
                        'selected_position': seasonal_info.get('selected_position'),
                        'roster_date': roster_date,
                        'season': season,
                        'league_key': league_key
                    })
                
                processed_rosters += 1
                
            except Exception as e:
                print(f"处理roster文件 {roster_file} 时出错: {e}")
        
        print(f"✓ 成功处理 {processed_rosters} 个roster文件")
        print(f"✓ 收集了 {len(all_static_players)} 个唯一静态球员")
        print(f"✓ 记录了 {len(roster_assignments)} 个roster分配")
        
        # 从统计数据文件中提取统计信息
        print(f"\n=== 第2步: 从统计数据中提取球员统计 ===")
        stats_files = list(PLAYER_STATS_DIR.glob("**/batch_*_season.json"))
        
        if not stats_files:
            print("⚠️  未找到任何统计数据文件")
        else:
            if sample_only:
                # 按联盟分组，每个联盟只取一部分文件
                league_stats_files = {}
                for stats_file in stats_files:
                    league_key = stats_file.parent.name
                    if league_key not in league_stats_files:
                        league_stats_files[league_key] = []
                    league_stats_files[league_key].append(stats_file)
                
                # 限制联盟数量
                limited_leagues = list(league_stats_files.keys())[:max_leagues]
                stats_files = []
                for league_key in limited_leagues:
                    stats_files.extend(league_stats_files[league_key])
            
            for stats_file in stats_files:
                try:
                    league_key = stats_file.parent.name
                    
                    stats_data = load_json_data(stats_file)
                    if not stats_data or "players_stats" not in stats_data:
                        continue
                    
                    if league_key not in player_stats:
                        player_stats[league_key] = {}
                    
                    player_stats[league_key].update(stats_data["players_stats"])
                    
                    print(f"✓ 处理统计文件: {league_key}/{stats_file.name}")
                    
                except Exception as e:
                    print(f"✗ 处理统计文件 {stats_file} 时出错: {e}")
        
        # 保存新的统一数据结构
        print(f"\n=== 第3步: 保存新的统一数据结构 ===")
        
        unified_data = {
            "static_players": all_static_players,
            "seasonal_info": all_seasonal_info,
            "player_stats": player_stats,
            "roster_assignments": roster_assignments,
            "metadata": {
                "total_static_players": len(all_static_players),
                "total_leagues": len(all_seasonal_info),
                "total_seasons": sum(len(seasons) for seasons in all_seasonal_info.values()),
                "total_stats_records": sum(len(stats) for stats in player_stats.values()),
                "total_roster_assignments": len(roster_assignments),
                "processed_roster_files": processed_rosters,
                "created_at": datetime.now().isoformat(),
                "sample_only": sample_only,
                "data_structure": "separated_static_seasonal"
            }
        }
        
        unified_players_file = PLAYERS_DIR / "unified_players_database_v2.json"
        
        if save_json_data(unified_data, unified_players_file):
            print(f"✓ 成功保存新的统一球员数据库到: {unified_players_file}")
            print(f"  - 静态球员数量: {len(all_static_players)}")
            print(f"  - 联盟数量: {len(all_seasonal_info)}")
            print(f"  - 季节记录数量: {sum(len(seasons) for seasons in all_seasonal_info.values())}")
            print(f"  - 统计记录数量: {sum(len(stats) for stats in player_stats.values())}")
            print(f"  - Roster分配记录: {len(roster_assignments)}")
            
            return True
        else:
            print("✗ 保存新的统一球员数据库失败")
            return False
    
    def extract_team_key_from_roster(self, roster_data):
        """从roster数据中提取团队键"""
        try:
            return roster_data["fantasy_content"]["team"][0][0]["team_key"]
        except (KeyError, IndexError):
            return None
    
    def extract_roster_date(self, roster_data):
        """从roster数据中提取日期"""
        try:
            return roster_data["fantasy_content"]["team"][1]["roster"]["date"]
        except (KeyError, IndexError):
            return None
    
    def extract_players_from_roster(self, roster_data):
        """从roster数据中提取球员信息"""
        players = []
        try:
            # 数据结构: fantasy_content -> team[1] -> roster -> "0" -> players -> "0", "1", ... -> player
            team_data = roster_data["fantasy_content"]["team"]
            if isinstance(team_data, list) and len(team_data) > 1:
                roster = team_data[1]["roster"]
                
                # roster中的"0"键包含球员数据
                if "0" in roster and "players" in roster["0"]:
                    players_container = roster["0"]["players"]
                    
                    # 遍历所有球员
                    for player_index in players_container:
                        if player_index.isdigit():  # 跳过count等非球员键
                            player_container = players_container[player_index]
                            if "player" in player_container:
                                player_data = player_container["player"]
                                
                                # player_data是一个数组，第一个元素是球员基本信息数组
                                if isinstance(player_data, list) and len(player_data) > 0:
                                    player_basic_info = player_data[0]  # 这是一个包含多个字典的数组
                                    
                                    # 合并球员基本信息
                                    merged_info = {}
                                    if isinstance(player_basic_info, list):
                                        for info_item in player_basic_info:
                                            if isinstance(info_item, dict):
                                                merged_info.update(info_item)
                                    
                                    # 添加选择位置信息（如果存在）
                                    if len(player_data) > 1 and isinstance(player_data[1], dict):
                                        selected_pos_data = player_data[1].get("selected_position")
                                        if selected_pos_data and isinstance(selected_pos_data, list) and len(selected_pos_data) > 1:
                                            if isinstance(selected_pos_data[1], dict) and "position" in selected_pos_data[1]:
                                                merged_info["selected_position"] = selected_pos_data[1]["position"]
                                    
                                    if merged_info:  # 只有当有数据时才添加
                                        players.append(merged_info)
        except Exception as e:
            print(f"提取roster球员信息时出错: {e}")
        
        return players
    
    def extract_players_from_roster_with_season(self, roster_data, league_key):
        """从roster数据中提取球员信息，包含季节信息处理"""
        try:
            # 提取基本球员信息
            players = self.extract_players_from_roster(roster_data)
            
            # 从联盟键中推断season信息
            season = self.extract_season_from_league_key(league_key)
            
            # 为每个球员添加季节信息并分离静态/动态数据
            static_players = {}
            seasonal_roster_info = {}
            
            for player_info in players:
                player_key = player_info.get('player_key')
                if player_key:
                    # 提取静态信息
                    static_info = self.extract_static_player_info(player_info)
                    if static_info:
                        global_player_key = static_info.get("editorial_player_key", player_key)
                        static_players[global_player_key] = static_info
                    
                    # 提取季节性信息（roster特定）
                    seasonal_info = self.extract_seasonal_player_info(
                        player_info, season, league_key, "team_roster_api"
                    )
                    if seasonal_info:
                        seasonal_roster_info[player_key] = seasonal_info
            
            return {
                "static_players": static_players,
                "seasonal_info": seasonal_roster_info,
                "season": season
            }
            
        except Exception as e:
            print(f"提取roster球员信息（含季节）时出错: {e}")
            return {"static_players": {}, "seasonal_info": {}, "season": None}
    
    def extract_season_from_league_key(self, league_key):
        """从联盟键中推断season信息"""
        try:
            # 联盟键格式: 385.l.24889 (385是game_key，对应特定年份)
            # 需要查找对应的season信息
            all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
            if all_leagues_file.exists():
                all_leagues = load_json_data(all_leagues_file)
                for game_key, leagues in all_leagues.items():
                    for league in leagues:
                        if league.get("league_key") == league_key:
                            return league.get("season")
            
            # 如果找不到，尝试从game_key推断（这是一个备用方案）
            game_key = league_key.split('.')[0]
            game_season_mapping = {
                "364": "2016",
                "375": "2017", 
                "385": "2018",
                "395": "2019",
                "402": "2020",
                "410": "2021",
                "418": "2022",
                "428": "2023",
                "454": "2024"
            }
            return game_season_mapping.get(game_key, "unknown")
            
        except Exception as e:
            print(f"从联盟键推断season时出错: {e}")
            return "unknown"
    
    def extract_player_basic_info(self, player_data):
        """提取球员基本信息"""
        player_info = {}
        
        try:
            # 处理不同的数据结构
            if isinstance(player_data, list):
                # 合并所有字典
                for item in player_data:
                    if isinstance(item, dict):
                        player_info.update(item)
            elif isinstance(player_data, dict):
                player_info = player_data.copy()
            else:
                return {}
            
            # 标准化字段名
            standardized_info = {}
            
            # 基本字段
            if "player_key" in player_info:
                standardized_info["player_key"] = player_info["player_key"]
            if "player_id" in player_info:
                standardized_info["player_id"] = player_info["player_id"]
            if "editorial_player_key" in player_info:
                standardized_info["editorial_player_key"] = player_info["editorial_player_key"]
            
            # 姓名信息
            name_info = player_info.get("name")
            if isinstance(name_info, dict):
                if "full" in name_info:
                    standardized_info["full_name"] = name_info["full"]
                if "first" in name_info:
                    standardized_info["first_name"] = name_info["first"]
                if "last" in name_info:
                    standardized_info["last_name"] = name_info["last"]
            
            # 团队信息
            if "editorial_team_key" in player_info:
                standardized_info["current_team_key"] = player_info["editorial_team_key"]
            if "editorial_team_full_name" in player_info:
                standardized_info["current_team_name"] = player_info["editorial_team_full_name"]
            if "editorial_team_abbr" in player_info:
                standardized_info["current_team_abbr"] = player_info["editorial_team_abbr"]
            
            # 位置信息
            if "display_position" in player_info:
                standardized_info["display_position"] = player_info["display_position"]
            if "primary_position" in player_info:
                standardized_info["primary_position"] = player_info["primary_position"]
            if "position_type" in player_info:
                standardized_info["position_type"] = player_info["position_type"]
            
            # 其他信息
            if "uniform_number" in player_info:
                standardized_info["uniform_number"] = player_info["uniform_number"]
            if "url" in player_info:
                standardized_info["yahoo_url"] = player_info["url"]
            if "status" in player_info:
                standardized_info["status"] = player_info["status"]
            
            # 头像信息
            headshot_info = player_info.get("headshot")
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                standardized_info["headshot_url"] = headshot_info["url"]
            
            # 是否不可丢弃
            is_undroppable = player_info.get("is_undroppable")
            if is_undroppable is not None:
                standardized_info["is_undroppable"] = str(is_undroppable) == "1"
            
            # 位置资格
            eligible_positions = player_info.get("eligible_positions")
            if isinstance(eligible_positions, list):
                standardized_info["eligible_positions"] = eligible_positions
            
            # 选择位置（来自roster数据）
            if "selected_position" in player_info:
                standardized_info["selected_position"] = player_info["selected_position"]
            
            return standardized_info
            
        except Exception as e:
            print(f"提取球员基本信息时出错: {e}")
            return {}
    
    def extract_static_player_info(self, player_data):
        """提取球员静态信息（不随时间变化的信息）"""
        try:
            # 处理不同的数据结构
            if isinstance(player_data, list):
                # 合并所有字典
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
            
            # 基本标识信息（静态）
            if "player_id" in player_info:
                static_info["player_id"] = player_info["player_id"]
            if "editorial_player_key" in player_info:
                static_info["editorial_player_key"] = player_info["editorial_player_key"]
            
            # 姓名信息（静态）
            name_info = player_info.get("name")
            if isinstance(name_info, dict):
                if "full" in name_info:
                    static_info["full_name"] = name_info["full"]
                if "first" in name_info:
                    static_info["first_name"] = name_info["first"]
                if "last" in name_info:
                    static_info["last_name"] = name_info["last"]
            
            # 其他静态信息（如果API提供）
            if "birth_date" in player_info:
                static_info["birth_date"] = player_info["birth_date"]
            if "draft_year" in player_info:
                static_info["draft_year"] = player_info["draft_year"]
            if "height" in player_info:
                static_info["height"] = player_info["height"]
            if "weight" in player_info:
                static_info["weight"] = player_info["weight"]
            
            return static_info
            
        except Exception as e:
            print(f"提取球员静态信息时出错: {e}")
            return {}
    
    def extract_seasonal_player_info(self, player_data, season, league_key, data_source="league_players_api"):
        """提取球员季节性信息（随时间变化的信息）"""
        try:
            # 处理不同的数据结构
            if isinstance(player_data, list):
                # 合并所有字典
                merged_info = {}
                for item in player_data:
                    if isinstance(item, dict):
                        merged_info.update(item)
                player_info = merged_info
            elif isinstance(player_data, dict):
                player_info = player_data.copy()
            else:
                return {}
            
            seasonal_info = {}
            
            # 团队信息（季节性）
            if "editorial_team_key" in player_info:
                seasonal_info["current_team_key"] = player_info["editorial_team_key"]
            if "editorial_team_full_name" in player_info:
                seasonal_info["current_team_name"] = player_info["editorial_team_full_name"]
            if "editorial_team_abbr" in player_info:
                seasonal_info["current_team_abbr"] = player_info["editorial_team_abbr"]
            
            # 位置信息（季节性）
            if "display_position" in player_info:
                seasonal_info["display_position"] = player_info["display_position"]
            if "primary_position" in player_info:
                seasonal_info["primary_position"] = player_info["primary_position"]
            if "position_type" in player_info:
                seasonal_info["position_type"] = player_info["position_type"]
            
            # 其他季节性信息
            if "uniform_number" in player_info:
                seasonal_info["uniform_number"] = player_info["uniform_number"]
            if "status" in player_info:
                seasonal_info["status"] = player_info["status"]
            
            # 头像信息（可能随时间更新）
            headshot_info = player_info.get("headshot")
            if isinstance(headshot_info, dict) and "url" in headshot_info:
                seasonal_info["headshot_url"] = headshot_info["url"]
            
            # 是否不可丢弃（季节性）
            is_undroppable = player_info.get("is_undroppable")
            if is_undroppable is not None:
                seasonal_info["is_undroppable"] = str(is_undroppable) == "1"
            
            # 位置资格（可能随时间变化）
            eligible_positions = player_info.get("eligible_positions")
            if isinstance(eligible_positions, list):
                seasonal_info["eligible_positions"] = eligible_positions
            
            # Yahoo URL（可能随时间变化）
            if "url" in player_info:
                seasonal_info["yahoo_url"] = player_info["url"]
            
            # 选择位置（来自roster数据）
            if "selected_position" in player_info:
                seasonal_info["selected_position"] = player_info["selected_position"]
            
            # 添加元数据
            seasonal_info["season"] = season
            seasonal_info["league_key"] = league_key
            seasonal_info["data_source"] = data_source
            seasonal_info["last_updated"] = datetime.now().isoformat()
            
            return seasonal_info
            
        except Exception as e:
            print(f"提取球员季节性信息时出错: {e}")
            return {}
    
    def save_separated_player_data(self, league_key, players_data, season):
        """保存分离的球员数据（静态+动态）"""
        try:
            # 确保联盟球员目录存在
            league_players_dir = ensure_league_players_directory(league_key)
            
            # 提取分离的球员数据
            separated_data = self.extract_separated_players_data(players_data, season, league_key)
            
            if not separated_data:
                print(f"✗ 没有提取到有效的球员数据")
                return False
            
            # 保存静态球员信息到全局文件
            success_static = self.save_static_players_data(separated_data["static_players"])
            
            # 保存季节性信息到联盟特定文件
            seasonal_filename = f"seasonal_players_{season}.json"
            seasonal_file_path = league_players_dir / seasonal_filename
            
            seasonal_data = {
                "league_key": league_key,
                "season": season,
                "seasonal_info": separated_data["seasonal_info"],
                "metadata": {
                    "total_players": len(separated_data["seasonal_info"]),
                    "created_at": datetime.now().isoformat(),
                    "data_source": "league_players_api"
                }
            }
            
            success_seasonal = save_json_data(seasonal_data, seasonal_file_path)
            
            if success_static and success_seasonal:
                print(f"✓ 保存分离球员数据: {league_key}/seasonal_players_{season}.json")
                print(f"  - 静态球员: {len(separated_data['static_players'])}")
                print(f"  - 季节性记录: {len(separated_data['seasonal_info'])}")
                return True
            else:
                print(f"✗ 保存分离球员数据失败")
                return False
                
        except Exception as e:
            print(f"保存分离球员数据时出错: {e}")
            return False
    
    def extract_separated_players_data(self, players_data, season, league_key):
        """提取分离的球员数据"""
        try:
            if "fantasy_content" not in players_data:
                return {}
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            static_players = {}
            seasonal_info = {}
            
            # 提取球员数据
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
                            
                            # 获取球员键
                            player_key = merged_info.get("player_key")
                            if player_key:
                                # 提取静态信息
                                static_info = self.extract_static_player_info(merged_info)
                                if static_info:
                                    # 使用editorial_player_key作为全局唯一标识
                                    global_player_key = static_info.get("editorial_player_key", player_key)
                                    static_players[global_player_key] = static_info
                                
                                # 提取季节性信息
                                seasonal_player_info = self.extract_seasonal_player_info(
                                    merged_info, season, league_key, "league_players_api"
                                )
                                if seasonal_player_info:
                                    seasonal_info[player_key] = seasonal_player_info
            
            return {
                "static_players": static_players,
                "seasonal_info": seasonal_info
            }
            
        except Exception as e:
            print(f"提取分离球员数据时出错: {e}")
            return {}
    
    def save_static_players_data(self, static_players):
        """保存静态球员信息到全局文件"""
        try:
            # 全局静态球员文件路径
            static_players_file = PLAYERS_DIR / "static_players.json"
            
            # 如果文件已存在，加载现有数据并合并
            existing_static_players = {}
            if static_players_file.exists():
                existing_data = load_json_data(static_players_file)
                if existing_data and "static_players" in existing_data:
                    existing_static_players = existing_data["static_players"]
            
            # 合并新的静态球员信息
            existing_static_players.update(static_players)
            
            # 构建完整的静态数据结构
            static_data = {
                "static_players": existing_static_players,
                "metadata": {
                    "total_players": len(existing_static_players),
                    "last_updated": datetime.now().isoformat(),
                    "description": "全局静态球员信息，包含不随时间变化的球员数据"
                }
            }
            
            if save_json_data(static_data, static_players_file):
                print(f"✓ 更新全局静态球员数据: {len(static_players)} 个新球员")
                return True
            else:
                print(f"✗ 保存全局静态球员数据失败")
                return False
                
        except Exception as e:
            print(f"保存全局静态球员数据时出错: {e}")
            return False
    
    def extract_league_key_from_filename(self, filename):
        """从文件名中提取联盟键"""
        # 格式: player_stats_385.l.24889_season_combined.json
        parts = filename.split("_")
        if len(parts) >= 3:
            return parts[2]  # 385.l.24889
        return None
    
    def extract_player_stats_from_data(self, stats_data):
        """从统计数据中提取球员统计信息"""
        player_stats = {}
        
        try:
            if "batches" in stats_data:
                # 合并文件格式
                for batch in stats_data["batches"]:
                    if "fantasy_content" in batch and "league" in batch["fantasy_content"]:
                        league_data = batch["fantasy_content"]["league"]
                        # league是一个数组，球员数据在索引1的位置
                        if isinstance(league_data, list) and len(league_data) > 1:
                            players_data = league_data[1].get("players", {})
                            for player_index, player_data in players_data.items():
                                if not player_index.isdigit():  # 跳过count等非球员键
                                    continue
                                if "player" in player_data and len(player_data["player"]) > 1:
                                    # 获取球员键
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
            elif "fantasy_content" in stats_data:
                # 单批次格式
                league_data = stats_data["fantasy_content"]["league"]
                if isinstance(league_data, list) and len(league_data) > 1:
                    players_data = league_data[1].get("players", {})
                    for player_index, player_data in players_data.items():
                        if not player_index.isdigit():  # 跳过count等非球员键
                            continue
                        if "player" in player_data and len(player_data["player"]) > 1:
                            # 获取球员键
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
                        "value": stat_info.get("value"),
                        "name": stat_info.get("name"),
                        "display_name": stat_info.get("display_name"),
                        "group": stat_info.get("group"),
                        "abbr": stat_info.get("abbr")
                    }
        
        return normalized_stats

    def fetch_complete_database_ready_data(self, sample_only=False):
        """获取完整的数据库就绪数据
        
        Args:
            sample_only: 是否只获取样本数据
        """
        print("开始获取完整的数据库就绪数据...")
        
        success_steps = 0
        total_steps = 6
        
        # 1. 获取游戏数据
        print(f"\n=== 第1步/{total_steps}: 获取游戏数据 ===")
        if self.fetch_all_games():
            success_steps += 1
            print("✓ 游戏数据获取成功")
        else:
            print("✗ 游戏数据获取失败")
        
        # 2. 获取联盟基本数据
        print(f"\n=== 第2步/{total_steps}: 获取联盟基本数据 ===")
        if self.fetch_all_leagues():
            success_steps += 1
            print("✓ 联盟基本数据获取成功")
        else:
            print("✗ 联盟基本数据获取失败")
        
        # 3. 获取联盟详细数据（设置、排名、记分板）
        print(f"\n=== 第3步/{total_steps}: 获取联盟详细数据 ===")
        if self.fetch_all_league_details():
            success_steps += 1
            print("✓ 联盟详细数据获取成功")
        else:
            print("✗ 联盟详细数据获取失败")
        
        # 4. 获取团队数据
        print(f"\n=== 第4步/{total_steps}: 获取团队数据 ===")
        if self.fetch_all_teams_data():
            success_steps += 1
            print("✓ 团队数据获取成功")
        else:
            print("✗ 团队数据获取失败")
        
        # 5. 获取团队球员名单数据
        print(f"\n=== 第5步/{total_steps}: 获取团队球员名单数据 ===")
        if self.fetch_all_team_rosters(sample_only=sample_only):
            success_steps += 1
            print("✓ 团队球员名单数据获取成功")
        else:
            print("✗ 团队球员名单数据获取失败")
        
        # 6. 获取球员统计数据
        print(f"\n=== 第6步/{total_steps}: 获取球员统计数据 ===")
        if self.fetch_all_players_data(sample_only=sample_only):
            success_steps += 1
            print("✓ 球员统计数据获取成功")
        else:
            print("✗ 球员统计数据获取失败")
        
        # 7. 创建统一数据库
        print(f"\n=== 额外步骤: 创建统一球员数据库 ===")
        if self.create_unified_player_database(sample_only=sample_only):
            print("✓ 统一球员数据库创建成功")
        else:
            print("✗ 统一球员数据库创建失败")
        
        # 总结
        print(f"\n=== 数据获取完成总结 ===")
        print(f"成功步骤: {success_steps}/{total_steps}")
        print(f"成功率: {(success_steps/total_steps)*100:.1f}%")
        
        if success_steps == total_steps:
            print("🎉 所有数据获取成功！数据库准备就绪。")
            self.print_data_summary()
            return True
        else:
            print("⚠️  部分数据获取失败，请检查错误信息。")
            return False
    
    def print_data_summary(self):
        """打印数据获取总结"""
        print(f"\n=== 数据文件总结 ===")
        
        # 统计各类数据文件
        data_types = {
            "游戏数据": GAMES_DIR,
            "联盟数据": LEAGUES_DIR,
            "联盟设置": LEAGUE_SETTINGS_DIR,
            "联盟排名": LEAGUE_STANDINGS_DIR,
            "联盟记分板": LEAGUE_SCOREBOARDS_DIR,
            "团队数据": TEAMS_DIR,
            "球员数据": PLAYERS_DIR,
            "团队名单": TEAM_ROSTERS_DIR,
            "球员统计": PLAYER_STATS_DIR
        }
        
        total_files = 0
        total_size = 0
        
        for data_type, directory in data_types.items():
            if directory.exists():
                files = list(directory.glob("**/*.json"))
                file_count = len(files)
                dir_size = sum(f.stat().st_size for f in files if f.exists())
                
                print(f"  {data_type}: {file_count} 个文件, {dir_size/1024/1024:.1f} MB")
                total_files += file_count
                total_size += dir_size
        
        print(f"\n总计: {total_files} 个JSON文件, {total_size/1024/1024:.1f} MB")
        
        # 检查统一数据库
        unified_db = PLAYERS_DIR / "unified_players_database.json"
        if unified_db.exists():
            unified_size = unified_db.stat().st_size
            compression_ratio = (1 - unified_size / total_size) * 100 if total_size > 0 else 0
            print(f"统一数据库: {unified_size/1024/1024:.1f} MB (压缩率: {compression_ratio:.1f}%)")
    
    def fetch_all_data_for_database(self, sample_only=False):
        """获取所有数据用于数据库加载（别名方法）"""
        return self.fetch_complete_database_ready_data(sample_only)
    
    def save_optimized_player_stats(self, league_key, stats_data, batch_number, stats_type="season"):
        """保存优化的球员统计数据（去除冗余信息）
        
        Args:
            league_key: 联盟键
            stats_data: 原始统计数据
            batch_number: 批次号
            stats_type: 统计类型
        """
        try:
            # 确保联盟统计数据目录存在
            league_stats_dir = ensure_league_player_stats_directory(league_key)
            
            # 提取并优化数据
            optimized_data = self.extract_optimized_stats_data(stats_data)
            
            # 构建文件名
            filename = f"batch_{batch_number:03d}_{stats_type}.json"
            stats_file_path = league_stats_dir / filename
            
            if save_json_data(optimized_data, stats_file_path):
                print(f"✓ 保存优化统计数据: {league_key}/{filename}")
                return True
            else:
                print(f"✗ 保存优化统计数据失败: {league_key}/{filename}")
                return False
                
        except Exception as e:
            print(f"保存优化球员统计数据时出错: {e}")
            return False
    
    def extract_optimized_stats_data(self, stats_data):
        """提取优化的统计数据（去除冗余信息）"""
        try:
            if "fantasy_content" not in stats_data:
                return {}
            
            fantasy_content = stats_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 提取联盟基本信息（只保留关键信息）
            league_info = {}
            if isinstance(league_data, list) and len(league_data) > 0:
                league_basic = league_data[0]
                league_info = {
                    "league_key": league_basic.get("league_key"),
                    "league_id": league_basic.get("league_id"),
                    "name": league_basic.get("name"),
                    "season": league_basic.get("season"),
                    "game_code": league_basic.get("game_code")
                }
            
            # 提取球员统计数据（只保留统计信息）
            players_stats = {}
            if isinstance(league_data, list) and len(league_data) > 1:
                players_data = league_data[1].get("players", {})
                
                for player_index, player_data in players_data.items():
                    if not player_index.isdigit():
                        continue
                    
                    if "player" in player_data and len(player_data["player"]) > 1:
                        # 获取球员键
                        player_basic_info = player_data["player"][0]
                        player_key = None
                        if isinstance(player_basic_info, list):
                            for info_item in player_basic_info:
                                if isinstance(info_item, dict) and "player_key" in info_item:
                                    player_key = info_item["player_key"]
                                    break
                        
                        if player_key:
                            # 只保存统计数据
                            player_stats_data = player_data["player"][1].get("player_stats", {})
                            stats = player_stats_data.get("stats", [])
                            
                            # 简化统计数据结构
                            simplified_stats = {}
                            for stat_item in stats:
                                if "stat" in stat_item:
                                    stat_info = stat_item["stat"]
                                    stat_id = stat_info.get("stat_id")
                                    value = stat_info.get("value")
                                    if stat_id:
                                        simplified_stats[stat_id] = value
                            
                            players_stats[player_key] = {
                                "coverage_type": player_stats_data.get("0", {}).get("coverage_type"),
                                "season": player_stats_data.get("0", {}).get("season"),
                                "stats": simplified_stats
                            }
            
            return {
                "league": league_info,
                "players_stats": players_stats,
                "metadata": {
                    "total_players": len(players_stats),
                    "extracted_at": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            print(f"提取优化统计数据时出错: {e}")
            return {}
    
    def save_optimized_league_players(self, league_key, players_data):
        """保存优化的联盟球员数据（去除冗余信息）"""
        try:
            # 确保联盟球员目录存在
            league_players_dir = ensure_league_players_directory(league_key)
            
            # 提取并优化球员基本信息
            optimized_players = self.extract_optimized_players_data(players_data)
            
            # 构建文件名
            filename = "players_basic_info.json"
            players_file_path = league_players_dir / filename
            
            if save_json_data(optimized_players, players_file_path):
                print(f"✓ 保存优化球员数据: {league_key}/{filename}")
                return True
            else:
                print(f"✗ 保存优化球员数据失败: {league_key}/{filename}")
                return False
                
        except Exception as e:
            print(f"保存优化联盟球员数据时出错: {e}")
            return False
    
    def extract_optimized_players_data(self, players_data):
        """提取优化的球员基本信息"""
        try:
            if "fantasy_content" not in players_data:
                return {}
            
            fantasy_content = players_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 提取联盟基本信息
            league_info = {}
            if isinstance(league_data, list) and len(league_data) > 0:
                league_basic = league_data[0]
                league_info = {
                    "league_key": league_basic.get("league_key"),
                    "league_id": league_basic.get("league_id"),
                    "name": league_basic.get("name"),
                    "season": league_basic.get("season"),
                    "game_code": league_basic.get("game_code")
                }
            
            # 提取球员基本信息
            players_info = {}
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
                            
                            # 提取关键信息
                            player_key = merged_info.get("player_key")
                            if player_key:
                                players_info[player_key] = self.extract_player_basic_info(merged_info)
            
            return {
                "league": league_info,
                "players": players_info,
                "metadata": {
                    "total_players": len(players_info),
                    "extracted_at": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            print(f"提取优化球员数据时出错: {e}")
            return {}
    
    def create_league_unified_databases(self, sample_only=True, max_leagues=3):
        """为每个联盟创建独立的统一数据库
        
        Args:
            sample_only: 是否只处理样本数据
            max_leagues: 最大处理联盟数量
        """
        print("开始为每个联盟创建独立的统一数据库...")
        
        # 获取所有联盟键
        all_leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if not all_leagues_file.exists():
            print("找不到联盟汇总数据文件")
            return False
        
        all_leagues = load_json_data(all_leagues_file)
        league_keys = []
        for game_key, leagues in all_leagues.items():
            for league in leagues:
                if "league_key" in league:
                    league_keys.append(league["league_key"])
        
        if sample_only:
            league_keys = league_keys[:max_leagues]
            print(f"样本模式：处理前 {len(league_keys)} 个联盟")
        
        success_count = 0
        
        for league_key in league_keys:
            print(f"\n=== 处理联盟: {league_key} ===")
            
            if self.create_single_league_unified_database(league_key):
                success_count += 1
                print(f"✓ 联盟 {league_key} 统一数据库创建成功")
            else:
                print(f"✗ 联盟 {league_key} 统一数据库创建失败")
        
        print(f"\n=== 联盟统一数据库创建完成 ===")
        print(f"成功: {success_count}/{len(league_keys)}")
        
        return success_count > 0
    
    def create_single_league_unified_database(self, league_key):
        """为单个联盟创建统一数据库"""
        try:
            # 1. 收集该联盟的roster数据
            roster_data = self.collect_league_roster_data(league_key)
            
            # 2. 收集该联盟的统计数据
            stats_data = self.collect_league_stats_data(league_key)
            
            # 3. 收集该联盟的球员基本信息
            players_data = self.collect_league_players_data(league_key)
            
            # 4. 创建统一数据结构
            unified_data = {
                "league_key": league_key,
                "players_basic_info": players_data,
                "players_stats": stats_data,
                "roster_assignments": roster_data,
                "metadata": {
                    "total_unique_players": len(players_data),
                    "total_stats_records": len(stats_data),
                    "total_roster_assignments": len(roster_data),
                    "created_at": datetime.now().isoformat()
                }
            }
            
            # 5. 保存到联盟专用目录
            league_dir = PLAYERS_DIR / league_key
            league_dir.mkdir(exist_ok=True)
            unified_file = league_dir / "unified_database.json"
            
            if save_json_data(unified_data, unified_file):
                print(f"  ✓ 保存统一数据库: {league_key}/unified_database.json")
                print(f"  - 球员数量: {len(players_data)}")
                print(f"  - 统计记录: {len(stats_data)}")
                print(f"  - Roster记录: {len(roster_data)}")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"创建联盟 {league_key} 统一数据库时出错: {e}")
            return False
    
    def collect_league_roster_data(self, league_key):
        """收集指定联盟的roster数据"""
        roster_assignments = []
        
        # 查找该联盟的roster文件
        roster_files = list(TEAM_ROSTERS_DIR.glob(f"{league_key}/*.json"))
        
        for roster_file in roster_files:
            try:
                roster_data = load_json_data(roster_file)
                if roster_data:
                    team_key = self.extract_team_key_from_roster(roster_data)
                    roster_date = self.extract_roster_date(roster_data)
                    players_in_roster = self.extract_players_from_roster(roster_data)
                    
                    for player_info in players_in_roster:
                        player_key = player_info.get('player_key')
                        if player_key:
                            roster_assignments.append({
                                'team_key': team_key,
                                'player_key': player_key,
                                'selected_position': player_info.get('selected_position'),
                                'roster_date': roster_date
                            })
            except Exception as e:
                print(f"  处理roster文件 {roster_file} 时出错: {e}")
        
        return roster_assignments
    
    def collect_league_stats_data(self, league_key):
        """收集指定联盟的统计数据"""
        players_stats = {}
        
        # 查找该联盟的统计数据文件
        league_stats_dir = PLAYER_STATS_DIR / league_key
        if league_stats_dir.exists():
            stats_files = list(league_stats_dir.glob("batch_*_season.json"))
            
            for stats_file in stats_files:
                try:
                    stats_data = load_json_data(stats_file)
                    if stats_data and "players_stats" in stats_data:
                        players_stats.update(stats_data["players_stats"])
                except Exception as e:
                    print(f"  处理统计文件 {stats_file} 时出错: {e}")
        
        return players_stats
    
    def collect_league_players_data(self, league_key):
        """收集指定联盟的球员基本信息"""
        players_info = {}
        
        # 查找该联盟的球员基本信息文件
        league_players_dir = PLAYERS_DIR / league_key
        players_file = league_players_dir / "players_basic_info.json"
        
        if players_file.exists():
            try:
                players_data = load_json_data(players_file)
                if players_data and "players" in players_data:
                    players_info = players_data["players"]
            except Exception as e:
                print(f"  处理球员文件 {players_file} 时出错: {e}")
        
        return players_info


def main():
    """主函数，处理命令行参数并执行相应操作"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy数据获取工具")
    
    # 数据类型选项
    parser.add_argument("--games", action="store_true", help="获取游戏数据")
    parser.add_argument("--leagues", action="store_true", help="获取所有联盟基本数据")
    parser.add_argument("--league-details", action="store_true", help="获取所有联盟的详细数据")
    parser.add_argument("--teams", action="store_true", help="获取所有联盟的团队数据")
    parser.add_argument("--complete", action="store_true", help="获取完整的Fantasy数据")
    
    # 球员数据选项
    parser.add_argument("--team-rosters", action="store_true", help="获取所有团队球员名单数据")
    parser.add_argument("--sample-rosters", action="store_true", help="获取团队球员名单数据（样本）")
    parser.add_argument("--league-players", action="store_true", help="获取联盟球员数据（样本）")
    parser.add_argument("--all-rosters", action="store_true", help="获取所有团队球员名单数据（已弃用，使用--team-rosters）")
    parser.add_argument("--all-players", action="store_true", help="获取所有联盟球员数据")
    parser.add_argument("--players-data", action="store_true", help="获取完整的球员数据（基本信息+统计数据）")
    parser.add_argument("--sample-players-data", action="store_true", help="获取球员数据样本（基本信息+统计数据）")
    parser.add_argument("--extract-player-keys", action="store_true", help="从roster数据中提取球员键")
    parser.add_argument("--enrich-player-stats", action="store_true", help="为球员统计数据添加元数据信息")
    parser.add_argument("--create-unified-db", action="store_true", help="创建统一的球员数据库（去重复）")
    parser.add_argument("--sample-unified-db", action="store_true", help="创建统一的球员数据库样本")
    parser.add_argument("--league-unified-dbs", action="store_true", help="为每个联盟创建独立的统一数据库")
    parser.add_argument("--sample-league-unified-dbs", action="store_true", help="为样本联盟创建独立的统一数据库")
    parser.add_argument("--database-ready", action="store_true", help="获取完整的数据库就绪数据")
    parser.add_argument("--sample-database-ready", action="store_true", help="获取数据库就绪数据样本")
    parser.add_argument("--optimized-data", action="store_true", help="获取优化存储的完整数据")
    parser.add_argument("--sample-optimized-data", action="store_true", help="获取优化存储的样本数据")
    
    # 单独获取选项
    parser.add_argument("--game-key", type=str, help="获取指定游戏的联盟数据")
    parser.add_argument("--league-key", type=str, help="获取指定联盟的详细数据")
    parser.add_argument("--league-teams", type=str, help="获取指定联盟的团队数据")
    parser.add_argument("--team-key", type=str, help="获取指定团队的球员名单")
    parser.add_argument("--league-players-key", type=str, help="获取指定联盟的球员数据")
    parser.add_argument("--league-stats", type=str, help="获取指定联盟的球员统计数据")
    
    # 球员数据参数
    parser.add_argument("--week", type=int, help="NFL游戏的周次")
    parser.add_argument("--date", type=str, help="MLB/NBA/NHL游戏的日期（YYYY-MM-DD）")
    parser.add_argument("--position", type=str, help="球员位置过滤")
    parser.add_argument("--status", type=str, help="球员状态过滤（A/FA/W/T/K）")
    
    # 工具选项
    parser.add_argument("--extract-keys", action="store_true", help="提取并显示所有游戏键")
    parser.add_argument("--consolidate", action="store_true", help="整合联盟数据到汇总文件")
    parser.add_argument("--overview", action="store_true", help="显示数据概览和统计信息")
    parser.add_argument("--delay", type=int, default=2, help="请求间隔时间（秒），默认2秒")
    
    args = parser.parse_args()
    
    # 创建数据获取器
    fetcher = FantasyDataFetcher(delay=args.delay)
    
    # 处理命令行参数
    if args.complete:
        # 获取完整数据
        fetcher.fetch_complete_data()
    elif args.games:
        # 获取游戏数据
        fetcher.fetch_all_games()
    elif args.leagues:
        # 获取联盟基本数据
        fetcher.fetch_all_leagues()
    elif args.league_details:
        # 获取联盟详细数据
        fetcher.fetch_all_league_details()
    elif args.teams:
        # 获取团队数据
        fetcher.fetch_all_teams_data()
    elif args.team_rosters:
        # 获取所有团队球员名单数据
        fetcher.fetch_all_team_rosters(sample_only=False)
    elif args.sample_rosters:
        # 获取团队球员名单数据（样本）
        fetcher.fetch_all_team_rosters(sample_only=True, max_teams=5)
    elif args.league_players:
        # 获取联盟球员数据（样本）
        fetcher.fetch_all_league_players(sample_only=True, max_leagues=3)
    elif args.all_rosters:
        # 获取所有团队球员名单数据（已弃用，使用--team-rosters）
        print("注意：--all-rosters 选项已弃用，请使用 --team-rosters")
        fetcher.fetch_all_team_rosters(sample_only=False)
    elif args.all_players:
        # 获取所有联盟球员数据
        fetcher.fetch_all_league_players(sample_only=False)
    elif args.players_data:
        # 获取完整的球员数据（基本信息+统计数据）
        fetcher.fetch_all_players_data(sample_only=False)
    elif args.sample_players_data:
        # 获取球员数据样本（基本信息+统计数据）
        fetcher.fetch_all_players_data(sample_only=True, max_leagues=3)
    elif args.extract_player_keys:
        fetcher.extract_all_player_keys_from_rosters()
    elif args.enrich_player_stats:
        fetcher.process_and_enrich_all_player_stats()
    elif args.create_unified_db:
        fetcher.create_unified_player_database()
    elif args.sample_unified_db:
        fetcher.create_unified_player_database(sample_only=True)
    elif args.league_unified_dbs:
        fetcher.create_league_unified_databases()
    elif args.sample_league_unified_dbs:
        fetcher.create_league_unified_databases(sample_only=True)
    elif args.database_ready:
        fetcher.fetch_complete_database_ready_data()
    elif args.sample_database_ready:
        fetcher.fetch_complete_database_ready_data(sample_only=True)
    elif args.optimized_data:
        fetcher.fetch_complete_database_ready_data(sample_only=True)
    elif args.sample_optimized_data:
        fetcher.fetch_complete_database_ready_data(sample_only=True)
    elif args.game_key:
        # 获取指定游戏的联盟数据
        fetcher.fetch_single_game_leagues(args.game_key)
    elif args.league_key:
        # 获取指定联盟的详细数据
        fetcher.fetch_single_league_details(args.league_key)
    elif args.league_teams:
        # 获取指定联盟的团队数据
        fetcher.fetch_single_league_teams(args.league_teams)
    elif args.team_key:
        # 获取指定团队的球员名单
        fetcher.fetch_single_team_roster(args.team_key, args.week, args.date)
    elif args.league_players_key:
        # 获取指定联盟的球员数据
        kwargs = {}
        if args.position:
            kwargs['position'] = args.position
        if args.status:
            kwargs['status'] = args.status
        fetcher.fetch_single_league_players(args.league_players_key, **kwargs)
    elif args.league_stats:
        # 获取指定联盟的球员统计数据
        all_player_keys = fetcher.extract_all_player_keys_from_rosters()
        if all_player_keys:
            stats_data = fetcher.fetch_players_stats_batch(args.league_stats, all_player_keys)
            if stats_data:
                saved_count = fetcher.save_players_stats_batch(args.league_stats, stats_data)
                if saved_count > 0:
                    print(f"\n成功获取并保存 {len(stats_data)} 个球员的统计数据")
            else:
                print("获取球员统计数据失败")
        else:
            print("未找到任何球员键")
    elif args.extract_keys:
        # 提取游戏键
        games_data = fetcher.fetch_all_games()
        if games_data:
            game_keys = fetcher.extract_game_keys(games_data)
            print(f"\n成功提取了 {len(game_keys)} 个游戏键: {', '.join(game_keys)}")
    elif args.consolidate:
        # 整合联盟数据
        fetcher.consolidate_league_data()
    elif args.overview:
        # 显示数据概览
        print_data_overview()
    else:
        # 默认交互式模式
        print("Yahoo Fantasy数据获取工具")
        print("1. 获取完整数据")
        print("2. 获取游戏数据")
        print("3. 获取联盟基本数据")
        print("4. 获取联盟详细数据")
        print("5. 获取团队数据")
        print("6. 获取所有团队球员名单数据")
        print("7. 获取团队球员名单数据（样本）")
        print("8. 获取联盟球员数据（样本）")
        print("9. 获取完整的球员数据（基本信息+统计数据）")
        print("10. 获取球员数据样本（基本信息+统计数据）")
        print("11. 从roster数据中提取球员键")
        print("12. 为球员统计数据添加元数据信息")
        print("13. 创建统一的球员数据库（去重复）")
        print("14. 创建统一的球员数据库样本")
        print("15. 获取完整的数据库就绪数据")
        print("16. 获取数据库就绪数据样本")
        print("17. 显示数据概览和统计信息")
        print("18. 为每个联盟创建独立的统一数据库")
        print("19. 为样本联盟创建独立的统一数据库")
        print("20. 获取优化存储的完整数据")
        
        choice = input("\n请选择操作 (1-20): ").strip()
        
        if choice == "1":
            fetcher.fetch_complete_data()
        elif choice == "2":
            fetcher.fetch_all_games()
        elif choice == "3":
            fetcher.fetch_all_leagues()
        elif choice == "4":
            fetcher.fetch_all_league_details()
        elif choice == "5":
            fetcher.fetch_all_teams_data()
        elif choice == "6":
            fetcher.fetch_all_team_rosters(sample_only=False)
        elif choice == "7":
            fetcher.fetch_all_team_rosters(sample_only=True, max_teams=5)
        elif choice == "8":
            fetcher.fetch_all_league_players(sample_only=True, max_leagues=3)
        elif choice == "9":
            fetcher.fetch_all_players_data(sample_only=False)
        elif choice == "10":
            fetcher.fetch_all_players_data(sample_only=True, max_leagues=3)
        elif choice == "11":
            all_player_keys = fetcher.extract_all_player_keys_from_rosters()
            print(f"\n成功提取了 {len(all_player_keys)} 个球员键")
            if all_player_keys:
                print(f"前10个球员键: {', '.join(all_player_keys[:10])}")
                if len(all_player_keys) > 10:
                    print(f"... 还有 {len(all_player_keys) - 10} 个球员键")
        elif choice == "12":
            fetcher.process_and_enrich_all_player_stats()
        elif choice == "13":
            fetcher.create_unified_player_database()
        elif choice == "14":
            fetcher.create_unified_player_database(sample_only=True)
        elif choice == "15":
            fetcher.fetch_complete_database_ready_data()
        elif choice == "16":
            fetcher.fetch_complete_database_ready_data(sample_only=True)
        elif choice == "17":
            print_data_overview()
        elif choice == "18":
            fetcher.create_league_unified_databases()
        elif choice == "19":
            fetcher.create_league_unified_databases(sample_only=True)
        elif choice == "20":
            fetcher.fetch_complete_database_ready_data(sample_only=True)
        else:
            print("无效选择，获取完整数据...")
            fetcher.fetch_complete_data()
    
    print("数据获取完成!")


if __name__ == "__main__":
    main() 