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
    get_api_data, load_json_data, save_json_data,
    GAMES_DIR, LEAGUES_DIR,
    select_league_interactively
)
from database_writer import FantasyDatabaseWriter
from model import Roster, Player

class YahooFantasyDataFetcher:
    """Yahoo Fantasy统一数据获取器"""
    
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
    
    def fetch_and_select_league(self, use_existing_data: bool = True) -> bool:
        """获取基础数据并选择联盟"""
        print("🚀 开始获取基础数据和联盟选择...")
        
        # 检查是否使用现有数据或直接从API获取
        leagues_file = LEAGUES_DIR / "all_leagues_data.json"
        if use_existing_data and leagues_file.exists():
            print("📋 使用现有联盟数据...")
            leagues_data = load_json_data(leagues_file)
        else:
            print("📋 获取联盟数据...")
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
        
        print(f"✓ 联盟选择完成: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
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
            
            # 同时保存JSON文件以便选择联盟
            leagues_file = LEAGUES_DIR / "all_leagues_data.json"
            save_json_data(all_leagues, leagues_file)
            print(f"✓ 联盟数据获取完成")
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
                        
                        leagues.append(league_info)
                break
        
        except Exception as e:
            print(f"提取联盟数据时出错: {e}")
        
        return leagues

    # ===== 单联盟深度数据获取 =====
    
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
        
        # 2. 获取完整球员数据（优先获取，为后续步骤提供依赖）
        print("\n📋 步骤2: 获取完整球员数据")
        if not self.fetch_complete_players_data():
            print("⚠️ 球员数据获取失败，但继续其他步骤")
        
        # 3. 获取团队和rosters数据
        print("\n📋 步骤3: 获取团队和rosters数据")
        teams_data = self.fetch_teams_data()
        if teams_data:
            self.fetch_team_rosters(teams_data)
        
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
            
            print(f"成功提取团队数据: {team_dict.get('team_key')} - {team_dict.get('name', 'Unknown')}")
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
            print(f"  获取团队roster {i+1}/{len(team_keys)}: {team_key}")
            
            try:
                roster_data = self._fetch_team_roster(team_key)
                if roster_data:
                    # 直接处理roster数据并写入数据库
                    if self._process_roster_data_to_db(roster_data, team_key):
                        success_count += 1
                        print(f"    ✓ 团队 {team_key} roster 数据处理成功")
                    else:
                        print(f"    ✗ 团队 {team_key} roster 数据处理失败")
                else:
                    print(f"    ✗ 团队 {team_key} roster 数据获取失败")
            except Exception as e:
                print(f"    ✗ 团队 {team_key} 处理出错: {e}")
            
            # 避免过快请求
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
                print(f"    roster信息未找到在 {team_key}")
                return False
            
            coverage_date = roster_info.get("date")
            is_prescoring = bool(roster_info.get("is_prescoring", False))
            is_editable = bool(roster_info.get("is_editable", False))
            
            print(f"    处理roster数据: date={coverage_date}, prescoring={is_prescoring}, editable={is_editable}")
            
            # 获取球员信息
            players_container = None
            if "0" in roster_info and "players" in roster_info["0"]:
                players_container = roster_info["0"]["players"]
            
            if not players_container:
                print(f"    球员容器未找到在 {team_key}")
                return False
            
            roster_list = []
            players_count = int(players_container.get("count", 0))
            print(f"    找到 {players_count} 个球员")
            
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
                    print(f"    写入roster记录失败: {e}")
                    # 回滚当前事务
                    self.db_writer.session.rollback()
                    continue
            
            # 最终提交剩余的记录
            if count > 0:
                try:
                    self.db_writer.session.commit()
                    self.db_writer.stats['rosters'] += count
                    print(f"    成功写入 {count} 个roster记录")
                except Exception as e:
                    print(f"    最终提交roster记录失败: {e}")
                    self.db_writer.session.rollback()
            
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
            print(f"  获取团队统计 {i+1}/{len(team_keys)}: {team_key}")
            
            try:
                # 获取团队赛季统计
                stats_data = self._fetch_team_stats(team_key)
                if stats_data:
                    if self._process_team_stats_to_db(stats_data, team_key, league_key, season):
                        success_count += 1
                        print(f"    ✓ 团队 {team_key} 统计数据处理成功")
                    else:
                        print(f"    ✗ 团队 {team_key} 统计数据处理失败")
                else:
                    print(f"    ✗ 团队 {team_key} 统计数据获取失败")
                    
                # 获取团队matchups数据
                matchups_data = self._fetch_team_matchups(team_key)
                if matchups_data:
                    matchups_count = self._process_team_matchups_to_db(matchups_data, team_key, league_key, season)
                    print(f"    ✓ 团队 {team_key} matchups数据: {matchups_count} 个")
                    
            except Exception as e:
                print(f"    ✗ 团队 {team_key} 处理出错: {e}")
            
            # 避免过快请求
            if i < len(team_keys) - 1:
                self.wait()
        
        print(f"✓ Team stats获取完成: {success_count}/{len(team_keys)}")
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
                print(f"    team统计信息未找到在 {team_key}")
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
            
            # 写入数据库
            return self.db_writer.write_team_stats(
                team_key=team_key,
                league_key=league_key,
                stats_data=normalized_stats,
                coverage_type=coverage_type,
                season=season
            )
            
        except Exception as e:
            print(f"    处理团队统计失败 {team_key}: {e}")
            return False
    
    def _process_team_matchups_to_db(self, matchups_data: Dict, team_key: str, league_key: str, season: str) -> int:
        """处理团队matchups数据并写入数据库"""
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
                
                # 提取matchup信息
                week = matchup_data.get("week")
                is_playoff = bool(matchup_data.get("is_playoffs", 0))
                
                # 提取对手信息和比分
                teams_data = matchup_data.get("0", {}).get("teams", {})
                opponent_team_key = None
                total_points = None
                opponent_points = None
                win = None
                
                teams_count = int(teams_data.get("count", 0))
                for j in range(teams_count):
                    team_index = str(j)
                    if team_index not in teams_data:
                        continue
                    
                    team_info = teams_data[team_index]["team"]
                    current_team_key = None
                    points = None
                    
                    if isinstance(team_info, list):
                        for info_item in team_info:
                            if isinstance(info_item, dict):
                                if "team_key" in info_item:
                                    current_team_key = info_item["team_key"]
                                elif "team_points" in info_item and "total" in info_item["team_points"]:
                                    points = info_item["team_points"]["total"]
                    
                    if current_team_key == team_key:
                        total_points = points
                    else:
                        opponent_team_key = current_team_key
                        opponent_points = points
                
                # 判断输赢
                if total_points is not None and opponent_points is not None:
                    try:
                        total_pts = float(total_points)
                        opp_pts = float(opponent_points)
                        win = total_pts > opp_pts
                    except (ValueError, TypeError):
                        win = None
                
                # 写入数据库
                if self.db_writer.write_team_stats(
                    team_key=team_key,
                    league_key=league_key,
                    stats_data={"matchup_week": week, "total_points": total_points, "opponent_points": opponent_points},
                    coverage_type="week",
                    season=season,
                    week=int(week) if week else None,
                    total_points=total_points,
                    opponent_team_key=opponent_team_key,
                    is_playoff=is_playoff,
                    win=win
                ):
                    processed_count += 1
            
            return processed_count
            
        except Exception as e:
            print(f"    处理团队matchups失败 {team_key}: {e}")
            return 0

    # ===== 时间序列数据获取功能 =====
    
    def fetch_historical_rosters(self, start_week: int = 1, end_week: Optional[int] = None,
                                start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """获取历史名单数据"""
        if not self.selected_league:
            print("✗ 未选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        season = self.selected_league.get('season', '2024')
        game_code = self.selected_league.get('game_code', 'nfl')
        
        print(f"🔄 开始获取历史名单数据: {league_key}")
        
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
        
        # 根据游戏类型选择时间范围
        if game_code.lower() == 'nfl':
            return self._fetch_rosters_by_weeks(team_keys, league_key, season, start_week, end_week)
        else:
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
                    self._process_roster_data_to_history_db(roster_data, team_key, league_key, 
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
                    self._process_roster_data_to_history_db(roster_data, team_key, league_key,
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
        
        # 首先获取所有球员的基础信息并确保存在于数据库中
        print("📋 获取球员基础数据...")
        all_players = self._fetch_all_league_players(league_key)
        if not all_players:
            print("✗ 获取球员基础信息失败")
            return False
        
        print(f"✓ 获取了 {len(all_players)} 个球员的基础信息")
        
        # 确保球员数据存在于数据库中
        print("📋 确保球员数据存在于数据库中...")
        self._ensure_players_exist_in_db(all_players, league_key)
        
        # 提取球员键
        player_keys = [player.get('player_key') for player in all_players if player.get('player_key')]
        if not player_keys:
            print("✗ 未找到有效的球员键")
            return False
        
        print(f"找到 {len(player_keys)} 个球员键")
        
        # 根据游戏类型选择时间范围
        if game_code.lower() == 'nfl':
            return self._fetch_player_stats_by_weeks(player_keys, all_players, league_key, season, start_week, end_week)
        else:
            return self._fetch_player_stats_by_dates(player_keys, all_players, league_key, season, start_date, end_date)
    
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
            
            print(f"✓ 球员数据检查完成: 已存在 {existing_count} 个，新创建 {created_count} 个")
            
        except Exception as e:
            print(f"确保球员数据存在时出错: {e}")
            self.db_writer.session.rollback()
    
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
        return self.db_writer.clear_database(confirm=confirm)
    
    def show_database_summary(self) -> None:
        """显示数据库摘要"""
        summary = self.db_writer.get_database_summary()
        print("\n📊 数据库摘要:")
        for table_name, count in summary.items():
            if count >= 0:
                print(f"  {table_name}: {count} 条记录")
            else:
                print(f"  {table_name}: 查询失败")
    
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
    
    def run_historical_data_fetch(self, weeks_back: int = 5, days_back: int = 30) -> bool:
        """执行历史数据获取流程"""
        print("🚀 开始Yahoo Fantasy历史数据获取...")
        
        # 检查是否已选择联盟
        if not self.selected_league:
            print("✗ 尚未选择联盟，请先选择联盟")
            return False
        
        league_key = self.selected_league['league_key']
        game_code = self.selected_league.get('game_code', 'nfl')
        current_week = int(self.selected_league.get('current_week', 1))
        
        # 获取历史名单数据
        print(f"\n📋 步骤1: 获取历史名单数据")
        if game_code.lower() == 'nfl':
            start_week = max(1, current_week - weeks_back)
            self.fetch_historical_rosters(start_week=start_week, end_week=current_week)
        else:
            start_date = date.today() - timedelta(days=days_back)
            self.fetch_historical_rosters(start_date=start_date)
        
        # 获取历史球员统计数据
        print(f"\n📋 步骤2: 获取历史球员统计数据")
        if game_code.lower() == 'nfl':
            start_week = max(1, current_week - weeks_back)
            self.fetch_historical_player_stats(start_week=start_week, end_week=current_week)
        else:
            start_date = date.today() - timedelta(days=days_back)
            self.fetch_historical_player_stats(start_date=start_date)
        
        # 显示统计摘要
        print(f"\n📊 数据获取统计:")
        print(self.db_writer.get_stats_summary())
        
        print("🎉 历史数据获取成功！")
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
        print("1. 单联盟完整数据获取")
        print("2. 时间序列历史数据获取")
        print("3. 选择联盟")
        print("4. 显示数据库摘要")
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
                    # 单联盟完整数据获取
                    print("\n🚀 开始单联盟完整数据获取...")
                    self.show_database_summary()  # 显示开始前的状态
                    
                    if self.run_complete_league_fetch():
                        self.show_database_summary()  # 显示结束后的状态
                    else:
                        print("\n❌ 单联盟数据获取失败")
                
                elif choice == "2":
                    # 时间序列历史数据获取
                    print("\n配置历史数据获取参数:")
                    weeks_back = self._get_int_input("回溯周数 (NFL, 默认5): ", 5)
                    days_back = self._get_int_input("回溯天数 (其他运动, 默认30): ", 30)
                    
                    print(f"\n🚀 开始历史数据获取 (回溯 {weeks_back} 周 / {days_back} 天)...")
                    self.show_database_summary()  # 显示开始前的状态
                    
                    if self.run_historical_data_fetch(weeks_back=weeks_back, days_back=days_back):
                        self.show_database_summary()  # 显示结束后的状态
                    else:
                        print("\n❌ 历史数据获取失败")
                
                elif choice == "3":
                    # 选择联盟
                    print("\n🔍 联盟选择...")
                    if self.select_league_interactive():
                        print("✅ 联盟选择成功")
                    else:
                        print("❌ 联盟选择失败")
                
                elif choice == "4":
                    # 显示数据库摘要
                    self.show_database_summary()
                
                elif choice == "5":
                    # 清空数据库
                    print("\n⚠️ 即将清空数据库，所有数据将被删除！")
                    confirm = input("请输入 'YES' 确认清空数据库: ").strip()
                    if confirm == "YES":
                        if self.clear_database(confirm=True):
                            print("✅ 数据库已清空")
                        else:
                            print("❌ 数据库清空失败")
                    else:
                        print("❌ 操作已取消")
                
                elif choice == "6":
                    # 退出
                    print("\n👋 感谢使用！再见！")
                    break
                
                else:
                    print("❌ 无效选择，请输入1-6之间的数字")
                
                # 等待用户确认后继续
                if choice in ["1", "2", "3", "4", "5"]:
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


