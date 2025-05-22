#!/usr/bin/env python3
"""
联盟数据获取工具
从Yahoo Fantasy API获取联盟数据并保存为JSON文件
"""
import os
import sys
import time
from datetime import datetime

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import get_api_data, save_json_data, load_json_data, DATA_DIR
from fetch_games import fetch_games_data, extract_game_keys

def fetch_leagues_data(game_key):
    """获取指定game下用户的leagues数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取game {game_key}的leagues数据")
        return data
    return None

def fetch_teams_data(league_key):
    """获取指定league下的teams数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取league {league_key}的teams数据")
        return data
    return None

def fetch_players_data(team_key):
    """获取指定team的players数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取team {team_key}的players数据")
        return data
    return None

def fetch_and_save_league_data(game_key):
    """获取并保存指定游戏的联盟数据"""
    try:
        # 获取联盟数据
        print(f"获取游戏 {game_key} 的联盟数据...")
        leagues_data = fetch_leagues_data(game_key)
        
        if not leagues_data:
            print(f"无法获取游戏 {game_key} 的联盟数据")
            return False
        
        # 保存原始数据到文件
        file_name = f"league_data_{game_key}.json"
        return save_json_data(leagues_data, file_name, data_type="leagues")
        
    except Exception as e:
        print(f"获取游戏 {game_key} 的联盟数据时出错: {str(e)}")
        return False

def fetch_all_leagues():
    """获取所有游戏的联盟数据"""
    print("开始获取所有联盟数据...")
    
    # 获取游戏数据
    games_data = fetch_games_data()
    
    if not games_data:
        print("无法获取游戏数据")
        return
    
    # 提取游戏键
    game_keys = extract_game_keys(games_data)
    
    if not game_keys:
        print("未找到任何游戏键")
        return
    
    print(f"找到 {len(game_keys)} 个游戏键")
    
    # 获取每个游戏的联盟数据
    success_count = 0
    for i, game_key in enumerate(game_keys):
        print(f"\n处理游戏 {i+1}/{len(game_keys)}: {game_key}")
        
        if fetch_and_save_league_data(game_key):
            success_count += 1
        
        # 避免请求过于频繁，添加延迟
        if i < len(game_keys) - 1:
            wait_time = 2  # 固定延迟2秒
            print(f"等待 {wait_time} 秒...")
            time.sleep(wait_time)
    
    print(f"\n所有联盟数据获取完成! 成功: {success_count}/{len(game_keys)}")
    
    # 生成包含所有联盟数据的汇总文件
    consolidate_league_data()

def consolidate_league_data():
    """整合所有联盟数据到一个文件"""
    all_leagues = {}
    
    try:
        # 获取所有联盟数据文件
        league_files = list(DATA_DIR.glob("league_data_*.json"))
        
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
                extracted_leagues = extract_leagues_from_data(data, game_key)
                
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {str(e)}")
        
        # 保存整合的数据
        file_name = "all_leagues_data.json"
        save_json_data(all_leagues, file_name, data_type="leagues")
    
    except Exception as e:
        print(f"整合联盟数据时出错: {str(e)}")

def extract_leagues_from_data(data, game_key):
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

def fetch_league_settings(league_key):
    """获取指定联盟的设置数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取联盟 {league_key} 的设置数据")
        return data
    return None

def fetch_league_standings(league_key):
    """获取指定联盟的排名数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取联盟 {league_key} 的排名数据")
        return data
    return None

def fetch_league_scoreboard(league_key):
    """获取指定联盟的记分板数据"""
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard?format=json"
    data = get_api_data(url)
    
    if data:
        print(f"成功获取联盟 {league_key} 的记分板数据")
        return data
    return None

def fetch_and_save_league_details(league_key):
    """获取并保存指定联盟的详细数据"""
    try:
        # 获取联盟设置数据
        print(f"获取联盟 {league_key} 的设置数据...")
        settings_data = fetch_league_settings(league_key)
        if settings_data:
            file_name = f"league_settings_{league_key}.json"
            save_json_data(settings_data, file_name, data_type="league_settings")
        
        # 获取联盟排名数据
        print(f"获取联盟 {league_key} 的排名数据...")
        standings_data = fetch_league_standings(league_key)
        if standings_data:
            file_name = f"league_standings_{league_key}.json"
            save_json_data(standings_data, file_name, data_type="league_standings")
        
        # 获取联盟记分板数据
        print(f"获取联盟 {league_key} 的记分板数据...")
        scoreboard_data = fetch_league_scoreboard(league_key)
        if scoreboard_data:
            file_name = f"league_scoreboard_{league_key}.json"
            save_json_data(scoreboard_data, file_name, data_type="league_scoreboards")
        
        print(f"联盟 {league_key} 的详细数据获取完成")
        return True
    except Exception as e:
        print(f"获取联盟 {league_key} 的详细数据时出错: {str(e)}")
        return False

def get_league_key_from_data(leagues_data, game_key):
    """从联盟数据中提取第一个联盟键"""
    try:
        if not leagues_data or "fantasy_content" not in leagues_data:
            return None
            
        fantasy_content = leagues_data["fantasy_content"]
        
        if "users" not in fantasy_content or "0" not in fantasy_content["users"]:
            return None
            
        user_data = fantasy_content["users"]["0"]["user"]
        
        if len(user_data) < 2 or "games" not in user_data[1]:
            return None
            
        games_container = user_data[1]["games"]
        
        # 查找包含指定game_key的游戏
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
                
            # 找到了匹配的游戏，提取第一个联盟键
            if len(game_data) > 1 and "leagues" in game_data[1]:
                leagues_container = game_data[1]["leagues"]
                
                if leagues_container.get("count", 0) > 0 and "0" in leagues_container:
                    league_container = leagues_container["0"]
                    
                    if "league" in league_container:
                        league_data = league_container["league"]
                        
                        if isinstance(league_data, list) and len(league_data) > 0 and isinstance(league_data[0], dict):
                            return league_data[0].get("league_key")
    
    except Exception as e:
        print(f"从联盟数据中提取联盟键时出错: {str(e)}")
        
    return None

def fetch_all_leagues_details():
    """获取所有联盟的详细数据"""
    # 首先加载联盟汇总数据
    all_leagues_file = "all_leagues_data.json"
    all_leagues = load_json_data(all_leagues_file, data_type="leagues")
    
    if not all_leagues:
        print("找不到联盟汇总数据文件，请先运行fetch_all_leagues获取联盟基本数据")
        return
    
    league_keys = []
    for game_key, leagues in all_leagues.items():
        for league in leagues:
            if "league_key" in league:
                league_keys.append(league["league_key"])
    
    print(f"找到 {len(league_keys)} 个联盟，开始获取详细数据")
    
    success_count = 0
    for i, league_key in enumerate(league_keys):
        print(f"\n处理联盟 {i+1}/{len(league_keys)}: {league_key}")
        
        if fetch_and_save_league_details(league_key):
            success_count += 1
        
        # 避免请求过于频繁，添加延迟
        if i < len(league_keys) - 1:
            wait_time = 2  # 固定延迟2秒
            print(f"等待 {wait_time} 秒...")
            time.sleep(wait_time)
    
    print(f"\n所有联盟详细数据获取完成! 成功: {success_count}/{len(league_keys)}")

if __name__ == "__main__":
    print("开始获取Yahoo Fantasy联盟数据...")
    fetch_all_leagues()
    
    # 询问是否获取联盟详细数据
    answer = input("\n是否获取所有联盟的详细数据(settings/standings/scoreboard)? (y/n): ")
    if answer.lower() == 'y':
        fetch_all_leagues_details()
    
    print("联盟数据获取完成!") 