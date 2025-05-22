#!/usr/bin/env python3
"""
获取游戏数据脚本
从Yahoo Fantasy API获取所有游戏数据
"""
import os
import sys
import json
from datetime import datetime

# 根据运行位置调整导入方式
if __name__ == "__main__":
    # 当作为主脚本运行时，确保可以导入本地模块
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from utils.yahoo_api_utils import get_api_data, save_json_data, DATA_DIR
else:
    # 当作为模块导入时，使用相对导入
    from utils.yahoo_api_utils import get_api_data, save_json_data, DATA_DIR

def fetch_games_data():
    """获取用户的games数据"""
    url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
    data = get_api_data(url)
    
    if data:
        print("成功获取用户的games数据")
        # 自动保存原始数据到文件
        save_json_data(data, "games_data.json", data_type="games")
        return data
    return None

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

def extract_game_info(game_data):
    """从游戏数据中提取游戏信息"""
    try:
        if isinstance(game_data, list) and len(game_data) > 0 and isinstance(game_data[0], dict):
            # 提取基本信息
            game_info = game_data[0].copy()
            
            # 提取其他信息
            for item in game_data[1:]:
                if isinstance(item, dict):
                    game_info.update(item)
            
            return game_info
    except Exception as e:
        print(f"提取游戏信息时出错: {str(e)}")
    
    return {}

def extract_game_keys(games_data):
    """从游戏数据中提取所有游戏键"""
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
        
        # 提取每个游戏的game_key
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
            
            # 提取game_key
            if isinstance(game_data, list) and len(game_data) > 0 and isinstance(game_data[0], dict):
                game_key = game_data[0].get("game_key")
                if game_key:
                    game_keys.append(game_key)
                    print(f"提取到游戏键: {game_key}")
            
    except Exception as e:
        print(f"提取游戏键时出错: {str(e)}")
    
    return game_keys

def get_first_game_key(games_data=None):
    """获取第一个游戏的键，用于测试"""
    if not games_data:
        games_data = fetch_games_data()
    
    if not games_data or "fantasy_content" not in games_data:
        print("无法获取游戏数据")
        return None
    
    try:
        fantasy_content = games_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        game_data = games_container["0"]["game"]
        
        if isinstance(game_data, list):
            game_key = game_data[0]["game_key"]
        else:
            game_key = game_data["game_key"]
            
        return game_key
    except (KeyError, IndexError) as e:
        print(f"获取第一个游戏键时出错: {str(e)}")
        return None

if __name__ == "__main__":
    # 获取用户的games数据
    games_data = fetch_games_data()
    
    if games_data:
        # 提取所有游戏键
        game_keys = extract_game_keys(games_data)
        print(f"\n成功提取了 {len(game_keys)} 个游戏键: {', '.join(game_keys)}")
        
        # 获取第一个游戏键进行测试
        first_game_key = get_first_game_key(games_data)
        if first_game_key:
            print(f"\n第一个游戏键: {first_game_key}")
        
        print("\n游戏数据获取和提取完成!")
    else:
        print("获取游戏数据失败!") 