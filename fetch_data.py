#!/usr/bin/env python3
"""
Yahoo Fantasy数据获取整合工具
统一入口，用于获取各类Yahoo Fantasy数据
"""
import os
import sys
import argparse
import time
from datetime import datetime

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import DATA_DIR, save_json_data
from fetch_games import fetch_games_data, extract_game_keys
from fetch_leagues import fetch_leagues_data, fetch_teams_data, fetch_players_data, fetch_all_leagues, fetch_and_save_league_details, fetch_all_leagues_details

def fetch_all_data():
    """获取所有类型的数据"""
    print("开始获取所有Yahoo Fantasy数据...")
    
    # 1. 首先获取游戏数据
    games_data = fetch_games_data()
    if not games_data:
        print("获取游戏数据失败，无法继续")
        return
    
    # 2. 获取所有联盟数据
    fetch_all_leagues()
    
    # 3. 未来可以添加获取团队、玩家等数据的功能
    
    print("所有数据获取完成!")

def fetch_specific_game_data(game_key):
    """获取特定游戏的所有相关数据"""
    print(f"开始获取游戏 {game_key} 的所有相关数据...")
    
    # 1. 获取联盟数据
    leagues_data = fetch_leagues_data(game_key)
    if not leagues_data:
        print(f"获取游戏 {game_key} 的联盟数据失败")
        return
    
    # 保存联盟数据
    file_name = f"league_data_{game_key}.json"
    save_json_data(leagues_data, file_name, data_type="leagues")
    
    # 2. 从联盟数据中提取联盟键
    try:
        fantasy_content = leagues_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        
        # 查找匹配的游戏
        for i in range(int(games_container.get("count", 0))):
            game_container = games_container[str(i)]
            if "game" not in game_container:
                continue
                
            game_data = game_container["game"]
            current_game_key = None
            
            if isinstance(game_data, list) and len(game_data) > 0:
                current_game_key = game_data[0].get("game_key")
            
            if current_game_key != game_key:
                continue
            
            # 找到匹配游戏，获取联盟数据
            if len(game_data) > 1 and "leagues" in game_data[1]:
                leagues_container = game_data[1]["leagues"]
                leagues_count = int(leagues_container.get("count", 0))
                
                print(f"游戏 {game_key} 有 {leagues_count} 个联盟")
                
                # 获取每个联盟的团队数据
                for j in range(leagues_count):
                    league_container = leagues_container.get(str(j))
                    if not league_container or "league" not in league_container:
                        continue
                    
                    league_data = league_container["league"]
                    league_key = None
                    
                    if isinstance(league_data, list) and len(league_data) > 0:
                        league_key = league_data[0].get("league_key")
                    
                    if not league_key:
                        continue
                    
                    print(f"\n获取联盟 {league_key} 的团队数据")
                    teams_data = fetch_teams_data(league_key)
                    
                    if teams_data:
                        # 保存团队数据
                        file_name = f"teams_data_{league_key}.json"
                        save_json_data(teams_data, file_name, data_type="teams")
                        
                        # 未来可以添加获取玩家数据的功能
                    
                    # 避免请求过于频繁
                    time.sleep(1)
    
    except Exception as e:
        print(f"处理游戏 {game_key} 的数据时出错: {str(e)}")
    
    print(f"游戏 {game_key} 的数据获取完成!")

def main():
    """主函数，处理命令行参数并执行相应操作"""
    parser = argparse.ArgumentParser(description="Yahoo Fantasy数据获取工具")
    parser.add_argument("--games", action="store_true", help="只获取游戏数据")
    parser.add_argument("--leagues", action="store_true", help="获取所有联盟数据")
    parser.add_argument("--game-key", type=str, help="获取特定游戏的所有相关数据")
    parser.add_argument("--league-key", type=str, help="获取特定联盟的详细数据")
    parser.add_argument("--league-details", action="store_true", help="获取所有联盟的详细数据")
    parser.add_argument("--all", action="store_true", help="获取所有类型的数据")
    
    args = parser.parse_args()
    
    if args.games:
        # 只获取游戏数据
        print("只获取游戏数据...")
        fetch_games_data()
    elif args.leagues:
        # 获取所有联盟数据
        print("获取所有联盟数据...")
        fetch_all_leagues()
    elif args.game_key:
        # 获取特定游戏的所有相关数据
        fetch_specific_game_data(args.game_key)
    elif args.league_key:
        # 获取特定联盟的详细数据
        fetch_and_save_league_details(args.league_key)
    elif args.league_details:
        # 获取所有联盟的详细数据
        fetch_all_leagues_details()
    elif args.all:
        # 获取所有数据
        fetch_all_data()
        # 获取所有联盟的详细数据
        fetch_all_leagues_details()
    else:
        # 默认只获取游戏数据
        print("未指定获取类型，默认只获取游戏数据...")
        fetch_games_data()
    
    print("数据获取任务完成!")

if __name__ == "__main__":
    main() 