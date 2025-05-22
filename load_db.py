#!/usr/bin/env python3
"""
数据库加载工具
从JSON文件加载数据到数据库
"""
import os
import sys
import argparse
import json
from datetime import datetime
from pathlib import Path

# 确保可以正确导入模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.yahoo_api_utils import DATA_DIR, load_json_data, get_data_dir
from models import Game, League, Team, User, get_db, create_tables, SessionLocal, Base, engine, LeagueSetting, TeamStanding, Scoreboard, MatchupTeam, UserGame

def load_games_data():
    """加载游戏数据到数据库"""
    print("加载游戏数据到数据库...")
    # 检查游戏数据文件
    games_file = "games_data.json"
    
    # 加载JSON数据
    games_data = load_json_data(games_file, data_type="games")
    if not games_data:
        print("游戏数据加载失败")
        return False
    
    try:
        # 解析游戏数据
        games = []
        if "fantasy_content" in games_data and "users" in games_data["fantasy_content"]:
            fantasy_content = games_data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            
            if len(user_data) >= 2 and "games" in user_data[1]:
                games_container = user_data[1]["games"]
                games_count = int(games_container.get("count", 0))
                
                # 获取数据库会话
                db = SessionLocal()
                
                # 遍历所有游戏
                for i in range(games_count):
                    str_index = str(i)
                    
                    if str_index not in games_container:
                        continue
                    
                    game_container = games_container[str_index]
                    
                    if "game" not in game_container:
                        continue
                    
                    game_data = game_container["game"]
                    
                    # 提取游戏数据
                    game_info = {}
                    if isinstance(game_data, list):
                        # 合并所有字典
                        for item in game_data:
                            if isinstance(item, dict):
                                game_info.update(item)
                    else:
                        game_info = game_data
                    
                    # 检查必要字段
                    if "game_key" not in game_info:
                        continue
                    
                    # 检查游戏是否已存在
                    game_key = game_info["game_key"]
                    existing_game = db.query(Game).filter(Game.game_key == game_key).first()
                    
                    if existing_game:
                        # 更新现有游戏
                        for key, value in game_info.items():
                            if hasattr(existing_game, key) and key != "id":
                                setattr(existing_game, key, value)
                        print(f"更新游戏: {game_key}")
                    else:
                        # 创建新游戏
                        new_game = Game(
                            game_key=game_key,
                            game_id=game_info.get("game_id", ""),
                            name=game_info.get("name", ""),
                            code=game_info.get("code", ""),
                            type=game_info.get("type", ""),
                            url=game_info.get("url", ""),
                            season=game_info.get("season", "")
                        )
                        db.add(new_game)
                        print(f"添加新游戏: {game_key}")
                
                # 提交更改
                db.commit()
                print(f"成功处理 {games_count} 个游戏")
                db.close()
                return True
    
    except Exception as e:
        print(f"加载游戏数据时出错: {str(e)}")
        return False

def load_leagues_to_db():
    """加载联盟数据到数据库"""
    print("加载联盟数据到数据库...")
    # 检查联盟数据文件
    league_files = list(get_data_dir("leagues").glob("league_data_*.json"))
    
    if not league_files:
        print("未找到任何联盟数据文件")
        return False
    
    print(f"找到 {len(league_files)} 个联盟数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    for file_path in league_files:
        try:
            # 提取游戏键
            game_key = file_path.stem.replace("league_data_", "")
            
            # 查找对应的游戏
            game = db.query(Game).filter(Game.game_key == game_key).first()
            if not game:
                print(f"找不到游戏: {game_key}")
                continue
            
            # 加载联盟数据
            leagues_data = load_json_data(file_path)
            if not leagues_data:
                print(f"无法加载联盟数据: {file_path}")
                continue
            
            # 处理联盟数据
            if "fantasy_content" in leagues_data and "users" in leagues_data["fantasy_content"]:
                fantasy_content = leagues_data["fantasy_content"]
                user_data = fantasy_content["users"]["0"]["user"]
                
                if len(user_data) >= 2 and "games" in user_data[1]:
                    games_container = user_data[1]["games"]
                    
                    # 查找包含当前game_key的游戏
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
                        
                        # 找到匹配的游戏，提取联盟数据
                        if len(game_data) > 1 and "leagues" in game_data[1]:
                            leagues_container = game_data[1]["leagues"]
                            leagues_count = int(leagues_container.get("count", 0))
                            
                            # 遍历所有联盟
                            for j in range(leagues_count):
                                str_league_index = str(j)
                                
                                if str_league_index not in leagues_container:
                                    continue
                                
                                league_container = leagues_container[str_league_index]
                                
                                if "league" not in league_container:
                                    continue
                                
                                league_data = league_container["league"]
                                
                                # 提取联盟信息
                                league_info = {}
                                if isinstance(league_data, list):
                                    for item in league_data:
                                        if isinstance(item, dict):
                                            league_info.update(item)
                                else:
                                    league_info = league_data
                                
                                # 检查必要字段
                                if "league_key" not in league_info:
                                    continue
                                
                                # 检查联盟是否已存在
                                league_key = league_info["league_key"]
                                existing_league = db.query(League).filter(League.league_key == league_key).first()
                                
                                if existing_league:
                                    # 更新现有联盟
                                    for key, value in league_info.items():
                                        if hasattr(existing_league, key) and key != "id" and key != "game_id":
                                            # 处理特殊字段
                                            if key in ["start_date", "end_date"] and value:
                                                value = datetime.strptime(value, "%Y-%m-%d")
                                            elif key == "is_finished":
                                                value = bool(int(value))
                                            
                                            setattr(existing_league, key, value)
                                    
                                    print(f"更新联盟: {league_key}")
                                else:
                                    # 创建新联盟
                                    start_date = None
                                    end_date = None
                                    
                                    if "start_date" in league_info and league_info["start_date"]:
                                        try:
                                            start_date = datetime.strptime(league_info["start_date"], "%Y-%m-%d")
                                        except:
                                            pass
                                    
                                    if "end_date" in league_info and league_info["end_date"]:
                                        try:
                                            end_date = datetime.strptime(league_info["end_date"], "%Y-%m-%d")
                                        except:
                                            pass
                                    
                                    # 检查是否有league_type字段
                                    league_type = league_info.get("league_type", "unknown")
                                    
                                    # 处理is_finished字段
                                    is_finished = False
                                    if "is_finished" in league_info:
                                        is_finished = bool(int(league_info["is_finished"]))
                                    
                                    new_league = League(
                                        league_key=league_key,
                                        league_id=league_info.get("league_id", ""),
                                        name=league_info.get("name", ""),
                                        url=league_info.get("url", ""),
                                        draft_status=league_info.get("draft_status", ""),
                                        num_teams=int(league_info.get("num_teams", 0)),
                                        scoring_type=league_info.get("scoring_type", ""),
                                        league_type=league_type,
                                        current_week=int(league_info.get("current_week", 0)),
                                        start_week=int(league_info.get("start_week", 0)),
                                        end_week=int(league_info.get("end_week", 0)),
                                        start_date=start_date,
                                        end_date=end_date,
                                        is_finished=is_finished,
                                        game_id=game.id
                                    )
                                    db.add(new_league)
                                    print(f"添加新联盟: {league_key}")
                            
                            success_count += 1
                            break
        
        except Exception as e:
            print(f"处理联盟数据文件 {file_path} 时出错: {str(e)}")
    
    # 提交更改
    db.commit()
    db.close()
    
    print(f"成功处理 {success_count} 个联盟数据文件")
    return success_count > 0

def load_teams_to_db():
    """加载团队数据到数据库"""
    print("加载团队数据到数据库...")
    # 检查团队数据文件
    team_files = list(get_data_dir("teams").glob("teams_data_*.json"))
    
    if not team_files:
        print("未找到任何团队数据文件")
        return False
    
    print(f"找到 {len(team_files)} 个团队数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    for file_path in team_files:
        try:
            # 提取联盟键
            league_key = file_path.stem.replace("teams_data_", "")
            
            # 查找对应的联盟
            league = db.query(League).filter(League.league_key == league_key).first()
            if not league:
                print(f"找不到联盟: {league_key}")
                continue
            
            # 加载团队数据
            teams_data = load_json_data(file_path)
            if not teams_data:
                print(f"无法加载团队数据: {file_path}")
                continue
            
            # 处理团队数据
            if "fantasy_content" in teams_data and "league" in teams_data["fantasy_content"]:
                fantasy_content = teams_data["fantasy_content"]
                league_data = fantasy_content["league"]
                
                # 提取联盟信息（可选）
                # 如果需要更新联盟信息，可以在这里处理
                
                # 提取团队数据
                if len(league_data) > 1 and "teams" in league_data[1]:
                    teams_container = league_data[1]["teams"]
                    teams_count = int(teams_container.get("count", 0))
                    
                    # 遍历所有团队
                    for i in range(teams_count):
                        str_team_index = str(i)
                        
                        if str_team_index not in teams_container:
                            continue
                        
                        team_container = teams_container[str_team_index]
                        
                        if "team" not in team_container:
                            continue
                        
                        team_data = team_container["team"]
                        
                        # 提取团队信息
                        team_info = {}
                        if isinstance(team_data, list) and len(team_data) > 0:
                            for item in team_data:
                                if isinstance(item, list):
                                    for sub_item in item:
                                        if isinstance(sub_item, dict):
                                            team_info.update(sub_item)
                                elif isinstance(item, dict):
                                    team_info.update(item)
                        
                        # 检查必要字段
                        if "team_key" not in team_info:
                            continue
                        
                        # 提取经理信息
                        manager_info = None
                        if "managers" in team_info and team_info["managers"]:
                            managers = team_info["managers"]
                            if isinstance(managers, list) and len(managers) > 0:
                                manager_data = managers[0].get("manager", {})
                                if manager_data:
                                    manager_info = manager_data
                        
                        # 检查是否需要创建或更新用户
                        manager_user_id = None
                        if manager_info and "guid" in manager_info:
                            yahoo_guid = manager_info["guid"]
                            
                            # 检查用户是否已存在
                            if yahoo_guid != "--":  # 排除隐藏用户
                                existing_user = db.query(User).filter(User.yahoo_guid == yahoo_guid).first()
                                
                                if existing_user:
                                    # 更新现有用户
                                    manager_user_id = existing_user.id
                                else:
                                    # 创建新用户
                                    new_user = User(
                                        yahoo_guid=yahoo_guid,
                                        name=manager_info.get("nickname", ""),
                                        nickname=manager_info.get("nickname", ""),
                                        email=manager_info.get("email", "")
                                    )
                                    db.add(new_user)
                                    db.flush()  # 获取新用户ID
                                    manager_user_id = new_user.id
                                    print(f"添加新用户: {new_user.nickname}")
                        
                        # 检查团队是否已存在
                        team_key = team_info["team_key"]
                        existing_team = db.query(Team).filter(Team.team_key == team_key).first()
                        
                        # 处理队徽URL
                        team_logo_url = None
                        if "team_logos" in team_info and team_info["team_logos"]:
                            logos = team_info["team_logos"]
                            if isinstance(logos, list) and len(logos) > 0:
                                logo_data = logos[0].get("team_logo", {})
                                if logo_data:
                                    team_logo_url = logo_data.get("url")
                        
                        if existing_team:
                            # 更新现有团队
                            existing_team.name = team_info.get("name", "")
                            existing_team.is_owned_by_current_user = "is_owned_by_current_login" in team_info
                            existing_team.url = team_info.get("url", "")
                            existing_team.team_logo = team_logo_url
                            existing_team.waiver_priority = team_info.get("waiver_priority")
                            existing_team.number_of_moves = team_info.get("number_of_moves", 0)
                            existing_team.number_of_trades = int(team_info.get("number_of_trades", 0))
                            
                            if manager_user_id:
                                existing_team.manager_user_id = manager_user_id
                            
                            print(f"更新团队: {team_key}")
                        else:
                            # 创建新团队
                            new_team = Team(
                                team_key=team_key,
                                team_id=team_info.get("team_id", ""),
                                name=team_info.get("name", ""),
                                is_owned_by_current_user="is_owned_by_current_login" in team_info,
                                url=team_info.get("url", ""),
                                team_logo=team_logo_url,
                                waiver_priority=team_info.get("waiver_priority"),
                                number_of_moves=team_info.get("number_of_moves", 0),
                                number_of_trades=int(team_info.get("number_of_trades", 0)),
                                league_id=league.id,
                                manager_user_id=manager_user_id
                            )
                            db.add(new_team)
                            print(f"添加新团队: {team_key}")
                    
                    success_count += 1
        
        except Exception as e:
            print(f"处理团队数据文件 {file_path} 时出错: {str(e)}")
    
    # 提交更改
    db.commit()
    db.close()
    
    print(f"成功处理 {success_count} 个团队数据文件")
    return success_count > 0

def load_league_settings_to_db():
    """加载联盟设置数据到数据库"""
    print("加载联盟设置数据到数据库...")
    # 检查联盟设置数据文件
    settings_files = list(get_data_dir("league_settings").glob("league_settings_*.json"))
    
    if not settings_files:
        print("未找到任何联盟设置数据文件")
        return False
    
    print(f"找到 {len(settings_files)} 个联盟设置数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    for file_path in settings_files:
        try:
            # 提取联盟键
            league_key = file_path.stem.replace("league_settings_", "")
            
            # 查找对应的联盟
            league = db.query(League).filter(League.league_key == league_key).first()
            if not league:
                print(f"找不到联盟: {league_key}")
                continue
            
            # 加载设置数据
            settings_data = load_json_data(file_path)
            if not settings_data:
                print(f"无法加载联盟设置数据: {file_path}")
                continue
            
            # 处理设置数据
            if "fantasy_content" in settings_data and "league" in settings_data["fantasy_content"]:
                league_data = settings_data["fantasy_content"]["league"]
                
                # 提取设置数据
                settings_json = {}
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        settings_json = item["settings"]
                        break
                
                if settings_json:
                    # 检查是否已存在设置记录
                    existing_settings = db.query(LeagueSetting).filter(LeagueSetting.league_id == league.id).first()
                    
                    if existing_settings:
                        # 更新现有设置
                        existing_settings.settings_data = json.dumps(settings_json)
                        print(f"更新联盟设置: {league_key}")
                    else:
                        # 创建新设置记录
                        new_settings = LeagueSetting(
                            league_id=league.id,
                            settings_data=json.dumps(settings_json)
                        )
                        db.add(new_settings)
                        print(f"添加新联盟设置: {league_key}")
                    
                    success_count += 1
        
        except Exception as e:
            print(f"处理联盟设置数据文件 {file_path} 时出错: {str(e)}")
    
    # 提交更改
    db.commit()
    db.close()
    
    print(f"成功处理 {success_count} 个联盟设置数据文件")
    return success_count > 0

def load_league_standings_to_db():
    """加载联盟排名数据到数据库"""
    print("加载联盟排名数据到数据库...")
    # 检查联盟排名数据文件
    standings_files = list(get_data_dir("league_standings").glob("league_standings_*.json"))
    
    if not standings_files:
        print("未找到任何联盟排名数据文件")
        return False
    
    print(f"找到 {len(standings_files)} 个联盟排名数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    for file_path in standings_files:
        try:
            # 提取联盟键
            league_key = file_path.stem.replace("league_standings_", "")
            
            # 查找对应的联盟
            league = db.query(League).filter(League.league_key == league_key).first()
            if not league:
                print(f"找不到联盟: {league_key}")
                continue
            
            # 加载排名数据
            standings_data = load_json_data(file_path)
            if not standings_data:
                print(f"无法加载联盟排名数据: {file_path}")
                continue
            
            # 处理排名数据
            if "fantasy_content" in standings_data and "league" in standings_data["fantasy_content"]:
                league_data = standings_data["fantasy_content"]["league"]
                
                # 提取排名数据
                standings_json = {}
                for item in league_data:
                    if isinstance(item, dict) and "standings" in item:
                        standings_json = item["standings"]
                        break
                
                if standings_json and "teams" in standings_json:
                    teams_container = standings_json["teams"]
                    
                    for i in range(int(teams_container.get("count", 0))):
                        team_container = teams_container.get(str(i))
                        if not team_container or "team" not in team_container:
                            continue
                        
                        team_data = team_container["team"]
                        
                        # 提取团队键和排名数据
                        team_key = None
                        team_standings = {}
                        
                        for item in team_data:
                            if isinstance(item, dict):
                                if "team_key" in item:
                                    team_key = item["team_key"]
                                elif "team_standings" in item:
                                    team_standings = item["team_standings"]
                        
                        if not team_key or not team_standings:
                            continue
                        
                        # 查找对应的团队
                        team = db.query(Team).filter(Team.team_key == team_key).first()
                        if not team:
                            print(f"找不到团队: {team_key}")
                            continue
                        
                        # 提取排名详情
                        rank = int(team_standings.get("rank", 0))
                        points_for = float(team_standings.get("points_for", 0))
                        points_against = float(team_standings.get("points_against", 0))
                        
                        # 提取胜负记录
                        outcome_totals = team_standings.get("outcome_totals", {})
                        wins = int(outcome_totals.get("wins", 0))
                        losses = int(outcome_totals.get("losses", 0))
                        ties = int(outcome_totals.get("ties", 0))
                        
                        percentage = float(team_standings.get("percentage", 0))
                        streak = team_standings.get("streak", "")
                        
                        # 检查是否已存在排名记录
                        existing_standing = db.query(TeamStanding).filter(TeamStanding.team_id == team.id).first()
                        
                        if existing_standing:
                            # 更新现有排名
                            existing_standing.rank = rank
                            existing_standing.points_for = points_for
                            existing_standing.points_against = points_against
                            existing_standing.wins = wins
                            existing_standing.losses = losses
                            existing_standing.ties = ties
                            existing_standing.percentage = percentage
                            existing_standing.streak = streak
                            print(f"更新团队排名: {team_key}")
                        else:
                            # 创建新排名记录
                            new_standing = TeamStanding(
                                team_id=team.id,
                                rank=rank,
                                points_for=points_for,
                                points_against=points_against,
                                wins=wins,
                                losses=losses,
                                ties=ties,
                                percentage=percentage,
                                streak=streak
                            )
                            db.add(new_standing)
                            print(f"添加新团队排名: {team_key}")
                    
                    success_count += 1
        
        except Exception as e:
            print(f"处理联盟排名数据文件 {file_path} 时出错: {str(e)}")
    
    # 提交更改
    db.commit()
    db.close()
    
    print(f"成功处理 {success_count} 个联盟排名数据文件")
    return success_count > 0

def load_league_scoreboards_to_db():
    """加载联盟记分板数据到数据库"""
    print("加载联盟记分板数据到数据库...")
    # 检查联盟记分板数据文件
    scoreboard_files = list(get_data_dir("league_scoreboards").glob("league_scoreboard_*.json"))
    
    if not scoreboard_files:
        print("未找到任何联盟记分板数据文件")
        return False
    
    print(f"找到 {len(scoreboard_files)} 个联盟记分板数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    for file_path in scoreboard_files:
        try:
            # 提取联盟键
            league_key = file_path.stem.replace("league_scoreboard_", "")
            
            # 查找对应的联盟
            league = db.query(League).filter(League.league_key == league_key).first()
            if not league:
                print(f"找不到联盟: {league_key}")
                continue
            
            # 加载记分板数据
            scoreboard_data = load_json_data(file_path)
            if not scoreboard_data:
                print(f"无法加载联盟记分板数据: {file_path}")
                continue
            
            # 处理记分板数据
            if "fantasy_content" in scoreboard_data and "league" in scoreboard_data["fantasy_content"]:
                league_data = scoreboard_data["fantasy_content"]["league"]
                
                # 提取记分板数据
                scoreboard_json = {}
                week = None
                
                for item in league_data:
                    if isinstance(item, dict):
                        if "current_week" in item:
                            week = int(item["current_week"])
                        if "scoreboard" in item:
                            scoreboard_json = item["scoreboard"]
                
                if not scoreboard_json or not week:
                    continue
                
                # 检查是否已存在记分板记录
                existing_scoreboard = db.query(Scoreboard).filter(
                    Scoreboard.league_id == league.id,
                    Scoreboard.week == week
                ).first()
                
                if existing_scoreboard:
                    # 清除旧的对决数据
                    for matchup in existing_scoreboard.matchups:
                        db.query(MatchupTeam).filter(MatchupTeam.matchup_id == matchup.id).delete()
                    db.query(Matchup).filter(Matchup.scoreboard_id == existing_scoreboard.id).delete()
                    scoreboard_id = existing_scoreboard.id
                    print(f"更新记分板: {league_key} 第 {week} 周")
                else:
                    # 创建新记分板记录
                    new_scoreboard = Scoreboard(
                        league_id=league.id,
                        week=week,
                        status=scoreboard_json.get("status", ""),
                        is_playoffs=scoreboard_json.get("is_playoffs", "0") == "1",
                        is_consolation=scoreboard_json.get("is_consolation", "0") == "1"
                    )
                    db.add(new_scoreboard)
                    db.flush()  # 获取新ID
                    scoreboard_id = new_scoreboard.id
                    print(f"添加新记分板: {league_key} 第 {week} 周")
                
                # 处理对决数据
                if "matchups" in scoreboard_json:
                    matchups_container = scoreboard_json["matchups"]
                    
                    for i in range(int(matchups_container.get("count", 0))):
                        matchup_container = matchups_container.get(str(i))
                        if not matchup_container or "matchup" not in matchup_container:
                            continue
                        
                        matchup_data = matchup_container["matchup"]
                        
                        # 提取对决信息
                        is_playoffs = matchup_data.get("is_playoffs", "0") == "1"
                        is_consolation = matchup_data.get("is_consolation", "0") == "1"
                        is_tied = matchup_data.get("is_tied", "0") == "1"
                        status = matchup_data.get("status", "")
                        winner_team_key = matchup_data.get("winner_team_key")
                        winner_team_id = None
                        
                        if winner_team_key:
                            winner_team = db.query(Team).filter(Team.team_key == winner_team_key).first()
                            if winner_team:
                                winner_team_id = winner_team.id
                        
                        # 创建对决记录
                        new_matchup = Matchup(
                            scoreboard_id=scoreboard_id,
                            week=week,
                            status=status,
                            is_playoffs=is_playoffs,
                            is_consolation=is_consolation,
                            is_tied=is_tied,
                            winner_team_id=winner_team_id
                        )
                        db.add(new_matchup)
                        db.flush()  # 获取新ID
                        
                        # 处理对决中的团队
                        if "teams" in matchup_data:
                            teams_container = matchup_data["teams"]
                            
                            for j in range(int(teams_container.get("count", 0))):
                                team_container = teams_container.get(str(j))
                                if not team_container or "team" not in team_container:
                                    continue
                                
                                team_data = team_container["team"]
                                
                                # 提取团队键和得分
                                team_key = None
                                team_points = None
                                team_projected_points = None
                                
                                for item in team_data:
                                    if isinstance(item, dict):
                                        if "team_key" in item:
                                            team_key = item["team_key"]
                                        elif "team_points" in item:
                                            team_points = float(item["team_points"]["total"])
                                        elif "team_projected_points" in item:
                                            team_projected_points = float(item["team_projected_points"]["total"])
                                
                                if not team_key or team_points is None:
                                    continue
                                
                                # 查找对应的团队
                                team = db.query(Team).filter(Team.team_key == team_key).first()
                                if not team:
                                    print(f"找不到团队: {team_key}")
                                    continue
                                
                                # 创建对决团队记录
                                new_matchup_team = MatchupTeam(
                                    matchup_id=new_matchup.id,
                                    team_id=team.id,
                                    points=team_points,
                                    projected_points=team_projected_points
                                )
                                db.add(new_matchup_team)
                    
                    success_count += 1
        
        except Exception as e:
            print(f"处理联盟记分板数据文件 {file_path} 时出错: {str(e)}")
    
    # 提交更改
    db.commit()
    db.close()
    
    print(f"成功处理 {success_count} 个联盟记分板数据文件")
    return success_count > 0

def update_user_games():
    """更新用户游戏关联表"""
    print("更新用户游戏关联表...")
    
    # 获取数据库会话
    db = SessionLocal()
    
    try:
        # 删除所有现有的关联
        db.query(UserGame).delete()
        
        # 查询所有带有manager_user_id的团队
        teams_with_managers = db.query(Team).filter(Team.manager_user_id != None).all()
        
        # 收集用户-游戏关联
        user_game_pairs = set()
        for team in teams_with_managers:
            # 获取团队所属的联盟
            league = db.query(League).filter(League.id == team.league_id).first()
            if not league:
                continue
                
            # 记录用户-游戏关联
            user_game_pairs.add((team.manager_user_id, league.game_id))
        
        # 创建新的关联记录
        for user_id, game_id in user_game_pairs:
            # 检查关联是否已存在
            existing = db.query(UserGame).filter(
                UserGame.user_id == user_id,
                UserGame.game_id == game_id
            ).first()
            
            if not existing:
                new_user_game = UserGame(
                    user_id=user_id,
                    game_id=game_id
                )
                db.add(new_user_game)
                
        # 提交更改
        db.commit()
        
        # 获取更新后的关联数量
        count = db.query(UserGame).count()
        print(f"成功更新用户游戏关联表，创建了 {count} 条关联记录")
        
        db.close()
        return True
    
    except Exception as e:
        print(f"更新用户游戏关联表时出错: {str(e)}")
        db.rollback()
        db.close()
        return False

def ensure_db_tables():
    """确保所有数据库表已创建"""
    try:
        create_tables()
        print("数据库表已创建或已存在")
        return True
    except Exception as e:
        print(f"创建数据库表时出错: {str(e)}")
        return False

def recreate_db_tables():
    """删除并重新创建所有数据库表"""
    try:
        print("删除所有现有的数据库表...")
        Base.metadata.drop_all(bind=engine)
        print("重新创建所有数据库表...")
        Base.metadata.create_all(bind=engine)
        print("数据库表已重新创建")
        return True
    except Exception as e:
        print(f"重新创建数据库表时出错: {str(e)}")
        return False

def main():
    """主函数，处理命令行参数并执行相应操作"""
    parser = argparse.ArgumentParser(description="从JSON文件加载数据到数据库")
    parser.add_argument("--games", action="store_true", help="加载游戏数据")
    parser.add_argument("--leagues", action="store_true", help="加载联盟数据")
    parser.add_argument("--teams", action="store_true", help="加载团队数据")
    parser.add_argument("--league-settings", action="store_true", help="加载联盟设置数据")
    parser.add_argument("--league-standings", action="store_true", help="加载联盟排名数据")
    parser.add_argument("--league-scoreboards", action="store_true", help="加载联盟记分板数据")
    parser.add_argument("--update-user-games", action="store_true", help="更新用户游戏关联表")
    parser.add_argument("--init-db", action="store_true", help="初始化数据库表")
    parser.add_argument("--recreate-db", action="store_true", help="删除并重新创建所有数据库表")
    
    args = parser.parse_args()
    
    # 删除并重新创建数据库表
    if args.recreate_db:
        recreate_db_tables()
        return
    
    # 初始化数据库表
    if args.init_db:
        ensure_db_tables()
        return
    
    # 如果没有指定任何参数，默认加载所有数据
    if not (args.games or args.leagues or args.teams or args.league_settings or 
            args.league_standings or args.league_scoreboards or args.update_user_games):
        print("未指定加载类型，将加载所有数据")
        ensure_db_tables()
        load_games_data()
        load_leagues_to_db()
        load_teams_to_db()
        load_league_settings_to_db()
        load_league_standings_to_db()
        load_league_scoreboards_to_db()
        update_user_games()
    else:
        # 加载指定类型的数据
        if args.games:
            load_games_data()
        if args.leagues:
            load_leagues_to_db()
        if args.teams:
            load_teams_to_db()
        if args.league_settings:
            load_league_settings_to_db()
        if args.league_standings:
            load_league_standings_to_db()
        if args.league_scoreboards:
            load_league_scoreboards_to_db()
        if args.update_user_games:
            update_user_games()
    
    print("数据库加载完成!")

if __name__ == "__main__":
    main() 