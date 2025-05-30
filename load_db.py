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
from utils.yahoo_api_utils import GAMES_DIR, LEAGUES_DIR, LEAGUE_SETTINGS_DIR, LEAGUE_STANDINGS_DIR, LEAGUE_SCOREBOARDS_DIR, TEAMS_DIR, load_json_data, get_data_dir
from models import (Game, League, Team, User, get_db, create_tables, SessionLocal, Base, engine, 
                   LeagueSetting, TeamStanding, TeamSeasonStats, Scoreboard, MatchupTeam, UserGame, Matchup)

def safe_int(value, default=None):
    """安全地转换为整数，处理空字符串和None"""
    if value is None or value == "" or value == "--":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=None):
    """安全地转换为浮点数，处理空字符串和None"""
    if value is None or value == "" or value == "--":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value, default=False):
    """安全地转换为布尔值"""
    if value is None or value == "" or value == "--":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    try:
        return bool(int(value))
    except (ValueError, TypeError):
        return default

def safe_str(value, default=""):
    """安全地转换为字符串，处理None"""
    if value is None:
        return default
    return str(value)

def safe_date(value):
    """安全地转换为日期，处理空字符串和None"""
    if value is None or value == "" or value == "--":
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

def load_games_data():
    """加载游戏数据到数据库"""
    print("加载游戏数据到数据库...")
    # 从games子文件夹检查游戏数据文件
    games_file = GAMES_DIR / "games_data.json"
    if not games_file.exists():
        print(f"游戏数据文件不存在: {games_file}")
        return False
    
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
                
                try:
                    processed_count = 0
                    skipped_count = 0
                    
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
                        
                        # 检查游戏类型，只处理type为"full"的游戏
                        game_type = game_info.get("type", "")
                        game_key = game_info["game_key"]
                        game_name = game_info.get("name", "Unknown")
                        
                        if game_type != "full":
                            print(f"跳过游戏: {game_key} (类型: {game_type}, 名称: {game_name}) - 不是full类型")
                            skipped_count += 1
                            continue
                        
                        print(f"处理游戏: {game_key} (类型: {game_type}, 名称: {game_name})")
                        
                        # 检查游戏是否已存在
                        existing_game = db.query(Game).filter(Game.game_key == game_key).first()
                        
                        if existing_game:
                            # 更新现有游戏
                            existing_game.game_id = safe_str(game_info.get("game_id", ""))
                            existing_game.name = safe_str(game_info.get("name", ""))
                            existing_game.code = safe_str(game_info.get("code", ""))
                            existing_game.type = safe_str(game_info.get("type", ""))
                            existing_game.url = safe_str(game_info.get("url", ""))
                            existing_game.season = safe_str(game_info.get("season", ""))
                            existing_game.is_registration_over = safe_bool(game_info.get("is_registration_over"))
                            existing_game.is_game_over = safe_bool(game_info.get("is_game_over"))
                            existing_game.is_offseason = safe_bool(game_info.get("is_offseason"))
                            print(f"更新游戏: {game_key}")
                        else:
                            # 创建新游戏
                            new_game = Game(
                                game_key=game_key,
                                game_id=safe_str(game_info.get("game_id", "")),
                                name=safe_str(game_info.get("name", "")),
                                code=safe_str(game_info.get("code", "")),
                                type=safe_str(game_info.get("type", "")),
                                url=safe_str(game_info.get("url", "")),
                                season=safe_str(game_info.get("season", "")),
                                is_registration_over=safe_bool(game_info.get("is_registration_over")),
                                is_game_over=safe_bool(game_info.get("is_game_over")),
                                is_offseason=safe_bool(game_info.get("is_offseason"))
                            )
                            db.add(new_game)
                            print(f"添加新游戏: {game_key}")
                        
                        processed_count += 1
                    
                    # 提交更改
                    db.commit()
                    print(f"成功处理 {processed_count} 个full类型游戏，跳过 {skipped_count} 个非full类型游戏")
                    return True
                
                except Exception as e:
                    print(f"处理游戏数据时出错: {str(e)}")
                    db.rollback()
                    return False
                finally:
                    db.close()
    
    except Exception as e:
        print(f"加载游戏数据时出错: {str(e)}")
        return False

def load_leagues_to_db():
    """加载联盟数据到数据库"""
    print("加载联盟数据到数据库...")
    # 从leagues子文件夹检查联盟数据文件
    league_files = list(LEAGUES_DIR.glob("league_data_*.json"))
    
    if not league_files:
        print("未找到任何联盟数据文件")
        return False
    
    print(f"找到 {len(league_files)} 个联盟数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    try:
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
                                        _update_league_fields(existing_league, league_info)
                                        print(f"更新联盟: {league_key}")
                                    else:
                                        # 创建新联盟
                                        new_league = _create_new_league(league_info, game.id)
                                        db.add(new_league)
                                        print(f"添加新联盟: {league_key}")
                                
                                success_count += 1
                                break
            
            except Exception as e:
                print(f"处理联盟数据文件 {file_path} 时出错: {str(e)}")
                continue
        
        # 提交更改
        db.commit()
        print(f"成功处理 {success_count} 个联盟数据文件")
        return success_count > 0
    
    except Exception as e:
        print(f"加载联盟数据时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

def _update_league_fields(league, league_info):
    """更新联盟字段的辅助函数"""
    league.league_id = safe_str(league_info.get("league_id", ""))
    league.name = safe_str(league_info.get("name", ""))
    league.url = safe_str(league_info.get("url", ""))
    league.logo_url = league_info.get("logo_url") if league_info.get("logo_url") else None
    league.draft_status = safe_str(league_info.get("draft_status", ""))
    league.num_teams = safe_int(league_info.get("num_teams"), 0)
    league.edit_key = league_info.get("edit_key")
    league.weekly_deadline = league_info.get("weekly_deadline")
    league.league_update_timestamp = league_info.get("league_update_timestamp")
    league.scoring_type = safe_str(league_info.get("scoring_type", ""))
    league.league_type = safe_str(league_info.get("league_type", "unknown"))
    league.renew = league_info.get("renew")
    league.renewed = league_info.get("renewed")
    league.felo_tier = league_info.get("felo_tier")
    league.iris_group_chat_id = league_info.get("iris_group_chat_id")
    league.allow_add_to_dl_extra_pos = safe_int(league_info.get("allow_add_to_dl_extra_pos"), 0)
    league.is_pro_league = safe_str(league_info.get("is_pro_league", "0"))
    league.is_cash_league = safe_str(league_info.get("is_cash_league", "0"))
    league.current_week = safe_int(league_info.get("current_week"))
    league.start_week = safe_int(league_info.get("start_week"))
    league.end_week = safe_int(league_info.get("end_week"))
    league.start_date = safe_date(league_info.get("start_date"))
    league.end_date = safe_date(league_info.get("end_date"))
    league.is_finished = safe_bool(league_info.get("is_finished"))
    league.is_plus_league = safe_str(league_info.get("is_plus_league", "0"))
    league.game_code = league_info.get("game_code")
    league.season = league_info.get("season")

def _create_new_league(league_info, game_id):
    """创建新联盟的辅助函数"""
    return League(
        league_key=league_info["league_key"],
        league_id=safe_str(league_info.get("league_id", "")),
        name=safe_str(league_info.get("name", "")),
        url=safe_str(league_info.get("url", "")),
        logo_url=league_info.get("logo_url") if league_info.get("logo_url") else None,
        draft_status=safe_str(league_info.get("draft_status", "")),
        num_teams=safe_int(league_info.get("num_teams"), 0),
        edit_key=league_info.get("edit_key"),
        weekly_deadline=league_info.get("weekly_deadline"),
        league_update_timestamp=league_info.get("league_update_timestamp"),
        scoring_type=safe_str(league_info.get("scoring_type", "")),
        league_type=safe_str(league_info.get("league_type", "unknown")),
        renew=league_info.get("renew"),
        renewed=league_info.get("renewed"),
        felo_tier=league_info.get("felo_tier"),
        iris_group_chat_id=league_info.get("iris_group_chat_id"),
        allow_add_to_dl_extra_pos=safe_int(league_info.get("allow_add_to_dl_extra_pos"), 0),
        is_pro_league=safe_str(league_info.get("is_pro_league", "0")),
        is_cash_league=safe_str(league_info.get("is_cash_league", "0")),
        current_week=safe_int(league_info.get("current_week")),
        start_week=safe_int(league_info.get("start_week")),
        end_week=safe_int(league_info.get("end_week")),
        start_date=safe_date(league_info.get("start_date")),
        end_date=safe_date(league_info.get("end_date")),
        is_finished=safe_bool(league_info.get("is_finished")),
        is_plus_league=safe_str(league_info.get("is_plus_league", "0")),
        game_code=league_info.get("game_code"),
        season=league_info.get("season"),
        game_id=game_id
    )

def load_teams_to_db():
    """加载团队数据到数据库"""
    print("加载团队数据到数据库...")
    # 从teams子文件夹检查团队数据文件
    team_files = list(TEAMS_DIR.glob("teams_data_*.json"))
    
    if not team_files:
        print("未找到任何团队数据文件")
        return False
    
    print(f"找到 {len(team_files)} 个团队数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    try:
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
                                        # 更新用户信息
                                        existing_user.name = safe_str(manager_info.get("nickname", ""))
                                        existing_user.nickname = safe_str(manager_info.get("nickname", ""))
                                        existing_user.email = safe_str(manager_info.get("email", ""))
                                        existing_user.felo_score = safe_int(manager_info.get("felo_score"))
                                        existing_user.felo_tier = manager_info.get("felo_tier")
                                        existing_user.image_url = manager_info.get("image_url")
                                    else:
                                        # 创建新用户
                                        new_user = User(
                                            yahoo_guid=yahoo_guid,
                                            name=safe_str(manager_info.get("nickname", "")),
                                            nickname=safe_str(manager_info.get("nickname", "")),
                                            email=safe_str(manager_info.get("email", "")),
                                            felo_score=safe_int(manager_info.get("felo_score")),
                                            felo_tier=manager_info.get("felo_tier"),
                                            image_url=manager_info.get("image_url")
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
                                existing_team.name = safe_str(team_info.get("name", ""))
                                existing_team.is_owned_by_current_user = "is_owned_by_current_login" in team_info
                                existing_team.url = safe_str(team_info.get("url", ""))
                                existing_team.team_logo = team_logo_url
                                existing_team.waiver_priority = safe_int(team_info.get("waiver_priority"))
                                existing_team.number_of_moves = safe_int(team_info.get("number_of_moves"), 0)
                                existing_team.number_of_trades = safe_int(team_info.get("number_of_trades"), 0)
                                existing_team.clinched_playoffs = safe_bool(team_info.get("clinched_playoffs"))
                                existing_team.has_draft_grade = safe_bool(team_info.get("has_draft_grade"))
                                existing_team.league_scoring_type = team_info.get("league_scoring_type")
                                
                                if manager_user_id:
                                    existing_team.manager_user_id = manager_user_id
                                
                                print(f"更新团队: {team_key}")
                            else:
                                # 创建新团队
                                new_team = Team(
                                    team_key=team_key,
                                    team_id=safe_str(team_info.get("team_id", "")),
                                    name=safe_str(team_info.get("name", "")),
                                    is_owned_by_current_user="is_owned_by_current_login" in team_info,
                                    url=safe_str(team_info.get("url", "")),
                                    team_logo=team_logo_url,
                                    waiver_priority=safe_int(team_info.get("waiver_priority")),
                                    number_of_moves=safe_int(team_info.get("number_of_moves"), 0),
                                    number_of_trades=safe_int(team_info.get("number_of_trades"), 0),
                                    clinched_playoffs=safe_bool(team_info.get("clinched_playoffs")),
                                    has_draft_grade=safe_bool(team_info.get("has_draft_grade")),
                                    league_scoring_type=team_info.get("league_scoring_type"),
                                    league_id=league.id,
                                    manager_user_id=manager_user_id
                                )
                                db.add(new_team)
                                print(f"添加新团队: {team_key}")
                        
                        success_count += 1
            
            except Exception as e:
                print(f"处理团队数据文件 {file_path} 时出错: {str(e)}")
                # 回滚当前文件的更改，但继续处理下一个文件
                db.rollback()
                continue
        
        # 提交更改
        db.commit()
        print(f"成功处理 {success_count} 个团队数据文件")
        return success_count > 0
    
    except Exception as e:
        print(f"加载团队数据时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

def load_league_settings_to_db():
    """加载联盟设置数据到数据库"""
    print("加载联盟设置数据到数据库...")
    # 从league_settings子文件夹检查联盟设置数据文件
    settings_files = list(LEAGUE_SETTINGS_DIR.glob("league_settings_*.json"))
    
    if not settings_files:
        print("未找到任何联盟设置数据文件")
        return False
    
    print(f"找到 {len(settings_files)} 个联盟设置数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    try:
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
                    
                    if settings_json and isinstance(settings_json, list) and len(settings_json) > 0:
                        settings_info = settings_json[0]  # 取第一个设置对象
                        
                        # 检查是否已存在设置记录
                        existing_settings = db.query(LeagueSetting).filter(LeagueSetting.league_id == league.id).first()
                        
                        if existing_settings:
                            # 更新现有设置
                            _update_league_settings_fields(existing_settings, settings_info)
                            print(f"更新联盟设置: {league_key}")
                        else:
                            # 创建新设置记录
                            new_settings = _create_new_league_settings(league.id, settings_info)
                            db.add(new_settings)
                            print(f"添加新联盟设置: {league_key}")
                        
                        success_count += 1
            
            except Exception as e:
                print(f"处理联盟设置数据文件 {file_path} 时出错: {str(e)}")
                continue
        
        # 提交更改
        db.commit()
        print(f"成功处理 {success_count} 个联盟设置数据文件")
        return success_count > 0
    
    except Exception as e:
        print(f"加载联盟设置数据时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

def _update_league_settings_fields(settings, settings_info):
    """更新联盟设置字段的辅助函数"""
    # 基本设置
    settings.draft_type = settings_info.get("draft_type")
    settings.is_auction_draft = safe_bool(settings_info.get("is_auction_draft"))
    settings.scoring_type = settings_info.get("scoring_type")
    settings.uses_playoff = safe_bool(settings_info.get("uses_playoff"))
    settings.has_playoff_consolation_games = safe_bool(settings_info.get("has_playoff_consolation_games"))
    settings.playoff_start_week = safe_int(settings_info.get("playoff_start_week"))
    settings.uses_playoff_reseeding = safe_bool(settings_info.get("uses_playoff_reseeding"))
    settings.uses_lock_eliminated_teams = safe_bool(settings_info.get("uses_lock_eliminated_teams"))
    settings.num_playoff_teams = safe_int(settings_info.get("num_playoff_teams"))
    settings.num_playoff_consolation_teams = safe_int(settings_info.get("num_playoff_consolation_teams"))
    settings.has_multiweek_championship = safe_bool(settings_info.get("has_multiweek_championship"))
    settings.waiver_type = settings_info.get("waiver_type")
    settings.waiver_rule = settings_info.get("waiver_rule")
    settings.uses_faab = safe_bool(settings_info.get("uses_faab"))
    settings.draft_time = settings_info.get("draft_time")
    settings.draft_pick_time = safe_int(settings_info.get("draft_pick_time"))
    settings.post_draft_players = settings_info.get("post_draft_players")
    settings.max_teams = safe_int(settings_info.get("max_teams"))
    settings.waiver_time = safe_int(settings_info.get("waiver_time"))
    settings.trade_end_date = settings_info.get("trade_end_date")
    settings.trade_ratify_type = settings_info.get("trade_ratify_type")
    settings.trade_reject_time = safe_int(settings_info.get("trade_reject_time"))
    settings.player_pool = settings_info.get("player_pool")
    settings.cant_cut_list = settings_info.get("cant_cut_list")
    settings.draft_together = safe_bool(settings_info.get("draft_together"))
    settings.max_weekly_adds = safe_int(settings_info.get("max_weekly_adds"))
    settings.uses_median_score = safe_bool(settings_info.get("uses_median_score"))
    
    # 复杂数据存储为JSON
    settings.roster_positions = settings_info.get("roster_positions")
    settings.stat_categories = settings_info.get("stat_categories")
    settings.league_premium_features = settings_info.get("league_premium_features", [])

def _create_new_league_settings(league_id, settings_info):
    """创建新联盟设置的辅助函数"""
    return LeagueSetting(
        league_id=league_id,
        draft_type=settings_info.get("draft_type"),
        is_auction_draft=safe_bool(settings_info.get("is_auction_draft")),
        scoring_type=settings_info.get("scoring_type"),
        uses_playoff=safe_bool(settings_info.get("uses_playoff")),
        has_playoff_consolation_games=safe_bool(settings_info.get("has_playoff_consolation_games")),
        playoff_start_week=safe_int(settings_info.get("playoff_start_week")),
        uses_playoff_reseeding=safe_bool(settings_info.get("uses_playoff_reseeding")),
        uses_lock_eliminated_teams=safe_bool(settings_info.get("uses_lock_eliminated_teams")),
        num_playoff_teams=safe_int(settings_info.get("num_playoff_teams")),
        num_playoff_consolation_teams=safe_int(settings_info.get("num_playoff_consolation_teams")),
        has_multiweek_championship=safe_bool(settings_info.get("has_multiweek_championship")),
        waiver_type=settings_info.get("waiver_type"),
        waiver_rule=settings_info.get("waiver_rule"),
        uses_faab=safe_bool(settings_info.get("uses_faab")),
        draft_time=settings_info.get("draft_time"),
        draft_pick_time=safe_int(settings_info.get("draft_pick_time")),
        post_draft_players=settings_info.get("post_draft_players"),
        max_teams=safe_int(settings_info.get("max_teams")),
        waiver_time=safe_int(settings_info.get("waiver_time")),
        trade_end_date=settings_info.get("trade_end_date"),
        trade_ratify_type=settings_info.get("trade_ratify_type"),
        trade_reject_time=safe_int(settings_info.get("trade_reject_time")),
        player_pool=settings_info.get("player_pool"),
        cant_cut_list=settings_info.get("cant_cut_list"),
        draft_together=safe_bool(settings_info.get("draft_together")),
        max_weekly_adds=safe_int(settings_info.get("max_weekly_adds")),
        uses_median_score=safe_bool(settings_info.get("uses_median_score")),
        roster_positions=settings_info.get("roster_positions"),
        stat_categories=settings_info.get("stat_categories"),
        league_premium_features=settings_info.get("league_premium_features", [])
    )

def load_league_standings_to_db():
    """加载联盟排名数据到数据库"""
    print("加载联盟排名数据到数据库...")
    # 从league_standings子文件夹检查联盟排名数据文件
    standings_files = list(LEAGUE_STANDINGS_DIR.glob("league_standings_*.json"))
    
    if not standings_files:
        print("未找到任何联盟排名数据文件")
        return False
    
    print(f"找到 {len(standings_files)} 个联盟排名数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    try:
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
                    
                    if standings_json and isinstance(standings_json, list) and len(standings_json) > 0:
                        # standings是一个数组，取第一个元素
                        standings_data = standings_json[0]
                        
                        if "teams" in standings_data:
                            teams_container = standings_data["teams"]
                            
                            for i in range(int(teams_container.get("count", 0))):
                                team_container = teams_container.get(str(i))
                                if not team_container or "team" not in team_container:
                                    continue
                                
                                team_data = team_container["team"]
                                
                                # 提取团队键和排名数据
                                team_key = None
                                team_standings = {}
                                team_stats = {}
                                
                                # team_data是一个包含多个元素的列表
                                # 第一个元素是包含基本信息的列表
                                # 第二个元素通常包含team_stats
                                # 第三个元素包含team_standings
                                
                                if len(team_data) >= 1 and isinstance(team_data[0], list):
                                    # 从第一个元素（基本信息列表）中提取team_key
                                    for basic_info in team_data[0]:
                                        if isinstance(basic_info, dict) and "team_key" in basic_info:
                                            team_key = basic_info["team_key"]
                                            break
                                
                                # 从其他元素中提取统计和排名数据
                                for item in team_data:
                                    if isinstance(item, dict):
                                        if "team_stats" in item:
                                            team_stats = item["team_stats"]
                                        elif "team_standings" in item:
                                            team_standings = item["team_standings"]
                                
                                if not team_key or not team_standings:
                                    continue
                                
                                # 查找对应的团队
                                team = db.query(Team).filter(Team.team_key == team_key).first()
                                if not team:
                                    print(f"找不到团队: {team_key}")
                                    continue
                                
                                # 处理排名数据
                                _process_team_standings(db, team, team_standings)
                                
                                # 处理统计数据（如果存在）
                                if team_stats:
                                    _process_team_season_stats(db, team, team_stats)
                                
                                print(f"处理团队数据: {team_key} (统计: {'有' if team_stats else '无'})")
                            
                            # 成功处理了一个排名文件
                            success_count += 1
            
            except Exception as e:
                print(f"处理联盟排名数据文件 {file_path} 时出错: {str(e)}")
                continue
        
        # 提交更改
        db.commit()
        print(f"成功处理 {success_count} 个联盟排名数据文件")
        return success_count > 0
    
    except Exception as e:
        print(f"加载联盟排名数据时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

def _process_team_standings(db, team, team_standings):
    """处理团队排名数据"""
    # 提取排名详情
    rank = safe_int(team_standings.get("rank"))
    playoff_seed = team_standings.get("playoff_seed")
    
    # 提取胜负记录
    outcome_totals = team_standings.get("outcome_totals", {})
    wins = safe_int(outcome_totals.get("wins"), 0)
    losses = safe_int(outcome_totals.get("losses"), 0)
    ties = safe_int(outcome_totals.get("ties"), 0)
    percentage = safe_float(outcome_totals.get("percentage"))
    games_back = team_standings.get("games_back")
    
    # 检查是否已存在排名记录
    existing_standing = db.query(TeamStanding).filter(TeamStanding.team_id == team.id).first()
    
    if existing_standing:
        # 更新现有排名
        existing_standing.rank = rank
        existing_standing.playoff_seed = playoff_seed
        existing_standing.wins = wins
        existing_standing.losses = losses
        existing_standing.ties = ties
        existing_standing.percentage = percentage
        existing_standing.games_back = games_back
    else:
        # 创建新排名记录
        new_standing = TeamStanding(
            team_id=team.id,
            rank=rank,
            playoff_seed=playoff_seed,
            wins=wins,
            losses=losses,
            ties=ties,
            percentage=percentage,
            games_back=games_back
        )
        db.add(new_standing)

def _process_team_season_stats(db, team, team_stats):
    """处理团队赛季统计数据"""
    if "stats" not in team_stats:
        return
    
    stats_list = team_stats["stats"]
    season = team_stats.get("season", "")
    
    # 解析统计数据
    stats_dict = {}
    for stat_item in stats_list:
        if "stat" in stat_item:
            stat = stat_item["stat"]
            stat_id = stat.get("stat_id")
            value = stat.get("value", "")
            
            # 根据stat_id映射到对应字段
            if stat_id == "9004003":  # FGM/FGA
                if "/" in value:
                    fgm, fga = value.split("/")
                    stats_dict["field_goals_made"] = safe_int(fgm)
                    stats_dict["field_goals_attempted"] = safe_int(fga)
            elif stat_id == "5":  # FG%
                stats_dict["field_goal_percentage"] = safe_float(value)
            elif stat_id == "9007006":  # FTM/FTA
                if "/" in value:
                    ftm, fta = value.split("/")
                    stats_dict["free_throws_made"] = safe_int(ftm)
                    stats_dict["free_throws_attempted"] = safe_int(fta)
            elif stat_id == "8":  # FT%
                stats_dict["free_throw_percentage"] = safe_float(value)
            elif stat_id == "10":  # 3PTM
                stats_dict["three_pointers_made"] = safe_int(value)
            elif stat_id == "12":  # PTS
                stats_dict["points"] = safe_int(value)
            elif stat_id == "15":  # REB
                stats_dict["rebounds"] = safe_int(value)
            elif stat_id == "16":  # AST
                stats_dict["assists"] = safe_int(value)
            elif stat_id == "17":  # ST
                stats_dict["steals"] = safe_int(value)
            elif stat_id == "18":  # BLK
                stats_dict["blocks"] = safe_int(value)
            elif stat_id == "19":  # TO
                stats_dict["turnovers"] = safe_int(value)
    
    # 检查是否已存在统计记录
    existing_stats = db.query(TeamSeasonStats).filter(TeamSeasonStats.team_id == team.id).first()
    
    if existing_stats:
        # 更新现有统计
        for key, value in stats_dict.items():
            setattr(existing_stats, key, value)
        existing_stats.season = season
    else:
        # 创建新统计记录
        new_stats = TeamSeasonStats(
            team_id=team.id,
            season=season,
            **stats_dict
        )
        db.add(new_stats)

def load_league_scoreboards_to_db():
    """加载联盟记分板数据到数据库"""
    print("加载联盟记分板数据到数据库...")
    # 从league_scoreboards子文件夹检查联盟记分板数据文件
    scoreboard_files = list(LEAGUE_SCOREBOARDS_DIR.glob("league_scoreboard_*.json"))
    
    if not scoreboard_files:
        print("未找到任何联盟记分板数据文件")
        return False
    
    print(f"找到 {len(scoreboard_files)} 个联盟记分板数据文件")
    
    # 获取数据库会话
    db = SessionLocal()
    
    success_count = 0
    try:
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
                                week = safe_int(item["current_week"])
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
                        
                        # 更新记分板信息
                        existing_scoreboard.week_start = scoreboard_json.get("week_start")
                        existing_scoreboard.week_end = scoreboard_json.get("week_end")
                        existing_scoreboard.status = safe_str(scoreboard_json.get("status", ""))
                        existing_scoreboard.is_playoffs = safe_bool(scoreboard_json.get("is_playoffs"))
                        existing_scoreboard.is_consolation = safe_bool(scoreboard_json.get("is_consolation"))
                        
                        print(f"更新记分板: {league_key} 第 {week} 周")
                    else:
                        # 创建新记分板记录
                        new_scoreboard = Scoreboard(
                            league_id=league.id,
                            week=week,
                            week_start=scoreboard_json.get("week_start"),
                            week_end=scoreboard_json.get("week_end"),
                            status=safe_str(scoreboard_json.get("status", "")),
                            is_playoffs=safe_bool(scoreboard_json.get("is_playoffs")),
                            is_consolation=safe_bool(scoreboard_json.get("is_consolation"))
                        )
                        db.add(new_scoreboard)
                        db.flush()  # 获取新ID
                        scoreboard_id = new_scoreboard.id
                        print(f"添加新记分板: {league_key} 第 {week} 周")
                    
                    # 处理对决数据 - 修复嵌套结构解析
                    # scoreboard结构: scoreboard -> "0" -> matchups -> "0", "1", ... -> matchup
                    if "0" in scoreboard_json and "matchups" in scoreboard_json["0"]:
                        matchups_container = scoreboard_json["0"]["matchups"]
                        
                        for i in range(int(matchups_container.get("count", 0))):
                            matchup_container = matchups_container.get(str(i))
                            if not matchup_container or "matchup" not in matchup_container:
                                continue
                            
                            matchup_data = matchup_container["matchup"]
                            
                            # 提取对决信息
                            is_playoffs = safe_bool(matchup_data.get("is_playoffs"))
                            is_consolation = safe_bool(matchup_data.get("is_consolation"))
                            is_tied = safe_bool(matchup_data.get("is_tied"))
                            status = safe_str(matchup_data.get("status", ""))
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
                                week_start=matchup_data.get("week_start"),
                                week_end=matchup_data.get("week_end"),
                                status=status,
                                is_playoffs=is_playoffs,
                                is_consolation=is_consolation,
                                is_tied=is_tied,
                                winner_team_id=winner_team_id
                            )
                            db.add(new_matchup)
                            db.flush()  # 获取新ID
                            
                            # 处理对决中的团队 - 修复嵌套结构
                            # 对决数据可能在 matchup["0"]["teams"] 中
                            teams_data = None
                            if "0" in matchup_data and "teams" in matchup_data["0"]:
                                teams_data = matchup_data["0"]["teams"]
                            elif "teams" in matchup_data:
                                teams_data = matchup_data["teams"]
                            
                            if teams_data:
                                for j in range(int(teams_data.get("count", 0))):
                                    team_container = teams_data.get(str(j))
                                    if not team_container or "team" not in team_container:
                                        continue
                                    
                                    team_data = team_container["team"]
                                    
                                    # 提取团队键和得分
                                    team_key = None
                                    team_points = None
                                    team_projected_points = None
                                    team_stats = {}
                                    
                                    # 根据调试结果，team_data是一个包含2个元素的列表：
                                    # [0] = 包含基本信息的列表（包含team_key）
                                    # [1] = 包含team_stats, team_points等的字典
                                    
                                    if len(team_data) >= 2:
                                        # 从第一个元素（基本信息列表）中提取team_key
                                        if isinstance(team_data[0], list):
                                            for basic_info in team_data[0]:
                                                if isinstance(basic_info, dict) and "team_key" in basic_info:
                                                    team_key = basic_info["team_key"]
                                                    break
                                        
                                        # 从第二个元素（统计字典）中提取得分和统计数据
                                        if isinstance(team_data[1], dict):
                                            stats_dict = team_data[1]
                                            
                                            # 提取team_points
                                            if "team_points" in stats_dict:
                                                team_points_data = stats_dict["team_points"]
                                                if isinstance(team_points_data, dict) and "total" in team_points_data:
                                                    team_points = safe_float(team_points_data["total"])
                                            
                                            # 提取team_projected_points（如果存在）
                                            if "team_projected_points" in stats_dict:
                                                projected_points_data = stats_dict["team_projected_points"]
                                                if isinstance(projected_points_data, dict) and "total" in projected_points_data:
                                                    team_projected_points = safe_float(projected_points_data["total"])
                                            
                                            # 提取team_stats
                                            if "team_stats" in stats_dict:
                                                team_stats = _extract_matchup_team_stats(stats_dict["team_stats"])
                                    
                                    if not team_key:
                                        print(f"无法提取team_key，跳过团队数据")
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
                                        projected_points=team_projected_points,
                                        **team_stats
                                    )
                                    db.add(new_matchup_team)
                                    print(f"添加对决团队记录: {team_key}, 得分: {team_points}")
                    
                    # 成功处理了一个记分板文件
                    success_count += 1
            
            except Exception as e:
                print(f"处理联盟记分板数据文件 {file_path} 时出错: {str(e)}")
                continue
        
        # 提交更改
        db.commit()
        print(f"成功处理 {success_count} 个联盟记分板数据文件")
        return success_count > 0
    
    except Exception as e:
        print(f"加载联盟记分板数据时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

def _extract_matchup_team_stats(team_stats_data):
    """从对决团队统计数据中提取详细统计信息"""
    stats_dict = {}
    
    if "stats" not in team_stats_data:
        return stats_dict
    
    stats_list = team_stats_data["stats"]
    
    for stat_item in stats_list:
        if "stat" in stat_item:
            stat = stat_item["stat"]
            stat_id = stat.get("stat_id")
            value = stat.get("value", "")
            
            # 根据stat_id映射到对应字段
            if stat_id == "9004003":  # FGM/FGA
                if "/" in value:
                    fgm, fga = value.split("/")
                    stats_dict["field_goals_made"] = safe_int(fgm)
                    stats_dict["field_goals_attempted"] = safe_int(fga)
            elif stat_id == "5":  # FG%
                stats_dict["field_goal_percentage"] = safe_float(value)
            elif stat_id == "9007006":  # FTM/FTA
                if "/" in value:
                    ftm, fta = value.split("/")
                    stats_dict["free_throws_made"] = safe_int(ftm)
                    stats_dict["free_throws_attempted"] = safe_int(fta)
            elif stat_id == "8":  # FT%
                stats_dict["free_throw_percentage"] = safe_float(value)
            elif stat_id == "10":  # 3PTM
                stats_dict["three_pointers_made"] = safe_int(value)
            elif stat_id == "12":  # PTS
                stats_dict["total_points"] = safe_int(value)
            elif stat_id == "15":  # REB
                stats_dict["rebounds"] = safe_int(value)
            elif stat_id == "16":  # AST
                stats_dict["assists"] = safe_int(value)
            elif stat_id == "17":  # ST
                stats_dict["steals"] = safe_int(value)
            elif stat_id == "18":  # BLK
                stats_dict["blocks"] = safe_int(value)
            elif stat_id == "19":  # TO
                stats_dict["turnovers"] = safe_int(value)
    
    return stats_dict

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
        
        return True
    
    except Exception as e:
        print(f"更新用户游戏关联表时出错: {str(e)}")
        db.rollback()
        return False
    finally:
        db.close()

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