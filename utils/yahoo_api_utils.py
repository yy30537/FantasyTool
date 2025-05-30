#!/usr/bin/env python3
"""
Yahoo Fantasy API 通用工具模块 - 单联盟模式
专注于单个联盟的深度数据获取
"""
import requests
import json
import os
import pickle
import pathlib
import time
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 路径配置 - 使用项目根目录
BASE_DIR = pathlib.Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOKENS_DIR = BASE_DIR / "tokens"
DATA_DIR = BASE_DIR / "data"

# 基础数据目录
GAMES_DIR = DATA_DIR
LEAGUES_DIR = DATA_DIR

# OAuth配置（最好从环境变量加载）
CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

def ensure_data_directories():
    """确保基础数据目录存在"""
    directories = [TOKENS_DIR, DATA_DIR]
    
    for directory in directories:
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            print(f"创建目录: {directory}")

def create_league_directory(league_key):
    """为选定的联盟创建专用目录结构
    
    Args:
        league_key: 联盟键，如 "385.l.24889"
    
    Returns:
        dict: 包含所有子目录路径的字典
    """
    league_dir = DATA_DIR / f"selected_league_{league_key}"
    
    # 创建子目录结构
    directories = {
        'base': league_dir,
        'rosters': league_dir / "rosters",
        'players': league_dir / "players"
    }
    
    for dir_path in directories.values():
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"创建联盟目录: {dir_path}")
    
    return directories

def get_league_directory(league_key):
    """获取联盟目录路径
    
    Args:
        league_key: 联盟键
        
    Returns:
        dict: 包含所有子目录路径的字典
    """
    league_dir = DATA_DIR / f"selected_league_{league_key}"
    
    return {
        'base': league_dir,
        'rosters': league_dir / "rosters",
        'players': league_dir / "players"
    }

# 在模块加载时创建基础目录
ensure_data_directories()

# 令牌文件路径
DEFAULT_TOKEN_FILE = TOKENS_DIR / "yahoo_token.token"

def load_token():
    """从文件加载令牌"""
    # 首先尝试从当前目录加载
    if DEFAULT_TOKEN_FILE.exists():
        try:
            with open(DEFAULT_TOKEN_FILE, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"加载令牌时出错: {str(e)}")
    else:
        # 如果当前目录不存在，尝试从其他可能的位置加载
        possible_token_paths = [
            BASE_DIR / "tokens" / "yahoo_token.token",  # 项目根目录
            pathlib.Path("tokens/yahoo_token.token"),  # 当前目录下的tokens
        ]
        
        for token_path in possible_token_paths:
            if token_path.exists():
                try:
                    with open(token_path, 'rb') as f:
                        token = pickle.load(f)
                        # 将令牌保存到正确的位置
                        with open(DEFAULT_TOKEN_FILE, 'wb') as new_f:
                            pickle.dump(token, new_f)
                        print(f"从 {token_path} 复制令牌到 {DEFAULT_TOKEN_FILE}")
                        return token
                except Exception as e:
                    print(f"从 {token_path} 加载令牌时出错: {str(e)}")
        
        print("未找到token文件，请先运行app.py完成授权")
    
    return None

def save_json_data(data, file_path):
    """保存数据到JSON文件
    
    Args:
        data: 要保存的数据
        file_path: 文件路径
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"数据已保存到: {file_path}")
        return True
    except Exception as e:
        print(f"保存数据到 {file_path} 时出错: {str(e)}")
        return False

def load_json_data(file_path):
    """从JSON文件加载数据
    
    Args:
        file_path: 文件路径
    """
    try:
        if not os.path.exists(file_path):
            print(f"文件不存在: {file_path}")
            return None
            
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"从 {file_path} 加载数据时出错: {str(e)}")
        return None

def refresh_token_if_needed(token):
    """检查并刷新令牌（如果已过期）"""
    if not token:
        return None
    
    # 检查令牌是否过期
    now = datetime.now().timestamp()
    expires_at = token.get('expires_at', 0)
    
    # 如果令牌已过期或即将过期（提前60秒刷新）
    if now >= (expires_at - 60):
        try:
            print("刷新令牌...")
            refresh_token = token.get('refresh_token')
            
            data = {
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(TOKEN_URL, data=data)
            
            if response.status_code == 200:
                new_token = response.json()
                # 设置过期时间
                expires_in = new_token.get('expires_in', 3600)
                new_token['expires_at'] = now + int(expires_in)
                # 保留refresh_token（如果新令牌中没有）
                if 'refresh_token' not in new_token and refresh_token:
                    new_token['refresh_token'] = refresh_token
                
                # 保存更新的令牌
                with open(DEFAULT_TOKEN_FILE, 'wb') as f:
                    pickle.dump(new_token, f)
                
                print("令牌刷新成功")
                return new_token
            else:
                print(f"令牌刷新失败: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"刷新令牌时出错: {str(e)}")
            return None
    
    return token

def get_api_data(url, max_retries=3):
    """通用函数：获取Yahoo Fantasy API数据，带重试机制"""
    # 加载令牌
    token = load_token()
    if not token:
        print("未找到有效令牌，请先运行app.py完成授权")
        return None
    
    # 刷新令牌（如果需要）
    token = refresh_token_if_needed(token)
    if not token:
        print("令牌刷新失败")
        return None
    
    # 设置请求头
    headers = {
        'Authorization': f"Bearer {token['access_token']}",
        'Content-Type': 'application/json'
    }
    
    # 重试机制
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # 授权问题，尝试刷新令牌
                print("授权失败，尝试刷新令牌...")
                token = refresh_token_if_needed(token)
                if token:
                    headers['Authorization'] = f"Bearer {token['access_token']}"
                    continue
                else:
                    print("令牌刷新失败，无法继续请求")
                    return None
            else:
                print(f"请求失败: {response.status_code} - {response.text}")
                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 指数退避
                    print(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                return None
        except Exception as e:
            print(f"请求时出错: {str(e)}")
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
                continue
            return None
    
    return None

def parse_yahoo_date(date_str):
    """解析Yahoo日期格式"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None

def print_league_selection_info(leagues_data):
    """打印联盟选择信息"""
    print("\n" + "="*80)
    print("可选择的Fantasy联盟")
    print("="*80)
    
    all_leagues = []
    league_counter = 1
    
    for game_key, leagues in leagues_data.items():
        for league in leagues:
            league_info = {
                'index': league_counter,
                'league_key': league.get('league_key'),
                'name': league.get('name', '未知联盟'),
                'season': league.get('season', '未知赛季'),
                'num_teams': league.get('num_teams', 0),
                'game_code': league.get('game_code', '未知运动'),
                'scoring_type': league.get('scoring_type', '未知'),
                'is_finished': league.get('is_finished', 0) == 1
            }
            all_leagues.append(league_info)
            
            # 打印联盟信息
            status = "已结束" if league_info['is_finished'] else "进行中"
            print(f"{league_counter:2d}. {league_info['name']}")
            print(f"    联盟ID: {league_info['league_key']}")
            print(f"    运动类型: {league_info['game_code'].upper()} | 赛季: {league_info['season']} | 状态: {status}")
            print(f"    球队数量: {league_info['num_teams']} | 计分方式: {league_info['scoring_type']}")
            print()
            
            league_counter += 1
    
    print("="*80)
    return all_leagues

def select_league_interactively(leagues_data):
    """交互式选择联盟"""
    all_leagues = print_league_selection_info(leagues_data)
    
    if not all_leagues:
        print("没有找到任何联盟")
        return None
    
    while True:
        try:
            choice = input(f"请选择联盟 (1-{len(all_leagues)}): ").strip()
            
            if not choice:
                continue
                
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(all_leagues):
                selected_league = all_leagues[choice_num - 1]
                print(f"\n✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                return selected_league
            else:
                print(f"请输入1到{len(all_leagues)}之间的数字")
                
        except ValueError:
            print("请输入有效的数字")
        except KeyboardInterrupt:
            print("\n用户取消选择")
            return None

def get_league_data_overview(league_key):
    """获取联盟数据概览"""
    league_dirs = get_league_directory(league_key)
    overview = {
        'league_key': league_key,
        'data_exists': league_dirs['base'].exists(),
        'files': {},
        'total_size': 0
    }
    
    if not overview['data_exists']:
        return overview
    
    # 检查各个数据文件
    file_checks = {
        'league_info': league_dirs['base'] / "league_info.json",
        'teams': league_dirs['base'] / "teams.json",
        'static_players': league_dirs['players'] / "static_players.json",
        'dynamic_players': league_dirs['players'] / "dynamic_players.json",
        'player_stats': league_dirs['players'] / "player_stats.json"
    }
    
    # 检查rosters目录
    roster_files = list(league_dirs['rosters'].glob("*.json")) if league_dirs['rosters'].exists() else []
    
    for file_type, file_path in file_checks.items():
        if file_path.exists():
            size = file_path.stat().st_size
            overview['files'][file_type] = {
                'exists': True,
                'size_mb': size / 1024 / 1024,
                'path': str(file_path)
            }
            overview['total_size'] += size
        else:
            overview['files'][file_type] = {'exists': False}
    
    # 添加rosters信息
    overview['files']['rosters'] = {
        'exists': len(roster_files) > 0,
        'count': len(roster_files),
        'total_size_mb': sum(f.stat().st_size for f in roster_files) / 1024 / 1024 if roster_files else 0
    }
    
    overview['total_size'] += sum(f.stat().st_size for f in roster_files)
    overview['total_size_mb'] = overview['total_size'] / 1024 / 1024
    
    return overview

def print_data_overview(league_key=None):
    """打印数据概览"""
    print("\n" + "="*50)
    print("Yahoo Fantasy 数据概览 - 单联盟模式")
    print("="*50)
    
    # 检查基础数据
    games_file = GAMES_DIR / "games_data.json"
    leagues_file = LEAGUES_DIR / "all_leagues_data.json"
    
    print("=== 基础数据 ===")
    print(f"Games数据: {'✓' if games_file.exists() else '✗'}")
    print(f"Leagues数据: {'✓' if leagues_file.exists() else '✗'}")
    
    # 如果指定了联盟，显示联盟数据概览
    if league_key:
        print(f"\n=== 联盟数据: {league_key} ===")
        overview = get_league_data_overview(league_key)
        
        if overview['data_exists']:
            for file_type, file_info in overview['files'].items():
                if file_type == 'rosters':
                    status = "✓" if file_info['exists'] else "✗"
                    print(f"{status} {file_type}: {file_info['count']} 个文件, {file_info['total_size_mb']:.1f} MB")
                else:
                    status = "✓" if file_info['exists'] else "✗"
                    size_info = f", {file_info['size_mb']:.1f} MB" if file_info['exists'] else ""
                    print(f"{status} {file_type}{size_info}")
            
            print(f"\n总大小: {overview['total_size_mb']:.1f} MB")
        else:
            print("✗ 联盟数据目录不存在")
    else:
        # 查找现有的联盟数据
        league_dirs = list(DATA_DIR.glob("selected_league_*"))
        if league_dirs:
            print(f"\n=== 现有联盟数据 ===")
            for league_dir in league_dirs:
                league_key = league_dir.name.replace("selected_league_", "")
                overview = get_league_data_overview(league_key)
                print(f"联盟 {league_key}: {overview['total_size_mb']:.1f} MB")
    
    print("="*50) 