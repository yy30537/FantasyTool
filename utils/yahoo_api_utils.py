#!/usr/bin/env python3
"""
Yahoo Fantasy API 通用工具模块
包含共享的API调用、数据解析和文件操作功能
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

# 数据子目录
GAMES_DIR = DATA_DIR / "games"
LEAGUES_DIR = DATA_DIR / "leagues"
LEAGUE_SETTINGS_DIR = DATA_DIR / "league_settings"
LEAGUE_STANDINGS_DIR = DATA_DIR / "league_standings"
LEAGUE_SCOREBOARDS_DIR = DATA_DIR / "league_scoreboards"
TEAMS_DIR = DATA_DIR / "teams"
PLAYERS_DIR = DATA_DIR / "players"

# 创建必要的目录
def ensure_directories():
    """确保所有必要的目录存在"""
    directories = [
        TOKENS_DIR, 
        DATA_DIR, 
        GAMES_DIR, 
        LEAGUES_DIR, 
        LEAGUE_SETTINGS_DIR,
        LEAGUE_STANDINGS_DIR,
        LEAGUE_SCOREBOARDS_DIR,
        TEAMS_DIR,
        PLAYERS_DIR
    ]
    
    for directory in directories:
        if not directory.exists():
            directory.mkdir(exist_ok=True, parents=True)
            print(f"创建目录: {directory.absolute()}")

# 初始化目录
ensure_directories()

# 令牌文件路径
DEFAULT_TOKEN_FILE = TOKENS_DIR / "yahoo_token.token"

# OAuth配置（最好从环境变量加载）
CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

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

# 获取特定数据类型的目录
def get_data_dir(data_type):
    """根据数据类型返回相应的目录"""
    data_dirs = {
        "games": GAMES_DIR,
        "leagues": LEAGUES_DIR,
        "league_settings": LEAGUE_SETTINGS_DIR,
        "league_standings": LEAGUE_STANDINGS_DIR,
        "league_scoreboards": LEAGUE_SCOREBOARDS_DIR,
        "teams": TEAMS_DIR,
        "players": PLAYERS_DIR
    }
    
    return data_dirs.get(data_type, DATA_DIR)

def save_json_data(data, file_path, data_type=None):
    """保存数据到JSON文件
    
    Args:
        data: 要保存的数据
        file_path: 文件路径，可以是相对于数据目录的路径或绝对路径
        data_type: 数据类型，用于确定保存目录，如果为None则使用file_path
    """
    try:
        # 如果提供了数据类型，则使用对应的目录
        if data_type and isinstance(file_path, (str, pathlib.Path)):
            # 获取文件名
            file_name = os.path.basename(str(file_path))
            # 拼接新路径
            dir_path = get_data_dir(data_type)
            file_path = dir_path / file_name
        
        # 确保目录存在
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"数据已保存到: {file_path}")
        return True
    except Exception as e:
        print(f"保存数据到 {file_path} 时出错: {str(e)}")
        return False

def load_json_data(file_path, data_type=None):
    """从JSON文件加载数据
    
    Args:
        file_path: 文件路径，可以是相对于数据目录的路径或绝对路径
        data_type: 数据类型，用于确定加载目录，如果为None则使用file_path
    """
    try:
        # 如果提供了数据类型，则使用对应的目录
        if data_type and isinstance(file_path, (str, pathlib.Path)):
            # 获取文件名
            file_name = os.path.basename(str(file_path))
            # 拼接新路径
            dir_path = get_data_dir(data_type)
            file_path = dir_path / file_name
        
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

def extract_value_from_path(data, path):
    """从嵌套字典中提取值"""
    try:
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            elif isinstance(current, list) and isinstance(key, int) and key < len(current):
                current = current[key]
            else:
                return None
        return current
    except Exception:
        return None

def parse_yahoo_date(date_str):
    """解析Yahoo日期格式"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None 