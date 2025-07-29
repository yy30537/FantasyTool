"""
API客户端基础设施
从 archive/yahoo_api_utils.py 迁移基础API功能
"""

from typing import Optional, Dict
import time
import requests
import json
import os
import pickle
import pathlib
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 路径配置
BASE_DIR = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
TOKENS_DIR = BASE_DIR / "tokens"

# OAuth配置
CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

# 令牌文件路径
DEFAULT_TOKEN_FILE = TOKENS_DIR / "yahoo_token.token"


def ensure_tokens_directory():
    """确保tokens目录存在"""
    if not TOKENS_DIR.exists():
        TOKENS_DIR.mkdir(parents=True, exist_ok=True)
        print(f"创建目录: {TOKENS_DIR}")

# 在模块加载时创建tokens目录
ensure_tokens_directory()


class APIClient:
    """Yahoo Fantasy API客户端基础类"""
    
    def __init__(self, delay: int = 2):
        """
        初始化API客户端
        
        Args:
            delay: API请求间延迟时间(秒)
        """
        self.delay = delay
        
    def wait(self, message: Optional[str] = None):
        """
        实现API速率限制等待机制
        
        迁移自: archive/yahoo_api_data.py YahooFantasyDataFetcher.wait() 第35行
        """
        if message:
            print(f"⏳ {message}")
        time.sleep(self.delay)
        
    def save_token(self, token):
        """
        保存令牌到文件
        
        迁移自: archive/yahoo_api_utils.py save_token() 第39行
        """
        try:
            # 确保目录存在
            DEFAULT_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(DEFAULT_TOKEN_FILE, 'wb') as f:
                pickle.dump(token, f)
            print(f"令牌已保存到: {DEFAULT_TOKEN_FILE}")
            return True
        except Exception as e:
            print(f"保存令牌时出错: {str(e)}")
            return False

    def load_token(self):
        """
        从文件加载令牌
        
        迁移自: archive/yahoo_api_utils.py load_token() 第52行
        """
        # 首先尝试从统一位置加载
        if DEFAULT_TOKEN_FILE.exists():
            try:
                with open(DEFAULT_TOKEN_FILE, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"加载令牌时出错: {str(e)}")
        else:
            # 如果统一位置不存在，尝试从其他可能的位置加载并迁移
            possible_token_paths = [
                pathlib.Path("tokens/yahoo_token.token"),  # 当前目录下的tokens
                pathlib.Path("scripts/tokens/yahoo_token.token"),  # scripts目录下的tokens
            ]
            
            for token_path in possible_token_paths:
                if token_path.exists():
                    try:
                        with open(token_path, 'rb') as f:
                            token = pickle.load(f)
                            # 迁移到统一位置
                            if self.save_token(token):
                                print(f"已将令牌从 {token_path} 迁移到 {DEFAULT_TOKEN_FILE}")
                            return token
                    except Exception as e:
                        print(f"从 {token_path} 加载令牌时出错: {str(e)}")
            
            print("未找到token文件，请先运行app.py完成授权")
        
        return None

    def refresh_token_if_needed(self, token):
        """
        检查并刷新令牌（如果已过期）
        
        迁移自: archive/yahoo_api_utils.py refresh_token_if_needed() 第84行
        """
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
                    self.save_token(new_token)
                    
                    print("令牌刷新成功")
                    return new_token
                else:
                    print(f"令牌刷新失败: {response.status_code} - {response.text}")
                    return None
            except Exception as e:
                print(f"刷新令牌时出错: {str(e)}")
                return None
        
        return token

    def get_api_data(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        """
        通用API数据获取函数，带重试机制
        
        迁移自: archive/yahoo_api_utils.py get_api_data() 第131行
        
        Args:
            url: API端点URL
            max_retries: 最大重试次数
            
        Returns:
            API响应的JSON数据或None
        """
        # 加载令牌
        token = self.load_token()
        if not token:
            print("未找到有效令牌，请先运行app.py完成授权")
            return None
        
        # 刷新令牌（如果需要）
        token = self.refresh_token_if_needed(token)
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
                    token = self.refresh_token_if_needed(token)
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