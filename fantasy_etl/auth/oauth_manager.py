"""
OAuth管理器 (OAuth Manager)
========================

统一的OAuth认证管理模块，负责Yahoo Fantasy API的OAuth2.0认证流程。

【主要职责】
1. Yahoo OAuth2.0认证流程管理
2. 访问令牌的获取、刷新和验证
3. API请求的统一认证处理
4. 令牌生命周期管理

【功能特性】
- 强大的错误处理和重试机制
- 支持多种令牌存储后端
- 完整的日志记录和监控
- 配置验证和环境检查
"""

import requests
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# 导入令牌存储模块
from .token_storage import TokenStorage

# 加载环境变量
load_dotenv()

# OAuth配置（从环境变量加载）
CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"


class OAuthManager:
    """OAuth认证管理器"""
    
    def __init__(self, token_storage: Optional[TokenStorage] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 token_url: Optional[str] = None):
        """
        初始化OAuth管理器
        
        Args:
            token_storage: 令牌存储实例，默认使用文件存储
            client_id: Yahoo客户端ID，默认从环境变量获取
            client_secret: Yahoo客户端密钥，默认从环境变量获取
            token_url: 令牌获取URL，默认Yahoo API URL
        """
        self.token_storage = token_storage or TokenStorage()
        self.client_id = client_id or CLIENT_ID
        self.client_secret = client_secret or CLIENT_SECRET
        self.token_url = token_url or TOKEN_URL
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """加载令牌"""
        return self.token_storage.load_token()
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌"""
        return self.token_storage.save_token(token)
    
    def refresh_token(self, token: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        检查并刷新令牌（如果已过期）
        
        Args:
            token: 当前令牌字典
            
        Returns:
            Optional[Dict]: 刷新后的令牌或None（如果刷新失败）
        """
        if not token:
            return None
        
        # 检查令牌是否过期
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        # 如果令牌已过期或即将过期（提前60秒刷新）
        if now >= (expires_at - 60):
            try:
                refresh_token = token.get('refresh_token')
                
                data = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': refresh_token,
                    'grant_type': 'refresh_token'
                }
                
                response = requests.post(self.token_url, data=data)
                
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
                    return new_token
                else:
                    print(f"令牌刷新失败: {response.status_code} - {response.text}")
                    return None
            except Exception as e:
                print(f"刷新令牌时出错: {str(e)}")
                return None
        
        return token
    
    def make_authenticated_request(self, url: str, max_retries: int = 3,
                                 method: str = 'GET', **kwargs) -> Optional[Dict[str, Any]]:
        """
        发起认证的API请求
        
        Args:
            url: API请求URL
            max_retries: 最大重试次数
            method: HTTP方法
            **kwargs: 传递给requests的其他参数
            
        Returns:
            Optional[Dict]: API响应JSON或None
        """
        # 加载令牌
        token = self.load_token()
        if not token:
            print("未找到有效令牌")
            return None
        
        # 刷新令牌（如果需要）
        token = self.refresh_token(token)
        if not token:
            print("令牌刷新失败")
            return None
        
        # 设置请求头
        headers = kwargs.get('headers', {})
        headers.update({
            'Authorization': f"Bearer {token['access_token']}",
            'Content-Type': 'application/json'
        })
        kwargs['headers'] = headers
        
        # 重试机制
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, **kwargs)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    # 授权问题，尝试刷新令牌
                    token = self.refresh_token(token)
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
                        time.sleep(wait_time)
                        continue
                    return None
            except Exception as e:
                print(f"请求时出错: {str(e)}")
                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 指数退避
                    time.sleep(wait_time)
                    continue
                return None
        
        return None
    
    def validate_token(self, token: Optional[Dict[str, Any]] = None) -> bool:
        """
        验证令牌是否有效
        
        Args:
            token: 要验证的令牌，默认使用当前存储的令牌
            
        Returns:
            bool: 令牌是否有效
        """
        if not token:
            token = self.load_token()
        
        if not token:
            return False
        
        # 检查必要字段
        required_fields = ['access_token', 'expires_at']
        if not all(field in token for field in required_fields):
            return False
        
        # 检查是否过期
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        return now < expires_at
    
    def get_client_info(self) -> Dict[str, str]:
        """获取客户端配置信息"""
        return {
            'client_id': self.client_id[:10] + "..." if self.client_id else "未设置",
            'client_secret': "已设置" if self.client_secret else "未设置",
            'token_url': self.token_url
        }


class MultiUserOAuthManager(OAuthManager):
    """多用户OAuth管理器"""
    
    def __init__(self, user_id: str, **kwargs):
        """
        初始化多用户OAuth管理器
        
        Args:
            user_id: 用户ID，用于区分不同用户的令牌
        """
        self.user_id = user_id
        super().__init__(**kwargs)
    
    def get_user_token_key(self) -> str:
        """获取用户特定的令牌键"""
        return f"yahoo_token_{self.user_id}"


class OAuthTokenCache:
    """OAuth令牌缓存"""
    
    def __init__(self):
        self._cache = {}
        self._cache_timeout = 300  # 5分钟缓存超时
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """从缓存获取令牌"""
        if key in self._cache:
            token_data, timestamp = self._cache[key]
            if time.time() - timestamp < self._cache_timeout:
                return token_data
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, token: Dict[str, Any]) -> None:
        """设置令牌到缓存"""
        self._cache[key] = (token, time.time())
    
    def clear(self) -> None:
        """清空缓存"""
        self._cache.clear() 