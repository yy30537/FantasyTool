"""
Yahoo Fantasy API Client - 统一API客户端

负责Yahoo Fantasy API的认证、请求管理和错误处理
"""
import os
import time
import pickle
import pathlib
import requests
from datetime import datetime
from typing import Dict, Optional, Any
from dotenv import load_dotenv
import logging

from .rate_limiter import SyncRateLimiter

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)


class TokenManager:
    """令牌管理器 - 处理OAuth令牌的存储、加载和刷新"""
    
    def __init__(self, token_dir: Optional[str] = None):
        """初始化令牌管理器
        
        Args:
            token_dir: 令牌存储目录，默认使用项目根目录下的tokens
        """
        base_dir = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        self.token_dir = pathlib.Path(token_dir) if token_dir else base_dir / "tokens"
        self.token_file = self.token_dir / "yahoo_token.token"
        
        # OAuth配置
        self.client_id = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
        self.client_secret = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
        self.token_url = "https://api.login.yahoo.com/oauth2/get_token"
        
        # 确保令牌目录存在
        self._ensure_token_directory()
    
    def _ensure_token_directory(self) -> None:
        """确保令牌目录存在"""
        if not self.token_dir.exists():
            self.token_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建令牌目录: {self.token_dir}")
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌到文件"""
        try:
            self._ensure_token_directory()
            with open(self.token_file, 'wb') as f:
                pickle.dump(token, f)
            logger.info(f"令牌已保存到: {self.token_file}")
            return True
        except Exception as e:
            logger.error(f"保存令牌时出错: {str(e)}")
            return False
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """从文件加载令牌"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'rb') as f:
                    token = pickle.load(f)
                logger.debug("成功加载令牌")
                return token
            except Exception as e:
                logger.error(f"加载令牌时出错: {str(e)}")
        else:
            logger.warning("令牌文件不存在，请先完成OAuth授权")
        return None
    
    def refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        """刷新访问令牌"""
        try:
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
                now = datetime.now().timestamp()
                expires_in = new_token.get('expires_in', 3600)
                new_token['expires_at'] = now + int(expires_in)
                
                # 保留refresh_token（如果新令牌中没有）
                if 'refresh_token' not in new_token and refresh_token:
                    new_token['refresh_token'] = refresh_token
                
                # 保存更新的令牌
                self.save_token(new_token)
                logger.info("令牌刷新成功")
                return new_token
            else:
                logger.error(f"令牌刷新失败: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"刷新令牌时出错: {str(e)}")
        
        return None
    
    def is_token_expired(self, token: Dict[str, Any]) -> bool:
        """检查令牌是否过期"""
        if not token:
            return True
        
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        # 提前60秒判断为过期，给刷新留时间
        return now >= (expires_at - 60)


class CircuitBreaker:
    """熔断器 - 防止连续失败的API请求"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0):
        """初始化熔断器"""
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
    
    def call(self, func, *args, **kwargs):
        """通过熔断器调用函数"""
        current_time = time.time()
        
        if self.state == "open":
            if current_time - self.last_failure_time > self.timeout:
                self.state = "half-open"
                logger.info("熔断器状态: half-open")
            else:
                raise Exception("熔断器开启，请求被拒绝")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """成功回调"""
        self.failure_count = 0
        if self.state == "half-open":
            self.state = "closed"
            logger.info("熔断器状态: closed")
    
    def _on_failure(self):
        """失败回调"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"熔断器状态: open (连续失败 {self.failure_count} 次)")


class YahooFantasyClient:
    """Yahoo Fantasy API客户端 - 统一的API请求接口"""
    
    def __init__(self, requests_per_minute: int = 60, enable_circuit_breaker: bool = True):
        """初始化API客户端"""
        self.token_manager = TokenManager()
        self.rate_limiter = SyncRateLimiter(requests_per_minute)
        self.circuit_breaker = CircuitBreaker() if enable_circuit_breaker else None
        
        # API基础URL
        self.base_url = "https://fantasysports.yahooapis.com/fantasy/v2"
        
        logger.info(f"Yahoo Fantasy API客户端初始化完成 (速率限制: {requests_per_minute} req/min)")
    
    def _get_valid_token(self) -> Optional[Dict[str, Any]]:
        """获取有效的访问令牌"""
        token = self.token_manager.load_token()
        if not token:
            logger.error("未找到令牌，请先完成OAuth授权")
            return None
        
        # 检查是否过期并刷新
        if self.token_manager.is_token_expired(token):
            refresh_token = token.get('refresh_token')
            if refresh_token:
                token = self.token_manager.refresh_token(refresh_token)
                if not token:
                    logger.error("令牌刷新失败")
                    return None
            else:
                logger.error("没有refresh_token，无法刷新")
                return None
        
        return token
    
    def _make_request(self, url: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
        """发起API请求"""
        token = self._get_valid_token()
        if not token:
            return None
        
        headers = {
            'Authorization': f"Bearer {token['access_token']}",
            'Content-Type': 'application/json'
        }
        headers.update(kwargs.pop('headers', {}))
        
        try:
            # 应用速率限制
            self.rate_limiter.acquire()
            
            # 发起请求
            response = requests.request(method, url, headers=headers, **kwargs)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.warning("访问令牌无效，尝试刷新")
                # 令牌可能无效，尝试刷新
                refresh_token = token.get('refresh_token')
                if refresh_token:
                    new_token = self.token_manager.refresh_token(refresh_token)
                    if new_token:
                        # 用新令牌重试
                        headers['Authorization'] = f"Bearer {new_token['access_token']}"
                        response = requests.request(method, url, headers=headers, **kwargs)
                        if response.status_code == 200:
                            return response.json()
                
                logger.error("令牌刷新后仍然无法访问")
                return None
            else:
                logger.error(f"API请求失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"请求时出错: {str(e)}")
            return None
    
    def fetch_with_retry(self, url: str, max_retries: int = 3, **kwargs) -> Optional[Dict[str, Any]]:
        """带重试机制的请求"""
        def _request():
            return self._make_request(url, **kwargs)
        
        # 如果启用了熔断器，通过熔断器调用
        if self.circuit_breaker:
            try:
                return self.circuit_breaker.call(_request)
            except Exception as e:
                logger.error(f"熔断器拒绝请求: {str(e)}")
                return None
        
        # 标准重试逻辑
        for attempt in range(max_retries):
            try:
                result = _request()
                if result:
                    return result
            except Exception as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            
            # 指数退避
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        logger.error(f"请求最终失败: {url}")
        return None
    
    def get(self, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """GET请求"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        return self.fetch_with_retry(url, **kwargs)
    
    def post(self, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """POST请求"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        return self.fetch_with_retry(url, method="POST", **kwargs)
    
    def build_url(self, endpoint: str, format: str = "json") -> str:
        """构建完整的API URL"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        if format and not url.endswith(f"?format={format}"):
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}format={format}"
        return url 