"""
Yahoo Fantasy API Client
========================

Yahoo Fantasy Sports API的核心客户端实现
从scripts/yahoo_api_utils.py的get_api_data功能迁移而来

主要功能：
- OAuth认证的API请求处理
- 请求重试和错误处理机制
- 响应数据验证和解析
- API限流和速率控制集成

使用示例：
```python
from fantasy_etl.extract.yahoo_client import YahooAPIClient
from fantasy_etl.auth import OAuthManager
from fantasy_etl.config import APIConfig

oauth_manager = OAuthManager()
api_config = APIConfig()
client = YahooAPIClient(oauth_manager, api_config)

data = client.make_request("/users;use_login=1/games")
```
"""

import time
import json
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum

from ..auth import OAuthManager
from ..config import APIConfig
from .rate_limiter import RateLimiter


class APIRequestStatus(Enum):
    """API请求状态枚举"""
    SUCCESS = "success"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    AUTH_ERROR = "auth_error"
    NETWORK_ERROR = "network_error"
    INVALID_RESPONSE = "invalid_response"


@dataclass
class APIResponse:
    """API响应数据结构"""
    status: APIRequestStatus
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    status_code: Optional[int] = None
    retry_count: int = 0
    response_time: float = 0.0
    
    @property
    def is_success(self) -> bool:
        """检查请求是否成功"""
        return self.status == APIRequestStatus.SUCCESS and self.data is not None
    
    @property
    def is_rate_limited(self) -> bool:
        """检查是否被限流"""
        return self.status == APIRequestStatus.RATE_LIMITED


class YahooAPIClient:
    """
    Yahoo Fantasy API客户端
    
    提供统一的API请求接口，集成认证、重试、限流等功能
    """
    
    def __init__(self, oauth_manager: OAuthManager, api_config: APIConfig, 
                 rate_limiter: Optional[RateLimiter] = None):
        """
        初始化API客户端
        
        Args:
            oauth_manager: OAuth认证管理器
            api_config: API配置管理器
            rate_limiter: 可选的速率限制器
        """
        self.oauth_manager = oauth_manager
        self.api_config = api_config
        self.rate_limiter = rate_limiter or RateLimiter()
        
        # 请求配置
        self.max_retries = 3
        self.retry_delay = 1.0  # 基础重试间隔(秒)
        self.request_timeout = 30  # 请求超时(秒)
        
        # 统计信息
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
    
    def make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None,
                    format_type: str = "json", retry_auth: bool = True) -> APIResponse:
        """
        发起API请求
        
        Args:
            endpoint: API端点 (如 "/users;use_login=1/games")
            params: 请求参数字典
            format_type: 响应格式 ("json" 或 "xml")
            retry_auth: 认证失败时是否重试
            
        Returns:
            APIResponse: 包含响应数据和状态的对象
        """
        start_time = time.time()
        self.total_requests += 1
        
        # 构建完整URL
        url = self._build_url(endpoint, params, format_type)
        
        # 执行请求（包含重试逻辑）
        response = self._execute_request_with_retry(url, retry_auth)
        
        # 更新响应时间
        response.response_time = time.time() - start_time
        
        # 更新统计
        if response.is_success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        
        return response
    
    def make_batch_request(self, endpoints: list[str], 
                          params: Optional[Dict[str, str]] = None) -> list[APIResponse]:
        """
        批量API请求
        
        Args:
            endpoints: 端点列表
            params: 通用请求参数
            
        Returns:
            响应列表
        """
        responses = []
        
        for i, endpoint in enumerate(endpoints):
            response = self.make_request(endpoint, params)
            responses.append(response)
            
            # 批量请求间隔
            if i < len(endpoints) - 1:
                self.rate_limiter.wait_if_needed()
        
        return responses
    
    def _build_url(self, endpoint: str, params: Optional[Dict[str, str]], 
                   format_type: str) -> str:
        """构建完整的API URL"""
        # 确保endpoint以/开头
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
        
        # 构建基础URL
        base_url = self.api_config.base_api_url.rstrip('/')
        url = f"{base_url}{endpoint}"
        
        # 添加格式参数
        if format_type and not url.endswith('?format=json') and '?format=' not in url:
            separator = '&' if '?' in url else '?'
            url += f"{separator}format={format_type}"
        
        # 添加其他参数
        if params:
            separator = '&' if '?' in url else '?'
            param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
            url += f"{separator}{param_str}"
        
        return url
    
    def _execute_request_with_retry(self, url: str, retry_auth: bool) -> APIResponse:
        """执行带重试逻辑的API请求"""
        last_response = None
        
        for attempt in range(self.max_retries + 1):
            # 速率限制检查
            self.rate_limiter.wait_if_needed()
            
            # 执行单次请求
            response = self._execute_single_request(url)
            response.retry_count = attempt
            
            # 成功返回
            if response.is_success:
                return response
            
            # 认证错误处理
            if response.status == APIRequestStatus.AUTH_ERROR and retry_auth and attempt == 0:
                if self._try_refresh_token():
                    continue  # 刷新成功，重试
            
            # 速率限制处理
            if response.is_rate_limited:
                delay = self.retry_delay * (2 ** attempt)  # 指数退避
                time.sleep(delay)
                continue
            
            # 网络错误重试
            if response.status == APIRequestStatus.NETWORK_ERROR and attempt < self.max_retries:
                delay = self.retry_delay * (attempt + 1)
                time.sleep(delay)
                continue
            
            # 其他错误或最后一次尝试
            last_response = response
            break
        
        return last_response or APIResponse(
            status=APIRequestStatus.FAILED,
            error_message="All retry attempts failed"
        )
    
    def _execute_single_request(self, url: str) -> APIResponse:
        """执行单次API请求"""
        try:
            # 使用OAuth管理器发起认证请求
            response_data = self.oauth_manager.make_authenticated_request(url)
            
            if response_data is None:
                return APIResponse(
                    status=APIRequestStatus.AUTH_ERROR,
                    error_message="Authentication failed or no response data"
                )
            
            # 验证响应数据格式
            if not self._validate_response_format(response_data):
                return APIResponse(
                    status=APIRequestStatus.INVALID_RESPONSE,
                    error_message="Invalid response format",
                    data=response_data
                )
            
            return APIResponse(
                status=APIRequestStatus.SUCCESS,
                data=response_data
            )
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 根据错误类型分类
            if 'rate limit' in error_msg or 'too many requests' in error_msg:
                status = APIRequestStatus.RATE_LIMITED
            elif 'auth' in error_msg or 'token' in error_msg:
                status = APIRequestStatus.AUTH_ERROR
            elif 'network' in error_msg or 'connection' in error_msg:
                status = APIRequestStatus.NETWORK_ERROR
            else:
                status = APIRequestStatus.FAILED
            
            return APIResponse(
                status=status,
                error_message=str(e)
            )
    
    def _validate_response_format(self, data: Any) -> bool:
        """验证响应数据格式"""
        if not isinstance(data, dict):
            return False
        
        # Yahoo API通常返回包含fantasy_content的结构
        if 'fantasy_content' not in data:
            # 某些端点可能不包含fantasy_content，这也是有效的
            return True
        
        return isinstance(data['fantasy_content'], dict)
    
    def _try_refresh_token(self) -> bool:
        """尝试刷新认证令牌"""
        try:
            self.oauth_manager.refresh_token()
            return True
        except Exception:
            return False
    
    def get_client_stats(self) -> Dict[str, Union[int, float]]:
        """获取客户端统计信息"""
        success_rate = (self.successful_requests / self.total_requests * 100 
                       if self.total_requests > 0 else 0)
        
        return {
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': round(success_rate, 2)
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0


# 便捷函数，保持与旧脚本的接口兼容性（仅在迁移期间使用）
def create_default_client() -> YahooAPIClient:
    """创建默认的Yahoo API客户端"""
    from ..auth import OAuthManager
    from ..config import APIConfig
    
    oauth_manager = OAuthManager()
    api_config = APIConfig()
    
    return YahooAPIClient(oauth_manager, api_config)
