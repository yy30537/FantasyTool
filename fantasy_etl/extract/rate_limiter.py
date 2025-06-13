"""
Rate Limiter - API请求速率控制

实现令牌桶算法控制Yahoo Fantasy API请求频率
"""
import asyncio
import time
from typing import Optional
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """API请求速率限制器
    
    使用令牌桶算法控制请求频率，支持突发请求和平滑限流
    """
    
    def __init__(self, requests_per_minute: int = 60, burst_size: Optional[int] = None):
        """初始化速率限制器
        
        Args:
            requests_per_minute: 每分钟允许的请求数
            burst_size: 突发请求大小（默认为requests_per_minute的20%）
        """
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size or max(1, requests_per_minute // 5)
        
        # 令牌桶参数
        self.tokens = self.burst_size  # 初始令牌数
        self.max_tokens = self.burst_size
        self.refill_rate = requests_per_minute / 60.0  # 每秒补充的令牌数
        self.last_refill = time.time()
        
        # 同步控制
        self._lock = asyncio.Lock()
        
        logger.info(f"速率限制器初始化: {requests_per_minute} req/min, 突发大小: {self.burst_size}")
    
    async def _refill_tokens(self) -> None:
        """补充令牌桶中的令牌"""
        now = time.time()
        time_passed = now - self.last_refill
        
        # 计算应该添加的令牌数
        tokens_to_add = time_passed * self.refill_rate
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now
    
    async def acquire(self, tokens: int = 1) -> float:
        """获取令牌，如果没有足够令牌则等待
        
        Args:
            tokens: 需要的令牌数量
            
        Returns:
            等待时间（秒）
        """
        async with self._lock:
            await self._refill_tokens()
            
            if self.tokens >= tokens:
                # 有足够令牌，立即使用
                self.tokens -= tokens
                return 0.0
            else:
                # 没有足够令牌，计算需要等待的时间
                needed_tokens = tokens - self.tokens
                wait_time = needed_tokens / self.refill_rate
                
                logger.debug(f"速率限制触发，需要等待 {wait_time:.2f} 秒")
                await asyncio.sleep(wait_time)
                
                # 等待后重新补充并使用令牌
                await self._refill_tokens()
                self.tokens = max(0, self.tokens - tokens)
                
                return wait_time
    
    @asynccontextmanager
    async def limit(self, tokens: int = 1):
        """上下文管理器形式的速率限制
        
        Args:
            tokens: 需要的令牌数量
        """
        wait_time = await self.acquire(tokens)
        try:
            yield wait_time
        finally:
            pass  # 令牌已在acquire中消费
    
    @property
    def available_tokens(self) -> float:
        """获取当前可用令牌数"""
        return self.tokens
    
    def reset(self) -> None:
        """重置令牌桶"""
        self.tokens = self.max_tokens
        self.last_refill = time.time()
        logger.info("速率限制器已重置")


class SyncRateLimiter:
    """同步版本的速率限制器（用于兼容现有代码）"""
    
    def __init__(self, requests_per_minute: int = 60, burst_size: Optional[int] = None):
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size or max(1, requests_per_minute // 5)
        
        self.tokens = self.burst_size
        self.max_tokens = self.burst_size
        self.refill_rate = requests_per_minute / 60.0
        self.last_refill = time.time()
        
        logger.info(f"同步速率限制器初始化: {requests_per_minute} req/min")
    
    def _refill_tokens(self) -> None:
        """补充令牌桶中的令牌"""
        now = time.time()
        time_passed = now - self.last_refill
        
        tokens_to_add = time_passed * self.refill_rate
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def acquire(self, tokens: int = 1) -> float:
        """获取令牌，如果没有足够令牌则等待
        
        Args:
            tokens: 需要的令牌数量
            
        Returns:
            等待时间（秒）
        """
        self._refill_tokens()
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return 0.0
        else:
            needed_tokens = tokens - self.tokens
            wait_time = needed_tokens / self.refill_rate
            
            logger.debug(f"同步速率限制触发，等待 {wait_time:.2f} 秒")
            time.sleep(wait_time)
            
            self._refill_tokens()
            self.tokens = max(0, self.tokens - tokens)
            
            return wait_time
    
    def __enter__(self):
        """上下文管理器进入"""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        pass 