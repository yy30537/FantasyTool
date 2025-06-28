"""
API Rate Limiter
================

API请求速率限制器，用于控制Yahoo Fantasy API的请求频率

主要功能：
- 请求间隔控制
- 动态速率调整
- 并发请求管理
- 限流状态监控

设计说明：
- Yahoo Fantasy API没有公开的明确限流政策
- 采用保守策略：默认1秒间隔，可动态调整
- 支持突发请求处理和平滑限流

使用示例：
```python
from fantasy_etl.extract.rate_limiter import RateLimiter

limiter = RateLimiter(base_delay=1.0, max_delay=5.0)
limiter.wait_if_needed()  # 等待直到可以发起请求
```
"""

import time
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class RateLimitStats:
    """速率限制统计信息"""
    total_requests: int = 0
    total_waits: int = 0
    total_wait_time: float = 0.0
    avg_request_interval: float = 0.0
    last_request_time: Optional[datetime] = None
    current_delay: float = 1.0
    
    def update_request(self, wait_time: float = 0.0):
        """更新请求统计"""
        self.total_requests += 1
        if wait_time > 0:
            self.total_waits += 1
            self.total_wait_time += wait_time
        self.last_request_time = datetime.now()
    
    def get_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        avg_wait = (self.total_wait_time / self.total_waits 
                   if self.total_waits > 0 else 0)
        
        return {
            'total_requests': self.total_requests,
            'total_waits': self.total_waits,
            'total_wait_time': round(self.total_wait_time, 2),
            'average_wait_time': round(avg_wait, 2),
            'current_delay': self.current_delay,
            'last_request': self.last_request_time.isoformat() if self.last_request_time else None
        }


class RateLimiter:
    """
    API请求速率限制器
    
    实现智能的请求间隔控制，防止触发API限流
    """
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 5.0, 
                 min_delay: float = 0.1, burst_threshold: int = 5):
        """
        初始化速率限制器
        
        Args:
            base_delay: 基础请求间隔(秒)
            max_delay: 最大请求间隔(秒)
            min_delay: 最小请求间隔(秒)
            burst_threshold: 突发请求阈值
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.burst_threshold = burst_threshold
        
        # 当前状态
        self.current_delay = base_delay
        self.last_request_time: Optional[float] = None
        self.consecutive_fast_requests = 0
        self.recent_request_times: list[float] = []
        
        # 线程锁，确保并发安全
        self._lock = threading.Lock()
        
        # 统计信息
        self.stats = RateLimitStats()
        
        # 动态调整参数
        self.adjustment_factor = 1.2  # 延迟调整因子
        self.recovery_factor = 0.9    # 恢复因子
        self.monitoring_window = 60   # 监控窗口(秒)
    
    def wait_if_needed(self) -> float:
        """
        如果需要，等待直到可以发起下一个请求
        
        Returns:
            实际等待的时间(秒)
        """
        with self._lock:
            current_time = time.time()
            wait_time = 0.0
            
            if self.last_request_time is not None:
                elapsed = current_time - self.last_request_time
                required_delay = self._calculate_required_delay()
                
                if elapsed < required_delay:
                    wait_time = required_delay - elapsed
                    time.sleep(wait_time)
                    current_time = time.time()
            
            # 更新状态
            self._update_state(current_time)
            self.stats.update_request(wait_time)
            
            return wait_time
    
    def _calculate_required_delay(self) -> float:
        """计算当前需要的请求间隔"""
        # 基于最近请求频率动态调整
        self._adjust_delay_based_on_frequency()
        
        return max(self.min_delay, min(self.current_delay, self.max_delay))
    
    def _adjust_delay_based_on_frequency(self):
        """基于请求频率动态调整延迟"""
        current_time = time.time()
        
        # 清理过期的请求记录
        cutoff_time = current_time - self.monitoring_window
        self.recent_request_times = [
            t for t in self.recent_request_times if t > cutoff_time
        ]
        
        # 检查是否有过多的快速请求
        if len(self.recent_request_times) > self.burst_threshold:
            # 增加延迟
            self.current_delay = min(
                self.current_delay * self.adjustment_factor,
                self.max_delay
            )
            self.consecutive_fast_requests += 1
        else:
            # 逐渐恢复到基础延迟
            if self.current_delay > self.base_delay:
                self.current_delay = max(
                    self.current_delay * self.recovery_factor,
                    self.base_delay
                )
            self.consecutive_fast_requests = max(0, self.consecutive_fast_requests - 1)
    
    def _update_state(self, current_time: float):
        """更新内部状态"""
        self.last_request_time = current_time
        self.recent_request_times.append(current_time)
        
        # 限制记录的长度
        max_records = self.burst_threshold * 2
        if len(self.recent_request_times) > max_records:
            self.recent_request_times = self.recent_request_times[-max_records:]
    
    def handle_rate_limit_response(self, retry_after: Optional[int] = None):
        """
        处理API返回的限流响应
        
        Args:
            retry_after: API返回的建议重试间隔(秒)
        """
        with self._lock:
            if retry_after:
                # 使用API建议的间隔，但不超过最大延迟
                suggested_delay = min(retry_after, self.max_delay)
                self.current_delay = max(suggested_delay, self.current_delay)
            else:
                # 没有建议间隔，增加当前延迟
                self.current_delay = min(
                    self.current_delay * (self.adjustment_factor * 1.5),
                    self.max_delay
                )
    
    def reset_to_normal(self):
        """重置到正常速率"""
        with self._lock:
            self.current_delay = self.base_delay
            self.consecutive_fast_requests = 0
            self.recent_request_times.clear()
    
    def set_delay(self, delay: float):
        """
        手动设置请求间隔
        
        Args:
            delay: 新的请求间隔(秒)
        """
        with self._lock:
            self.current_delay = max(self.min_delay, min(delay, self.max_delay))
    
    def get_current_delay(self) -> float:
        """获取当前请求间隔"""
        return self.current_delay
    
    def get_time_until_next_request(self) -> float:
        """获取距离下次可请求的时间"""
        if self.last_request_time is None:
            return 0.0
        
        elapsed = time.time() - self.last_request_time
        required_delay = self._calculate_required_delay()
        
        return max(0.0, required_delay - elapsed)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取速率限制统计信息"""
        with self._lock:
            stats_summary = self.stats.get_summary()
            stats_summary.update({
                'consecutive_fast_requests': self.consecutive_fast_requests,
                'recent_requests_count': len(self.recent_request_times),
                'time_until_next_request': self.get_time_until_next_request()
            })
            return stats_summary
    
    def is_ready_for_request(self) -> bool:
        """检查是否准备好发起下一个请求"""
        return self.get_time_until_next_request() <= 0


class AdaptiveRateLimiter(RateLimiter):
    """
    自适应速率限制器
    
    根据API响应和错误自动调整请求频率
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 自适应参数
        self.success_count = 0
        self.error_count = 0
        self.last_error_time: Optional[float] = None
        
        # 学习参数
        self.learning_rate = 0.1
        self.stability_threshold = 10  # 连续成功请求数阈值
    
    def record_request_result(self, success: bool, is_rate_limited: bool = False):
        """
        记录请求结果，用于自适应调整
        
        Args:
            success: 请求是否成功
            is_rate_limited: 是否因限流失败
        """
        with self._lock:
            current_time = time.time()
            
            if success:
                self.success_count += 1
                self.error_count = 0  # 重置错误计数
                
                # 连续成功一定数量后，尝试减少延迟
                if self.success_count >= self.stability_threshold:
                    self._try_decrease_delay()
                    self.success_count = 0
                    
            else:
                self.error_count += 1
                self.success_count = 0  # 重置成功计数
                self.last_error_time = current_time
                
                if is_rate_limited:
                    self._handle_rate_limit_error()
                else:
                    self._handle_general_error()
    
    def _try_decrease_delay(self):
        """尝试减少延迟（当连续成功时）"""
        if self.current_delay > self.base_delay:
            reduction = self.current_delay * self.learning_rate
            self.current_delay = max(
                self.base_delay,
                self.current_delay - reduction
            )
    
    def _handle_rate_limit_error(self):
        """处理限流错误"""
        # 显著增加延迟
        increase = self.current_delay * (self.adjustment_factor - 1) * 2
        self.current_delay = min(
            self.current_delay + increase,
            self.max_delay
        )
    
    def _handle_general_error(self):
        """处理一般错误"""
        # 适度增加延迟
        increase = self.current_delay * self.learning_rate
        self.current_delay = min(
            self.current_delay + increase,
            self.max_delay
        )
    
    def get_adaptive_stats(self) -> Dict[str, Any]:
        """获取自适应统计信息"""
        stats = self.get_stats()
        stats.update({
            'success_count': self.success_count,
            'error_count': self.error_count,
            'last_error_time': self.last_error_time,
            'adaptation_active': self.current_delay != self.base_delay
        })
        return stats


# 便捷函数
def create_default_rate_limiter() -> RateLimiter:
    """创建默认的速率限制器"""
    return RateLimiter(
        base_delay=1.0,      # 保守的1秒间隔
        max_delay=5.0,       # 最大不超过5秒
        min_delay=0.2,       # 最小200毫秒
        burst_threshold=3    # 允许3个突发请求
    )


def create_adaptive_rate_limiter() -> AdaptiveRateLimiter:
    """创建自适应速率限制器"""
    return AdaptiveRateLimiter(
        base_delay=0.8,      # 稍微激进一些
        max_delay=10.0,      # 允许更长的最大延迟
        min_delay=0.1,       # 更短的最小延迟
        burst_threshold=5    # 更多的突发请求
    )
