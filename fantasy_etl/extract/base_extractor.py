"""
Base Extractor
==============

基础数据提取器抽象类，为所有数据提取器提供统一接口

主要功能：
- 统一的提取器接口定义
- 通用的错误处理和重试逻辑
- 数据验证和清理功能
- 提取结果标准化
- 日志和监控集成

设计模式：
- 模板方法模式：定义提取流程框架
- 策略模式：支持不同的提取策略
- 观察者模式：支持进度监控

使用示例：
```python
class MyExtractor(BaseExtractor):
    def _extract_data(self, **params):
        # 实现具体的数据提取逻辑
        return self.client.make_request("/my/endpoint")
    
    def _validate_extracted_data(self, data):
        # 实现数据验证逻辑
        return data is not None
```
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from .yahoo_client import YahooAPIClient, APIResponse, APIRequestStatus
from .api_models import ExtractResult, ExtractStatus


class ExtractorType(Enum):
    """提取器类型枚举"""
    CORE_ENTITY = "core_entity"         # 核心业务实体 (games, leagues, teams, players)
    TIME_SERIES = "time_series"         # 时间序列数据
    TRANSACTIONAL = "transactional"     # 交易类数据
    AUXILIARY = "auxiliary"             # 辅助数据
    METADATA = "metadata"               # 元数据 (settings, stat_categories, schedules)
    STATISTICS = "statistics"           # 统计数据 (player_stats, team_stats)
    OPERATIONAL = "operational"         # 运营数据 (matchups, transactions, rosters)


@dataclass
class ExtractionContext:
    """提取上下文信息"""
    extractor_name: str
    extractor_type: ExtractorType
    start_time: datetime
    league_key: Optional[str] = None
    season: Optional[str] = None
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class BaseExtractor(ABC):
    """
    基础数据提取器抽象类
    
    定义所有提取器的通用接口和行为
    """
    
    def __init__(self, yahoo_client: YahooAPIClient, extractor_name: str,
                 extractor_type: ExtractorType):
        """
        初始化基础提取器
        
        Args:
            yahoo_client: Yahoo API客户端
            extractor_name: 提取器名称
            extractor_type: 提取器类型
        """
        self.client = yahoo_client
        self.extractor_name = extractor_name
        self.extractor_type = extractor_type
        
        # 提取配置
        self.max_retries = 3
        self.retry_delay = 1.0
        self.validate_data = True
        self.clean_data = True
        
        # 统计信息
        self.extraction_count = 0
        self.success_count = 0
        self.error_count = 0
        self.total_extraction_time = 0.0
        
        # 回调函数
        self.progress_callback: Optional[Callable] = None
        self.error_callback: Optional[Callable] = None
    
    def extract(self, **params) -> ExtractResult:
        """
        执行数据提取的主要入口方法
        
        Args:
            **params: 提取参数
            
        Returns:
            ExtractResult: 标准化的提取结果
        """
        context = ExtractionContext(
            extractor_name=self.extractor_name,
            extractor_type=self.extractor_type,
            start_time=datetime.now(),
            parameters=params
        )
        
        self.extraction_count += 1
        
        try:
            # 前置验证
            validation_result = self._validate_parameters(params)
            if not validation_result.is_valid:
                return ExtractResult(
                    status=ExtractStatus.INVALID_PARAMETERS,
                    error_message=validation_result.error_message,
                    context=context
                )
            
            # 执行提取
            result = self._execute_extraction(context)
            
            # 更新统计
            if result.is_success:
                self.success_count += 1
            else:
                self.error_count += 1
            
            # 更新总时间
            execution_time = (datetime.now() - context.start_time).total_seconds()
            self.total_extraction_time += execution_time
            result.execution_time = execution_time
            
            return result
            
        except Exception as e:
            self.error_count += 1
            
            # 调用错误回调
            if self.error_callback:
                try:
                    self.error_callback(context, e)
                except Exception:
                    pass  # 忽略回调错误
            
            return ExtractResult(
                status=ExtractStatus.EXTRACTION_ERROR,
                error_message=f"Unexpected error: {str(e)}",
                context=context
            )
    
    def _execute_extraction(self, context: ExtractionContext) -> ExtractResult:
        """执行数据提取的核心逻辑"""
        last_result = None
        
        for attempt in range(self.max_retries + 1):
            try:
                # 调用进度回调
                if self.progress_callback:
                    self.progress_callback(context, attempt, self.max_retries + 1)
                
                # 执行具体的数据提取
                raw_data = self._extract_data(**context.parameters)
                
                # 验证原始数据
                if self.validate_data:
                    if not self._validate_extracted_data(raw_data):
                        raise ValueError("Data validation failed")
                
                # 清理和转换数据
                if self.clean_data:
                    cleaned_data = self._clean_and_transform_data(raw_data)
                else:
                    cleaned_data = raw_data
                
                # 构建成功结果
                return ExtractResult(
                    status=ExtractStatus.SUCCESS,
                    data=cleaned_data,
                    raw_data=raw_data,
                    context=context,
                    retry_count=attempt
                )
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # 判断是否应该重试
                should_retry = (
                    attempt < self.max_retries and
                    self._should_retry_on_error(e)
                )
                
                if should_retry:
                    # 等待后重试
                    delay = self.retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
                
                # 构建失败结果
                last_result = ExtractResult(
                    status=self._classify_error(e),
                    error_message=str(e),
                    context=context,
                    retry_count=attempt
                )
                break
        
        return last_result or ExtractResult(
            status=ExtractStatus.EXTRACTION_ERROR,
            error_message="All retry attempts failed",
            context=context
        )
    
    @abstractmethod
    def _extract_data(self, **params) -> Any:
        """
        子类必须实现的数据提取方法
        
        Args:
            **params: 提取参数
            
        Returns:
            提取的原始数据
        """
        pass
    
    def _validate_parameters(self, params: Dict[str, Any]) -> 'ValidationResult':
        """
        验证提取参数
        
        子类可以重写此方法来实现特定的参数验证
        
        Args:
            params: 提取参数
            
        Returns:
            ValidationResult: 验证结果
        """
        return ValidationResult(is_valid=True)
    
    def _validate_extracted_data(self, data: Any) -> bool:
        """
        验证提取的数据
        
        子类可以重写此方法来实现特定的数据验证
        
        Args:
            data: 提取的原始数据
            
        Returns:
            bool: 数据是否有效
        """
        return data is not None
    
    def _clean_and_transform_data(self, data: Any) -> Any:
        """
        清理和转换数据
        
        子类可以重写此方法来实现特定的数据清理逻辑
        
        Args:
            data: 原始数据
            
        Returns:
            清理后的数据
        """
        return data
    
    def _should_retry_on_error(self, error: Exception) -> bool:
        """
        判断是否应该在错误时重试
        
        Args:
            error: 发生的错误
            
        Returns:
            bool: 是否应该重试
        """
        error_msg = str(error).lower()
        
        # 网络相关错误通常可以重试
        retryable_errors = [
            'network', 'connection', 'timeout', 'rate limit',
            'server error', 'temporary', 'unavailable'
        ]
        
        return any(keyword in error_msg for keyword in retryable_errors)
    
    def _classify_error(self, error: Exception) -> ExtractStatus:
        """
        对错误进行分类
        
        Args:
            error: 发生的错误
            
        Returns:
            ExtractStatus: 错误状态
        """
        error_msg = str(error).lower()
        
        if 'auth' in error_msg or 'token' in error_msg:
            return ExtractStatus.AUTH_ERROR
        elif 'rate limit' in error_msg:
            return ExtractStatus.RATE_LIMITED
        elif 'network' in error_msg or 'connection' in error_msg:
            return ExtractStatus.NETWORK_ERROR
        elif 'validation' in error_msg or 'invalid' in error_msg:
            return ExtractStatus.DATA_VALIDATION_ERROR
        else:
            return ExtractStatus.EXTRACTION_ERROR
    
    def set_progress_callback(self, callback: Callable):
        """设置进度回调函数"""
        self.progress_callback = callback
    
    def set_error_callback(self, callback: Callable):
        """设置错误回调函数"""
        self.error_callback = callback
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """获取提取器统计信息"""
        success_rate = (self.success_count / self.extraction_count * 100
                       if self.extraction_count > 0 else 0)
        
        avg_execution_time = (self.total_extraction_time / self.extraction_count
                             if self.extraction_count > 0 else 0)
        
        return {
            'extractor_name': self.extractor_name,
            'extractor_type': self.extractor_type.value,
            'extraction_count': self.extraction_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': round(success_rate, 2),
            'total_execution_time': round(self.total_extraction_time, 2),
            'average_execution_time': round(avg_execution_time, 2)
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.extraction_count = 0
        self.success_count = 0
        self.error_count = 0
        self.total_extraction_time = 0.0


@dataclass
class ValidationResult:
    """参数验证结果"""
    is_valid: bool
    error_message: Optional[str] = None


class BatchExtractor(BaseExtractor):
    """
    批量数据提取器基类
    
    支持批量数据提取和并发处理
    """
    
    def __init__(self, yahoo_client: YahooAPIClient, extractor_name: str,
                 extractor_type: ExtractorType, batch_size: int = 25):
        super().__init__(yahoo_client, extractor_name, extractor_type)
        self.batch_size = batch_size
        self.concurrent_requests = False  # 默认不启用并发
    
    def extract_batch(self, items: List[Any], **params) -> List[ExtractResult]:
        """
        批量提取数据
        
        Args:
            items: 要提取的项目列表
            **params: 提取参数
            
        Returns:
            List[ExtractResult]: 提取结果列表
        """
        results = []
        
        # 分批处理
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            batch_results = self._extract_batch_data(batch, **params)
            results.extend(batch_results)
            
            # 批次间等待
            if i + self.batch_size < len(items):
                self.client.rate_limiter.wait_if_needed()
        
        return results
    
    @abstractmethod
    def _extract_batch_data(self, batch: List[Any], **params) -> List[ExtractResult]:
        """
        子类必须实现的批量数据提取方法
        
        Args:
            batch: 当前批次的项目
            **params: 提取参数
            
        Returns:
            List[ExtractResult]: 批次提取结果
        """
        pass


class PaginatedExtractor(BaseExtractor):
    """
    分页数据提取器基类
    
    支持分页数据的自动提取
    """
    
    def __init__(self, yahoo_client: YahooAPIClient, extractor_name: str,
                 extractor_type: ExtractorType, page_size: int = 25):
        super().__init__(yahoo_client, extractor_name, extractor_type)
        self.page_size = page_size
        self.max_pages = 100  # 防止无限循环
    
    def extract_all_pages(self, **params) -> ExtractResult:
        """
        提取所有分页数据
        
        Args:
            **params: 提取参数
            
        Returns:
            ExtractResult: 包含所有页面数据的结果
        """
        all_data = []
        page = 0
        
        while page < self.max_pages:
            page_params = params.copy()
            page_params.update(self._get_page_params(page))
            
            page_result = self.extract(**page_params)
            
            if not page_result.is_success:
                # 如果是第一页失败，返回错误
                if page == 0:
                    return page_result
                # 否则停止分页
                break
            
            page_data = self._extract_page_data(page_result.data)
            if not page_data:
                break  # 没有更多数据
            
            all_data.extend(page_data)
            
            # 检查是否有更多页面
            if not self._has_more_pages(page_result.data):
                break
            
            page += 1
        
        return ExtractResult(
            status=ExtractStatus.SUCCESS,
            data=all_data,
            context=ExtractionContext(
                extractor_name=self.extractor_name,
                extractor_type=self.extractor_type,
                start_time=datetime.now(),
                parameters=params
            )
        )
    
    @abstractmethod
    def _get_page_params(self, page: int) -> Dict[str, Any]:
        """获取分页参数"""
        pass
    
    @abstractmethod
    def _extract_page_data(self, page_response: Any) -> List[Any]:
        """从分页响应中提取数据"""
        pass
    
    @abstractmethod
    def _has_more_pages(self, page_response: Any) -> bool:
        """检查是否有更多页面"""
        pass
