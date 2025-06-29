"""
批量数据处理器 (Batch Processor)
===============================

【主要职责】
1. 批量数据处理协调和优化
2. 事务边界管理和批量提交
3. 错误处理和恢复机制
4. 性能监控和优化

【功能特性】
- 自适应批量大小调整
- 并发处理支持
- 内存使用优化
- 错误隔离和重试
- 详细的处理统计

【使用示例】
```python
from fantasy_etl.load.batch_processor import BatchProcessor

# 创建批量处理器
processor = BatchProcessor(batch_size=1000)

# 处理数据
results = processor.process_batch(data_list, process_function)

# 批量插入
results = processor.batch_insert(session, model_objects)
```
"""

import time
import logging
from typing import List, Dict, Any, Optional, Callable, TypeVar, Generic, Iterator
from datetime import datetime, timedelta
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError

from .database.session_manager import SessionManager

# 配置日志
logger = logging.getLogger(__name__)

# 泛型类型
T = TypeVar('T')


@dataclass
class BatchResult:
    """批量处理结果"""
    total_processed: int = 0
    total_success: int = 0
    total_failed: int = 0
    total_batches: int = 0
    processing_time: float = 0.0
    errors: List[str] = None
    performance_stats: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.performance_stats is None:
            self.performance_stats = {}
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_processed == 0:
            return 0.0
        return (self.total_success / self.total_processed) * 100
    
    @property
    def throughput(self) -> float:
        """吞吐量（条/秒）"""
        if self.processing_time == 0:
            return 0.0
        return self.total_processed / self.processing_time
    
    def add_error(self, error: str):
        """添加错误信息"""
        self.errors.append(error)
        self.total_failed += 1
    
    def combine(self, other: 'BatchResult') -> 'BatchResult':
        """合并两个批量结果"""
        combined = BatchResult(
            total_processed=self.total_processed + other.total_processed,
            total_success=self.total_success + other.total_success,
            total_failed=self.total_failed + other.total_failed,
            total_batches=self.total_batches + other.total_batches,
            processing_time=max(self.processing_time, other.processing_time),
            errors=self.errors + other.errors
        )
        
        # 合并性能统计
        combined.performance_stats = {**self.performance_stats, **other.performance_stats}
        return combined


@dataclass
class BatchConfig:
    """批量处理配置"""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_parallel: bool = False
    max_workers: int = 4
    memory_limit_mb: int = 512
    timeout_seconds: int = 300
    enable_adaptive_sizing: bool = True
    min_batch_size: int = 100
    max_batch_size: int = 5000
    performance_threshold: float = 1000.0  # 条/秒


class BatchProcessor(Generic[T]):
    """批量数据处理器"""
    
    def __init__(self, session_manager: Optional[SessionManager] = None, 
                 config: Optional[BatchConfig] = None):
        """
        初始化批量处理器
        
        Args:
            session_manager: 会话管理器实例
            config: 批量处理配置
        """
        self.session_manager = session_manager or SessionManager()
        self.config = config or BatchConfig()
        
        # 性能监控
        self.performance_history = []
        self.adaptive_batch_size = self.config.batch_size
        
        # 统计信息
        self.total_processed = 0
        self.total_batches = 0
        self.last_performance_check = datetime.now()
        
        logger.info(f"批量处理器初始化完成，批量大小: {self.config.batch_size}")
    
    def process_batch(self, data: List[T], processor_func: Callable[[List[T]], Any],
                     **kwargs) -> BatchResult:
        """
        处理批量数据
        
        Args:
            data: 要处理的数据列表
            processor_func: 处理函数，接收数据列表作为参数
            **kwargs: 传递给处理函数的额外参数
            
        Returns:
            BatchResult: 处理结果
        """
        if not data:
            return BatchResult()
        
        start_time = time.time()
        result = BatchResult()
        
        # 如果启用并行处理
        if self.config.enable_parallel and len(data) > self.config.batch_size * 2:
            result = self._process_parallel(data, processor_func, **kwargs)
        else:
            result = self._process_sequential(data, processor_func, **kwargs)
        
        result.processing_time = time.time() - start_time
        
        # 更新性能统计
        self._update_performance_stats(result)
        
        # 自适应调整批量大小
        if self.config.enable_adaptive_sizing:
            self._adjust_batch_size(result)
        
        logger.info(f"批量处理完成: {result.total_success}/{result.total_processed} 成功, "
                   f"用时 {result.processing_time:.2f}s, 吞吐量 {result.throughput:.1f} 条/秒")
        
        return result
    
    def _process_sequential(self, data: List[T], processor_func: Callable[[List[T]], Any],
                           **kwargs) -> BatchResult:
        """顺序处理批量数据"""
        result = BatchResult()
        current_batch_size = self.adaptive_batch_size
        
        for i in range(0, len(data), current_batch_size):
            batch = data[i:i + current_batch_size]
            result.total_batches += 1
            
            batch_start_time = time.time()
            
            # 重试机制
            for attempt in range(self.config.max_retries + 1):
                try:
                    processor_func(batch, **kwargs)
                    result.total_success += len(batch)
                    break
                    
                except Exception as e:
                    if attempt == self.config.max_retries:
                        error_msg = f"批次 {result.total_batches} 处理失败 (最终尝试): {str(e)}"
                        result.add_error(error_msg)
                        logger.error(error_msg)
                    else:
                        logger.warning(f"批次 {result.total_batches} 处理失败 (尝试 {attempt + 1}): {str(e)}")
                        time.sleep(self.config.retry_delay * (attempt + 1))
            
            result.total_processed += len(batch)
            
            # 记录批次性能
            batch_time = time.time() - batch_start_time
            batch_throughput = len(batch) / batch_time if batch_time > 0 else 0
            
            logger.debug(f"批次 {result.total_batches}: {len(batch)} 条记录, "
                        f"用时 {batch_time:.2f}s, 吞吐量 {batch_throughput:.1f} 条/秒")
        
        return result
    
    def _process_parallel(self, data: List[T], processor_func: Callable[[List[T]], Any],
                         **kwargs) -> BatchResult:
        """并行处理批量数据"""
        result = BatchResult()
        current_batch_size = self.adaptive_batch_size
        
        # 创建批次
        batches = [data[i:i + current_batch_size] 
                  for i in range(0, len(data), current_batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # 提交所有批次
            future_to_batch = {
                executor.submit(self._process_single_batch, batch, processor_func, **kwargs): batch
                for batch in batches
            }
            
            # 收集结果
            for future in as_completed(future_to_batch, timeout=self.config.timeout_seconds):
                batch = future_to_batch[future]
                result.total_batches += 1
                
                try:
                    batch_result = future.result()
                    result.total_success += batch_result['success']
                    result.total_processed += len(batch)
                    
                    if batch_result['error']:
                        result.add_error(batch_result['error'])
                    
                except Exception as e:
                    error_msg = f"并行批次处理异常: {str(e)}"
                    result.add_error(error_msg)
                    result.total_processed += len(batch)
                    logger.error(error_msg)
        
        return result
    
    def _process_single_batch(self, batch: List[T], processor_func: Callable[[List[T]], Any],
                             **kwargs) -> Dict[str, Any]:
        """处理单个批次（用于并行处理）"""
        for attempt in range(self.config.max_retries + 1):
            try:
                processor_func(batch, **kwargs)
                return {'success': len(batch), 'error': None}
                
            except Exception as e:
                if attempt == self.config.max_retries:
                    return {'success': 0, 'error': f"批次处理失败: {str(e)}"}
                time.sleep(self.config.retry_delay * (attempt + 1))
        
        return {'success': 0, 'error': "未知错误"}
    
    def batch_insert(self, objects: List[T], model_class: type,
                    session: Optional[Session] = None) -> BatchResult:
        """
        批量插入对象到数据库
        
        Args:
            objects: 要插入的对象列表
            model_class: 模型类
            session: 数据库会话（可选）
            
        Returns:
            BatchResult: 插入结果
        """
        if not objects:
            return BatchResult()
        
        def insert_batch(batch_objects):
            nonlocal session
            session_provided = session is not None
            
            if not session_provided:
                with self.session_manager.transaction() as sess:
                    self._perform_bulk_insert(sess, batch_objects, model_class)
            else:
                self._perform_bulk_insert(session, batch_objects, model_class)
        
        return self.process_batch(objects, insert_batch)
    
    def batch_update(self, updates: List[Dict], model_class: type,
                    session: Optional[Session] = None) -> BatchResult:
        """
        批量更新记录
        
        Args:
            updates: 更新数据列表
            model_class: 模型类
            session: 数据库会话（可选）
            
        Returns:
            BatchResult: 更新结果
        """
        if not updates:
            return BatchResult()
        
        def update_batch(batch_updates):
            nonlocal session
            session_provided = session is not None
            
            if not session_provided:
                with self.session_manager.transaction() as sess:
                    sess.bulk_update_mappings(model_class, batch_updates)
            else:
                session.bulk_update_mappings(model_class, batch_updates)
        
        return self.process_batch(updates, update_batch)
    
    def _perform_bulk_insert(self, session: Session, objects: List[T], model_class: type):
        """执行批量插入"""
        try:
            session.bulk_save_objects(objects, return_defaults=False)
            session.flush()
        except IntegrityError as e:
            # 处理重复键错误
            logger.warning(f"批量插入遇到重复键，尝试逐条插入: {str(e)}")
            session.rollback()
            self._insert_objects_individually(session, objects, model_class)
        except Exception as e:
            session.rollback()
            raise e
    
    def _insert_objects_individually(self, session: Session, objects: List[T], model_class: type):
        """逐条插入对象（处理重复键情况）"""
        for obj in objects:
            try:
                session.merge(obj)  # 使用merge处理重复键
            except Exception as e:
                logger.warning(f"单条记录插入失败: {str(e)}")
                continue
        session.flush()
    
    def batch_delete(self, filters: List[Dict], model_class: type,
                    session: Optional[Session] = None) -> BatchResult:
        """
        批量删除记录
        
        Args:
            filters: 删除条件列表
            model_class: 模型类
            session: 数据库会话（可选）
            
        Returns:
            BatchResult: 删除结果
        """
        if not filters:
            return BatchResult()
        
        def delete_batch(batch_filters):
            nonlocal session
            session_provided = session is not None
            
            if not session_provided:
                with self.session_manager.transaction() as sess:
                    self._perform_bulk_delete(sess, batch_filters, model_class)
            else:
                self._perform_bulk_delete(session, batch_filters, model_class)
        
        return self.process_batch(filters, delete_batch)
    
    def _perform_bulk_delete(self, session: Session, filters: List[Dict], model_class: type):
        """执行批量删除"""
        for filter_dict in filters:
            query = session.query(model_class)
            for key, value in filter_dict.items():
                query = query.filter(getattr(model_class, key) == value)
            query.delete(synchronize_session=False)
        session.flush()
    
    def _update_performance_stats(self, result: BatchResult):
        """更新性能统计"""
        self.total_processed += result.total_processed
        self.total_batches += result.total_batches
        
        # 记录性能历史
        performance_record = {
            'timestamp': datetime.now(),
            'throughput': result.throughput,
            'success_rate': result.success_rate,
            'batch_count': result.total_batches,
            'processing_time': result.processing_time
        }
        self.performance_history.append(performance_record)
        
        # 保持历史记录在合理范围内
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-50:]
    
    def _adjust_batch_size(self, result: BatchResult):
        """自适应调整批量大小"""
        if not self.performance_history or len(self.performance_history) < 2:
            return
        
        current_performance = result.throughput
        target_performance = self.config.performance_threshold
        
        # 如果性能低于阈值，减小批量大小
        if current_performance < target_performance * 0.8:
            new_size = max(self.config.min_batch_size, 
                          int(self.adaptive_batch_size * 0.8))
            if new_size != self.adaptive_batch_size:
                self.adaptive_batch_size = new_size
                logger.info(f"性能较低，调整批量大小为: {self.adaptive_batch_size}")
        
        # 如果性能很好，尝试增加批量大小
        elif current_performance > target_performance * 1.2:
            new_size = min(self.config.max_batch_size, 
                          int(self.adaptive_batch_size * 1.2))
            if new_size != self.adaptive_batch_size:
                self.adaptive_batch_size = new_size
                logger.info(f"性能良好，调整批量大小为: {self.adaptive_batch_size}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """获取性能摘要"""
        if not self.performance_history:
            return {'message': '暂无性能数据'}
        
        recent_records = self.performance_history[-10:]  # 最近10次记录
        
        avg_throughput = sum(r['throughput'] for r in recent_records) / len(recent_records)
        avg_success_rate = sum(r['success_rate'] for r in recent_records) / len(recent_records)
        
        return {
            'total_processed': self.total_processed,
            'total_batches': self.total_batches,
            'current_batch_size': self.adaptive_batch_size,
            'average_throughput': avg_throughput,
            'average_success_rate': avg_success_rate,
            'performance_history_count': len(self.performance_history),
            'last_performance_check': self.last_performance_check.isoformat()
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.performance_history.clear()
        self.total_processed = 0
        self.total_batches = 0
        self.adaptive_batch_size = self.config.batch_size
        self.last_performance_check = datetime.now()
        logger.info("批量处理器统计信息已重置")


# 便利函数
def create_batch_processor(batch_size: int = 1000, 
                          enable_parallel: bool = False,
                          max_workers: int = 4) -> BatchProcessor:
    """创建预配置的批量处理器"""
    config = BatchConfig(
        batch_size=batch_size,
        enable_parallel=enable_parallel,
        max_workers=max_workers
    )
    return BatchProcessor(config=config)


def batch_process_data(data: List[T], processor_func: Callable[[List[T]], Any],
                      batch_size: int = 1000, **kwargs) -> BatchResult:
    """快速批量处理数据的便利函数"""
    processor = create_batch_processor(batch_size=batch_size)
    return processor.process_batch(data, processor_func, **kwargs) 