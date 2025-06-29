"""
基础加载器 (Base Loader)
=======================

【主要职责】
1. 定义加载器基础接口
2. 提供通用加载逻辑
3. 统一错误处理和日志记录
4. 数据验证和质量检查

【功能特性】
- 抽象基类模式
- 批量插入/更新支持
- 去重和验证逻辑
- 性能监控和统计
- 错误处理和重试机制

【使用示例】
```python
class GameLoader(BaseLoader):
    def _validate_record(self, record):
        # 具体验证逻辑
        pass
    
    def _create_model_instance(self, record):
        # 创建模型实例
        return Game(**record)
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Type, TypeVar, Union, Callable
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from fantasy_etl.load.database.session_manager import SessionManager
from fantasy_etl.load.database.models import Base

# 配置日志
logger = logging.getLogger(__name__)

# 泛型类型
T = TypeVar('T', bound=Base)


class LoadResult:
    """加载结果类"""
    
    def __init__(self):
        self.total_records = 0
        self.inserted_records = 0
        self.updated_records = 0
        self.skipped_records = 0
        self.failed_records = 0
        self.start_time = datetime.utcnow()
        self.end_time = None
        self.errors = []
        self.warnings = []
    
    def finish(self):
        """标记加载完成"""
        self.end_time = datetime.utcnow()
    
    @property
    def duration(self) -> float:
        """获取执行时间（秒）"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.utcnow() - self.start_time).total_seconds()
    
    @property
    def success_rate(self) -> float:
        """获取成功率"""
        if self.total_records == 0:
            return 0.0
        return (self.inserted_records + self.updated_records) / self.total_records * 100
    
    def add_error(self, error: str, record: Optional[Dict] = None):
        """添加错误"""
        self.errors.append({
            'error': error,
            'record': record,
            'timestamp': datetime.utcnow()
        })
        self.failed_records += 1
    
    def add_warning(self, warning: str, record: Optional[Dict] = None):
        """添加警告"""
        self.warnings.append({
            'warning': warning,
            'record': record,
            'timestamp': datetime.utcnow()
        })
    
    def summary(self) -> Dict[str, Any]:
        """获取加载摘要"""
        return {
            'total_records': self.total_records,
            'inserted_records': self.inserted_records,
            'updated_records': self.updated_records,
            'skipped_records': self.skipped_records,
            'failed_records': self.failed_records,
            'success_rate': self.success_rate,
            'duration_seconds': self.duration,
            'errors_count': len(self.errors),
            'warnings_count': len(self.warnings)
        }


class ValidationError(Exception):
    """数据验证错误"""
    pass


class LoaderError(Exception):
    """加载器错误"""
    pass


class BaseLoader(ABC):
    """
    基础加载器抽象类
    
    所有具体加载器都应该继承此类并实现抽象方法
    """
    
    def __init__(self, session_manager: Optional[SessionManager] = None, 
                 batch_size: int = 1000):
        """
        初始化基础加载器
        
        Args:
            session_manager: 会话管理器实例
            batch_size: 批量处理大小
        """
        self.session_manager = session_manager or SessionManager()
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """
        验证单条记录
        
        Args:
            record: 待验证的记录
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            ValidationError: 验证失败时抛出
        """
        pass
    
    @abstractmethod
    def _create_model_instance(self, record: Dict[str, Any]) -> T:
        """
        创建模型实例
        
        Args:
            record: 记录数据
            
        Returns:
            T: 模型实例
        """
        pass
    
    @abstractmethod
    def _get_unique_key(self, record: Dict[str, Any]) -> Union[str, tuple]:
        """
        获取记录的唯一键
        
        Args:
            record: 记录数据
            
        Returns:
            Union[str, tuple]: 唯一键值
        """
        pass
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        预处理记录（可重写）
        
        Args:
            record: 原始记录
            
        Returns:
            Dict[str, Any]: 处理后的记录
        """
        return record
    
    def _postprocess_record(self, instance: T, record: Dict[str, Any]) -> T:
        """
        后处理记录（可重写）
        
        Args:
            instance: 模型实例
            record: 原始记录
            
        Returns:
            T: 处理后的模型实例
        """
        return instance
    
    def _should_update_existing(self, existing: T, new_record: Dict[str, Any]) -> bool:
        """
        判断是否应该更新现有记录（可重写）
        
        Args:
            existing: 现有模型实例
            new_record: 新记录数据
            
        Returns:
            bool: 是否应该更新
        """
        return True  # 默认总是更新
    
    def _update_existing_record(self, existing: T, new_record: Dict[str, Any]) -> T:
        """
        更新现有记录（可重写）
        
        Args:
            existing: 现有模型实例
            new_record: 新记录数据
            
        Returns:
            T: 更新后的模型实例
        """
        # 默认实现：更新所有非None字段
        for key, value in new_record.items():
            if value is not None and hasattr(existing, key):
                setattr(existing, key, value)
        
        # 更新时间戳
        if hasattr(existing, 'updated_at'):
            existing.updated_at = datetime.utcnow()
        
        return existing
    
    def _find_existing_record(self, session: Session, record: Dict[str, Any]) -> Optional[T]:
        """
        查找现有记录（需要子类实现具体查询逻辑）
        
        Args:
            session: 数据库会话
            record: 记录数据
            
        Returns:
            Optional[T]: 现有记录实例，如果不存在则返回None
        """
        # 子类应该重写此方法以实现具体的查询逻辑
        return None
    
    def load_single_record(self, record: Dict[str, Any], 
                          session: Optional[Session] = None) -> LoadResult:
        """
        加载单条记录
        
        Args:
            record: 记录数据
            session: 数据库会话（可选）
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult()
        result.total_records = 1
        
        try:
            # 预处理
            processed_record = self._preprocess_record(record)
            
            # 验证
            if not self._validate_record(processed_record):
                result.add_error("Record validation failed", processed_record)
                result.finish()
                return result
            
            def _load_operation(session: Session):
                # 查找现有记录
                existing = self._find_existing_record(session, processed_record)
                
                if existing:
                    # 更新现有记录
                    if self._should_update_existing(existing, processed_record):
                        updated = self._update_existing_record(existing, processed_record)
                        updated = self._postprocess_record(updated, processed_record)
                        session.flush()
                        result.updated_records += 1
                        self.logger.debug(f"Updated existing record: {self._get_unique_key(processed_record)}")
                    else:
                        result.skipped_records += 1
                        self.logger.debug(f"Skipped existing record: {self._get_unique_key(processed_record)}")
                else:
                    # 创建新记录
                    instance = self._create_model_instance(processed_record)
                    instance = self._postprocess_record(instance, processed_record)
                    session.add(instance)
                    session.flush()
                    result.inserted_records += 1
                    self.logger.debug(f"Inserted new record: {self._get_unique_key(processed_record)}")
            
            if session:
                _load_operation(session)
            else:
                with self.session_manager.transaction() as session:
                    _load_operation(session)
            
        except ValidationError as e:
            result.add_error(f"Validation error: {e}", processed_record)
        except IntegrityError as e:
            result.add_error(f"Integrity error: {e}", processed_record)
        except Exception as e:
            result.add_error(f"Unexpected error: {e}", processed_record)
            self.logger.exception(f"Error loading record: {e}")
        
        result.finish()
        return result
    
    def load_batch_records(self, records: List[Dict[str, Any]], 
                          session: Optional[Session] = None,
                          skip_duplicates: bool = True) -> LoadResult:
        """
        批量加载记录
        
        Args:
            records: 记录列表
            session: 数据库会话（可选）
            skip_duplicates: 是否跳过重复记录
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult()
        result.total_records = len(records)
        
        if not records:
            result.finish()
            return result
        
        self.logger.info(f"开始批量加载 {len(records)} 条记录")
        
        def _batch_load_operation(session: Session):
            processed_records = []
            unique_keys_seen = set()
            
            # 预处理和验证
            for i, record in enumerate(records):
                try:
                    processed_record = self._preprocess_record(record)
                    
                    if not self._validate_record(processed_record):
                        result.add_error("Record validation failed", processed_record)
                        continue
                    
                    unique_key = self._get_unique_key(processed_record)
                    
                    if skip_duplicates and unique_key in unique_keys_seen:
                        result.skipped_records += 1
                        result.add_warning("Duplicate record in batch", processed_record)
                        continue
                    
                    unique_keys_seen.add(unique_key)
                    processed_records.append(processed_record)
                    
                except Exception as e:
                    result.add_error(f"Error preprocessing record {i}: {e}", record)
            
            # 批量处理
            batch_count = 0
            for i in range(0, len(processed_records), self.batch_size):
                batch = processed_records[i:i + self.batch_size]
                batch_result = self._process_batch(session, batch)
                
                result.inserted_records += batch_result['inserted']
                result.updated_records += batch_result['updated']
                result.skipped_records += batch_result['skipped']
                
                batch_count += 1
                
                if batch_count % 10 == 0:  # 每10个批次记录一次
                    self.logger.info(f"已处理 {i + len(batch)} / {len(processed_records)} 条记录")
        
        try:
            if session:
                _batch_load_operation(session)
            else:
                with self.session_manager.batch_transaction(self.batch_size) as session:
                    _batch_load_operation(session)
            
            self.logger.info(f"批量加载完成: 插入 {result.inserted_records}, 更新 {result.updated_records}, 跳过 {result.skipped_records}, 失败 {result.failed_records}")
            
        except Exception as e:
            result.add_error(f"Batch load operation failed: {e}")
            self.logger.exception(f"Error in batch load operation: {e}")
        
        result.finish()
        return result
    
    def _process_batch(self, session: Session, batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        处理单个批次
        
        Args:
            session: 数据库会话
            batch: 批次记录
            
        Returns:
            Dict[str, int]: 处理结果统计
        """
        stats = {'inserted': 0, 'updated': 0, 'skipped': 0}
        
        for record in batch:
            try:
                existing = self._find_existing_record(session, record)
                
                if existing:
                    if self._should_update_existing(existing, record):
                        updated = self._update_existing_record(existing, record)
                        updated = self._postprocess_record(updated, record)
                        stats['updated'] += 1
                    else:
                        stats['skipped'] += 1
                else:
                    instance = self._create_model_instance(record)
                    instance = self._postprocess_record(instance, record)
                    session.add(instance)
                    stats['inserted'] += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing record in batch: {e}")
                continue
        
        session.flush()
        return stats
    
    def load_records(self, records: Union[Dict[str, Any], List[Dict[str, Any]]], 
                    **kwargs) -> LoadResult:
        """
        统一的记录加载接口
        
        Args:
            records: 单条记录或记录列表
            **kwargs: 额外参数
            
        Returns:
            LoadResult: 加载结果
        """
        if isinstance(records, dict):
            return self.load_single_record(records, **kwargs)
        elif isinstance(records, list):
            return self.load_batch_records(records, **kwargs)
        else:
            raise LoaderError(f"Unsupported records type: {type(records)}")
    
    def get_loader_stats(self) -> Dict[str, Any]:
        """获取加载器统计信息"""
        return {
            'loader_class': self.__class__.__name__,
            'batch_size': self.batch_size,
            'session_manager_stats': self.session_manager.get_session_stats()
        }


class BulkLoader(BaseLoader):
    """
    批量加载器 - 专门用于大量数据的高效加载
    """
    
    def __init__(self, session_manager: Optional[SessionManager] = None, 
                 batch_size: int = 5000):
        super().__init__(session_manager, batch_size)
    
    def bulk_insert_only(self, records: List[Dict[str, Any]]) -> LoadResult:
        """
        纯批量插入（不检查重复，适用于全新数据）
        
        Args:
            records: 记录列表
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult()
        result.total_records = len(records)
        
        if not records:
            result.finish()
            return result
        
        try:
            with self.session_manager.batch_transaction(self.batch_size) as session:
                instances = []
                
                for record in records:
                    try:
                        processed_record = self._preprocess_record(record)
                        if self._validate_record(processed_record):
                            instance = self._create_model_instance(processed_record)
                            instance = self._postprocess_record(instance, processed_record)
                            instances.append(instance)
                        else:
                            result.add_error("Validation failed", processed_record)
                    except Exception as e:
                        result.add_error(f"Error creating instance: {e}", record)
                
                # 批量插入
                inserted_count = self.session_manager.bulk_insert(session, instances, self.batch_size)
                result.inserted_records = inserted_count
                
                self.logger.info(f"批量插入完成: {inserted_count} 条记录")
        
        except Exception as e:
            result.add_error(f"Bulk insert failed: {e}")
            self.logger.exception(f"Error in bulk insert: {e}")
        
        result.finish()
        return result 