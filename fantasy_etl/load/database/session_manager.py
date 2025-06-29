"""
数据库会话管理器 (Database Session Manager)
==========================================

【主要职责】
1. SQLAlchemy会话创建和管理
2. 事务管理和上下文
3. 会话生命周期控制
4. 批量操作支持

【功能特性】
- 会话工厂模式
- 事务上下文管理器
- 自动提交和回滚
- 会话池和复用
- 错误处理和清理

【使用示例】
```python
from fantasy_etl.load.database.session_manager import SessionManager

# 获取会话管理器
session_mgr = SessionManager()

# 使用上下文管理器
with session_mgr.get_session() as session:
    # 数据库操作
    result = session.query(Player).all()

# 使用事务
with session_mgr.transaction() as session:
    # 自动事务管理
    session.add(new_player)
    # 自动提交或回滚
```
"""

import logging
from contextlib import contextmanager
from typing import Optional, Generator, Type, TypeVar, Any, Dict
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.engine import Engine

from .connection_manager import ConnectionManager

# 配置日志
logger = logging.getLogger(__name__)

# 泛型类型
T = TypeVar('T')


class SessionManager:
    """数据库会话管理器"""
    
    def __init__(self, connection_manager: Optional[ConnectionManager] = None):
        """
        初始化会话管理器
        
        Args:
            connection_manager: 连接管理器实例，如果为None则使用默认实例
        """
        self.connection_manager = connection_manager or ConnectionManager()
        self._session_factory = None
        self._scoped_session = None
        self._setup_session_factory()
    
    def _setup_session_factory(self) -> None:
        """设置会话工厂"""
        engine = self.connection_manager.get_engine()
        
        # 创建会话工厂
        self._session_factory = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,  # 手动控制flush时机
            expire_on_commit=False  # 避免访问已提交对象时的额外查询
        )
        
        # 创建线程安全的scoped_session
        self._scoped_session = scoped_session(self._session_factory)
        
        logger.info("会话工厂初始化完成")
    
    def get_session_factory(self) -> sessionmaker:
        """获取会话工厂"""
        if self._session_factory is None:
            self._setup_session_factory()
        return self._session_factory
    
    def get_scoped_session(self) -> scoped_session:
        """获取线程安全的scoped session"""
        if self._scoped_session is None:
            self._setup_session_factory()
        return self._scoped_session
    
    def create_session(self) -> Session:
        """创建新的数据库会话"""
        try:
            session_factory = self.get_session_factory()
            session = session_factory()
            logger.debug("新数据库会话已创建")
            return session
        except Exception as e:
            logger.error(f"创建数据库会话失败: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        获取数据库会话的上下文管理器
        
        使用示例:
        ```python
        with session_mgr.get_session() as session:
            # 数据库操作
            pass
        ```
        """
        session = None
        try:
            session = self.create_session()
            yield session
        except Exception as e:
            if session:
                session.rollback()
                logger.error(f"会话操作失败，已回滚: {e}")
            raise
        finally:
            if session:
                session.close()
                logger.debug("数据库会话已关闭")
    
    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        """
        事务上下文管理器 - 自动提交或回滚
        
        使用示例:
        ```python
        with session_mgr.transaction() as session:
            session.add(new_object)
            # 如果没有异常，自动提交
            # 如果有异常，自动回滚
        ```
        """
        session = None
        try:
            session = self.create_session()
            session.begin()
            
            yield session
            
            session.commit()
            logger.debug("事务已成功提交")
            
        except IntegrityError as e:
            if session:
                session.rollback()
                logger.warning(f"数据完整性错误，事务已回滚: {e}")
            raise
        except OperationalError as e:
            if session:
                session.rollback()
                logger.error(f"数据库操作错误，事务已回滚: {e}")
            raise
        except Exception as e:
            if session:
                session.rollback()
                logger.error(f"事务执行失败，已回滚: {e}")
            raise
        finally:
            if session:
                session.close()
                logger.debug("事务会话已关闭")
    
    @contextmanager
    def batch_transaction(self, batch_size: int = 1000) -> Generator[Session, None, None]:
        """
        批量事务上下文管理器
        
        Args:
            batch_size: 批量大小，每处理这么多条记录后flush一次
            
        使用示例:
        ```python
        with session_mgr.batch_transaction(batch_size=500) as session:
            for i, data in enumerate(large_dataset):
                session.add(create_object(data))
                if i % 500 == 0:
                    session.flush()  # 定期flush但不提交
        ```
        """
        session = None
        try:
            session = self.create_session()
            session.begin()
            
            # 设置批量处理标记
            session.info['batch_size'] = batch_size
            session.info['batch_count'] = 0
            
            yield session
            
            # 最终flush和提交
            session.flush()
            session.commit()
            logger.info(f"批量事务已成功提交，处理了 {session.info.get('batch_count', 0)} 条记录")
            
        except Exception as e:
            if session:
                session.rollback()
                logger.error(f"批量事务失败，已回滚: {e}")
            raise
        finally:
            if session:
                session.close()
                logger.debug("批量事务会话已关闭")
    
    def execute_in_transaction(self, func, *args, **kwargs) -> Any:
        """
        在事务中执行函数
        
        Args:
            func: 要执行的函数，第一个参数必须是session
            *args: 传递给函数的位置参数
            **kwargs: 传递给函数的关键字参数
            
        Returns:
            函数的返回值
        """
        with self.transaction() as session:
            return func(session, *args, **kwargs)
    
    def bulk_insert(self, session: Session, objects: list, batch_size: int = 1000) -> int:
        """
        批量插入对象
        
        Args:
            session: 数据库会话
            objects: 要插入的对象列表
            batch_size: 批量大小
            
        Returns:
            插入的记录数
        """
        if not objects:
            return 0
        
        total_inserted = 0
        
        try:
            for i in range(0, len(objects), batch_size):
                batch = objects[i:i + batch_size]
                session.bulk_save_objects(batch)
                session.flush()
                
                total_inserted += len(batch)
                logger.debug(f"已插入 {len(batch)} 条记录，总计 {total_inserted}/{len(objects)}")
            
            logger.info(f"批量插入完成，总计 {total_inserted} 条记录")
            return total_inserted
            
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            raise
    
    def bulk_update(self, session: Session, model_class: Type[T], 
                   updates: list, batch_size: int = 1000) -> int:
        """
        批量更新记录
        
        Args:
            session: 数据库会话
            model_class: 模型类
            updates: 更新数据列表，格式: [{'id': 1, 'field': 'value'}, ...]
            batch_size: 批量大小
            
        Returns:
            更新的记录数
        """
        if not updates:
            return 0
        
        total_updated = 0
        
        try:
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                session.bulk_update_mappings(model_class, batch)
                session.flush()
                
                total_updated += len(batch)
                logger.debug(f"已更新 {len(batch)} 条记录，总计 {total_updated}/{len(updates)}")
            
            logger.info(f"批量更新完成，总计 {total_updated} 条记录")
            return total_updated
            
        except Exception as e:
            logger.error(f"批量更新失败: {e}")
            raise
    
    def safe_execute(self, session: Session, operation, *args, **kwargs) -> Optional[Any]:
        """
        安全执行数据库操作，带错误处理
        
        Args:
            session: 数据库会话
            operation: 要执行的操作（函数或方法）
            *args: 传递给操作的位置参数
            **kwargs: 传递给操作的关键字参数
            
        Returns:
            操作的返回值，失败时返回None
        """
        try:
            result = operation(*args, **kwargs)
            session.flush()
            return result
        except IntegrityError as e:
            session.rollback()
            logger.warning(f"数据完整性错误: {e}")
            return None
        except OperationalError as e:
            session.rollback()
            logger.error(f"数据库操作错误: {e}")
            return None
        except Exception as e:
            session.rollback()
            logger.error(f"未知错误: {e}")
            return None
    
    def get_session_stats(self) -> Dict[str, Any]:
        """获取会话统计信息"""
        try:
            scoped_session = self.get_scoped_session()
            
            # 获取当前会话信息（如果存在）
            current_session = scoped_session.registry()
            
            stats = {
                'has_active_session': current_session is not None,
                'session_factory_configured': self._session_factory is not None,
                'scoped_session_configured': self._scoped_session is not None
            }
            
            if current_session:
                stats.update({
                    'session_is_active': current_session.is_active,
                    'session_dirty_objects': len(current_session.dirty),
                    'session_new_objects': len(current_session.new),
                    'session_deleted_objects': len(current_session.deleted),
                    'session_identity_map_size': len(current_session.identity_map)
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"获取会话统计信息时出错: {e}")
            return {'error': str(e)}
    
    def cleanup_sessions(self) -> None:
        """清理所有会话"""
        try:
            if self._scoped_session:
                self._scoped_session.remove()
                logger.info("所有scoped sessions已清理")
        except Exception as e:
            logger.error(f"清理会话时出错: {e}")
    
    def reset_session_factory(self) -> None:
        """重置会话工厂"""
        try:
            self.cleanup_sessions()
            self._session_factory = None
            self._scoped_session = None
            self._setup_session_factory()
            logger.info("会话工厂已重置")
        except Exception as e:
            logger.error(f"重置会话工厂时出错: {e}")


class SessionError(Exception):
    """会话管理错误"""
    pass


class TransactionError(SessionError):
    """事务错误"""
    pass


class BatchOperationError(SessionError):
    """批量操作错误"""
    pass


# 便利函数
def get_default_session_manager() -> SessionManager:
    """获取默认会话管理器实例"""
    return SessionManager()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """快速获取数据库会话的上下文管理器"""
    session_mgr = get_default_session_manager()
    with session_mgr.get_session() as session:
        yield session


@contextmanager
def db_transaction() -> Generator[Session, None, None]:
    """快速获取事务的上下文管理器"""
    session_mgr = get_default_session_manager()
    with session_mgr.transaction() as session:
        yield session 