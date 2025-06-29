"""
数据库连接管理器 (Database Connection Manager)
==============================================

【主要职责】
1. 数据库连接创建和管理
2. 连接池配置
3. 连接健康检查
4. 连接重试机制

【功能特性】
- 单例模式连接管理
- 自动重连机制
- 连接池优化
- 错误处理和日志记录
- 配置灵活性

【使用示例】
```python
from fantasy_etl.load.database.connection_manager import ConnectionManager

# 获取连接管理器实例
conn_mgr = ConnectionManager()

# 测试连接
if conn_mgr.test_connection():
    print("数据库连接成功")

# 获取引擎
engine = conn_mgr.get_engine()

# 获取数据库URL
url = conn_mgr.get_database_url()
```
"""

import os
import time
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import logging

# 加载环境变量
load_dotenv()

# 配置日志
logger = logging.getLogger(__name__)


class ConnectionManager:
    """数据库连接管理器 - 单例模式"""
    
    _instance = None
    _engine = None
    
    def __new__(cls) -> 'ConnectionManager':
        """确保单例模式"""
        if cls._instance is None:
            cls._instance = super(ConnectionManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化连接管理器"""
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._connection_config = self._load_config()
            self._max_retries = 3
            self._retry_delay = 1.0
            self._engine = None
    
    def _load_config(self) -> Dict[str, Any]:
        """加载数据库配置"""
        return {
            'db_user': os.getenv('DB_USER', 'fantasy_user'),
            'db_password': os.getenv('DB_PASSWORD', 'fantasyPassword'),
            'db_host': os.getenv('DB_HOST', 'localhost'),
            'db_port': os.getenv('DB_PORT', '5432'),
            'db_name': os.getenv('DB_NAME', 'fantasy_db'),
            'pool_size': int(os.getenv('DB_POOL_SIZE', '10')),
            'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', '20')),
            'pool_timeout': int(os.getenv('DB_POOL_TIMEOUT', '30')),
            'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', '3600')),  # 1 hour
            'echo': os.getenv('DB_ECHO', 'false').lower() == 'true'
        }
    
    def get_database_url(self) -> str:
        """获取数据库连接URL"""
        config = self._connection_config
        return (f"postgresql://{config['db_user']}:{config['db_password']}"
                f"@{config['db_host']}:{config['db_port']}/{config['db_name']}")
    
    def create_engine(self) -> Engine:
        """创建数据库引擎"""
        config = self._connection_config
        database_url = self.get_database_url()
        
        # 连接池配置
        engine_kwargs = {
            'poolclass': QueuePool,
            'pool_size': config['pool_size'],
            'max_overflow': config['max_overflow'],
            'pool_timeout': config['pool_timeout'],
            'pool_recycle': config['pool_recycle'],
            'pool_pre_ping': True,  # 连接前预检查
            'echo': config['echo'],
            'connect_args': {
                'options': '-c timezone=UTC',  # 设置时区
                'connect_timeout': 10  # 连接超时
            }
        }
        
        try:
            engine = create_engine(database_url, **engine_kwargs)
            logger.info(f"数据库引擎创建成功: {config['db_host']}:{config['db_port']}/{config['db_name']}")
            return engine
        except Exception as e:
            logger.error(f"数据库引擎创建失败: {e}")
            raise
    
    def get_engine(self) -> Engine:
        """获取数据库引擎（单例）"""
        if self._engine is None:
            self._engine = self.create_engine()
        return self._engine
    
    def test_connection(self, max_retries: Optional[int] = None) -> bool:
        """
        测试数据库连接
        
        Args:
            max_retries: 最大重试次数，默认使用配置值
            
        Returns:
            bool: 连接是否成功
        """
        retries = max_retries or self._max_retries
        
        for attempt in range(retries):
            try:
                engine = self.get_engine()
                with engine.connect() as conn:
                    # 执行简单查询测试连接
                    result = conn.execute(text("SELECT 1"))
                    result.fetchone()
                    logger.info("数据库连接测试成功")
                    return True
                    
            except OperationalError as e:
                logger.warning(f"数据库连接失败 (尝试 {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(self._retry_delay * (attempt + 1))  # 指数退避
                    continue
                else:
                    logger.error(f"数据库连接失败，已达到最大重试次数: {e}")
                    return False
                    
            except Exception as e:
                logger.error(f"数据库连接测试时发生未知错误: {e}")
                return False
        
        return False
    
    def check_database_exists(self) -> bool:
        """检查数据库是否存在"""
        try:
            # 创建连接到默认数据库的临时引擎
            config = self._connection_config
            temp_url = (f"postgresql://{config['db_user']}:{config['db_password']}"
                       f"@{config['db_host']}:{config['db_port']}/postgres")
            
            temp_engine = create_engine(temp_url)
            
            with temp_engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT 1 FROM pg_database WHERE datname = :db_name"
                ), {"db_name": config['db_name']})
                
                exists = result.fetchone() is not None
                temp_engine.dispose()
                return exists
                
        except Exception as e:
            logger.error(f"检查数据库存在性时出错: {e}")
            return False
    
    def create_database_if_not_exists(self) -> bool:
        """如果数据库不存在则创建"""
        if self.check_database_exists():
            logger.info(f"数据库 {self._connection_config['db_name']} 已存在")
            return True
        
        try:
            config = self._connection_config
            temp_url = (f"postgresql://{config['db_user']}:{config['db_password']}"
                       f"@{config['db_host']}:{config['db_port']}/postgres")
            
            temp_engine = create_engine(temp_url, isolation_level='AUTOCOMMIT')
            
            with temp_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE {config['db_name']}"))
                logger.info(f"数据库 {config['db_name']} 创建成功")
                
            temp_engine.dispose()
            return True
            
        except Exception as e:
            logger.error(f"创建数据库时出错: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接信息（隐藏敏感信息）"""
        config = self._connection_config
        return {
            'host': config['db_host'],
            'port': config['db_port'],
            'database': config['db_name'],
            'user': config['db_user'],
            'pool_size': config['pool_size'],
            'max_overflow': config['max_overflow'],
            'pool_timeout': config['pool_timeout'],
            'pool_recycle': config['pool_recycle']
        }
    
    def close_all_connections(self) -> None:
        """关闭所有数据库连接"""
        if self._engine:
            try:
                self._engine.dispose()
                logger.info("所有数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭数据库连接时出错: {e}")
            finally:
                self._engine = None
    
    def reset_connection(self) -> bool:
        """重置数据库连接"""
        try:
            self.close_all_connections()
            self._engine = None
            return self.test_connection()
        except Exception as e:
            logger.error(f"重置数据库连接时出错: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                # 获取连接池状态
                pool = engine.pool
                pool_stats = {
                    'pool_size': pool.size(),
                    'checked_in': pool.checkedin(),
                    'checked_out': pool.checkedout(),
                    'overflow': pool.overflow(),
                    'invalid': pool.invalid()
                }
                
                # 获取数据库基本信息
                result = conn.execute(text("""
                    SELECT 
                        current_database() as database_name,
                        current_user as current_user,
                        version() as version,
                        inet_server_addr() as server_address,
                        inet_server_port() as server_port
                """))
                
                db_info = dict(result.fetchone()._mapping)
                
                return {
                    'pool_stats': pool_stats,
                    'database_info': db_info,
                    'connection_config': self.get_connection_info()
                }
                
        except Exception as e:
            logger.error(f"获取数据库统计信息时出错: {e}")
            return {'error': str(e)}


class ConnectionError(Exception):
    """数据库连接错误"""
    pass


class DatabaseNotFoundError(ConnectionError):
    """数据库不存在错误"""
    pass


class InvalidConfigurationError(ConnectionError):
    """无效配置错误"""
    pass


# 便利函数
def get_default_connection_manager() -> ConnectionManager:
    """获取默认连接管理器实例"""
    return ConnectionManager()


def test_database_connection() -> bool:
    """快速测试数据库连接"""
    conn_mgr = get_default_connection_manager()
    return conn_mgr.test_connection()


def get_database_engine() -> Engine:
    """快速获取数据库引擎"""
    conn_mgr = get_default_connection_manager()
    return conn_mgr.get_engine() 