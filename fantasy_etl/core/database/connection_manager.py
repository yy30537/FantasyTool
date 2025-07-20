"""
数据库连接管理器
提供统一的数据库连接和会话管理
"""
import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

from .models import Base

# 加载环境变量
load_dotenv()

class DatabaseConnectionManager:
    """数据库连接管理器"""
    
    def __init__(self):
        self.database_url = self._get_database_url()
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def _get_database_url(self) -> str:
        """获取数据库连接URL"""
        db_user = os.getenv('DB_USER', 'fantasy_user')
        db_password = os.getenv('DB_PASSWORD', 'fantasyPassword')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'fantasy_db')
        
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    def _create_engine(self):
        """创建数据库引擎"""
        return create_engine(
            self.database_url,
            echo=False,  # 关闭详细日志
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # 连接验证
            pool_recycle=3600,   # 1小时回收连接
        )
    
    def create_tables(self):
        """创建所有表"""
        Base.metadata.create_all(self.engine)
    
    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()
    
    @contextmanager
    def session_scope(self):
        """会话上下文管理器，自动处理提交和回滚"""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def close(self):
        """关闭数据库引擎"""
        if hasattr(self, 'engine'):
            self.engine.dispose()

# 全局数据库连接管理器实例
db_manager = DatabaseConnectionManager()