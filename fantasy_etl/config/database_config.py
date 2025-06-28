"""
数据库配置管理器 (Database Configuration Manager)
=============================================

PostgreSQL数据库连接和配置管理

主要职责：
1. 数据库连接配置管理
2. SQLAlchemy引擎和会话管理
3. 数据库URL构建和验证
4. 连接池和性能配置

功能特性：
- 连接池配置和优化
- 数据库健康检查
- 连接重试机制
- 多数据库支持
- SSL和安全配置

数据库操作支持：
- 表创建和删除
- 模式迁移
- 数据备份和恢复
- 性能监控
- 连接状态检查

环境变量配置：
- DB_USER: 数据库用户名 (默认: 'fantasy_user')
- DB_PASSWORD: 数据库密码 (默认: 'fantasyPassword')
- DB_HOST: 数据库主机 (默认: 'localhost')
- DB_PORT: 数据库端口 (默认: '5432')
- DB_NAME: 数据库名称 (默认: 'fantasy_db')

使用示例：
```python
from fantasy_etl.config import DatabaseConfig
db_config = DatabaseConfig()
engine = db_config.create_engine()
session = db_config.get_session()
```

连接配置：
- URL格式: postgresql://user:password@host:port/database
- 连接池: 默认配置优化
- 超时设置: 可配置的连接和查询超时
- SSL模式: 支持多种SSL配置
"""

import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# 加载环境变量
load_dotenv()


class DatabaseConfig:
    """
    数据库配置管理器
    
    管理PostgreSQL数据库的连接配置和会话
    """
    
    def __init__(self):
        """初始化数据库配置"""
        # 数据库连接参数
        self.user = os.getenv('DB_USER', 'fantasy_user')
        self.password = os.getenv('DB_PASSWORD', 'fantasyPassword')
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.name = os.getenv('DB_NAME', 'fantasy_db')
        
        # 连接池配置
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))
        self.pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', '30'))
        self.pool_recycle = int(os.getenv('DB_POOL_RECYCLE', '3600'))
        
        # 连接选项
        self.echo = os.getenv('DB_ECHO', 'false').lower() == 'true'
        self.echo_pool = os.getenv('DB_ECHO_POOL', 'false').lower() == 'true'
        
        # SSL配置
        self.ssl_mode = os.getenv('DB_SSL_MODE', 'prefer')
        
        self._engine: Optional[Engine] = None
        self._session_maker: Optional[sessionmaker] = None
    
    def get_url(self) -> str:
        """获取数据库连接URL"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
    
    def get_engine_config(self) -> Dict[str, Any]:
        """获取SQLAlchemy引擎配置"""
        config = {
            'echo': self.echo,
            'echo_pool': self.echo_pool,
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': True  # 启用连接预检查
        }
        
        # 添加SSL配置（如果需要）
        if self.ssl_mode != 'disable':
            config['connect_args'] = {'sslmode': self.ssl_mode}
        
        return config
    
    def create_engine(self) -> Engine:
        """创建数据库引擎"""
        if self._engine is None:
            database_url = self.get_url()
            engine_config = self.get_engine_config()
            self._engine = create_engine(database_url, **engine_config)
        
        return self._engine
    
    def get_session_maker(self) -> sessionmaker:
        """获取会话制造器"""
        if self._session_maker is None:
            engine = self.create_engine()
            self._session_maker = sessionmaker(bind=engine)
        
        return self._session_maker
    
    def get_session(self):
        """获取数据库会话"""
        session_maker = self.get_session_maker()
        return session_maker()
    
    def create_tables(self, metadata):
        """创建所有表"""
        engine = self.create_engine()
        metadata.create_all(engine)
    
    def drop_tables(self, metadata):
        """删除所有表"""
        engine = self.create_engine()
        metadata.drop_all(engine)
    
    def recreate_tables(self, metadata) -> bool:
        """重新创建所有表（先删除再创建）"""
        print("🔄 重新创建数据库表...")
        
        try:
            engine = self.create_engine()
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # 首先查询数据库中的所有表
                    result = conn.execute(text("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public' 
                        ORDER BY tablename;
                    """))
                    existing_tables = [row[0] for row in result.fetchall()]
                    
                    if existing_tables:
                        print(f"发现 {len(existing_tables)} 个现有表")
                        
                        # 删除所有现有表，使用CASCADE处理依赖
                        for table_name in existing_tables:
                            try:
                                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                                print(f"✓ 删除表 {table_name}")
                            except Exception as e:
                                print(f"删除表 {table_name} 时出错: {e}")
                                
                        # 确保删除可能遗留的旧表
                        legacy_tables = ['rosters', 'roster_history', 'player_stats_history']
                        for table_name in legacy_tables:
                            try:
                                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                                print(f"✓ 删除遗留表 {table_name}")
                            except Exception as e:
                                print(f"删除遗留表 {table_name} 时出错（可能不存在）: {e}")
                    else:
                        print("✓ 数据库中没有现有表")
                    
                    trans.commit()
                    print("✓ 所有表删除完成")
                    
                except Exception as e:
                    trans.rollback()
                    raise e
        
        except Exception as e:
            print(f"删除表时出错: {e}")
            print("尝试使用SQLAlchemy标准方法...")
            try:
                # 如果CASCADE删除失败，尝试标准删除
                self.drop_tables(metadata)
                print("✓ 使用标准方法删除表成功")
            except Exception as e2:
                print(f"标准删除也失败: {e2}")
                print("⚠️ 无法自动删除表，请手动执行以下SQL:")
                print("DROP SCHEMA public CASCADE;")
                print("CREATE SCHEMA public;")
                print("然后重新运行程序")
                return False
        
        # 重新创建所有表
        try:
            self.create_tables(metadata)
            print("✅ 数据库表重新创建完成")
            return True
        except Exception as e:
            print(f"创建表失败: {e}")
            return False
    
    def test_connection(self) -> tuple[bool, Optional[str]]:
        """测试数据库连接"""
        try:
            engine = self.create_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, None
        except SQLAlchemyError as e:
            return False, str(e)
        except Exception as e:
            return False, f"未知错误: {str(e)}"
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接信息（隐藏敏感信息）"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.name,
            'user': self.user,
            'ssl_mode': self.ssl_mode,
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow
        }
    
    def validate_config(self) -> tuple[bool, list]:
        """验证数据库配置"""
        errors = []
        
        if not self.user:
            errors.append("数据库用户名未设置")
        
        if not self.password:
            errors.append("数据库密码未设置")
        
        if not self.host:
            errors.append("数据库主机未设置")
        
        if not self.name:
            errors.append("数据库名称未设置")
        
        try:
            int(self.port)
        except ValueError:
            errors.append("数据库端口必须是数字")
        
        return len(errors) == 0, errors
    
    def close(self):
        """关闭数据库连接"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_maker = None


class ConnectionManager:
    """
    数据库连接管理器
    """
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.db_config = db_config or DatabaseConfig()
        self._connections = {}
    
    def get_connection(self, name: str = 'default'):
        """获取指定名称的连接"""
        if name not in self._connections:
            self._connections[name] = self.db_config.create_engine()
        return self._connections[name]
    
    def close_all(self):
        """关闭所有连接"""
        for conn in self._connections.values():
            conn.dispose()
        self._connections.clear()


class DatabaseMigrator:
    """
    数据库迁移管理器
    """
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.db_config = db_config or DatabaseConfig()
    
    def backup_database(self, backup_file: str) -> bool:
        """备份数据库（需要pg_dump）"""
        import subprocess
        
        try:
            cmd = [
                'pg_dump',
                '-h', self.db_config.host,
                '-p', self.db_config.port,
                '-U', self.db_config.user,
                '-d', self.db_config.name,
                '-f', backup_file
            ]
            
            subprocess.run(cmd, check=True, env={'PGPASSWORD': self.db_config.password})
            return True
        except subprocess.CalledProcessError as e:
            print(f"备份失败: {e}")
            return False
        except FileNotFoundError:
            print("pg_dump 未找到，请确保已安装PostgreSQL客户端工具")
            return False
    
    def restore_database(self, backup_file: str) -> bool:
        """恢复数据库（需要psql）"""
        import subprocess
        
        try:
            cmd = [
                'psql',
                '-h', self.db_config.host,
                '-p', self.db_config.port,
                '-U', self.db_config.user,
                '-d', self.db_config.name,
                '-f', backup_file
            ]
            
            subprocess.run(cmd, check=True, env={'PGPASSWORD': self.db_config.password})
            return True
        except subprocess.CalledProcessError as e:
            print(f"恢复失败: {e}")
            return False
        except FileNotFoundError:
            print("psql 未找到，请确保已安装PostgreSQL客户端工具")
            return False


# 数据库配置模块已完整迁移，无需兼容性代码 