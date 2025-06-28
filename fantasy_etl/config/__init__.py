"""
Fantasy ETL Configuration Module
==============================

提供统一的配置管理接口，整合了API、数据库和应用配置

主要组件：
- APIConfig: Yahoo Fantasy API配置管理
- DatabaseConfig: PostgreSQL数据库配置管理  
- Settings: 统一配置管理器

使用示例：
```python
from fantasy_etl.config import Settings, APIConfig, DatabaseConfig

# 使用统一配置
settings = Settings()
print(settings.get_summary())

# 或使用独立配置
api_config = APIConfig()
db_config = DatabaseConfig()
```
"""

# 导入配置类
from .api_config import APIConfig, OAuthConfig, EndpointManager
from .settings import Settings, PathConfig, WebConfig, ETLConfig, LoggingConfig

# 可选导入database_config，如果SQLAlchemy未安装则跳过
try:
    from .database_config import DatabaseConfig, ConnectionManager, DatabaseMigrator
    HAS_DATABASE_CONFIG = True
except ImportError:
    # 创建占位符类
    class DatabaseConfig:
        def __init__(self):
            raise ImportError("数据库配置需要安装SQLAlchemy: pip install sqlalchemy psycopg2-binary")
    
    ConnectionManager = DatabaseMigrator = DatabaseConfig
    HAS_DATABASE_CONFIG = False

__all__ = [
    'Settings',
    'APIConfig',
    'DatabaseConfig',
    'OAuthConfig',
    'EndpointManager',
    'PathConfig',
    'WebConfig', 
    'ETLConfig',
    'LoggingConfig',
    'ConnectionManager',
    'DatabaseMigrator',
    'HAS_DATABASE_CONFIG'
] 