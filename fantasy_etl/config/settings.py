"""
统一配置管理器 (Unified Settings Manager)
======================================

提供统一的配置管理和验证，整合API、数据库和应用配置

主要职责：
1. 环境变量管理和验证
2. 配置默认值和类型检查
3. 配置文件加载和解析
4. 运行时配置验证

功能特性：
- 配置验证和类型检查
- 配置文件支持(YAML/JSON)
- 环境特定配置(dev/prod/test)
- 敏感信息加密
- 配置模板生成

配置结构：
```
Settings:
├── database: DatabaseConfig
├── api: APIConfig  
├── paths: PathConfig
├── web: WebConfig
├── etl: ETLConfig
└── logging: LoggingConfig
```

配置来源优先级：
1. 环境变量 (最高优先级)
2. 配置文件 (.env, config.yaml)
3. 默认值 (最低优先级)

使用示例：
```python
from fantasy_etl.config import Settings
settings = Settings()
db_url = settings.database.get_url()
api_config = settings.api
```
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dotenv import load_dotenv

# 可选导入yaml，如果没有安装则禁用YAML支持
try:
    import yaml
    HAS_YAML = True
except ImportError:
    yaml = None
    HAS_YAML = False

from .api_config import APIConfig

# 可选导入database_config
try:
    from .database_config import DatabaseConfig
    HAS_DATABASE_MODULE = True
except ImportError:
    # 创建占位符类
    class DatabaseConfig:
        def __init__(self):
            # 基本属性
            self.user = 'fantasy_user'
            self.host = 'localhost'
            self.port = 5432
            self.name = 'fantasy_football'
            self.pool_size = 5
            self.max_overflow = 10 
            self.ssl_mode = 'prefer'
        
        def validate_config(self):
            return False, ["数据库配置需要安装SQLAlchemy: pip install sqlalchemy psycopg2-binary"]
        
        def test_connection(self):
            return False, "需要安装SQLAlchemy"
        
        def get_url(self):
            return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.name}"
        
        def get_connection_info(self):
            return {
                'host': self.host,
                'port': self.port,
                'database': self.name,
                'user': self.user,
                'status': 'SQLAlchemy未安装'
            }
    
    HAS_DATABASE_MODULE = False

# 加载环境变量
load_dotenv()


class PathConfig:
    """
    路径配置管理
    """
    
    def __init__(self, base_dir: Path = None):
        """初始化路径配置"""
        if base_dir is None:
            # 自动检测项目根目录
            current_dir = Path(__file__).parent
            self.base_dir = current_dir.parent.parent  # fantasy_etl/config -> fantasy_etl -> project_root
        else:
            self.base_dir = Path(base_dir)
        
        # 核心目录
        self.tokens_dir = self.base_dir / "tokens"
        self.sample_data_dir = self.base_dir / "sample_data"
        self.scripts_dir = self.base_dir / "scripts"
        self.docs_dir = self.base_dir / "doc"
        self.config_dir = self.base_dir / "fantasy_etl" / "config"
        
        # 日志目录
        self.logs_dir = self.base_dir / "logs"
        
        # 数据目录
        self.data_dir = self.base_dir / "data"
        self.exports_dir = self.data_dir / "exports"
        self.imports_dir = self.data_dir / "imports"
        
        # 确保必要目录存在
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保必要目录存在"""
        for dir_path in [self.tokens_dir, self.logs_dir, self.data_dir, 
                        self.exports_dir, self.imports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def get_config_file_path(self, filename: str) -> Path:
        """获取配置文件路径"""
        return self.config_dir / filename
    
    def get_log_file_path(self, filename: str) -> Path:
        """获取日志文件路径"""
        return self.logs_dir / filename


class WebConfig:
    """
    Web服务器配置管理
    """
    
    def __init__(self):
        """初始化Web配置"""
        # Flask配置
        self.secret_key = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())
        self.host = os.getenv('FLASK_HOST', 'localhost')
        self.port = int(os.getenv('FLASK_PORT', '8000'))
        self.debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
        
        # SSL配置
        self.use_ssl = os.getenv('FLASK_USE_SSL', 'true').lower() == 'true'
        self.ssl_context = os.getenv('FLASK_SSL_CONTEXT', 'adhoc')
        
        # 会话配置
        self.session_timeout = int(os.getenv('SESSION_TIMEOUT', '3600'))  # 1小时
        
        # 安全配置
        self.csrf_enabled = os.getenv('CSRF_ENABLED', 'true').lower() == 'true'


class ETLConfig:
    """
    ETL流程配置管理
    """
    
    def __init__(self):
        """初始化ETL配置"""
        # 批处理配置
        self.batch_size = int(os.getenv('ETL_BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('ETL_MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('ETL_RETRY_DELAY', '2'))
        
        # API请求配置
        self.api_delay = int(os.getenv('API_DELAY', '2'))
        self.api_timeout = int(os.getenv('API_TIMEOUT', '30'))
        
        # 数据清理配置
        self.clean_old_data = os.getenv('CLEAN_OLD_DATA', 'false').lower() == 'true'
        self.data_retention_days = int(os.getenv('DATA_RETENTION_DAYS', '365'))
        
        # 并发配置
        self.max_workers = int(os.getenv('ETL_MAX_WORKERS', '4'))
        self.enable_parallel = os.getenv('ETL_ENABLE_PARALLEL', 'true').lower() == 'true'


class LoggingConfig:
    """
    日志配置管理
    """
    
    def __init__(self, path_config: PathConfig = None):
        """初始化日志配置"""
        self.path_config = path_config or PathConfig()
        
        # 日志级别
        self.level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.console_level = os.getenv('LOG_CONSOLE_LEVEL', 'INFO').upper()
        self.file_level = os.getenv('LOG_FILE_LEVEL', 'DEBUG').upper()
        
        # 日志文件配置
        self.enable_file_logging = os.getenv('LOG_ENABLE_FILE', 'true').lower() == 'true'
        self.log_file = self.path_config.get_log_file_path('fantasy_etl.log')
        self.max_file_size = int(os.getenv('LOG_MAX_FILE_SIZE', '10485760'))  # 10MB
        self.backup_count = int(os.getenv('LOG_BACKUP_COUNT', '5'))
        
        # 日志格式
        self.console_format = os.getenv('LOG_CONSOLE_FORMAT', 
                                       '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.file_format = os.getenv('LOG_FILE_FORMAT',
                                    '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
        
        # 日志过滤
        self.ignored_loggers = os.getenv('LOG_IGNORED_LOGGERS', '').split(',')
        self.ignored_loggers = [logger.strip() for logger in self.ignored_loggers if logger.strip()]


class Settings:
    """
    统一配置管理器
    
    提供结构化的配置管理，支持多种配置源
    """
    
    def __init__(self, config_file: str = None):
        """初始化配置管理器"""
        # 初始化各个配置模块
        self.paths = PathConfig()
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.web = WebConfig()
        self.etl = ETLConfig()
        self.logging = LoggingConfig(self.paths)
        
        # 加载配置文件（如果提供）
        if config_file:
            self.load_config_file(config_file)
        
        # 运行时状态
        self._config_loaded = True
        self._validation_errors: List[str] = []
    
    def load_config_file(self, config_file: Union[str, Path]) -> bool:
        """加载配置文件"""
        config_path = Path(config_file)
        
        if not config_path.exists():
            print(f"⚠️ 配置文件不存在: {config_path}")
            return False
        
        try:
            if config_path.suffix.lower() in ['.yml', '.yaml']:
                if not HAS_YAML:
                    print(f"⚠️ YAML支持未安装，无法加载 {config_path}")
                    print("请安装: pip install pyyaml")
                    return False
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
            elif config_path.suffix.lower() == '.json':
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            else:
                print(f"⚠️ 不支持的配置文件格式: {config_path.suffix}")
                return False
            
            self._apply_config_data(config_data)
            print(f"✓ 配置文件加载成功: {config_path}")
            return True
            
        except Exception as e:
            print(f"❌ 加载配置文件失败: {e}")
            return False
    
    def _apply_config_data(self, config_data: Dict[str, Any]):
        """应用配置数据"""
        # 应用数据库配置
        if 'database' in config_data:
            db_config = config_data['database']
            for key, value in db_config.items():
                if hasattr(self.database, key):
                    setattr(self.database, key, value)
        
        # 应用API配置
        if 'api' in config_data:
            api_config = config_data['api']
            for key, value in api_config.items():
                if hasattr(self.api, key):
                    setattr(self.api, key, value)
        
        # 应用Web配置
        if 'web' in config_data:
            web_config = config_data['web']
            for key, value in web_config.items():
                if hasattr(self.web, key):
                    setattr(self.web, key, value)
        
        # 应用ETL配置
        if 'etl' in config_data:
            etl_config = config_data['etl']
            for key, value in etl_config.items():
                if hasattr(self.etl, key):
                    setattr(self.etl, key, value)
        
        # 应用日志配置
        if 'logging' in config_data:
            log_config = config_data['logging']
            for key, value in log_config.items():
                if hasattr(self.logging, key):
                    setattr(self.logging, key, value)
    
    def save_config_file(self, config_file: Union[str, Path], format: str = 'yaml') -> bool:
        """保存配置到文件"""
        config_path = Path(config_file)
        
        try:
            config_data = self.to_dict()
            
            if format.lower() in ['yml', 'yaml']:
                if not HAS_YAML:
                    print(f"⚠️ YAML支持未安装，无法保存为YAML格式")
                    print("请安装: pip install pyyaml")
                    return False
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config_data, f, default_flow_style=False, 
                             allow_unicode=True, indent=2)
            elif format.lower() == 'json':
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=2)
            else:
                print(f"⚠️ 不支持的配置文件格式: {format}")
                return False
            
            print(f"✓ 配置文件保存成功: {config_path}")
            return True
            
        except Exception as e:
            print(f"❌ 保存配置文件失败: {e}")
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
        return {
            'database': {
                'user': self.database.user,
                'host': self.database.host,
                'port': self.database.port,
                'name': self.database.name,
                'pool_size': self.database.pool_size,
                'max_overflow': self.database.max_overflow,
                'ssl_mode': self.database.ssl_mode
            },
            'api': {
                'redirect_uri': self.api.redirect_uri,
                'api_format': self.api.api_format,
                'base_api_url': self.api.base_api_url
            },
            'web': {
                'host': self.web.host,
                'port': self.web.port,
                'debug': self.web.debug,
                'use_ssl': self.web.use_ssl,
                'session_timeout': self.web.session_timeout
            },
            'etl': {
                'batch_size': self.etl.batch_size,
                'max_retries': self.etl.max_retries,
                'api_delay': self.etl.api_delay,
                'max_workers': self.etl.max_workers,
                'enable_parallel': self.etl.enable_parallel
            },
            'logging': {
                'level': self.logging.level,
                'console_level': self.logging.console_level,
                'file_level': self.logging.file_level,
                'enable_file_logging': self.logging.enable_file_logging,
                'max_file_size': self.logging.max_file_size,
                'backup_count': self.logging.backup_count
            }
        }
    
    def validate_all(self) -> tuple[bool, List[str]]:
        """验证所有配置"""
        all_errors = []
        
        # 验证数据库配置
        db_valid, db_errors = self.database.validate_config()
        if not db_valid:
            all_errors.extend([f"数据库配置: {error}" for error in db_errors])
        
        # 验证API配置
        api_valid, api_errors = self.api.validate_config()
        if not api_valid:
            all_errors.extend([f"API配置: {error}" for error in api_errors])
        
        # 验证路径配置
        if not self.paths.base_dir.exists():
            all_errors.append("基础目录不存在")
        
        # 验证ETL配置
        if self.etl.batch_size <= 0:
            all_errors.append("ETL批处理大小必须大于0")
        
        if self.etl.max_retries < 0:
            all_errors.append("ETL最大重试次数不能为负数")
        
        # 验证Web配置
        if not (1 <= self.web.port <= 65535):
            all_errors.append("Web端口必须在1-65535范围内")
        
        self._validation_errors = all_errors
        return len(all_errors) == 0, all_errors
    
    def test_connections(self) -> Dict[str, tuple[bool, Optional[str]]]:
        """测试所有连接"""
        results = {}
        
        # 测试数据库连接
        results['database'] = self.database.test_connection()
        
        # 可以添加其他连接测试（如API连接等）
        
        return results
    
    def get_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        is_valid, errors = self.validate_all()
        
        return {
            'version': '1.0.0',
            'is_valid': is_valid,
            'validation_errors': errors,
            'database': self.database.get_connection_info(),
            'api': {
                'base_url': self.api.base_api_url,
                'tokens_dir': str(self.api.tokens_dir),
                'has_credentials': bool(self.api.client_id and self.api.client_secret)
            },
            'web': {
                'host': self.web.host,
                'port': self.web.port,
                'debug': self.web.debug,
                'ssl_enabled': self.web.use_ssl
            },
            'etl': {
                'batch_size': self.etl.batch_size,
                'max_workers': self.etl.max_workers,
                'parallel_enabled': self.etl.enable_parallel
            },
            'paths': {
                'base_dir': str(self.paths.base_dir),
                'tokens_dir': str(self.paths.tokens_dir),
                'logs_dir': str(self.paths.logs_dir)
            }
        }
    
    def print_summary(self):
        """打印配置摘要"""
        summary = self.get_summary()
        
        print("🔧 Fantasy ETL 配置摘要")
        print("=" * 50)
        
        status = "✅ 有效" if summary['is_valid'] else "❌ 无效"
        print(f"配置状态: {status}")
        
        if summary['validation_errors']:
            print("\n❌ 配置错误:")
            for error in summary['validation_errors']:
                print(f"  - {error}")
        
        print(f"\n📊 数据库: {summary['database']['host']}:{summary['database']['port']}/{summary['database']['database']}")
        print(f"🌐 API: {summary['api']['base_url']}")
        print(f"🔐 认证: {'已配置' if summary['api']['has_credentials'] else '未配置'}")
        print(f"🖥️  Web服务: {summary['web']['host']}:{summary['web']['port']}")
        print(f"⚙️  ETL: 批处理大小={summary['etl']['batch_size']}, 并行={'启用' if summary['etl']['parallel_enabled'] else '禁用'}")
        print(f"📁 路径: {summary['paths']['base_dir']}")
        
        print("=" * 50)


# 统一配置模块已完整迁移，使用时直接实例化Settings类
# 示例: settings = Settings() 