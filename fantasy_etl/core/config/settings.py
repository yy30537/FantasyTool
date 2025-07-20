"""
配置管理模块
统一管理所有配置项
"""
import os
from typing import Optional
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

@dataclass
class DatabaseConfig:
    """数据库配置"""
    user: str
    password: str
    host: str
    port: str
    name: str
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

@dataclass
class YahooAPIConfig:
    """Yahoo API配置"""
    client_id: str
    client_secret: str
    redirect_uri: str
    token_file: Path
    
    @property
    def is_valid(self) -> bool:
        return bool(self.client_id and self.client_secret)

@dataclass
class AppConfig:
    """应用配置"""
    debug: bool
    batch_size: int
    api_delay: float
    max_retries: int
    web_server_host: str
    web_server_port: int

class Settings:
    """全局设置管理器"""
    
    def __init__(self):
        self._load_settings()
    
    def _load_settings(self):
        """加载所有配置"""
        # 项目根目录
        self.project_root = Path(__file__).parent.parent.parent.parent
        
        # 数据库配置
        self.database = DatabaseConfig(
            user=os.getenv('DB_USER', 'fantasy_user'),
            password=os.getenv('DB_PASSWORD', 'fantasyPassword'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            name=os.getenv('DB_NAME', 'fantasy_db')
        )
        
        # Yahoo API配置
        tokens_dir = self.project_root / "tokens"
        tokens_dir.mkdir(exist_ok=True)
        
        self.yahoo_api = YahooAPIConfig(
            client_id=os.getenv('YAHOO_CLIENT_ID', ''),
            client_secret=os.getenv('YAHOO_CLIENT_SECRET', ''),
            redirect_uri=os.getenv('YAHOO_REDIRECT_URI', 'oob'),
            token_file=tokens_dir / "yahoo_token.token"
        )
        
        # 应用配置
        self.app = AppConfig(
            debug=os.getenv('DEBUG', 'False').lower() in ('true', '1', 'yes'),
            batch_size=int(os.getenv('BATCH_SIZE', '100')),
            api_delay=float(os.getenv('API_DELAY', '1.0')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            web_server_host=os.getenv('WEB_SERVER_HOST', 'localhost'),
            web_server_port=int(os.getenv('WEB_SERVER_PORT', '8000'))
        )
    
    def validate(self) -> list[str]:
        """验证配置，返回错误列表"""
        errors = []
        
        if not self.yahoo_api.is_valid:
            errors.append("缺少Yahoo API配置：YAHOO_CLIENT_ID 或 YAHOO_CLIENT_SECRET")
        
        if not self.database.user or not self.database.password:
            errors.append("缺少数据库配置：DB_USER 或 DB_PASSWORD")
        
        return errors
    
    def print_summary(self):
        """打印配置摘要"""
        print("📋 当前配置摘要:")
        print("=" * 50)
        print(f"🗄️  数据库: {self.database.host}:{self.database.port}/{self.database.name}")
        print(f"🔑 Yahoo API: {'✅ 已配置' if self.yahoo_api.is_valid else '❌ 未配置'}")
        print(f"🌐 Web服务器: {self.app.web_server_host}:{self.app.web_server_port}")
        print(f"⚡ API延迟: {self.app.api_delay}s")
        print(f"📦 批处理大小: {self.app.batch_size}")
        print(f"🔄 最大重试: {self.app.max_retries}")
        print(f"🐛 调试模式: {'开启' if self.app.debug else '关闭'}")
        print("=" * 50)

# 全局设置实例
settings = Settings()