"""
API配置管理器 (API Configuration Manager)
=======================================

Yahoo Fantasy API和OAuth2.0配置管理

主要职责：
1. Yahoo Fantasy API配置管理
2. OAuth2.0客户端配置  
3. API端点和URL管理
4. 请求参数和头部配置

功能特性：
- API版本管理
- 端点URL模板化
- 请求头部标准化
- 配置验证
- 多环境配置支持

API端点管理：
- 用户信息: /users;use_login=1
- 游戏列表: /users;use_login=1/games
- 联盟信息: /league/{league_key}
- 球员信息: /league/{league_key}/players
- 统计数据: /league/{league_key}/teams/stats

使用示例：
```python
from fantasy_etl.config import APIConfig
api_config = APIConfig()
headers = api_config.get_api_headers(access_token)
url = api_config.get_endpoint_url("games")
```
"""

import os
import pathlib
from typing import Dict, Optional
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class APIConfig:
    """
    API配置管理器
    
    管理Yahoo Fantasy API的所有配置和端点
    """
    
    def __init__(self):
        """初始化API配置"""
        # 基础路径配置
        self.base_dir = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        self.tokens_dir = self.base_dir / "tokens"
        
        # OAuth配置 
        self.client_id = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
        self.client_secret = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
        self.redirect_uri = os.getenv("REDIRECT_URI", "oob")
        
        # API URLs
        self.token_url = "https://api.login.yahoo.com/oauth2/get_token"
        self.auth_url = "https://api.login.yahoo.com/oauth2/request_auth"
        self.base_api_url = "https://fantasysports.yahooapis.com/fantasy/v2"
        
        # OAuth权限范围
        self.scope = ["fspt-w"]  # Fantasy Sports读写权限
        
        # API配置
        self.api_format = "json"
        self.api_version = "v2"
        
        # 确保tokens目录存在
        self._ensure_tokens_directory()
    
    def _ensure_tokens_directory(self):
        """确保tokens目录存在"""
        if not self.tokens_dir.exists():
            self.tokens_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def default_token_file(self) -> pathlib.Path:
        """获取默认令牌文件路径"""
        return self.tokens_dir / "yahoo_token.token"
    
    def get_oauth_config(self) -> Dict:
        """获取OAuth配置"""
        return {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'token_url': self.token_url,
            'auth_url': self.auth_url,
            'redirect_uri': self.redirect_uri,
            'scope': self.scope
        }
    
    def get_api_headers(self, access_token: str) -> Dict[str, str]:
        """获取API请求头部"""
        return {
            'Authorization': f"Bearer {access_token}",
            'Content-Type': 'application/json'
        }
    
    def get_endpoint_url(self, endpoint: str) -> str:
        """获取完整的API端点URL"""
        # 移除开头的斜杠（如果有）
        endpoint = endpoint.lstrip('/')
        
        # 添加格式参数
        format_param = f"?format={self.api_format}"
        if '?' in endpoint:
            format_param = f"&format={self.api_format}"
        
        return f"{self.base_api_url}/{endpoint}{format_param}"
    
    def get_user_games_url(self) -> str:
        """获取用户游戏列表URL"""
        return self.get_endpoint_url("users;use_login=1/games")
    
    def get_league_info_url(self, league_key: str) -> str:
        """获取联盟信息URL"""
        return self.get_endpoint_url(f"league/{league_key}")
    
    def get_league_url(self, league_key: str) -> str:
        """获取联盟URL（get_league_info_url的别名）"""
        return self.get_league_info_url(league_key)
    
    def get_league_settings_url(self, league_key: str) -> str:
        """获取联盟设置URL"""
        return self.get_endpoint_url(f"league/{league_key}/settings")
    
    def get_league_teams_url(self, league_key: str) -> str:
        """获取联盟团队URL"""
        return self.get_endpoint_url(f"league/{league_key}/teams")
    
    def get_league_players_url(self, league_key: str, start: int = 0, count: int = 25) -> str:
        """获取联盟球员URL"""
        return self.get_endpoint_url(f"league/{league_key}/players;start={start};count={count}")
    
    def get_league_standings_url(self, league_key: str) -> str:
        """获取联盟排名URL"""
        return self.get_endpoint_url(f"league/{league_key}/standings")
    
    def get_league_transactions_url(self, league_key: str, count: int = 50) -> str:
        """获取联盟交易URL"""
        return self.get_endpoint_url(f"league/{league_key}/transactions;count={count}")
    
    def get_team_roster_url(self, team_key: str, date: str = None) -> str:
        """获取团队名单URL"""
        if date:
            return self.get_endpoint_url(f"team/{team_key}/roster;date={date}")
        return self.get_endpoint_url(f"team/{team_key}/roster")
    
    def get_team_matchups_url(self, team_key: str) -> str:
        """获取团队对战URL"""
        return self.get_endpoint_url(f"team/{team_key}/matchups")
    
    def get_player_stats_url(self, player_keys: list, stat_type: str = "stats") -> str:
        """获取球员统计URL"""
        players_param = ",".join(player_keys)
        return self.get_endpoint_url(f"players;player_keys={players_param}/{stat_type}")
    
    def validate_config(self) -> tuple[bool, list]:
        """验证API配置"""
        errors = []
        
        if not self.client_id:
            errors.append("缺少YAHOO_CLIENT_ID环境变量")
        
        if not self.client_secret:
            errors.append("缺少YAHOO_CLIENT_SECRET环境变量")
        
        if not self.tokens_dir.exists():
            try:
                self._ensure_tokens_directory()
            except Exception as e:
                errors.append(f"无法创建tokens目录: {e}")
        
        return len(errors) == 0, errors


class OAuthConfig:
    """
    OAuth配置管理器
    """
    
    def __init__(self, api_config: APIConfig = None):
        self.api_config = api_config or APIConfig()
    
    def get_oauth_session_config(self) -> Dict:
        """获取OAuth2Session配置"""
        return {
            'client_id': self.api_config.client_id,
            'redirect_uri': self.api_config.redirect_uri,
            'scope': self.api_config.scope
        }
    
    def get_token_request_data(self, refresh_token: str) -> Dict:
        """获取令牌刷新请求数据"""
        return {
            'client_id': self.api_config.client_id,
            'client_secret': self.api_config.client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }


class EndpointManager:
    """
    API端点管理器
    """
    
    def __init__(self, api_config: APIConfig = None):
        self.api_config = api_config or APIConfig()
    
    def build_url(self, endpoint: str, params: Dict = None) -> str:
        """构建完整URL"""
        url = self.api_config.get_endpoint_url(endpoint)
        
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            connector = "&" if "?" in url else "?"
            url = f"{url}{connector}{param_str}"
        
        return url


# API配置模块已完整迁移，无需兼容性代码 