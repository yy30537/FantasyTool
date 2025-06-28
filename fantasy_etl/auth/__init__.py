"""
Fantasy ETL Authentication Module
================================

提供Yahoo Fantasy API的OAuth2.0认证管理功能

主要组件：
- OAuthManager: OAuth认证管理器
- TokenStorage: 令牌存储管理器
- WebAuthServer: Web授权服务器

使用示例：
```python
from fantasy_etl.auth import OAuthManager, TokenStorage, WebAuthServer

# OAuth管理
oauth_manager = OAuthManager()
token = oauth_manager.load_token()

# 令牌存储
token_storage = TokenStorage()
token_storage.save_token(token)

# Web授权服务器
server = WebAuthServer()
server.start()
```
"""

from .oauth_manager import OAuthManager, MultiUserOAuthManager, OAuthTokenCache
from .token_storage import TokenStorage, TokenStorageInterface, FileTokenStorage
from .web_auth_server import WebAuthServer

__all__ = [
    'OAuthManager',
    'MultiUserOAuthManager', 
    'OAuthTokenCache',
    'TokenStorage',
    'TokenStorageInterface',
    'FileTokenStorage',
    'WebAuthServer'
] 