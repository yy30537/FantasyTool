"""
认证模块 (Auth Module)
===================

迁移设计说明：
- 从 scripts/yahoo_api_utils.py 和 scripts/app.py 迁移认证相关功能
- 统一管理Yahoo OAuth认证流程
- 保持与现有脚本的兼容性

模块组织：
- oauth_manager.py: 核心OAuth管理功能
- token_storage.py: 令牌存储管理
- web_auth_server.py: Web授权服务器

向后兼容性：
- 保留scripts/yahoo_api_utils.py中的所有公共函数接口
- 保留scripts/app.py中的Flask OAuth流程
"""

# 主要认证组件导入
# 迁移完成后将启用以下导入
# from .oauth_manager import OAuthManager
# from .token_storage import TokenStorage
# from .web_auth_server import WebAuthServer

# TODO: 迁移阶段 - 保持对旧脚本的引用
# 确保ETL系统可以无缝切换到新的认证模块

__version__ = "1.0.0"
__author__ = "Fantasy ETL Team" 