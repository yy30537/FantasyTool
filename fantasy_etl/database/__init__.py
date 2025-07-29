"""
数据库模块 - 数据库查询和连接管理

包含所有 get_* 函数
"""

from .queries import DatabaseQueries
from .connection import DatabaseConnection

__all__ = ['DatabaseQueries', 'DatabaseConnection']