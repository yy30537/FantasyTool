"""
Fantasy ETL - Yahoo Fantasy Sports ETL Pipeline
模块化重构版本

基于命名约定的函数分组：
- fetch_*: Yahoo API调用
- get_*: 数据库查询  
- transform_*: 数据转换
- load_*: 数据库写入
- verify_*: 验证和检查
"""

__version__ = "2.0.0"

# 主要模块导入
from . import api
from . import database  
from . import transformers
from . import loaders
from . import validators
from . import utils

__all__ = [
    'api',
    'database', 
    'transformers',
    'loaders',
    'validators', 
    'utils'
]