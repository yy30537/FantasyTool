"""
API接口层

提供对外的服务接口和命令行界面
"""

from .fantasy_data_service import FantasyDataService
from .cli_interface import CLIInterface, main

__all__ = ['FantasyDataService', 'CLIInterface', 'main']