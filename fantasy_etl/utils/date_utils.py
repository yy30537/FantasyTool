"""
日期处理工具
从 archive/database_writer.py 迁移日期相关函数
"""

from typing import Union, Optional
from datetime import date, datetime


class DateUtils:
    """日期处理工具类"""
    
    @staticmethod
    def parse_coverage_date(date_str: Union[str, None]) -> Optional[date]:
        """
        解析日期字符串为date对象
        
        迁移自: archive/database_writer.py parse_coverage_date() 第1732行
        
        Args:
            date_str: 日期字符串，格式为 "YYYY-MM-DD"
            
        Returns:
            解析后的date对象，失败返回None
        """
        if not date_str:
            return None
        try:
            if isinstance(date_str, str):
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            return date_str
        except:
            return None
        
    @staticmethod
    def parse_week(week_str: Union[str, int, None]) -> Optional[int]:
        """
        解析周数
        
        迁移自: archive/database_writer.py parse_week() 第1743行
        
        Args:
            week_str: 周数字符串或整数
            
        Returns:
            解析后的周数，失败返回None
        """
        if week_str is None:
            return None
        try:
            return int(week_str)
        except:
            return None
        
    @staticmethod
    def parse_yahoo_date(date_str: str) -> Optional[datetime]:
        """
        解析Yahoo日期格式
        
        迁移自: archive/yahoo_api_utils.py parse_yahoo_date() 第189行
        
        Args:
            date_str: Yahoo API返回的日期字符串
            
        Returns:
            解析后的datetime对象，失败返回None
        """
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            return None
    
    @staticmethod
    def format_date_for_api(date_obj: Union[date, datetime]) -> str:
        """
        格式化日期为Yahoo API所需的格式
        
        Args:
            date_obj: date或datetime对象
            
        Returns:
            格式化后的日期字符串 "YYYY-MM-DD"
        """
        if isinstance(date_obj, datetime):
            return date_obj.strftime("%Y-%m-%d")
        elif isinstance(date_obj, date):
            return date_obj.strftime("%Y-%m-%d")
        else:
            return str(date_obj)
    
    @staticmethod
    def get_date_range(start_date: date, end_date: date) -> list[date]:
        """
        获取日期范围内的所有日期
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            日期列表
        """
        from datetime import timedelta
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        return dates
    
    @staticmethod
    def is_weekend(date_obj: date) -> bool:
        """
        判断是否为周末
        
        Args:
            date_obj: 日期对象
            
        Returns:
            是否为周末（周六或周日）
        """
        return date_obj.weekday() >= 5
    
    @staticmethod
    def get_week_of_year(date_obj: date) -> int:
        """
        获取一年中的第几周
        
        Args:
            date_obj: 日期对象
            
        Returns:
            周数（1-53）
        """
        return date_obj.isocalendar()[1]
    
    @staticmethod
    def get_quarter(date_obj: date) -> int:
        """
        获取季度
        
        Args:
            date_obj: 日期对象
            
        Returns:
            季度（1-4）
        """
        return ((date_obj.month - 1) // 3) + 1