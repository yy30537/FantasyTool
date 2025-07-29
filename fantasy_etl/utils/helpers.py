"""
通用辅助函数
从 archive/database_writer.py 迁移安全转换函数
"""

from typing import Optional, Union, Any


class SafeConverters:
    """安全类型转换器"""
    
    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        """
        安全转换为整数
        
        迁移自: archive/database_writer.py _safe_int() 第1822行
        
        Args:
            value: 待转换的值
            
        Returns:
            转换后的整数，失败返回None
        """
        try:
            if value is None or value == '':
                return None
            return int(float(value))  # 先转float再转int，处理'1.0'格式
        except (ValueError, TypeError):
            return None
        
    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        """
        安全转换为浮点数
        
        迁移自: archive/database_writer.py _safe_float() 第1831行
        
        Args:
            value: 待转换的值
            
        Returns:
            转换后的浮点数，失败返回None
        """
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
        
    @staticmethod
    def safe_bool(value: Any) -> bool:
        """
        安全转换为布尔值
        
        迁移自: archive/database_writer.py _safe_bool() 第1150行
        
        Args:
            value: 待转换的值
            
        Returns:
            转换后的布尔值，默认False
        """
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes')
        if isinstance(value, (int, float)):
            return value != 0
        return False
        
    @staticmethod
    def parse_percentage(pct_str: Any) -> Optional[float]:
        """
        解析百分比字符串，返回百分比值（0-100）
        
        迁移自: archive/database_writer.py _parse_percentage() 第1840行
        
        Args:
            pct_str: 百分比字符串（如 ".500", "50%", "0.500"）
            
        Returns:
            百分比数值（0-100），失败返回None
        """
        try:
            if not pct_str or pct_str == '-':
                return None
            
            pct_str = str(pct_str).strip()
            
            # 移除%符号
            if '%' in pct_str:
                clean_value = pct_str.replace('%', '')
                val = SafeConverters.safe_float(clean_value)
                return round(val, 3) if val is not None else None
            
            # 处理小数形式（如 .500 或 0.500）
            clean_value = SafeConverters.safe_float(pct_str)
            if clean_value is not None:
                # 如果是小数形式（0-1），转换为百分比（0-100）
                if 0 <= clean_value <= 1:
                    return round(clean_value * 100, 3)
                # 如果已经是百分比形式（0-100），直接返回
                elif 0 <= clean_value <= 100:
                    return round(clean_value, 3)
            
            return None
        except (ValueError, TypeError):
            return None


class DataHelpers:
    """数据处理辅助函数"""
    
    @staticmethod
    def extract_nested_value(data: dict, path: str, default=None) -> Any:
        """
        从嵌套字典中安全提取值
        
        新增方法，用于处理Yahoo API的复杂嵌套结构
        
        Args:
            data: 嵌套字典
            path: 访问路径，如 "fantasy_content.league.0.name"
            default: 默认值
            
        Returns:
            提取的值或默认值
        """
        try:
            keys = path.split('.')
            result = data
            
            for key in keys:
                if result is None:
                    return default
                
                # 尝试将key转换为整数（用于列表索引）
                try:
                    key = int(key)
                except ValueError:
                    pass
                
                # 尝试获取值
                if isinstance(result, dict):
                    result = result.get(key, default)
                elif isinstance(result, list) and isinstance(key, int):
                    if 0 <= key < len(result):
                        result = result[key]
                    else:
                        return default
                else:
                    return default
            
            return result
        except Exception:
            return default
        
    @staticmethod
    def flatten_dict(data: dict, separator: str = "_", prefix: str = "") -> dict:
        """
        扁平化嵌套字典
        
        新增方法，用于简化复杂数据结构
        
        Args:
            data: 嵌套字典
            separator: 键分隔符
            prefix: 键前缀
            
        Returns:
            扁平化后的字典
        """
        result = {}
        
        def _flatten(obj, parent_key=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{parent_key}{separator}{key}" if parent_key else key
                    _flatten(value, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_key = f"{parent_key}{separator}{i}" if parent_key else str(i)
                    _flatten(item, new_key)
            else:
                result[parent_key] = obj
        
        _flatten(data, prefix)
        return result
    
    @staticmethod
    def clean_dict(data: dict, remove_none: bool = True, remove_empty: bool = False) -> dict:
        """
        清理字典中的无效值
        
        Args:
            data: 原始字典
            remove_none: 是否移除None值
            remove_empty: 是否移除空字符串和空容器
            
        Returns:
            清理后的字典
        """
        cleaned = {}
        
        for key, value in data.items():
            if remove_none and value is None:
                continue
            if remove_empty and (
                value == "" or 
                (isinstance(value, (list, dict, set)) and len(value) == 0)
            ):
                continue
            
            cleaned[key] = value
        
        return cleaned
    
    @staticmethod
    def merge_dicts(*dicts: dict) -> dict:
        """
        合并多个字典
        
        Args:
            *dicts: 要合并的字典
            
        Returns:
            合并后的字典（后面的字典会覆盖前面的同名键）
        """
        result = {}
        for d in dicts:
            if isinstance(d, dict):
                result.update(d)
        return result