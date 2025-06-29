# 数据清洗器
#
# 迁移来源: 分散在旧脚本中的数据清洗逻辑
# 主要职责: 集中化数据清洗和预处理逻辑
#
# 职责:
#   - 字符串清洗：
#     * 去除多余空白字符
#     * 处理特殊字符和编码问题
#     * 统一字符串格式（大小写等）
#   - 数值清洗：
#     * 处理数值字符串转换
#     * 清理数值中的非数字字符
#     * 处理科学记数法和百分比格式
#   - 日期时间清洗：
#     * 统一日期格式
#     * 处理时区信息
#     * 修复不完整的日期数据
#   - 布尔值清洗：
#     * 统一布尔值表示：'0'/'1' -> False/True
#     * 处理字符串布尔值：'true'/'false'
#     * 处理数字布尔值：0/1
#   - 列表和字典清洗：
#     * 移除空值和无效元素
#     * 扁平化嵌套结构
#     * 处理重复项
#   - 缺失值处理：
#     * 标识和标记缺失值
#     * 应用默认值策略
#     * 缺失值插值和估计
#   - 异常值处理：
#     * 识别和标记异常值
#     * 异常值修正策略
#     * 保留原始值的同时提供清洗值
#
# 输入: 原始或部分处理的数据对象
# 输出: 清洗后的数据对象，保持原始结构 

import re
import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


class DataCleaner:
    """数据清洗器 - 集中化所有数据清洗逻辑"""
    
    def __init__(self):
        """初始化数据清洗器"""
        self.stats = {
            'cleaned_strings': 0,
            'cleaned_numbers': 0,
            'cleaned_booleans': 0,
            'cleaned_dates': 0,
            'cleaned_lists': 0,
            'handled_missing_values': 0,
            'handled_exceptions': 0
        }
    
    # ===== 字符串清洗 =====
    
    def clean_string(self, value: Any, 
                    strip_whitespace: bool = True,
                    remove_special_chars: bool = False,
                    normalize_case: Optional[str] = None) -> Optional[str]:
        """清洗字符串数据
        
        Args:
            value: 输入值
            strip_whitespace: 是否去除首尾空白
            remove_special_chars: 是否移除特殊字符
            normalize_case: 大小写标准化 ('upper', 'lower', 'title')
            
        Returns:
            清洗后的字符串或None
        """
        try:
            if value is None:
                return None
            
            # 转换为字符串
            if not isinstance(value, str):
                str_value = str(value)
            else:
                str_value = value
            
            # 去除空白字符
            if strip_whitespace:
                str_value = str_value.strip()
            
            # 检查空字符串
            if str_value == '':
                return None
            
            # 移除特殊字符（保留字母、数字、空格、常用标点）
            if remove_special_chars:
                str_value = re.sub(r'[^\w\s\-\.\,\(\)\/]', '', str_value)
            
            # 大小写标准化
            if normalize_case:
                if normalize_case.lower() == 'upper':
                    str_value = str_value.upper()
                elif normalize_case.lower() == 'lower':
                    str_value = str_value.lower()
                elif normalize_case.lower() == 'title':
                    str_value = str_value.title()
            
            self.stats['cleaned_strings'] += 1
            return str_value
            
        except Exception as e:
            logger.warning(f"字符串清洗失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None
    
    def clean_player_name(self, name: Any) -> Optional[str]:
        """清洗球员姓名"""
        if not name:
            return None
        
        cleaned = self.clean_string(name, strip_whitespace=True, remove_special_chars=False)
        if not cleaned:
            return None
        
        # 特殊姓名格式处理
        cleaned = re.sub(r'\s+', ' ', cleaned)  # 多个空格合并为一个
        cleaned = cleaned.replace('  ', ' ')    # 双空格处理
        
        return cleaned
    
    def clean_team_name(self, name: Any) -> Optional[str]:
        """清洗团队名称"""
        if not name:
            return None
        
        cleaned = self.clean_string(name, strip_whitespace=True)
        if not cleaned:
            return None
        
        # 团队名称特殊处理
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned
    
    # ===== 数值清洗 =====
    
    def safe_int_conversion(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        """安全整数转换 - 迁移自database_writer._safe_int"""
        try:
            if value is None or value == '':
                return default
            
            if isinstance(value, (int, float)):
                return int(value)
            
            if isinstance(value, str):
                # 清理字符串中的非数字字符（保留负号）
                cleaned = re.sub(r'[^\d\-]', '', value.strip())
                if cleaned == '' or cleaned == '-':
                    return default
                return int(cleaned)
            
            # 尝试直接转换
            return int(value)
            
        except (ValueError, TypeError, OverflowError) as e:
            logger.debug(f"整数转换失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return default
    
    def safe_float_conversion(self, value: Any, default: Optional[float] = None) -> Optional[float]:
        """安全浮点数转换 - 迁移自database_writer._safe_float"""
        try:
            if value is None or value == '':
                return default
            
            if isinstance(value, (int, float)):
                return float(value)
            
            if isinstance(value, str):
                # 清理字符串，保留数字、小数点、负号、科学记数法
                cleaned = value.strip()
                
                # 处理百分号
                if '%' in cleaned:
                    cleaned = cleaned.replace('%', '')
                    result = self.safe_float_conversion(cleaned)
                    return result / 100 if result is not None else default
                
                # 清理其他非数字字符（保留小数点、负号、e/E）
                cleaned = re.sub(r'[^\d\.\-eE]', '', cleaned)
                if cleaned == '' or cleaned == '-' or cleaned == '.':
                    return default
                
                return float(cleaned)
            
            # 尝试直接转换
            return float(value)
            
        except (ValueError, TypeError, OverflowError) as e:
            logger.debug(f"浮点数转换失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return default
    
    def safe_decimal_conversion(self, value: Any, precision: int = 2, 
                              default: Optional[Decimal] = None) -> Optional[Decimal]:
        """安全Decimal转换，用于高精度计算"""
        try:
            if value is None or value == '':
                return default
            
            if isinstance(value, Decimal):
                return value.quantize(Decimal('0.' + '0' * precision))
            
            # 先转换为浮点数
            float_val = self.safe_float_conversion(value)
            if float_val is None:
                return default
            
            decimal_val = Decimal(str(float_val))
            return decimal_val.quantize(Decimal('0.' + '0' * precision))
            
        except (InvalidOperation, ValueError) as e:
            logger.debug(f"Decimal转换失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return default
    
    def parse_percentage_value(self, value: Any, as_decimal: bool = False) -> Optional[float]:
        """解析百分比值 - 迁移自database_writer._parse_percentage
        
        Args:
            value: 百分比值（可能包含%符号）
            as_decimal: True返回0-1范围，False返回0-100范围
            
        Returns:
            解析后的百分比值
        """
        try:
            if not value or value == '-':
                return None
            
            value_str = str(value).strip()
            
            # 移除%符号
            if '%' in value_str:
                clean_value = value_str.replace('%', '')
                val = self.safe_float_conversion(clean_value)
                if val is not None:
                    result = round(val, 3)
                    return result / 100 if as_decimal else result
            
            # 处理小数形式
            clean_value = self.safe_float_conversion(value_str)
            if clean_value is not None:
                # 如果是小数形式（0-1），根据需要转换
                if 0 <= clean_value <= 1:
                    result = round(clean_value * 100, 3) if not as_decimal else round(clean_value, 3)
                    return result
                # 如果已经是百分比形式（0-100）
                elif 0 <= clean_value <= 100:
                    result = round(clean_value, 3) if not as_decimal else round(clean_value / 100, 3)
                    return result
            
            return None
            
        except (ValueError, TypeError) as e:
            logger.debug(f"百分比解析失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None
    
    def parse_made_attempted_stat(self, value: Any) -> Tuple[Optional[int], Optional[int]]:
        """解析'made/attempted'格式的统计数据
        
        Args:
            value: 'made/attempted'格式的字符串
            
        Returns:
            (made, attempted)元组
        """
        try:
            if not value:
                return None, None
            
            value_str = str(value).strip()
            if '/' not in value_str:
                return None, None
            
            parts = value_str.split('/')
            if len(parts) != 2:
                return None, None
            
            made = self.safe_int_conversion(parts[0].strip())
            attempted = self.safe_int_conversion(parts[1].strip())
            
            return made, attempted
            
        except Exception as e:
            logger.debug(f"made/attempted解析失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None, None
    
    def clean_numeric_string(self, value: Any, remove_commas: bool = True,
                           remove_currency: bool = True) -> Optional[str]:
        """清洗数字字符串，移除非数字字符"""
        if not value:
            return None
        
        value_str = str(value).strip()
        
        # 移除货币符号
        if remove_currency:
            value_str = re.sub(r'[\$\€\£\¥]', '', value_str)
        
        # 移除逗号
        if remove_commas:
            value_str = value_str.replace(',', '')
        
        # 保留数字、小数点、负号、科学记数法
        cleaned = re.sub(r'[^\d\.\-eE]', '', value_str)
        
        return cleaned if cleaned else None
    
    # ===== 布尔值清洗 =====
    
    def safe_bool_conversion(self, value: Any, default: bool = False) -> bool:
        """安全布尔值转换 - 迁移自database_writer._safe_bool"""
        try:
            if value is None:
                return default
            
            if isinstance(value, bool):
                return value
            
            if isinstance(value, str):
                value_lower = value.strip().lower()
                if value_lower in ('1', 'true', 'yes', 'on', 'y', 't'):
                    return True
                elif value_lower in ('0', 'false', 'no', 'off', 'n', 'f', ''):
                    return False
                return default
            
            if isinstance(value, (int, float)):
                return value != 0
            
            return default
            
        except Exception as e:
            logger.debug(f"布尔值转换失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return default
    
    # ===== 日期时间清洗 =====
    
    def clean_date_string(self, value: Any, 
                         input_formats: Optional[List[str]] = None) -> Optional[date]:
        """清洗日期字符串"""
        if not value:
            return None
        
        # 默认日期格式
        if input_formats is None:
            input_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y/%m/%d',
                '%Y-%m-%d %H:%M:%S',
                '%m/%d/%Y %H:%M:%S'
            ]
        
        value_str = str(value).strip()
        
        for fmt in input_formats:
            try:
                dt = datetime.strptime(value_str, fmt)
                self.stats['cleaned_dates'] += 1
                return dt.date()
            except ValueError:
                continue
        
        logger.debug(f"日期解析失败: {value}")
        self.stats['handled_exceptions'] += 1
        return None
    
    def clean_datetime_string(self, value: Any,
                            input_formats: Optional[List[str]] = None) -> Optional[datetime]:
        """清洗日期时间字符串"""
        if not value:
            return None
        
        # 默认日期时间格式
        if input_formats is None:
            input_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S.%f',
                '%m/%d/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S'
            ]
        
        value_str = str(value).strip()
        
        for fmt in input_formats:
            try:
                dt = datetime.strptime(value_str, fmt)
                self.stats['cleaned_dates'] += 1
                return dt
            except ValueError:
                continue
        
        logger.debug(f"日期时间解析失败: {value}")
        self.stats['handled_exceptions'] += 1
        return None
    
    # ===== 列表和字典清洗 =====
    
    def clean_list(self, value: Any, 
                  remove_empty: bool = True,
                  remove_duplicates: bool = False,
                  flatten: bool = False) -> Optional[List]:
        """清洗列表数据"""
        try:
            if not value:
                return None
            
            if not isinstance(value, list):
                # 尝试转换
                if isinstance(value, str):
                    # 尝试解析逗号分隔的字符串
                    if ',' in value:
                        result = [item.strip() for item in value.split(',')]
                    else:
                        result = [value]
                else:
                    result = [value]
            else:
                result = value.copy()
            
            # 扁平化嵌套列表
            if flatten:
                flattened = []
                for item in result:
                    if isinstance(item, list):
                        flattened.extend(item)
                    else:
                        flattened.append(item)
                result = flattened
            
            # 移除空值
            if remove_empty:
                result = [item for item in result if item is not None and item != '']
            
            # 移除重复项
            if remove_duplicates:
                seen = set()
                unique_result = []
                for item in result:
                    if item not in seen:
                        seen.add(item)
                        unique_result.append(item)
                result = unique_result
            
            self.stats['cleaned_lists'] += 1
            return result if result else None
            
        except Exception as e:
            logger.debug(f"列表清洗失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None
    
    def clean_dict(self, value: Any, 
                  remove_empty_values: bool = True,
                  clean_keys: bool = True) -> Optional[Dict]:
        """清洗字典数据"""
        try:
            if not value:
                return None
            
            if not isinstance(value, dict):
                return None
            
            result = {}
            
            for key, val in value.items():
                # 清洗键
                if clean_keys:
                    clean_key = self.clean_string(key, strip_whitespace=True)
                    if not clean_key:
                        continue
                else:
                    clean_key = key
                
                # 处理值
                if remove_empty_values and (val is None or val == ''):
                    continue
                
                result[clean_key] = val
            
            return result if result else None
            
        except Exception as e:
            logger.debug(f"字典清洗失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None
    
    # ===== 缺失值处理 =====
    
    def handle_missing_value(self, value: Any, 
                           field_type: str = 'string',
                           default_strategy: str = 'none') -> Any:
        """处理缺失值
        
        Args:
            value: 输入值
            field_type: 字段类型 ('string', 'int', 'float', 'bool', 'date')
            default_strategy: 默认值策略 ('none', 'zero', 'empty', 'median')
            
        Returns:
            处理后的值
        """
        try:
            # 检查是否为缺失值
            if value is None or value == '':
                self.stats['handled_missing_values'] += 1
                
                if default_strategy == 'none':
                    return None
                elif default_strategy == 'zero':
                    if field_type in ('int', 'float'):
                        return 0
                    elif field_type == 'bool':
                        return False
                    else:
                        return None
                elif default_strategy == 'empty':
                    if field_type == 'string':
                        return ''
                    elif field_type in ('int', 'float'):
                        return 0
                    elif field_type == 'bool':
                        return False
                    else:
                        return None
                        
            return value
            
        except Exception as e:
            logger.debug(f"缺失值处理失败: {value} - {e}")
            self.stats['handled_exceptions'] += 1
            return None
    
    # ===== 异常值处理 =====
    
    def detect_outliers(self, values: List[Union[int, float]], 
                       method: str = 'iqr') -> List[bool]:
        """检测异常值
        
        Args:
            values: 数值列表
            method: 检测方法 ('iqr', 'zscore', 'percentile')
            
        Returns:
            布尔列表，True表示异常值
        """
        try:
            if not values:
                return []
            
            clean_values = [v for v in values if v is not None]
            if len(clean_values) < 3:
                return [False] * len(values)
            
            outliers = [False] * len(values)
            
            if method == 'iqr':
                # 四分位数方法
                clean_values.sort()
                n = len(clean_values)
                q1 = clean_values[n // 4]
                q3 = clean_values[3 * n // 4]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                for i, value in enumerate(values):
                    if value is not None and (value < lower_bound or value > upper_bound):
                        outliers[i] = True
            
            elif method == 'percentile':
                # 百分位数方法（5%和95%）
                clean_values.sort()
                n = len(clean_values)
                p5 = clean_values[int(n * 0.05)]
                p95 = clean_values[int(n * 0.95)]
                
                for i, value in enumerate(values):
                    if value is not None and (value < p5 or value > p95):
                        outliers[i] = True
            
            return outliers
            
        except Exception as e:
            logger.debug(f"异常值检测失败: {e}")
            return [False] * len(values)
    
    def handle_outlier(self, value: Union[int, float], 
                      replacement_value: Optional[Union[int, float]] = None,
                      cap_at_percentile: Optional[float] = None) -> Union[int, float]:
        """处理单个异常值"""
        if replacement_value is not None:
            return replacement_value
        
        # 如果指定了百分位数上限，则截断
        if cap_at_percentile is not None:
            # 这里需要上下文中的其他值来计算百分位数
            # 简单实现：返回原值
            pass
        
        return value
    
    # ===== 批量清洗方法 =====
    
    def clean_record(self, record: Dict[str, Any], 
                    field_configs: Optional[Dict[str, Dict]] = None) -> Dict[str, Any]:
        """清洗单条记录
        
        Args:
            record: 输入记录
            field_configs: 字段配置，格式: {field_name: {type: str, options: dict}}
            
        Returns:
            清洗后的记录
        """
        if not record or not isinstance(record, dict):
            return {}
        
        cleaned_record = {}
        
        for field_name, value in record.items():
            try:
                if field_configs and field_name in field_configs:
                    config = field_configs[field_name]
                    field_type = config.get('type', 'string')
                    options = config.get('options', {})
                    
                    if field_type == 'string':
                        cleaned_value = self.clean_string(value, **options)
                    elif field_type == 'int':
                        cleaned_value = self.safe_int_conversion(value, **options)
                    elif field_type == 'float':
                        cleaned_value = self.safe_float_conversion(value, **options)
                    elif field_type == 'bool':
                        cleaned_value = self.safe_bool_conversion(value, **options)
                    elif field_type == 'percentage':
                        cleaned_value = self.parse_percentage_value(value, **options)
                    elif field_type == 'date':
                        cleaned_value = self.clean_date_string(value, **options)
                    elif field_type == 'list':
                        cleaned_value = self.clean_list(value, **options)
                    else:
                        cleaned_value = value
                else:
                    # 默认处理：基于值类型自动清洗
                    cleaned_value = self._auto_clean_value(value)
                
                cleaned_record[field_name] = cleaned_value
                
            except Exception as e:
                logger.warning(f"字段清洗失败 {field_name}: {e}")
                cleaned_record[field_name] = value
        
        return cleaned_record
    
    def _auto_clean_value(self, value: Any) -> Any:
        """自动清洗值（基于类型推断）"""
        if value is None:
            return None
        
        if isinstance(value, str):
            return self.clean_string(value)
        elif isinstance(value, list):
            return self.clean_list(value)
        elif isinstance(value, dict):
            return self.clean_dict(value)
        else:
            return value
    
    def get_cleaning_stats(self) -> Dict[str, int]:
        """获取清洗统计信息"""
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        for key in self.stats:
            self.stats[key] = 0


# 全局清洗器实例
default_cleaner = DataCleaner()

# 便捷函数
def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """安全整数转换便捷函数"""
    return default_cleaner.safe_int_conversion(value, default)

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """安全浮点数转换便捷函数"""
    return default_cleaner.safe_float_conversion(value, default)

def safe_bool(value: Any, default: bool = False) -> bool:
    """安全布尔值转换便捷函数"""
    return default_cleaner.safe_bool_conversion(value, default)

def parse_percentage(value: Any, as_decimal: bool = False) -> Optional[float]:
    """百分比解析便捷函数"""
    return default_cleaner.parse_percentage_value(value, as_decimal)

def clean_string(value: Any, **kwargs) -> Optional[str]:
    """字符串清洗便捷函数"""
    return default_cleaner.clean_string(value, **kwargs) 