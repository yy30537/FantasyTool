"""
Base Transformer - 转换器基类

提供数据转换的统一接口和标准化结果格式
"""
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, date
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """数据验证错误"""
    field: str
    value: Any
    error: str
    severity: str = "error"  # error, warning


@dataclass
class TransformResult:
    """转换结果包装类"""
    success: bool
    data: Optional[Dict] = None
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list) 
    metadata: Dict = field(default_factory=dict)
    
    def add_error(self, field: str, value: Any, error: str):
        """添加错误"""
        self.errors.append(ValidationError(field, value, error, "error"))
        
    def add_warning(self, field: str, value: Any, warning: str):
        """添加警告"""
        self.warnings.append(ValidationError(field, value, warning, "warning"))
    
    def has_errors(self) -> bool:
        """是否有错误"""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """是否有警告"""
        return len(self.warnings) > 0


class BaseTransformer(ABC):
    """转换器基类"""
    
    def __init__(self, strict_mode: bool = False):
        """
        初始化转换器
        
        Args:
            strict_mode: 严格模式，遇到验证错误时是否停止处理
        """
        self.strict_mode = strict_mode
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def transform(self, raw_data: Dict) -> TransformResult:
        """
        转换数据的抽象方法
        
        Args:
            raw_data: 原始数据
            
        Returns:
            TransformResult: 转换结果
        """
        pass
    
    def validate_required_fields(self, data: Dict, required_fields: List[str]) -> List[ValidationError]:
        """验证必需字段"""
        errors = []
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(ValidationError(field, None, f"必需字段 {field} 缺失"))
        return errors
    
    def validate_field_type(self, data: Dict, field: str, expected_type: type) -> Optional[ValidationError]:
        """验证字段类型"""
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_type):
                return ValidationError(
                    field, 
                    data[field], 
                    f"字段 {field} 类型错误，期望 {expected_type.__name__}，实际 {type(data[field]).__name__}"
                )
        return None
    
    def clean_string(self, value: Any) -> Optional[str]:
        """清理字符串数据"""
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip() if value.strip() else None
        return str(value).strip() if str(value).strip() else None
    
    def safe_int(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        """安全的整数转换"""
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value.strip()) if value.strip() else default
            except ValueError:
                return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def safe_float(self, value: Any, default: Optional[float] = None) -> Optional[float]:
        """安全的浮点数转换"""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip()) if value.strip() else default
            except ValueError:
                return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def safe_bool(self, value: Any, default: bool = False) -> bool:
        """安全的布尔值转换"""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ('true', '1', 'yes', 'on')
        if isinstance(value, (int, float)):
            return value != 0
        return default
    
    def parse_date(self, value: Any) -> Optional[date]:
        """解析日期"""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                # 尝试常见的日期格式
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        return datetime.strptime(value.strip(), fmt).date()
                    except ValueError:
                        continue
            except Exception:
                pass
        return None
    
    def extract_position_string(self, position_data: Any) -> Optional[str]:
        """从位置数据中提取位置字符串"""
        if not position_data:
            return None
        
        if isinstance(position_data, str):
            return position_data.strip() if position_data.strip() else None
        
        if isinstance(position_data, dict):
            return self.clean_string(position_data.get("position"))
        
        if isinstance(position_data, list) and len(position_data) > 0:
            if isinstance(position_data[0], str):
                return self.clean_string(position_data[0])
            elif isinstance(position_data[0], dict):
                return self.clean_string(position_data[0].get("position"))
        
        return None
    
    def extract_nested_value(self, data: Dict, path: str, default: Any = None) -> Any:
        """从嵌套字典中提取值，使用点号分隔路径"""
        keys = path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current
    
    def batch_transform(self, raw_data_list: List[Dict]) -> List[TransformResult]:
        """批量转换数据"""
        results = []
        for raw_data in raw_data_list:
            try:
                result = self.transform(raw_data)
                results.append(result)
            except Exception as e:
                error_result = TransformResult(success=False)
                error_result.add_error("transform", raw_data, f"转换异常: {str(e)}")
                results.append(error_result)
        return results 