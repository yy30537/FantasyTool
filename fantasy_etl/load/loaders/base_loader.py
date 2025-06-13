"""
Base Loader - 加载器基类

提供数据写入的统一接口和标准化结果格式
"""
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


@dataclass 
class LoadError:
    """数据加载错误"""
    record: Any
    error: str
    table_name: Optional[str] = None
    operation: Optional[str] = None


@dataclass
class LoadResult:
    """加载结果包装类"""
    success: bool
    records_processed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    errors: List[LoadError] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    
    def add_error(self, record: Any, error: str, table_name: Optional[str] = None, operation: Optional[str] = None):
        """添加错误"""
        self.errors.append(LoadError(record, error, table_name, operation))
        self.records_failed += 1
    
    def add_success(self, count: int = 1):
        """添加成功记录"""
        self.records_loaded += count
    
    def process_record(self):
        """处理记录计数"""
        self.records_processed += 1
    
    @property 
    def success_rate(self) -> float:
        """成功率"""
        if self.records_processed == 0:
            return 0.0
        return self.records_loaded / self.records_processed
    
    def has_errors(self) -> bool:
        """是否有错误"""
        return len(self.errors) > 0


class BaseLoader(ABC):
    """加载器基类"""
    
    def __init__(self, db_writer, batch_size: int = 100, strict_mode: bool = False):
        """
        初始化加载器
        
        Args:
            db_writer: 数据库写入器实例
            batch_size: 批量处理大小
            strict_mode: 严格模式，遇到错误时是否停止处理
        """
        self.db_writer = db_writer
        self.batch_size = batch_size
        self.strict_mode = strict_mode
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载数据的抽象方法
        
        Args:
            data: 要加载的数据
            **kwargs: 其他参数
            
        Returns:
            LoadResult: 加载结果
        """
        pass
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """
        加载单条记录的抽象方法
        
        Args:
            record: 要加载的记录
            **kwargs: 其他参数
            
        Returns:
            bool: 是否加载成功
        """
        return False
    
    def batch_load(self, records: List[Dict], **kwargs) -> LoadResult:
        """批量加载数据"""
        result = LoadResult(success=False)
        
        try:
            # 分批处理
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                batch_result = self._process_batch(batch, **kwargs)
                
                # 合并结果
                result.records_processed += batch_result.records_processed
                result.records_loaded += batch_result.records_loaded
                result.records_failed += batch_result.records_failed
                result.errors.extend(batch_result.errors)
                
                # 严格模式下遇到错误停止
                if self.strict_mode and batch_result.has_errors():
                    break
            
            result.success = result.records_loaded > 0
            result.metadata["batch_size"] = self.batch_size
            result.metadata["total_batches"] = (len(records) + self.batch_size - 1) // self.batch_size
            result.metadata["load_time"] = datetime.now()
            
        except Exception as e:
            result.add_error(records, f"批量加载异常: {str(e)}")
        
        return result
    
    def _process_batch(self, batch: List[Dict], **kwargs) -> LoadResult:
        """处理单个批次"""
        result = LoadResult(success=False)
        
        for record in batch:
            result.process_record()
            
            try:
                if self.load_single_record(record, **kwargs):
                    result.add_success()
                else:
                    result.add_error(record, "记录加载失败")
                    
            except Exception as e:
                result.add_error(record, f"记录处理异常: {str(e)}")
        
        result.success = result.records_loaded > 0
        return result
    
    def validate_record(self, record: Dict, required_fields: List[str]) -> List[str]:
        """验证记录的必需字段"""
        missing_fields = []
        for field in required_fields:
            if field not in record or record[field] is None:
                missing_fields.append(field)
        return missing_fields
    
    def clean_record_for_db(self, record: Dict) -> Dict:
        """清理记录用于数据库写入"""
        cleaned = {}
        for key, value in record.items():
            if value is not None:
                # 清理字符串
                if isinstance(value, str):
                    cleaned[key] = value.strip() if value.strip() else None
                else:
                    cleaned[key] = value
        return cleaned
    
    def log_result(self, result: LoadResult, operation: str = "load"):
        """记录加载结果"""
        if result.success:
            self.logger.info(
                f"{operation}成功: 处理{result.records_processed}条, "
                f"成功{result.records_loaded}条, 失败{result.records_failed}条, "
                f"成功率{result.success_rate:.2%}"
            )
        else:
            self.logger.error(f"{operation}失败: {len(result.errors)}个错误")
        
        # 记录错误详情
        for error in result.errors[:5]:  # 只显示前5个错误
            self.logger.error(f"加载错误: {error.error}")
    
    def get_or_create_foreign_key(self, table_name: str, key_field: str, key_value: Any) -> bool:
        """获取或创建外键记录"""
        try:
            # 这里应该根据具体的ORM实现来检查外键是否存在
            # 这是一个通用的模板，具体实现需要在子类中重写
            return True
        except Exception as e:
            self.logger.error(f"外键检查失败 {table_name}.{key_field}={key_value}: {e}")
            return False 