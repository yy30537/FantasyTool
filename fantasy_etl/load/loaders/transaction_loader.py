"""
Transaction Loader - 交易数据加载器

负责将转换后的交易数据写入数据库
"""
from typing import Dict, List, Optional, Union
import logging

from .base_loader import BaseLoader, LoadResult

logger = logging.getLogger(__name__)


class TransactionLoader(BaseLoader):
    """交易数据加载器"""
    
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载交易数据
        
        Args:
            data: 转换后的交易数据，可以是单个结果或列表
            **kwargs: 其他参数，如league_key等
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult(success=False)
        
        try:
            # 处理输入数据格式
            if isinstance(data, dict):
                # 单个TransformResult的data部分
                transaction_data_list = [data]
            elif isinstance(data, list):
                transaction_data_list = data
            else:
                result.add_error(data, "不支持的数据格式")
                return result
            
            # 批量加载所有交易数据
            for transaction_data in transaction_data_list:
                single_result = self._load_single_transaction(transaction_data, **kwargs)
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "交易数据加载")
            
        except Exception as e:
            result.add_error(data, f"交易数据加载异常: {str(e)}")
        
        return result
    
    def _load_single_transaction(self, transaction_data: Dict, **kwargs) -> LoadResult:
        """加载单个交易的数据"""
        result = LoadResult(success=False)
        
        try:
            # 判断数据类型
            if "transactions" in transaction_data:
                # 批量交易数据
                return self._load_transactions_batch(transaction_data["transactions"], **kwargs)
            elif "transaction_key" in transaction_data:
                # 单个交易数据
                return self._load_single_transaction_info(transaction_data, **kwargs)
            else:
                result.add_error(transaction_data, "无法识别的交易数据格式")
                
        except Exception as e:
            result.add_error(transaction_data, f"交易数据处理异常: {str(e)}")
        
        return result
    
    def _load_transactions_batch(self, transactions: List[Dict], **kwargs) -> LoadResult:
        """批量加载交易数据"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(transactions, "缺少league_key参数")
                return result
            
            # 使用database_writer的批量写入方法
            count = self.db_writer.write_transactions_batch(transactions, league_key)
            
            result.records_processed = len(transactions)
            result.records_loaded = count
            result.records_failed = len(transactions) - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(transactions, f"批量加载交易失败: {str(e)}")
        
        return result
    
    def _load_single_transaction_info(self, transaction_data: Dict, **kwargs) -> LoadResult:
        """加载单个交易信息"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(transaction_data, "缺少league_key参数")
                return result
            
            # 使用批量方法加载单个交易
            count = self.db_writer.write_transactions_batch([transaction_data], league_key)
            
            result.records_processed = 1
            result.records_loaded = count
            result.records_failed = 1 - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(transaction_data, f"加载交易信息失败: {str(e)}")
        
        return result
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """加载单条交易记录（实现基类抽象方法）"""
        try:
            single_result = self._load_single_transaction(record, **kwargs)
            return single_result.success
        except Exception as e:
            self.logger.error(f"加载单条交易记录失败: {e}")
            return False
    
    def load_transactions_batch(self, transactions_data: List[Dict], league_key: str) -> LoadResult:
        """批量加载交易数据的便捷方法"""
        return self.load({"transactions": transactions_data}, league_key=league_key)
    
    def load_single_transaction(self, transaction_data: Dict, league_key: str) -> LoadResult:
        """加载单个交易的便捷方法"""
        return self.load(transaction_data, league_key=league_key) 