"""
Base Extractor - 提取器基类

定义所有数据提取器的通用接口和功能
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Generator
import logging
import time
from datetime import date, datetime

from ..yahoo_client import YahooFantasyClient

logger = logging.getLogger(__name__)


class ExtractionResult:
    """提取结果包装类"""
    
    def __init__(self, success: bool, data: Any = None, error: str = None, 
                 total_records: int = 0, metadata: Dict = None):
        self.success = success
        self.data = data
        self.error = error
        self.total_records = total_records
        self.metadata = metadata or {}
        self.timestamp = datetime.now()
    
    def __repr__(self):
        status = "成功" if self.success else "失败"
        return f"ExtractionResult({status}, {self.total_records} 条记录)"


class BaseExtractor(ABC):
    """提取器基类 - 定义通用的提取器接口"""
    
    def __init__(self, client: YahooFantasyClient, batch_size: int = 25):
        """初始化提取器
        
        Args:
            client: Yahoo Fantasy API客户端
            batch_size: 批处理大小
        """
        self.client = client
        self.batch_size = batch_size
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        self.logger.info(f"{self.__class__.__name__} 初始化完成 (批大小: {batch_size})")
    
    @abstractmethod
    def extract(self, **kwargs) -> ExtractionResult:
        """抽象方法：执行数据提取
        
        Args:
            **kwargs: 提取参数，由具体提取器定义
            
        Returns:
            ExtractionResult: 提取结果
        """
        pass
    
    def extract_paginated(self, endpoint_template: str, max_pages: int = 100, 
                         start_param: str = "start", **kwargs) -> Generator[Dict, None, None]:
        """分页提取数据的通用方法
        
        Args:
            endpoint_template: API端点模板，支持格式化参数
            max_pages: 最大页数限制
            start_param: 分页参数名
            **kwargs: 其他参数
            
        Yields:
            每页的原始API响应数据
        """
        start = 0
        page_count = 0
        
        while page_count < max_pages:
            try:
                # 构建端点URL
                endpoint = endpoint_template.format(**kwargs, **{start_param: start})
                
                self.logger.debug(f"请求第 {page_count + 1} 页: {endpoint}")
                
                # 请求数据
                response_data = self.client.get(endpoint)
                if not response_data:
                    self.logger.warning(f"第 {page_count + 1} 页无数据，停止分页")
                    break
                
                yield response_data
                
                # 检查是否还有更多数据
                extracted_count = self._count_records_in_response(response_data)
                if extracted_count < self.batch_size:
                    self.logger.debug(f"当前页只有 {extracted_count} 条记录，少于批大小 {self.batch_size}，停止分页")
                    break
                
                # 准备下一页
                start += self.batch_size
                page_count += 1
                
                # 添加请求间隔
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"分页提取第 {page_count + 1} 页时出错: {str(e)}")
                break
    
    def _count_records_in_response(self, response_data: Dict) -> int:
        """计算响应中的记录数量 - 子类可重写此方法
        
        Args:
            response_data: API响应数据
            
        Returns:
            记录数量
        """
        # 默认实现：尝试从常见的计数字段获取
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            
            # 尝试从不同的数据结构中获取count
            for key in ["league", "game", "team"]:
                if key in fantasy_content:
                    data_container = fantasy_content[key]
                    if isinstance(data_container, list) and len(data_container) > 1:
                        # 查找包含count的容器
                        for item in data_container:
                            if isinstance(item, dict):
                                for sub_key in ["players", "teams", "transactions", "leagues"]:
                                    if sub_key in item and "count" in item[sub_key]:
                                        return int(item[sub_key]["count"])
            
            return 0
        except Exception:
            return 0
    
    def _extract_from_container(self, container_data: Dict, container_key: str) -> List[Dict]:
        """从容器中提取数据项的通用方法
        
        Args:
            container_data: 包含数据的容器
            container_key: 容器键名（如 "players", "teams"）
            
        Returns:
            提取的数据项列表
        """
        items = []
        
        try:
            if container_key not in container_data:
                return items
            
            data_container = container_data[container_key]
            count = int(data_container.get("count", 0))
            
            for i in range(count):
                item_key = str(i)
                if item_key in data_container:
                    item_data = data_container[item_key]
                    items.append(item_data)
            
        except Exception as e:
            self.logger.error(f"从容器 {container_key} 提取数据时出错: {str(e)}")
        
        return items
    
    def _safe_get(self, data: Dict, *keys, default=None) -> Any:
        """安全地从嵌套字典中获取值
        
        Args:
            data: 数据字典
            *keys: 键路径
            default: 默认值
            
        Returns:
            获取的值或默认值
        """
        try:
            current = data
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
                    current = current[key]
                else:
                    return default
            return current
        except Exception:
            return default
    
    def _build_metadata(self, **kwargs) -> Dict[str, Any]:
        """构建提取元数据
        
        Args:
            **kwargs: 元数据键值对
            
        Returns:
            元数据字典
        """
        metadata = {
            "extractor": self.__class__.__name__,
            "timestamp": datetime.now().isoformat(),
            "batch_size": self.batch_size
        }
        metadata.update(kwargs)
        return metadata
    
    def validate_required_params(self, params: Dict, required_keys: List[str]) -> bool:
        """验证必需参数
        
        Args:
            params: 参数字典
            required_keys: 必需的键列表
            
        Returns:
            是否通过验证
        """
        missing_keys = [key for key in required_keys if key not in params or params[key] is None]
        
        if missing_keys:
            self.logger.error(f"缺少必需参数: {missing_keys}")
            return False
        
        return True 