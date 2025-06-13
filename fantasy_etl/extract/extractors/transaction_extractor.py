"""
Transaction Extractor - 交易数据提取器

负责提取联盟交易相关的原始数据，包括球员交易、waiver claims等
"""
from typing import Dict, List, Optional
import logging

from .base_extractor import BaseExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class TransactionExtractor(BaseExtractor):
    """交易数据提取器 - 负责提取交易相关的原始数据"""
    
    def extract_league_transactions(self, league_key: str, start: int = 0, count: int = 25) -> ExtractionResult:
        """提取联盟交易数据（分页）
        
        Args:
            league_key: 联盟键
            start: 分页起始位置
            count: 每页记录数
            
        Returns:
            ExtractionResult: 包含transactions数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_transactions")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的交易数据 (起始位置: {start}, 数量: {count})")
            
            # 构建端点
            endpoint = f"league/{league_key}/transactions"
            params = []
            if start > 0:
                params.append(f"start={start}")
            params.append(f"count={count}")
            
            if params:
                endpoint += f";{';'.join(params)}"
            endpoint += "?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的交易数据",
                    metadata=self._build_metadata(
                        operation="extract_league_transactions",
                        league_key=league_key,
                        start=start,
                        count=count
                    )
                )
            
            # 提取transactions数据
            transactions_data = self._extract_transactions_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的 {len(transactions_data)} 个交易记录")
            
            return ExtractionResult(
                success=True,
                data=transactions_data,
                total_records=len(transactions_data),
                metadata=self._build_metadata(
                    operation="extract_league_transactions",
                    league_key=league_key,
                    start=start,
                    count=count,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 交易数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_transactions",
                    league_key=league_key,
                    start=start,
                    count=count
                )
            )
    
    def extract_all_league_transactions(self, league_key: str, max_pages: int = 100) -> ExtractionResult:
        """提取联盟所有交易数据（自动分页）
        
        Args:
            league_key: 联盟键
            max_pages: 最大页数限制
            
        Returns:
            ExtractionResult: 包含所有transactions数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_all_league_transactions")
                )
            
            self.logger.info(f"开始分页提取联盟 {league_key} 的所有交易数据")
            
            all_transactions = []
            endpoint_template = f"league/{league_key}/transactions"
            
            # 使用基类的分页提取方法
            for page_data in self.extract_paginated(
                endpoint_template + ";start={start};count=" + str(self.batch_size) + "?format=json",
                max_pages=max_pages,
                start_param="start"
            ):
                transactions_data = self._extract_transactions_from_response(page_data)
                all_transactions.extend(transactions_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的总共 {len(all_transactions)} 个交易记录")
            
            return ExtractionResult(
                success=True,
                data=all_transactions,
                total_records=len(all_transactions),
                metadata=self._build_metadata(
                    operation="extract_all_league_transactions",
                    league_key=league_key,
                    total_pages=max_pages
                )
            )
            
        except Exception as e:
            self.logger.error(f"分页提取联盟 {league_key} 交易数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_all_league_transactions",
                    league_key=league_key
                )
            )
    
    def extract_team_transactions(self, team_key: str, start: int = 0, count: int = 25) -> ExtractionResult:
        """提取团队交易数据（分页）
        
        Args:
            team_key: 团队键
            start: 分页起始位置
            count: 每页记录数
            
        Returns:
            ExtractionResult: 包含team transactions数据的提取结果
        """
        try:
            if not team_key:
                return ExtractionResult(
                    success=False,
                    error="team_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_team_transactions")
                )
            
            self.logger.info(f"开始提取团队 {team_key} 的交易数据 (起始位置: {start}, 数量: {count})")
            
            # 构建端点
            endpoint = f"team/{team_key}/transactions"
            params = []
            if start > 0:
                params.append(f"start={start}")
            params.append(f"count={count}")
            
            if params:
                endpoint += f";{';'.join(params)}"
            endpoint += "?format=json"
            
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取团队 {team_key} 的交易数据",
                    metadata=self._build_metadata(
                        operation="extract_team_transactions",
                        team_key=team_key,
                        start=start,
                        count=count
                    )
                )
            
            # 提取transactions数据
            transactions_data = self._extract_team_transactions_from_response(response_data)
            
            self.logger.info(f"成功提取团队 {team_key} 的 {len(transactions_data)} 个交易记录")
            
            return ExtractionResult(
                success=True,
                data=transactions_data,
                total_records=len(transactions_data),
                metadata=self._build_metadata(
                    operation="extract_team_transactions",
                    team_key=team_key,
                    start=start,
                    count=count,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取团队 {team_key} 交易数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_team_transactions",
                    team_key=team_key,
                    start=start,
                    count=count
                )
            )
    
    def extract(self, operation: str, **kwargs) -> ExtractionResult:
        """统一的提取接口
        
        Args:
            operation: 操作类型 ('league_transactions', 'all_league_transactions', 'team_transactions')
            **kwargs: 操作参数
            
        Returns:
            ExtractionResult: 提取结果
        """
        if operation == "league_transactions":
            return self.extract_league_transactions(
                kwargs.get("league_key"),
                kwargs.get("start", 0),
                kwargs.get("count", 25)
            )
        elif operation == "all_league_transactions":
            return self.extract_all_league_transactions(
                kwargs.get("league_key"),
                kwargs.get("max_pages", 100)
            )
        elif operation == "team_transactions":
            return self.extract_team_transactions(
                kwargs.get("team_key"),
                kwargs.get("start", 0),
                kwargs.get("count", 25)
            )
        else:
            return ExtractionResult(
                success=False,
                error=f"不支持的操作类型: {operation}",
                metadata=self._build_metadata(operation=operation)
            )
    
    def _extract_transactions_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取transactions数据（联盟级别）"""
        transactions = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
            # 查找transactions容器
            transactions_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            elif isinstance(league_data, dict) and "transactions" in league_data:
                transactions_container = league_data["transactions"]
            
            if not transactions_container:
                self.logger.warning("响应中未找到transactions容器")
                return transactions
            
            transactions_count = int(transactions_container.get("count", 0))
            
            for i in range(transactions_count):
                str_index = str(i)
                if str_index not in transactions_container:
                    continue
                
                transaction_container = transactions_container[str_index]
                if "transaction" not in transaction_container:
                    continue
                
                transaction_data = transaction_container["transaction"]
                transaction_info = self._process_transaction_data(transaction_data)
                
                if transaction_info:
                    transactions.append(transaction_info)
        
        except Exception as e:
            self.logger.error(f"解析交易数据时出错: {str(e)}")
        
        return transactions
    
    def _extract_team_transactions_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取transactions数据（团队级别）"""
        transactions = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            team_data = fantasy_content.get("team")
            
            # 查找transactions容器
            transactions_container = None
            if isinstance(team_data, list):
                for item in team_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            elif isinstance(team_data, dict) and "transactions" in team_data:
                transactions_container = team_data["transactions"]
            
            if not transactions_container:
                self.logger.warning("响应中未找到team transactions容器")
                return transactions
            
            transactions_count = int(transactions_container.get("count", 0))
            
            for i in range(transactions_count):
                str_index = str(i)
                if str_index not in transactions_container:
                    continue
                
                transaction_container = transactions_container[str_index]
                if "transaction" not in transaction_container:
                    continue
                
                transaction_data = transaction_container["transaction"]
                transaction_info = self._process_transaction_data(transaction_data)
                
                if transaction_info:
                    transactions.append(transaction_info)
        
        except Exception as e:
            self.logger.error(f"解析团队交易数据时出错: {str(e)}")
        
        return transactions
    
    def _process_transaction_data(self, transaction_data) -> Optional[Dict]:
        """处理交易数据，提取关键信息"""
        try:
            transaction_dict = {}
            
            # 处理transaction数据结构
            if isinstance(transaction_data, list):
                for item in transaction_data:
                    if isinstance(item, dict):
                        transaction_dict.update(item)
            elif isinstance(transaction_data, dict):
                transaction_dict.update(transaction_data)
            
            # 验证必要字段
            if not transaction_dict.get("transaction_key"):
                return None
            
            return transaction_dict
            
        except Exception as e:
            self.logger.error(f"处理交易数据时出错: {str(e)}")
            return None
    
    def _count_records_in_response(self, response_data: Dict) -> int:
        """重写基类方法：计算transactions响应中的记录数量"""
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            
            # 尝试从league数据中获取
            league_data = fantasy_content.get("league")
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        return int(item["transactions"].get("count", 0))
            elif isinstance(league_data, dict) and "transactions" in league_data:
                return int(league_data["transactions"].get("count", 0))
            
            # 尝试从team数据中获取
            team_data = fantasy_content.get("team")
            if isinstance(team_data, list):
                for item in team_data:
                    if isinstance(item, dict) and "transactions" in item:
                        return int(item["transactions"].get("count", 0))
            elif isinstance(team_data, dict) and "transactions" in team_data:
                return int(team_data["transactions"].get("count", 0))
            
            return 0
        except Exception:
            return 0 