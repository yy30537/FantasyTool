#!/usr/bin/env python3
"""
交易数据提取器
提取联盟的交易记录和球员交易详情
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class TransactionExtractor(BaseExtractor):
    """交易数据提取器
    
    提取联盟交易数据，包括：
    - Add/Drop交易
    - Trade交易
    - Waiver申请
    - Free Agent获取
    - 交易涉及的球员详情
    - Draft Pick交易
    
    对应旧脚本中的 fetch_complete_transactions_data()
    """
    
    def __init__(self, api_client: YahooAPIClient, batch_size: int = 50):
        """
        初始化交易数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
            batch_size: 分页大小，默认50
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="TransactionExtractor",
            extractor_type=ExtractorType.OPERATIONAL
        )
        self.batch_size = batch_size
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，包含：
                - league_key: 联盟键（必需）
                - max_count: 最大提取数量（可选，默认500）
                - transaction_types: 交易类型过滤（可选）
                
        Returns:
            List[Dict]: 提取的交易数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("TransactionExtractor requires 'league_key' parameter")
        
        max_count = params.get('max_count', 500)
        transaction_types = params.get('transaction_types', [])
        
        return self._extract_all_transactions(league_key, max_count, transaction_types)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["LeagueExtractor", "TeamExtractor"]  # 依赖联盟和团队数据
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return True  # 交易数据会持续增加
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 4 * 3600  # 4小时更新一次
    
    def _extract_all_transactions(self, league_key: str, max_count: int, transaction_types: List[str]) -> List[Dict[str, Any]]:
        """
        提取所有交易数据
        
        Args:
            league_key: 联盟键
            max_count: 最大提取数量
            transaction_types: 交易类型过滤
            
        Returns:
            List[Dict]: 所有交易数据
        """
        all_transactions = []
        start = 0
        
        self.logger.info(f"Starting transaction extraction for league {league_key}")
        
        while len(all_transactions) < max_count:
            current_count = min(self.batch_size, max_count - len(all_transactions))
            
            try:
                # 构建API URL
                url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions"
                url += f";count={current_count};start={start}?format=json"
                
                # 调用API
                response_data = self.client.get(url)
                if not response_data:
                    self.logger.warning(f"No response data for transactions start={start}")
                    break
                
                # 解析交易数据
                batch_transactions = self._parse_transactions_response(response_data, league_key)
                
                if not batch_transactions:
                    self.logger.info(f"No more transactions found, stopping at {len(all_transactions)} transactions")
                    break
                
                # 应用类型过滤
                if transaction_types:
                    batch_transactions = [
                        t for t in batch_transactions 
                        if t.get('type') in transaction_types
                    ]
                
                all_transactions.extend(batch_transactions)
                self.logger.info(f"Extracted {len(batch_transactions)} transactions (total: {len(all_transactions)})")
                
                # 如果返回的交易数量少于请求数量，说明已经到底了
                if len(batch_transactions) < current_count:
                    break
                
                start += current_count
                
                # 速率限制
                self.client.rate_limiter.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Failed to extract transactions batch start={start}: {e}")
                break
        
        self.logger.info(f"Transaction extraction completed: {len(all_transactions)} transactions")
        return all_transactions
    
    def _parse_transactions_response(self, response_data: Dict, league_key: str) -> List[Dict[str, Any]]:
        """
        解析交易API响应
        
        Args:
            response_data: API响应数据
            league_key: 联盟键
            
        Returns:
            List[Dict]: 解析的交易数据
        """
        transactions = []
        
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找transactions容器
            transactions_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            elif isinstance(league_data, dict) and "transactions" in league_data:
                transactions_container = league_data["transactions"]
            
            if not transactions_container:
                return transactions
            
            transactions_count = int(transactions_container.get("count", 0))
            
            for i in range(transactions_count):
                str_index = str(i)
                if str_index not in transactions_container:
                    continue
                
                transaction_container = transactions_container[str_index]
                if "transaction" not in transaction_container:
                    continue
                
                transaction_data = self._parse_single_transaction(
                    transaction_container["transaction"], league_key
                )
                if transaction_data:
                    transactions.append(transaction_data)
                    
        except Exception as e:
            self.logger.error(f"Failed to parse transactions response: {e}")
        
        return transactions
    
    def _parse_single_transaction(self, transaction_info: List, league_key: str) -> Optional[Dict[str, Any]]:
        """
        解析单个交易记录
        
        Args:
            transaction_info: 交易信息列表
            league_key: 联盟键
            
        Returns:
            Dict: 标准化的交易数据，如果解析失败则返回None
        """
        try:
            if not isinstance(transaction_info, list) or len(transaction_info) < 2:
                return None
            
            # 提取基本交易信息
            basic_info = transaction_info[0]
            transaction_details = transaction_info[1]
            
            # 解析基本信息
            transaction_basic = self._extract_transaction_basic_info(basic_info)
            if not transaction_basic:
                return None
            
            # 解析交易详情
            transaction_extended = self._extract_transaction_details(transaction_details)
            
            # 合并数据
            transaction_data = {
                **transaction_basic,
                **transaction_extended,
                "league_key": league_key
            }
            
            return transaction_data
            
        except Exception as e:
            self.logger.error(f"Failed to parse single transaction: {e}")
            return None
    
    def _extract_transaction_basic_info(self, basic_info) -> Optional[Dict[str, Any]]:
        """提取交易基本信息"""
        try:
            if isinstance(basic_info, list):
                # 合并列表中的所有字典
                merged_info = {}
                for item in basic_info:
                    if isinstance(item, dict):
                        merged_info.update(item)
                basic_info = merged_info
            
            if not isinstance(basic_info, dict):
                return None
            
            return {
                "transaction_key": basic_info.get("transaction_key"),
                "transaction_id": basic_info.get("transaction_id"),
                "type": basic_info.get("type"),
                "status": basic_info.get("status"),
                "timestamp": basic_info.get("timestamp")
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract transaction basic info: {e}")
            return None
    
    def _extract_transaction_details(self, transaction_details: Dict) -> Dict[str, Any]:
        """提取交易详细信息"""
        details = {}
        
        try:
            # 提取交易相关的团队信息（对于trade类型）
            if "trader_team_key" in transaction_details:
                details["trader_team_key"] = transaction_details["trader_team_key"]
            if "trader_team_name" in transaction_details:
                details["trader_team_name"] = transaction_details["trader_team_name"]
            if "tradee_team_key" in transaction_details:
                details["tradee_team_key"] = transaction_details["tradee_team_key"]
            if "tradee_team_name" in transaction_details:
                details["tradee_team_name"] = transaction_details["tradee_team_name"]
            
            # 提取draft picks信息
            if "picks" in transaction_details:
                details["picks_data"] = transaction_details["picks"]
            
            # 提取球员信息
            if "players" in transaction_details:
                details["players_data"] = transaction_details["players"]
                # 同时解析球员详情
                details["parsed_players"] = self._parse_transaction_players(transaction_details["players"])
            
        except Exception as e:
            self.logger.error(f"Failed to extract transaction details: {e}")
        
        return details
    
    def _parse_transaction_players(self, players_data: Dict) -> List[Dict[str, Any]]:
        """
        解析交易中的球员数据
        
        Args:
            players_data: 球员数据字典
            
        Returns:
            List[Dict]: 解析的球员交易详情
        """
        parsed_players = []
        
        try:
            if not isinstance(players_data, dict):
                return parsed_players
            
            players_count = int(players_data.get("count", 0))
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_data:
                    continue
                
                player_container = players_data[str_index]
                if "player" not in player_container:
                    continue
                
                player_info = player_container["player"]
                if not isinstance(player_info, list) or len(player_info) < 2:
                    continue
                
                # 提取球员基本信息
                player_basic = self._extract_player_basic_from_transaction(player_info[0])
                
                # 提取交易数据
                transaction_data = self._extract_player_transaction_data(player_info[1])
                
                if player_basic and transaction_data:
                    parsed_players.append({
                        **player_basic,
                        **transaction_data
                    })
                    
        except Exception as e:
            self.logger.error(f"Failed to parse transaction players: {e}")
        
        return parsed_players
    
    def _extract_player_basic_from_transaction(self, player_basic_info) -> Optional[Dict[str, Any]]:
        """从交易中提取球员基本信息"""
        try:
            player_data = {}
            
            if isinstance(player_basic_info, list):
                for item in player_basic_info:
                    if isinstance(item, dict):
                        player_data.update(item)
            elif isinstance(player_basic_info, dict):
                player_data = player_basic_info
            
            if not player_data:
                return None
            
            result = {
                "player_key": player_data.get("player_key"),
                "player_id": player_data.get("player_id"),
                "editorial_player_key": player_data.get("editorial_player_key")
            }
            
            # 提取球员姓名
            if "name" in player_data and isinstance(player_data["name"], dict):
                result["player_name"] = player_data["name"].get("full")
            
            # 提取其他基本信息
            for field in ["editorial_team_abbr", "display_position", "position_type"]:
                if field in player_data:
                    result[field] = player_data[field]
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to extract player basic from transaction: {e}")
            return None
    
    def _extract_player_transaction_data(self, transaction_container: Dict) -> Optional[Dict[str, Any]]:
        """提取球员的交易数据"""
        try:
            if "transaction_data" not in transaction_container:
                return None
            
            transaction_data_list = transaction_container["transaction_data"]
            if isinstance(transaction_data_list, list) and len(transaction_data_list) > 0:
                transaction_data = transaction_data_list[0]
            elif isinstance(transaction_data_list, dict):
                transaction_data = transaction_data_list
            else:
                return None
            
            return {
                "transaction_type": transaction_data.get("type"),
                "source_type": transaction_data.get("source_type"),
                "source_team_key": transaction_data.get("source_team_key"),
                "source_team_name": transaction_data.get("source_team_name"),
                "destination_type": transaction_data.get("destination_type"),
                "destination_team_key": transaction_data.get("destination_team_key"),
                "destination_team_name": transaction_data.get("destination_team_name")
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract player transaction data: {e}")
            return None
    
    def extract(self, league_key: str, max_count: int = 500, transaction_types: List[str] = None, **kwargs) -> ExtractionResult:
        """
        提取联盟交易数据
        
        Args:
            league_key: 联盟键
            max_count: 最大提取数量
            transaction_types: 交易类型过滤
            **kwargs: 其他参数
            
        Returns:
            ExtractionResult: 包含交易数据的提取结果
        """
        try:
            transactions_data = self._extract_data(
                league_key=league_key,
                max_count=max_count,
                transaction_types=transaction_types or [],
                **kwargs
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=transactions_data,
                total_records=len(transactions_data),
                message=f"Successfully extracted {len(transactions_data)} transactions for league {league_key}"
            )
            
        except Exception as e:
            self.logger.error(f"TransactionExtractor failed for league {league_key}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_recent_transactions(self, league_key: str, count: int = 50) -> ExtractionResult:
        """
        提取最近的交易数据
        
        Args:
            league_key: 联盟键
            count: 提取数量
            
        Returns:
            ExtractionResult: 包含最近交易数据的提取结果
        """
        return self.extract(league_key=league_key, max_count=count)
    
    def extract_trade_transactions(self, league_key: str, max_count: int = 200) -> ExtractionResult:
        """
        只提取trade类型的交易
        
        Args:
            league_key: 联盟键
            max_count: 最大提取数量
            
        Returns:
            ExtractionResult: 包含trade交易数据的提取结果
        """
        return self.extract(league_key=league_key, max_count=max_count, transaction_types=["trade"]) 