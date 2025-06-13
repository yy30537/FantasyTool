"""
Transaction Transformer - 交易数据转换器

负责将Yahoo Fantasy API返回的交易相关原始数据转换为标准化格式
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .base_transformer import BaseTransformer, TransformResult

logger = logging.getLogger(__name__)


class TransactionTransformer(BaseTransformer):
    """交易数据转换器"""
    
    def transform(self, raw_data: Dict) -> TransformResult:
        """转换交易基本信息数据"""
        result = TransformResult(success=False)
        
        try:
            # 验证输入数据
            if not self._validate_transaction_structure(raw_data, result):
                return result
            
            # 提取和转换交易基本信息
            transaction_info = self._extract_transaction_basic_info(raw_data, result)
            if not transaction_info:
                result.add_error("transaction_info", raw_data, "无法提取交易基本信息")
                return result
            
            # 提取交易球员信息
            transaction_players = self._extract_transaction_players(raw_data, result)
            
            # 标准化交易信息
            standardized_transaction = self._standardize_transaction_info(transaction_info, result)
            
            result.data = {
                "transaction": standardized_transaction,
                "players": transaction_players or []
            }
            result.success = True
            result.metadata["transformation_time"] = datetime.now()
            result.metadata["players_count"] = len(transaction_players) if transaction_players else 0
            
        except Exception as e:
            result.add_error("transform", raw_data, f"转换异常: {str(e)}")
            
        return result
    
    def transform_transaction_batch(self, transactions_data: List[Dict]) -> TransformResult:
        """批量转换交易数据"""
        result = TransformResult(success=False)
        
        try:
            transformed_transactions = []
            
            for transaction_data in transactions_data:
                single_result = self.transform(transaction_data)
                if single_result.success:
                    transformed_transactions.append(single_result.data)
                else:
                    # 将单个交易的错误添加到批量结果中
                    result.errors.extend(single_result.errors)
                    result.warnings.extend(single_result.warnings)
            
            result.data = {
                "transactions": transformed_transactions,
                "total_count": len(transformed_transactions),
                "success_count": len(transformed_transactions),
                "failed_count": len(transactions_data) - len(transformed_transactions)
            }
            result.success = len(transformed_transactions) > 0
            result.metadata["transformation_time"] = datetime.now()
            
        except Exception as e:
            result.add_error("batch_transform", transactions_data, f"批量转换异常: {str(e)}")
            
        return result
    
    def _validate_transaction_structure(self, raw_data: Dict, result: TransformResult) -> bool:
        """验证交易数据结构"""
        if not isinstance(raw_data, dict):
            result.add_error("raw_data", raw_data, "原始数据必须是字典格式")
            return False
        
        # 检查是否包含交易基本信息
        if "transaction_key" not in raw_data and "transaction_id" not in raw_data:
            result.add_error("transaction_key", raw_data, "缺少交易标识信息")
            return False
        
        return True
    
    def _extract_transaction_basic_info(self, raw_data: Dict, result: TransformResult) -> Optional[Dict]:
        """提取交易基本信息"""
        try:
            transaction_info = {}
            
            # 提取基本标识信息
            transaction_info["transaction_key"] = self.clean_string(raw_data.get("transaction_key"))
            transaction_info["transaction_id"] = self.clean_string(raw_data.get("transaction_id"))
            
            # 提取交易基本信息
            transaction_info["type"] = self.clean_string(raw_data.get("type"))
            transaction_info["status"] = self.clean_string(raw_data.get("status"))
            transaction_info["timestamp"] = self.clean_string(raw_data.get("timestamp"))
            
            # 解析时间戳
            if transaction_info["timestamp"]:
                try:
                    # Yahoo API时间戳格式通常是Unix时间戳
                    timestamp_int = int(transaction_info["timestamp"])
                    transaction_info["transaction_date"] = datetime.fromtimestamp(timestamp_int).date()
                    transaction_info["transaction_datetime"] = datetime.fromtimestamp(timestamp_int)
                except (ValueError, TypeError):
                    transaction_info["transaction_date"] = None
                    transaction_info["transaction_datetime"] = None
                    result.add_warning("timestamp", transaction_info["timestamp"], "时间戳解析失败")
            else:
                transaction_info["transaction_date"] = None
                transaction_info["transaction_datetime"] = None
            
            # 提取其他交易信息
            transaction_info["faab_bid"] = self.safe_int(raw_data.get("faab_bid"))
            transaction_info["trade_note"] = self.clean_string(raw_data.get("trade_note"))
            
            return transaction_info
            
        except Exception as e:
            result.add_error("extract_basic", raw_data, f"提取交易基本信息失败: {str(e)}")
            return None
    
    def _extract_transaction_players(self, raw_data: Dict, result: TransformResult) -> Optional[List[Dict]]:
        """提取交易球员信息"""
        try:
            players_list = []
            
            # 查找players容器
            players_container = raw_data.get("players")
            if not players_container:
                return players_list
            
            players_count = self.safe_int(players_container.get("count", 0), 0)
            
            for i in range(players_count):
                str_index = str(i)
                if str_index not in players_container:
                    continue
                
                player_container = players_container[str_index]
                if "player" not in player_container:
                    continue
                
                player_data = player_container["player"]
                
                # 提取球员交易信息
                player_transaction = self._extract_player_transaction_info(player_data, result)
                if player_transaction:
                    players_list.append(player_transaction)
            
            return players_list
            
        except Exception as e:
            result.add_error("extract_players", raw_data, f"提取交易球员信息失败: {str(e)}")
            return None
    
    def _extract_player_transaction_info(self, player_data: Any, result: TransformResult) -> Optional[Dict]:
        """从player数据中提取交易相关信息"""
        try:
            player_transaction = {}
            
            # player_data通常是一个包含多个部分的列表
            if isinstance(player_data, list) and len(player_data) > 0:
                # 第一部分通常包含球员基本信息
                player_basic_info = player_data[0]
                
                if isinstance(player_basic_info, list):
                    # 合并所有字典项
                    merged_info = {}
                    for item in player_basic_info:
                        if isinstance(item, dict):
                            merged_info.update(item)
                    player_basic_info = merged_info
                
                if isinstance(player_basic_info, dict):
                    player_transaction["player_key"] = self.clean_string(player_basic_info.get("player_key"))
                    player_transaction["player_id"] = self.clean_string(player_basic_info.get("player_id"))
                    player_transaction["editorial_player_key"] = self.clean_string(player_basic_info.get("editorial_player_key"))
                    
                    # 提取球员姓名信息
                    if "name" in player_basic_info:
                        name_info = player_basic_info["name"]
                        if isinstance(name_info, dict):
                            player_transaction["full_name"] = self.clean_string(name_info.get("full"))
                        elif isinstance(name_info, str):
                            player_transaction["full_name"] = self.clean_string(name_info)
                
                # 第二部分可能包含交易特定信息
                if len(player_data) > 1:
                    transaction_specific = player_data[1]
                    if isinstance(transaction_specific, dict):
                        # 提取交易类型和目标团队信息
                        player_transaction["transaction_data"] = transaction_specific.get("transaction_data", {})
                        
                        # 从transaction_data中提取具体信息
                        trans_data = player_transaction["transaction_data"]
                        if isinstance(trans_data, dict):
                            player_transaction["type"] = self.clean_string(trans_data.get("type"))
                            player_transaction["source_type"] = self.clean_string(trans_data.get("source_type"))
                            player_transaction["destination_type"] = self.clean_string(trans_data.get("destination_type"))
                            player_transaction["source_team_key"] = self.clean_string(trans_data.get("source_team_key"))
                            player_transaction["source_team_name"] = self.clean_string(trans_data.get("source_team_name"))
                            player_transaction["destination_team_key"] = self.clean_string(trans_data.get("destination_team_key"))
                            player_transaction["destination_team_name"] = self.clean_string(trans_data.get("destination_team_name"))
            
            # 验证必需字段
            if not player_transaction.get("player_key"):
                result.add_warning("player_key", player_data, "球员缺少player_key")
                return None
            
            return player_transaction
            
        except Exception as e:
            result.add_error("extract_player_transaction", player_data, f"提取球员交易信息失败: {str(e)}")
            return None
    
    def _standardize_transaction_info(self, transaction_info: Dict, result: TransformResult) -> Dict:
        """标准化交易信息"""
        try:
            standardized = {}
            
            # 必需字段
            standardized["transaction_key"] = transaction_info.get("transaction_key")
            standardized["transaction_id"] = transaction_info.get("transaction_id")
            
            # 交易基本信息
            standardized["type"] = transaction_info.get("type")
            standardized["status"] = transaction_info.get("status")
            standardized["timestamp"] = transaction_info.get("timestamp")
            standardized["transaction_date"] = transaction_info.get("transaction_date")
            standardized["transaction_datetime"] = transaction_info.get("transaction_datetime")
            
            # 其他信息
            standardized["faab_bid"] = transaction_info.get("faab_bid")
            standardized["trade_note"] = transaction_info.get("trade_note")
            
            # 添加时间戳
            standardized["last_updated"] = datetime.now()
            
            return standardized
            
        except Exception as e:
            result.add_error("standardize", transaction_info, f"标准化交易信息失败: {str(e)}")
            return {}
    
    def get_transaction_type_mapping(self) -> Dict[str, str]:
        """获取交易类型映射"""
        return {
            "add": "添加球员",
            "drop": "释放球员", 
            "add/drop": "添加/释放",
            "trade": "交易",
            "commish": "专员操作"
        }
    
    def get_transaction_status_mapping(self) -> Dict[str, str]:
        """获取交易状态映射"""
        return {
            "successful": "成功",
            "failed": "失败",
            "pending": "待处理"
        }
    
    def batch_transform_transactions(self, transactions_list: List[Dict]) -> List[TransformResult]:
        """批量转换交易数据"""
        results = []
        
        for transaction_data in transactions_list:
            result = self.transform(transaction_data)
            results.append(result)
        
        return results 