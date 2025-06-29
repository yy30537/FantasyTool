"""
Transaction Data Loader - Handles transactions and transaction players
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json

from .base_loader import BaseLoader, LoadResult
from ..database.models import Transaction, TransactionPlayer


class TransactionLoader(BaseLoader):
    """Loader for transaction information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate transaction record"""
        required_fields = ['transaction_key', 'transaction_id', 'league_key', 'type', 'status']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> Transaction:
        """Create Transaction model instance"""
        return Transaction(
            transaction_key=record['transaction_key'],
            transaction_id=record['transaction_id'],
            league_key=record['league_key'],
            type=record['type'],
            status=record['status'],
            timestamp=record.get('timestamp'),
            trader_team_key=record.get('trader_team_key'),
            trader_team_name=record.get('trader_team_name'),
            tradee_team_key=record.get('tradee_team_key'),
            tradee_team_name=record.get('tradee_team_name'),
            picks_data=record.get('picks_data'),
            players_data=record.get('players_data')
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for transaction"""
        return record['transaction_key']
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess transaction record"""
        # Handle JSON fields - ensure they are properly formatted
        json_fields = ['picks_data', 'players_data']
        
        for field in json_fields:
            if field in record and record[field]:
                if isinstance(record[field], str):
                    try:
                        record[field] = json.loads(record[field])
                    except json.JSONDecodeError:
                        record[field] = None
                elif not isinstance(record[field], (dict, list)):
                    record[field] = None
        
        return record


class TransactionPlayerLoader(BaseLoader):
    """Loader for transaction player information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate transaction player record"""
        required_fields = ['transaction_key', 'player_key', 'player_id', 'transaction_type']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> TransactionPlayer:
        """Create TransactionPlayer model instance"""
        return TransactionPlayer(
            transaction_key=record['transaction_key'],
            player_key=record['player_key'],
            player_id=record['player_id'],
            player_name=record.get('player_name'),
            editorial_team_abbr=record.get('editorial_team_abbr'),
            display_position=record.get('display_position'),
            position_type=record.get('position_type'),
            transaction_type=record['transaction_type'],
            source_type=record.get('source_type'),
            source_team_key=record.get('source_team_key'),
            source_team_name=record.get('source_team_name'),
            destination_type=record.get('destination_type'),
            destination_team_key=record.get('destination_team_key'),
            destination_team_name=record.get('destination_team_name')
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for transaction player"""
        return f"{record['transaction_key']}_{record['player_key']}"


class CompleteTransactionLoader:
    """Complete transaction data loader that handles transactions and their players"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loaders
        self.transaction_loader = TransactionLoader(connection_manager, batch_size)
        self.transaction_player_loader = TransactionPlayerLoader(connection_manager, batch_size)
    
    def load_transaction_data(self, transaction_data: Dict[str, Any]) -> LoadResult:
        """Load complete transaction data including players"""
        total_result = LoadResult()
        
        # Load basic transaction info
        transaction_result = self.transaction_loader.load([transaction_data])
        total_result.combine(transaction_result)
        
        # Load transaction players if present
        if 'players' in transaction_data:
            players = self._extract_transaction_players(
                transaction_data['transaction_key'], 
                transaction_data['players']
            )
            if players:
                player_result = self.transaction_player_loader.load(players)
                total_result.combine(player_result)
        
        return total_result
    
    def load_transactions_batch(self, transactions_data: List[Dict[str, Any]]) -> LoadResult:
        """Load multiple transactions with their players"""
        total_result = LoadResult()
        
        for transaction_data in transactions_data:
            transaction_result = self.load_transaction_data(transaction_data)
            total_result.combine(transaction_result)
        
        return total_result
    
    def _extract_transaction_players(self, transaction_key: str, players_data: Any) -> List[Dict[str, Any]]:
        """Extract transaction players from transaction data"""
        players = []
        
        try:
            # Handle different formats of players data
            if isinstance(players_data, dict):
                # Yahoo API format with count and numbered keys
                for key, player_data in players_data.items():
                    if key == "count":
                        continue
                    
                    if not isinstance(player_data, dict) or "player" not in player_data:
                        continue
                    
                    player_info_list = player_data["player"]
                    if not isinstance(player_info_list, list) or len(player_info_list) < 2:
                        continue
                    
                    # Extract player basic info
                    player_info = player_info_list[0]
                    transaction_info_container = player_info_list[1]
                    
                    if not isinstance(transaction_info_container, dict) or "transaction_data" not in transaction_info_container:
                        continue
                    
                    # Parse player basic information
                    player_record = self._parse_player_info(player_info)
                    if not player_record:
                        continue
                    
                    # Parse transaction data
                    transaction_info_data = transaction_info_container["transaction_data"]
                    if isinstance(transaction_info_data, list) and len(transaction_info_data) > 0:
                        transaction_info = transaction_info_data[0]
                    elif isinstance(transaction_info_data, dict):
                        transaction_info = transaction_info_data
                    else:
                        continue
                    
                    # Combine player and transaction info
                    player_record.update({
                        'transaction_key': transaction_key,
                        'transaction_type': transaction_info.get('type'),
                        'source_type': transaction_info.get('source_type'),
                        'source_team_key': transaction_info.get('source_team_key'),
                        'source_team_name': transaction_info.get('source_team_name'),
                        'destination_type': transaction_info.get('destination_type'),
                        'destination_team_key': transaction_info.get('destination_team_key'),
                        'destination_team_name': transaction_info.get('destination_team_name')
                    })
                    
                    players.append(player_record)
            
            elif isinstance(players_data, list):
                # Direct list format
                for player_data in players_data:
                    player_record = self._parse_player_data(player_data)
                    if player_record:
                        player_record['transaction_key'] = transaction_key
                        players.append(player_record)
        
        except Exception as e:
            print(f"Error extracting transaction players for {transaction_key}: {e}")
        
        return players
    
    def _parse_player_info(self, player_info: Any) -> Optional[Dict[str, Any]]:
        """Parse player basic information from various formats"""
        try:
            player_record = {}
            
            if isinstance(player_info, list):
                # Flatten list of dictionaries
                for item in player_info:
                    if isinstance(item, dict):
                        if "player_key" in item:
                            player_record["player_key"] = item["player_key"]
                        elif "player_id" in item:
                            player_record["player_id"] = item["player_id"]
                        elif "name" in item and isinstance(item["name"], dict):
                            player_record["player_name"] = item["name"].get("full")
                        elif "editorial_team_abbr" in item:
                            player_record["editorial_team_abbr"] = item["editorial_team_abbr"]
                        elif "display_position" in item:
                            player_record["display_position"] = item["display_position"]
                        elif "position_type" in item:
                            player_record["position_type"] = item["position_type"]
            
            elif isinstance(player_info, dict):
                player_record["player_key"] = player_info.get("player_key")
                player_record["player_id"] = player_info.get("player_id")
                
                if "name" in player_info and isinstance(player_info["name"], dict):
                    player_record["player_name"] = player_info["name"].get("full")
                
                player_record["editorial_team_abbr"] = player_info.get("editorial_team_abbr")
                player_record["display_position"] = player_info.get("display_position")
                player_record["position_type"] = player_info.get("position_type")
            
            # Validate required fields
            if not player_record.get("player_key") or not player_record.get("player_id"):
                return None
            
            return player_record
        
        except Exception as e:
            print(f"Error parsing player info: {e}")
            return None
    
    def _parse_player_data(self, player_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse player data from direct format"""
        try:
            if not isinstance(player_data, dict):
                return None
            
            required_fields = ['player_key', 'player_id', 'transaction_type']
            if not all(field in player_data for field in required_fields):
                return None
            
            return {
                'player_key': player_data['player_key'],
                'player_id': player_data['player_id'],
                'player_name': player_data.get('player_name'),
                'editorial_team_abbr': player_data.get('editorial_team_abbr'),
                'display_position': player_data.get('display_position'),
                'position_type': player_data.get('position_type'),
                'transaction_type': player_data['transaction_type'],
                'source_type': player_data.get('source_type'),
                'source_team_key': player_data.get('source_team_key'),
                'source_team_name': player_data.get('source_team_name'),
                'destination_type': player_data.get('destination_type'),
                'destination_team_key': player_data.get('destination_team_key'),
                'destination_team_name': player_data.get('destination_team_name')
            }
        
        except Exception as e:
            print(f"Error parsing direct player data: {e}")
            return None 