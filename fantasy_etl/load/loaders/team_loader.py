"""
Team Data Loader - Handles teams and managers
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from .base_loader import BaseLoader, LoadResult
from ..database.models import Team, Manager


class TeamLoader(BaseLoader):
    """Loader for team information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate team record"""
        required_fields = ['team_key', 'team_id', 'league_key', 'name']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> Team:
        """Create Team model instance"""
        return Team(
            team_key=record['team_key'],
            team_id=record['team_id'],
            league_key=record['league_key'],
            name=record['name'],
            url=record.get('url'),
            team_logo_url=record.get('team_logo_url'),
            division_id=record.get('division_id'),
            waiver_priority=self._safe_int(record.get('waiver_priority')),
            faab_balance=record.get('faab_balance'),
            number_of_moves=self._safe_int(record.get('number_of_moves', 0)),
            number_of_trades=self._safe_int(record.get('number_of_trades', 0)),
            roster_adds_week=str(record.get('roster_adds_week', '')),
            roster_adds_value=record.get('roster_adds_value'),
            clinched_playoffs=self._safe_bool(record.get('clinched_playoffs', False)),
            has_draft_grade=self._safe_bool(record.get('has_draft_grade', False))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for team"""
        return record['team_key']
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess team record"""
        # Handle boolean fields that might come as strings or integers
        bool_fields = ['clinched_playoffs', 'has_draft_grade']
        
        for field in bool_fields:
            if field in record:
                record[field] = self._safe_bool(record[field])
        
        # Handle integer fields
        int_fields = ['waiver_priority', 'number_of_moves', 'number_of_trades']
        
        for field in int_fields:
            if field in record:
                record[field] = self._safe_int(record[field])
        
        # Ensure roster_adds_week is string
        if 'roster_adds_week' in record:
            record['roster_adds_week'] = str(record['roster_adds_week'])
        
        return record


class ManagerLoader(BaseLoader):
    """Loader for manager information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate manager record"""
        required_fields = ['manager_id', 'team_key', 'nickname', 'guid']
        return all(field in record and record[field] for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> Manager:
        """Create Manager model instance"""
        return Manager(
            manager_id=record['manager_id'],
            team_key=record['team_key'],
            nickname=record['nickname'],
            guid=record['guid'],
            is_commissioner=self._safe_bool(record.get('is_commissioner', False)),
            email=record.get('email'),
            image_url=record.get('image_url'),
            felo_score=record.get('felo_score'),
            felo_tier=record.get('felo_tier')
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for manager"""
        return f"{record['manager_id']}_{record['team_key']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess manager record"""
        # Handle boolean fields
        if 'is_commissioner' in record:
            record['is_commissioner'] = self._safe_bool(record['is_commissioner'])
        
        return record


class CompleteTeamLoader:
    """Complete team data loader that handles teams and their managers"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loaders
        self.team_loader = TeamLoader(connection_manager, batch_size)
        self.manager_loader = ManagerLoader(connection_manager, batch_size)
    
    def load_team_data(self, team_data: Dict[str, Any]) -> LoadResult:
        """Load complete team data including managers"""
        total_result = LoadResult()
        
        # Load basic team info
        team_result = self.team_loader.load([team_data])
        total_result.combine(team_result)
        
        # Load managers if present
        if 'managers' in team_data:
            managers = self._extract_managers(team_data['team_key'], team_data['managers'])
            if managers:
                manager_result = self.manager_loader.load(managers)
                total_result.combine(manager_result)
        
        return total_result
    
    def load_teams_batch(self, teams_data: List[Dict[str, Any]]) -> LoadResult:
        """Load multiple teams with their managers"""
        total_result = LoadResult()
        
        for team_data in teams_data:
            team_result = self.load_team_data(team_data)
            total_result.combine(team_result)
        
        return total_result
    
    def _extract_managers(self, team_key: str, managers_data: List[Any]) -> List[Dict[str, Any]]:
        """Extract managers from team data"""
        managers = []
        
        try:
            for manager_data in managers_data:
                # Handle nested structure - managers might be wrapped in 'manager' key
                if isinstance(manager_data, dict):
                    if 'manager' in manager_data:
                        manager_info = manager_data['manager']
                    else:
                        manager_info = manager_data
                else:
                    continue
                
                if not manager_info.get('manager_id'):
                    continue
                
                manager_record = {
                    'manager_id': manager_info['manager_id'],
                    'team_key': team_key,
                    'nickname': manager_info.get('nickname', ''),
                    'guid': manager_info.get('guid', ''),
                    'is_commissioner': manager_info.get('is_commissioner', False),
                    'email': manager_info.get('email'),
                    'image_url': manager_info.get('image_url'),
                    'felo_score': manager_info.get('felo_score'),
                    'felo_tier': manager_info.get('felo_tier')
                }
                
                managers.append(manager_record)
        
        except Exception as e:
            print(f"Error extracting managers for team {team_key}: {e}")
        
        return managers 