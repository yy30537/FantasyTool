"""
Date Dimension Data Loader - Handles date dimension information
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta

from .base_loader import BaseLoader, LoadResult
from ..database.models import DateDimension


class DateDimensionLoader(BaseLoader):
    """Loader for date dimension information"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate date dimension record"""
        required_fields = ['date', 'league_key', 'season']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> DateDimension:
        """Create DateDimension model instance"""
        return DateDimension(
            date=record['date'],
            league_key=record['league_key'],
            season=record['season']
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for date dimension record"""
        date_str = record['date'].strftime('%Y-%m-%d') if isinstance(record['date'], date) else str(record['date'])
        return f"{date_str}_{record['league_key']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess date dimension record"""
        # Handle date field conversion
        if 'date' in record and record['date']:
            if isinstance(record['date'], str):
                try:
                    record['date'] = datetime.strptime(record['date'], '%Y-%m-%d').date()
                except ValueError:
                    print(f"Invalid date format: {record['date']}")
                    return record
        
        return record


class CompleteDateLoader:
    """Complete date dimension loader that handles date information"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loader
        self.date_loader = DateDimensionLoader(connection_manager, batch_size)
    
    def load_date_data(self, date_data: List[Dict[str, Any]]) -> LoadResult:
        """Load date dimension data"""
        return self.date_loader.load(date_data)
    
    def load_season_dates(self, league_key: str, season: str, 
                         start_date: date, end_date: date) -> LoadResult:
        """Load all dates in a season range"""
        date_records = []
        current_date = start_date
        
        while current_date <= end_date:
            date_record = {
                'date': current_date,
                'league_key': league_key,
                'season': season
            }
            date_records.append(date_record)
            current_date += timedelta(days=1)
        
        return self.date_loader.load(date_records)
    
    def load_dates_from_league_info(self, league_key: str, season: str,
                                   league_start_date: str, league_end_date: str) -> LoadResult:
        """Load dates based on league start/end dates"""
        try:
            start_date = datetime.strptime(league_start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(league_end_date, '%Y-%m-%d').date()
            
            return self.load_season_dates(league_key, season, start_date, end_date)
        
        except ValueError as e:
            print(f"Error parsing league dates: {e}")
            return LoadResult()
    
    def load_current_season_dates(self, league_key: str, season: str) -> LoadResult:
        """Load dates for current NBA season (Oct-Apr)"""
        try:
            # Parse season year (e.g., '2024' -> 2024-25 season)
            season_year = int(season)
            
            # NBA season typically runs from October to April
            start_date = date(season_year, 10, 1)  # October 1st
            end_date = date(season_year + 1, 4, 30)  # April 30th next year
            
            return self.load_season_dates(league_key, season, start_date, end_date)
        
        except ValueError as e:
            print(f"Error parsing season year: {e}")
            return LoadResult()
    
    def load_date_range(self, league_key: str, season: str,
                       start_date_str: str, end_date_str: str) -> LoadResult:
        """Load a specific date range"""
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            
            return self.load_season_dates(league_key, season, start_date, end_date)
        
        except ValueError as e:
            print(f"Error parsing date range: {e}")
            return LoadResult() 