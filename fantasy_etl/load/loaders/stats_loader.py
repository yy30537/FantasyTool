"""
Stats Data Loader - Handles player and team statistics
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, date
from decimal import Decimal

from .base_loader import BaseLoader, LoadResult
from ..database.models import PlayerDailyStats, PlayerSeasonStats, TeamStatsWeekly


class PlayerDailyStatsLoader(BaseLoader):
    """Loader for player daily statistics"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate player daily stats record"""
        required_fields = ['player_key', 'editorial_player_key', 'league_key', 'season', 'date']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> PlayerDailyStats:
        """Create PlayerDailyStats model instance"""
        return PlayerDailyStats(
            player_key=record['player_key'],
            editorial_player_key=record['editorial_player_key'],
            league_key=record['league_key'],
            season=record['season'],
            date=record['date'],
            week=self._safe_int(record.get('week')),
            # Core NBA stats
            field_goals_made=self._safe_int(record.get('field_goals_made')),
            field_goals_attempted=self._safe_int(record.get('field_goals_attempted')),
            field_goal_percentage=self._safe_decimal(record.get('field_goal_percentage')),
            free_throws_made=self._safe_int(record.get('free_throws_made')),
            free_throws_attempted=self._safe_int(record.get('free_throws_attempted')),
            free_throw_percentage=self._safe_decimal(record.get('free_throw_percentage')),
            three_pointers_made=self._safe_int(record.get('three_pointers_made')),
            points=self._safe_int(record.get('points')),
            rebounds=self._safe_int(record.get('rebounds')),
            assists=self._safe_int(record.get('assists')),
            steals=self._safe_int(record.get('steals')),
            blocks=self._safe_int(record.get('blocks')),
            turnovers=self._safe_int(record.get('turnovers'))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for player daily stats"""
        date_str = record['date'].strftime('%Y-%m-%d') if isinstance(record['date'], date) else str(record['date'])
        return f"{record['player_key']}_{date_str}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess player daily stats record"""
        # Handle date field
        if 'date' in record and record['date']:
            if isinstance(record['date'], str):
                try:
                    record['date'] = datetime.strptime(record['date'], '%Y-%m-%d').date()
                except ValueError:
                    print(f"Invalid date format: {record['date']}")
                    return record
        
        # Extract stats from stats_data if present
        if 'stats_data' in record:
            stats_data = record['stats_data']
            core_stats = self._extract_core_daily_stats(stats_data)
            record.update(core_stats)
        
        return record
    
    def _extract_core_daily_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract core NBA stats from Yahoo API stats format"""
        core_stats = {}
        
        try:
            # stat_id: 9004003 - Field Goals Made/Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 5 - Field Goal Percentage (FG%)
            core_stats['field_goal_percentage'] = self._parse_percentage(stats_data.get('5'))
            
            # stat_id: 9007006 - Free Throws Made/Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 8 - Free Throw Percentage (FT%)
            core_stats['free_throw_percentage'] = self._parse_percentage(stats_data.get('8'))
            
            # stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self._safe_int(stats_data.get('12'))
            
            # stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self._safe_int(stats_data.get('15'))
            
            # stat_id: 16 - Assists (AST)
            core_stats['assists'] = self._safe_int(stats_data.get('16'))
            
            # stat_id: 17 - Steals (ST)
            core_stats['steals'] = self._safe_int(stats_data.get('17'))
            
            # stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self._safe_int(stats_data.get('18'))
            
            # stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"Error extracting core daily stats: {e}")
        
        return core_stats


class PlayerSeasonStatsLoader(BaseLoader):
    """Loader for player season statistics"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate player season stats record"""
        required_fields = ['player_key', 'editorial_player_key', 'league_key', 'season']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> PlayerSeasonStats:
        """Create PlayerSeasonStats model instance"""
        return PlayerSeasonStats(
            player_key=record['player_key'],
            editorial_player_key=record['editorial_player_key'],
            league_key=record['league_key'],
            season=record['season'],
            # Core NBA stats (season totals)
            field_goals_made=self._safe_int(record.get('field_goals_made')),
            field_goals_attempted=self._safe_int(record.get('field_goals_attempted')),
            field_goal_percentage=self._safe_decimal(record.get('field_goal_percentage')),
            free_throws_made=self._safe_int(record.get('free_throws_made')),
            free_throws_attempted=self._safe_int(record.get('free_throws_attempted')),
            free_throw_percentage=self._safe_decimal(record.get('free_throw_percentage')),
            three_pointers_made=self._safe_int(record.get('three_pointers_made')),
            total_points=self._safe_int(record.get('total_points')),
            total_rebounds=self._safe_int(record.get('total_rebounds')),
            total_assists=self._safe_int(record.get('total_assists')),
            total_steals=self._safe_int(record.get('total_steals')),
            total_blocks=self._safe_int(record.get('total_blocks')),
            total_turnovers=self._safe_int(record.get('total_turnovers'))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for player season stats"""
        return f"{record['player_key']}_{record['season']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess player season stats record"""
        # Extract stats from stats_data if present
        if 'stats_data' in record:
            stats_data = record['stats_data']
            core_stats = self._extract_core_season_stats(stats_data)
            record.update(core_stats)
        
        return record
    
    def _extract_core_season_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract core NBA season stats from Yahoo API stats format"""
        core_stats = {}
        
        try:
            # Same extraction logic as daily stats but for season totals
            # stat_id: 9004003 - Field Goals Made/Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 5 - Field Goal Percentage (FG%)
            core_stats['field_goal_percentage'] = self._parse_percentage(stats_data.get('5'))
            
            # stat_id: 9007006 - Free Throws Made/Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 8 - Free Throw Percentage (FT%)
            core_stats['free_throw_percentage'] = self._parse_percentage(stats_data.get('8'))
            
            # stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # stat_id: 12 - Points Scored (PTS) - season total
            core_stats['total_points'] = self._safe_int(stats_data.get('12'))
            
            # stat_id: 15 - Total Rebounds (REB) - season total
            core_stats['total_rebounds'] = self._safe_int(stats_data.get('15'))
            
            # stat_id: 16 - Assists (AST) - season total
            core_stats['total_assists'] = self._safe_int(stats_data.get('16'))
            
            # stat_id: 17 - Steals (ST) - season total
            core_stats['total_steals'] = self._safe_int(stats_data.get('17'))
            
            # stat_id: 18 - Blocked Shots (BLK) - season total
            core_stats['total_blocks'] = self._safe_int(stats_data.get('18'))
            
            # stat_id: 19 - Turnovers (TO) - season total
            core_stats['total_turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"Error extracting core season stats: {e}")
        
        return core_stats


class TeamStatsWeeklyLoader(BaseLoader):
    """Loader for team weekly statistics"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate team weekly stats record"""
        required_fields = ['team_key', 'league_key', 'season', 'week']
        return all(field in record and record[field] is not None for field in required_fields)
    
    def _create_model_instance(self, record: Dict[str, Any]) -> TeamStatsWeekly:
        """Create TeamStatsWeekly model instance"""
        return TeamStatsWeekly(
            team_key=record['team_key'],
            league_key=record['league_key'],
            season=record['season'],
            week=record['week'],
            # Core NBA stats for team
            field_goals_made=self._safe_int(record.get('field_goals_made')),
            field_goals_attempted=self._safe_int(record.get('field_goals_attempted')),
            field_goal_percentage=self._safe_decimal(record.get('field_goal_percentage')),
            free_throws_made=self._safe_int(record.get('free_throws_made')),
            free_throws_attempted=self._safe_int(record.get('free_throws_attempted')),
            free_throw_percentage=self._safe_decimal(record.get('free_throw_percentage')),
            three_pointers_made=self._safe_int(record.get('three_pointers_made')),
            points=self._safe_int(record.get('points')),
            rebounds=self._safe_int(record.get('rebounds')),
            assists=self._safe_int(record.get('assists')),
            steals=self._safe_int(record.get('steals')),
            blocks=self._safe_int(record.get('blocks')),
            turnovers=self._safe_int(record.get('turnovers'))
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """Get unique identifier for team weekly stats"""
        return f"{record['team_key']}_{record['season']}_{record['week']}"
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess team weekly stats record"""
        # Extract stats from team_stats_data if present
        if 'team_stats_data' in record:
            stats_data = record['team_stats_data']
            core_stats = self._extract_core_team_stats(stats_data)
            record.update(core_stats)
        
        return record
    
    def _extract_core_team_stats(self, stats_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract core NBA team stats from Yahoo API stats format"""
        core_stats = {}
        
        try:
            # Parse stats list from team data
            stats_list = stats_data.get('stats', [])
            if not isinstance(stats_list, list):
                return core_stats
            
            # Build stat_id to value mapping
            stats_dict = {}
            for stat_item in stats_list:
                if isinstance(stat_item, dict) and 'stat' in stat_item:
                    stat_info = stat_item['stat']
                    stat_id = stat_info.get('stat_id')
                    value = stat_info.get('value')
                    if stat_id is not None:
                        stats_dict[str(stat_id)] = value
            
            # Extract same stats as individual players but for team
            # stat_id: 9004003 - Field Goals Made/Attempted (FGM/A)
            field_goals_data = stats_dict.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 5 - Field Goal Percentage (FG%)
            core_stats['field_goal_percentage'] = self._parse_percentage(stats_dict.get('5'))
            
            # stat_id: 9007006 - Free Throws Made/Attempted (FTM/A)
            free_throws_data = stats_dict.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    pass
            
            # stat_id: 8 - Free Throw Percentage (FT%)
            core_stats['free_throw_percentage'] = self._parse_percentage(stats_dict.get('8'))
            
            # stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_dict.get('10'))
            
            # stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self._safe_int(stats_dict.get('12'))
            
            # stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self._safe_int(stats_dict.get('15'))
            
            # stat_id: 16 - Assists (AST)
            core_stats['assists'] = self._safe_int(stats_dict.get('16'))
            
            # stat_id: 17 - Steals (ST)
            core_stats['steals'] = self._safe_int(stats_dict.get('17'))
            
            # stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self._safe_int(stats_dict.get('18'))
            
            # stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self._safe_int(stats_dict.get('19'))
            
        except Exception as e:
            print(f"Error extracting core team stats: {e}")
        
        return core_stats
    
    def _parse_percentage(self, pct_str: Any) -> Optional[Decimal]:
        """Parse percentage string to decimal"""
        try:
            if not pct_str or pct_str == '-':
                return None
            
            pct_str = str(pct_str).strip()
            
            # Remove % symbol if present
            if '%' in pct_str:
                clean_value = pct_str.replace('%', '')
                val = float(clean_value)
                return Decimal(str(round(val, 3)))
            
            # Handle decimal form (0-1) or percentage form (0-100)
            val = float(pct_str)
            if 0 <= val <= 1:
                return Decimal(str(round(val * 100, 3)))
            elif 0 <= val <= 100:
                return Decimal(str(round(val, 3)))
            
            return None
        except (ValueError, TypeError):
            return None
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal"""
        try:
            if value is None or value == '':
                return None
            return Decimal(str(float(value)))
        except (ValueError, TypeError):
            return None


class CompleteStatsLoader:
    """Complete stats data loader that handles all statistics tables"""
    
    def __init__(self, connection_manager, batch_size: int = 100):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        
        # Initialize sub-loaders
        self.player_daily_loader = PlayerDailyStatsLoader(connection_manager, batch_size)
        self.player_season_loader = PlayerSeasonStatsLoader(connection_manager, batch_size)
        self.team_weekly_loader = TeamStatsWeeklyLoader(connection_manager, batch_size)
    
    def load_player_daily_stats(self, stats_data: List[Dict[str, Any]]) -> LoadResult:
        """Load player daily statistics"""
        return self.player_daily_loader.load(stats_data)
    
    def load_player_season_stats(self, stats_data: List[Dict[str, Any]]) -> LoadResult:
        """Load player season statistics"""
        return self.player_season_loader.load(stats_data)
    
    def load_team_weekly_stats(self, stats_data: List[Dict[str, Any]]) -> LoadResult:
        """Load team weekly statistics"""
        return self.team_weekly_loader.load(stats_data) 