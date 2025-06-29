"""
Loader Manager - Orchestrates all data loaders for the Yahoo Fantasy ETL system
"""

from typing import Dict, List, Optional, Any, Type
from datetime import datetime, date, timedelta
import logging

from .database.connection_manager import ConnectionManager
from .database.session_manager import SessionManager
from .batch_processor import BatchProcessor, BatchResult
from .loaders.base_loader import BaseLoader, LoadResult
from .loaders.game_loader import GameLoader
from .loaders.player_loader import PlayerLoader
from .loaders.league_loader import (
    LeagueLoader, 
    LeagueSettingsLoader, 
    StatCategoryLoader, 
    LeagueRosterPositionLoader
)
from .loaders.team_loader import TeamLoader, ManagerLoader
from .loaders.transaction_loader import TransactionLoader, TransactionPlayerLoader
from .loaders.roster_loader import RosterDailyLoader
from .loaders.stats_loader import (
    PlayerDailyStatsLoader,
    PlayerSeasonStatsLoader,
    TeamStatsWeeklyLoader
)
from .loaders.standings_loader import LeagueStandingsLoader
from .loaders.matchup_loader import TeamMatchupsLoader
from .loaders.date_loader import DateDimensionLoader


class LoaderManager:
    """
    Central manager for all data loaders in the Yahoo Fantasy ETL system.
    
    This class provides a unified interface for loading all types of fantasy data
    and coordinates the execution of multiple loaders.
    """
    
    def __init__(self, connection_manager: Optional[ConnectionManager] = None, 
                 batch_size: int = 100):
        """
        Initialize the loader manager with database connection and configuration.
        
        Args:
            connection_manager: Database connection manager instance
            batch_size: Default batch size for bulk operations
        """
        self.connection_manager = connection_manager or ConnectionManager()
        self.session_manager = SessionManager(self.connection_manager)
        self.batch_processor = BatchProcessor(self.session_manager)
        self.batch_size = batch_size
        
        # Initialize all loaders
        self._initialize_loaders()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Statistics
        self.load_statistics = {
            'total_loads': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'last_load_time': None
        }
    
    def _initialize_loaders(self):
        """Initialize all data loaders"""
        # Basic data loaders
        self.game_loader = GameLoader(self.connection_manager, self.batch_size)
        self.player_loader = PlayerLoader(self.connection_manager, self.batch_size)
        
        # League-related loaders
        self.league_loader = LeagueLoader(self.connection_manager, self.batch_size)
        self.league_settings_loader = LeagueSettingsLoader(self.connection_manager, self.batch_size)
        self.stat_category_loader = StatCategoryLoader(self.connection_manager, self.batch_size)
        self.league_roster_position_loader = LeagueRosterPositionLoader(self.connection_manager, self.batch_size)
        
        # Team and manager loaders
        self.team_loader = TeamLoader(self.connection_manager, self.batch_size)
        self.manager_loader = ManagerLoader(self.connection_manager, self.batch_size)
        
        # Transaction loaders
        self.transaction_loader = TransactionLoader(self.connection_manager, self.batch_size)
        self.transaction_player_loader = TransactionPlayerLoader(self.connection_manager, self.batch_size)
        
        # Roster and time series loaders
        self.roster_daily_loader = RosterDailyLoader(self.connection_manager, self.batch_size)
        self.date_loader = DateDimensionLoader(self.connection_manager, self.batch_size)
        
        # Statistics loaders
        self.player_daily_stats_loader = PlayerDailyStatsLoader(self.connection_manager, self.batch_size)
        self.player_season_stats_loader = PlayerSeasonStatsLoader(self.connection_manager, self.batch_size)
        self.team_stats_weekly_loader = TeamStatsWeeklyLoader(self.connection_manager, self.batch_size)
        
        # Standings and matchup loaders
        self.standings_loader = LeagueStandingsLoader(self.connection_manager, self.batch_size)
        self.matchup_loader = TeamMatchupsLoader(self.connection_manager, self.batch_size)
    
    def load_complete_fantasy_data(self, fantasy_data: Dict[str, Any]) -> Dict[str, LoadResult]:
        """
        Load complete fantasy data from a comprehensive data structure.
        
        Args:
            fantasy_data: Dictionary containing all fantasy data types
            
        Returns:
            Dictionary mapping data type to LoadResult
        """
        results = {}
        self.load_statistics['total_loads'] += 1
        start_time = datetime.now()
        
        try:
            self.logger.info("开始加载完整fantasy数据...")
            
            # Load data in dependency order
            
            # 1. Load games (no dependencies)
            if 'games' in fantasy_data:
                self.logger.info("正在加载游戏数据...")
                results['games'] = self.game_loader.load(fantasy_data['games'])
            
            # 2. Load date dimensions (minimal dependencies)
            if 'dates' in fantasy_data:
                self.logger.info("正在加载日期维度...")
                results['dates'] = self.date_loader.load(fantasy_data['dates'])
            
            # 3. Load leagues with settings and stat categories
            if 'leagues' in fantasy_data:
                self.logger.info("正在加载联盟数据...")
                results['leagues'] = self._load_league_data(fantasy_data['leagues'])
            
            # 4. Load teams and managers (depends on leagues)
            if 'teams' in fantasy_data:
                self.logger.info("正在加载团队数据...")
                results['teams'] = self._load_team_data(fantasy_data['teams'])
            
            # 5. Load players (depends on leagues)
            if 'players' in fantasy_data:
                self.logger.info("正在加载球员数据...")
                results['players'] = self.player_loader.load(fantasy_data['players'])
            
            # 6. Load transactions (depends on teams and players)
            if 'transactions' in fantasy_data:
                self.logger.info("正在加载交易数据...")
                results['transactions'] = self._load_transaction_data(fantasy_data['transactions'])
            
            # 7. Load rosters (depends on teams and players)
            if 'rosters' in fantasy_data:
                self.logger.info("正在加载名单数据...")
                results['rosters'] = self.roster_daily_loader.load(fantasy_data['rosters'])
            
            # 8. Load statistics (depends on players and teams)
            if 'player_stats' in fantasy_data:
                self.logger.info("正在加载球员统计数据...")
                results['player_stats'] = self._load_player_statistics(fantasy_data['player_stats'])
            
            if 'team_stats' in fantasy_data:
                self.logger.info("正在加载团队统计数据...")
                results['team_stats'] = self.team_stats_weekly_loader.load(fantasy_data['team_stats'])
            
            # 9. Load standings (depends on teams)
            if 'standings' in fantasy_data:
                self.logger.info("正在加载排名数据...")
                results['standings'] = self.standings_loader.load(fantasy_data['standings'])
            
            # 10. Load matchups (depends on teams and statistics)
            if 'matchups' in fantasy_data:
                self.logger.info("正在加载对战数据...")
                results['matchups'] = self.matchup_loader.load(fantasy_data['matchups'])
            
            self.load_statistics['successful_loads'] += 1
            self.logger.info("完整fantasy数据加载完成")
            
        except Exception as e:
            self.load_statistics['failed_loads'] += 1
            self.logger.error(f"加载fantasy数据时出错: {e}")
            raise
        
        finally:
            self.load_statistics['last_load_time'] = datetime.now()
        
        return results
    
    def load_league_complete_data(self, league_key: str, league_data: Dict[str, Any]) -> Dict[str, LoadResult]:
        """
        Load complete data for a single league.
        
        Args:
            league_key: League identifier
            league_data: Complete league data structure
            
        Returns:
            Dictionary mapping data type to LoadResult
        """
        results = {}
        
        self.logger.info(f"开始加载联盟 {league_key} 的完整数据")
        
        try:
            # Load league basics
            if 'league_info' in league_data:
                results['league'] = self.league_loader.load([league_data['league_info']])
            
            # Load league settings
            if 'league_settings' in league_data:
                results['league_settings'] = self.league_settings_loader.load(league_data['league_settings'])
            
            # Load stat categories
            if 'stat_categories' in league_data:
                results['stat_categories'] = self.stat_category_loader.load(league_data['stat_categories'])
            
            # Load teams
            if 'teams' in league_data:
                results['teams'] = self._load_team_data(league_data['teams'])
            
            # Load players
            if 'players' in league_data:
                results['players'] = self.player_loader.load(league_data['players'])
            
            # Load other data types...
            for data_type in ['transactions', 'rosters', 'player_stats', 'team_stats', 'standings', 'matchups']:
                if data_type in league_data:
                    results[data_type] = self._load_data_by_type(data_type, league_data[data_type])
            
        except Exception as e:
            self.logger.error(f"加载联盟 {league_key} 数据时出错: {e}")
            raise
        
        return results
    
    def load_time_series_data(self, league_key: str, season: str, 
                             start_date: date, end_date: date,
                             data_types: List[str] = None) -> Dict[str, LoadResult]:
        """
        Load time series data for a league over a date range.
        
        Args:
            league_key: League identifier
            season: Season identifier
            start_date: Start date for time series
            end_date: End date for time series
            data_types: List of data types to load (default: all)
            
        Returns:
            Dictionary mapping data type to LoadResult
        """
        if data_types is None:
            data_types = ['dates', 'rosters', 'daily_stats']
        
        results = {}
        
        self.logger.info(f"开始加载 {league_key} 的时间序列数据: {start_date} 到 {end_date}")
        
        try:
            # Load date dimensions
            if 'dates' in data_types:
                dates_data = self._generate_date_range_data(league_key, season, start_date, end_date)
                results['dates'] = self.date_loader.load(dates_data)
            
            # Load roster history
            if 'rosters' in data_types:
                # Note: Actual roster data would need to be provided by the caller
                self.logger.info("时间序列名单数据需要调用方提供具体数据")
            
            # Load daily stats
            if 'daily_stats' in data_types:
                # Note: Actual stats data would need to be provided by the caller
                self.logger.info("时间序列统计数据需要调用方提供具体数据")
            
        except Exception as e:
            self.logger.error(f"加载时间序列数据时出错: {e}")
            raise
        
        return results
    
    def _load_league_data(self, leagues_data: List[Dict[str, Any]]) -> LoadResult:
        """Load league data including settings and categories"""
        combined_result = LoadResult()
        
        # Load basic league info
        league_result = self.league_loader.load(leagues_data)
        combined_result.combine(league_result)
        
        # Load related data for each league
        for league_data in leagues_data:
            if 'settings' in league_data:
                settings_result = self.league_settings_loader.load([league_data['settings']])
                combined_result.combine(settings_result)
            
            if 'stat_categories' in league_data:
                categories_result = self.stat_category_loader.load(league_data['stat_categories'])
                combined_result.combine(categories_result)
            
            if 'roster_positions' in league_data:
                positions_result = self.league_roster_position_loader.load(league_data['roster_positions'])
                combined_result.combine(positions_result)
        
        return combined_result
    
    def _load_team_data(self, teams_data: List[Dict[str, Any]]) -> LoadResult:
        """Load team data including managers"""
        combined_result = LoadResult()
        
        # Load basic team info
        team_result = self.team_loader.load(teams_data)
        combined_result.combine(team_result)
        
        # Load managers for each team
        all_managers = []
        for team_data in teams_data:
            if 'managers' in team_data:
                all_managers.extend(team_data['managers'])
        
        if all_managers:
            manager_result = self.manager_loader.load(all_managers)
            combined_result.combine(manager_result)
        
        return combined_result
    
    def _load_transaction_data(self, transactions_data: List[Dict[str, Any]]) -> LoadResult:
        """Load transaction data including transaction players"""
        combined_result = LoadResult()
        
        # Load basic transaction info
        transaction_result = self.transaction_loader.load(transactions_data)
        combined_result.combine(transaction_result)
        
        # Load transaction players
        all_transaction_players = []
        for transaction_data in transactions_data:
            if 'players' in transaction_data:
                all_transaction_players.extend(transaction_data['players'])
        
        if all_transaction_players:
            transaction_player_result = self.transaction_player_loader.load(all_transaction_players)
            combined_result.combine(transaction_player_result)
        
        return combined_result
    
    def _load_player_statistics(self, player_stats_data: Dict[str, Any]) -> LoadResult:
        """Load player statistics (both daily and season)"""
        combined_result = LoadResult()
        
        if 'daily' in player_stats_data:
            daily_result = self.player_daily_stats_loader.load(player_stats_data['daily'])
            combined_result.combine(daily_result)
        
        if 'season' in player_stats_data:
            season_result = self.player_season_stats_loader.load(player_stats_data['season'])
            combined_result.combine(season_result)
        
        return combined_result
    
    def _load_data_by_type(self, data_type: str, data: Any) -> LoadResult:
        """Load data by type using appropriate loader"""
        loader_map = {
            'transactions': self._load_transaction_data,
            'rosters': self.roster_daily_loader.load,
            'player_stats': self._load_player_statistics,
            'team_stats': self.team_stats_weekly_loader.load,
            'standings': self.standings_loader.load,
            'matchups': self.matchup_loader.load
        }
        
        if data_type in loader_map:
            return loader_map[data_type](data)
        else:
            self.logger.warning(f"未知的数据类型: {data_type}")
            return LoadResult()
    
    def _generate_date_range_data(self, league_key: str, season: str, 
                                 start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Generate date range data for date dimension loader"""
        dates_data = []
        current_date = start_date
        
        while current_date <= end_date:
            dates_data.append({
                'date': current_date,
                'league_key': league_key,
                'season': season
            })
            current_date += timedelta(days=1)
        
        return dates_data
    
    def get_load_summary(self, results: Dict[str, LoadResult]) -> Dict[str, Any]:
        """
        Generate a summary of load results.
        
        Args:
            results: Dictionary of LoadResult objects
            
        Returns:
            Summary dictionary with statistics
        """
        summary = {
            'total_records_loaded': 0,
            'total_records_updated': 0,
            'total_records_failed': 0,
            'data_types_loaded': [],
            'errors': [],
            'load_time': None,
            'performance_stats': {}
        }
        
        for data_type, result in results.items():
            summary['total_records_loaded'] += result.records_loaded
            summary['total_records_updated'] += result.records_updated
            summary['total_records_failed'] += len(result.errors)
            summary['data_types_loaded'].append(data_type)
            summary['errors'].extend([f"{data_type}: {error}" for error in result.errors])
            
            # Add performance stats
            if hasattr(result, 'processing_time') and result.processing_time:
                summary['performance_stats'][data_type] = {
                    'processing_time': result.processing_time,
                    'records_per_second': result.records_loaded / result.processing_time if result.processing_time > 0 else 0
                }
        
        summary['total_data_types'] = len(summary['data_types_loaded'])
        summary['success_rate'] = (
            (summary['total_records_loaded'] / 
             (summary['total_records_loaded'] + summary['total_records_failed']) * 100)
            if (summary['total_records_loaded'] + summary['total_records_failed']) > 0 else 0
        )
        
        return summary
    
    def validate_data_consistency(self, league_key: str) -> Dict[str, Any]:
        """
        Validate data consistency for a league.
        
        Args:
            league_key: League to validate
            
        Returns:
            Validation results
        """
        validation_results = {
            'league_key': league_key,
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'checks_performed': [],
            'validation_time': datetime.now().isoformat()
        }
        
        try:
            with self.session_manager.get_session() as session:
                # Check 1: All teams have managers
                self._validate_teams_have_managers(session, league_key, validation_results)
                
                # Check 2: All roster entries reference valid players
                self._validate_roster_player_references(session, league_key, validation_results)
                
                # Check 3: Statistics dates are within season range
                self._validate_stats_date_ranges(session, league_key, validation_results)
                
                # Check 4: Transaction consistency
                self._validate_transaction_consistency(session, league_key, validation_results)
                
        except Exception as e:
            validation_results['errors'].append(f"验证过程中出错: {str(e)}")
            validation_results['is_valid'] = False
        
        return validation_results
    
    def _validate_teams_have_managers(self, session, league_key: str, results: Dict):
        """Validate that all teams have managers"""
        from .database.models import Team, Manager
        
        teams_without_managers = session.query(Team).outerjoin(Manager).filter(
            Team.league_key == league_key,
            Manager.id.is_(None)
        ).all()
        
        results['checks_performed'].append('teams_have_managers')
        
        if teams_without_managers:
            for team in teams_without_managers:
                results['warnings'].append(f"团队 {team.name} ({team.team_key}) 没有管理员")
    
    def _validate_roster_player_references(self, session, league_key: str, results: Dict):
        """Validate that roster entries reference valid players"""
        from .database.models import RosterDaily, Player
        
        invalid_rosters = session.query(RosterDaily).outerjoin(Player).filter(
            RosterDaily.league_key == league_key,
            Player.player_key.is_(None)
        ).count()
        
        results['checks_performed'].append('roster_player_references')
        
        if invalid_rosters > 0:
            results['errors'].append(f"发现 {invalid_rosters} 条名单记录引用了无效的球员")
            results['is_valid'] = False
    
    def _validate_stats_date_ranges(self, session, league_key: str, results: Dict):
        """Validate that statistics dates are within season range"""
        from .database.models import League, PlayerDailyStats
        
        league = session.query(League).filter_by(league_key=league_key).first()
        if not league or not league.start_date or not league.end_date:
            results['warnings'].append("联盟缺少开始或结束日期，无法验证统计日期范围")
            return
        
        from datetime import datetime
        start_date = datetime.strptime(league.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(league.end_date, '%Y-%m-%d').date()
        
        invalid_stats = session.query(PlayerDailyStats).filter(
            PlayerDailyStats.league_key == league_key,
            (PlayerDailyStats.date < start_date) | (PlayerDailyStats.date > end_date)
        ).count()
        
        results['checks_performed'].append('stats_date_ranges')
        
        if invalid_stats > 0:
            results['warnings'].append(f"发现 {invalid_stats} 条统计记录的日期超出赛季范围")
    
    def _validate_transaction_consistency(self, session, league_key: str, results: Dict):
        """Validate transaction consistency"""
        from .database.models import Transaction, TransactionPlayer
        
        transactions_without_players = session.query(Transaction).outerjoin(TransactionPlayer).filter(
            Transaction.league_key == league_key,
            TransactionPlayer.id.is_(None)
        ).count()
        
        results['checks_performed'].append('transaction_consistency')
        
        if transactions_without_players > 0:
            results['warnings'].append(f"发现 {transactions_without_players} 个交易记录没有关联的球员信息")
    
    def get_manager_statistics(self) -> Dict[str, Any]:
        """Get loader manager statistics"""
        stats = dict(self.load_statistics)
        
        # Add batch processor stats if available
        if hasattr(self.batch_processor, 'get_performance_summary'):
            stats['batch_processor'] = self.batch_processor.get_performance_summary()
        
        # Add connection stats
        if hasattr(self.connection_manager, 'get_database_stats'):
            stats['database'] = self.connection_manager.get_database_stats()
        
        return stats
    
    def reset_statistics(self):
        """Reset all statistics"""
        self.load_statistics = {
            'total_loads': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'last_load_time': None
        }
        
        if hasattr(self.batch_processor, 'reset_stats'):
            self.batch_processor.reset_stats()
        
        self.logger.info("加载器管理器统计信息已重置")


# 便利函数
def create_loader_manager(batch_size: int = 100) -> LoaderManager:
    """Create a pre-configured loader manager"""
    return LoaderManager(batch_size=batch_size) 