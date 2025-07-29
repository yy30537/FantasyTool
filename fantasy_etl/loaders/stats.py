"""
统计数据专用加载器
包含统计相关的写入函数，从 archive/database_writer.py 迁移
"""

from typing import Dict, Optional, List
from datetime import date, datetime
from sqlalchemy.orm import Session
from fantasy_etl.database import (
    PlayerSeasonStats, PlayerDailyStats, TeamStatsWeekly, 
    LeagueStandings, TeamMatchups
)


class StatsLoaders:
    """统计数据加载器"""
    
    def __init__(self, session: Session):
        """
        初始化统计数据加载器
        
        Args:
            session: SQLAlchemy数据库会话
        """
        self.session = session
        self.stats = {
            'player_season_stats': 0,
            'player_season_stats_updated': 0,
            'player_daily_stats': 0,
            'player_daily_stats_updated': 0,
            'team_stats_weekly': 0,
            'team_stats_weekly_updated': 0,
            'league_standings': 0,
            'team_matchups': 0
        }
    
    # ============================================================================
    # 球员统计数据写入方法
    # ============================================================================
    
    def write_player_season_stats(self, player_key: str, editorial_player_key: str, 
                                 league_key: str, stats_data: Dict, season: str) -> bool:
        """
        写入球员赛季统计（旧接口兼容）
        
        迁移自: archive/database_writer.py write_player_season_stats() 第139行
        """
        return self.write_player_season_stat_values(
            player_key=player_key,
            editorial_player_key=editorial_player_key,
            league_key=league_key,
            season=season,
            stats_data=stats_data
        ) > 0
        
    def write_player_season_stat_values(self, player_key: str, editorial_player_key: str, 
                                       league_key: str, season: str, stats_data: Dict) -> int:
        """
        写入球员赛季统计值（只存储核心统计列）
        
        迁移自: archive/database_writer.py write_player_season_stat_values() 第423行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(PlayerSeasonStats).filter_by(
                player_key=player_key,
                season=season
            ).first()
            
            # 提取核心统计项
            core_stats = self.transform_player_season_stats(stats_data)
            
            if existing:
                # 更新现有记录
                # 更新所有11个统计项
                existing.field_goals_made = core_stats.get('field_goals_made')
                existing.field_goals_attempted = core_stats.get('field_goals_attempted')
                existing.field_goal_percentage = core_stats.get('field_goal_percentage')
                existing.free_throws_made = core_stats.get('free_throws_made')
                existing.free_throws_attempted = core_stats.get('free_throws_attempted')
                existing.free_throw_percentage = core_stats.get('free_throw_percentage')
                existing.three_pointers_made = core_stats.get('three_pointers_made')
                existing.total_points = core_stats.get('total_points')
                existing.total_rebounds = core_stats.get('total_rebounds')
                existing.total_assists = core_stats.get('total_assists')
                existing.total_steals = core_stats.get('total_steals')
                existing.total_blocks = core_stats.get('total_blocks')
                existing.total_turnovers = core_stats.get('total_turnovers')
                existing.updated_at = datetime.utcnow()
                self.stats['player_season_stats_updated'] += 1
            else:
                # 创建新记录
                player_stats = PlayerSeasonStats(
                    player_key=player_key,
                    editorial_player_key=editorial_player_key,
                    league_key=league_key,
                    season=season,
                    # 所有11个统计项
                    field_goals_made=core_stats.get('field_goals_made'),
                    field_goals_attempted=core_stats.get('field_goals_attempted'),
                    field_goal_percentage=core_stats.get('field_goal_percentage'),
                    free_throws_made=core_stats.get('free_throws_made'),
                    free_throws_attempted=core_stats.get('free_throws_attempted'),
                    free_throw_percentage=core_stats.get('free_throw_percentage'),
                    three_pointers_made=core_stats.get('three_pointers_made'),
                    total_points=core_stats.get('total_points'),
                    total_rebounds=core_stats.get('total_rebounds'),
                    total_assists=core_stats.get('total_assists'),
                    total_steals=core_stats.get('total_steals'),
                    total_blocks=core_stats.get('total_blocks'),
                    total_turnovers=core_stats.get('total_turnovers')
                )
                self.session.add(player_stats)
                self.stats['player_season_stats'] += 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            print(f"写入球员赛季统计失败 {player_key}: {e}")
            self.session.rollback()
            return 0
    
    def transform_player_season_stats(self, stats_data: Dict) -> Dict:
        """转换球员赛季统计数据为标准化的11个统计项"""
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_data.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_data.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['total_points'] = self._safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['total_rebounds'] = self._safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['total_assists'] = self._safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['total_steals'] = self._safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['total_blocks'] = self._safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['total_turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"提取核心赛季统计失败: {e}")
        
        return core_stats
        
    def write_player_daily_stats(self, player_key: str, editorial_player_key: str, 
                                league_key: str, stats_data: Dict, season: str, 
                                stats_date: date, week: Optional[int] = None) -> bool:
        """
        写入球员日期统计（旧接口兼容）
        
        迁移自: archive/database_writer.py write_player_daily_stats() 第151行
        """
        return self.write_player_daily_stat_values(
            player_key=player_key,
            editorial_player_key=editorial_player_key,
            league_key=league_key,
            season=season,
            date_obj=stats_date,
            stats_data=stats_data,
            week=week
        ) > 0
        
    def write_player_daily_stat_values(self, player_key: str, editorial_player_key: str, 
                                      league_key: str, season: str, date_obj: date, 
                                      stats_data: Dict, week: Optional[int] = None) -> int:
        """
        写入球员日期统计值（只存储核心统计列）
        
        迁移自: archive/database_writer.py write_player_daily_stat_values() 第562行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(PlayerDailyStats).filter_by(
                player_key=player_key,
                date=date_obj
            ).first()
            
            # 提取核心统计项
            core_stats = self.transform_player_daily_stats(stats_data)
            
            if existing:
                # 更新现有记录
                existing.week = week
                # 更新所有11个统计项
                existing.field_goals_made = core_stats.get('field_goals_made')
                existing.field_goals_attempted = core_stats.get('field_goals_attempted')
                existing.field_goal_percentage = core_stats.get('field_goal_percentage')
                existing.free_throws_made = core_stats.get('free_throws_made')
                existing.free_throws_attempted = core_stats.get('free_throws_attempted')
                existing.free_throw_percentage = core_stats.get('free_throw_percentage')
                existing.three_pointers_made = core_stats.get('three_pointers_made')
                existing.points = core_stats.get('points')
                existing.rebounds = core_stats.get('rebounds')
                existing.assists = core_stats.get('assists')
                existing.steals = core_stats.get('steals')
                existing.blocks = core_stats.get('blocks')
                existing.turnovers = core_stats.get('turnovers')
                existing.updated_at = datetime.utcnow()
                self.stats['player_daily_stats_updated'] += 1
            else:
                # 创建新记录
                daily_stats = PlayerDailyStats(
                    player_key=player_key,
                    editorial_player_key=editorial_player_key,
                    league_key=league_key,
                    season=season,
                    date=date_obj,
                    week=week,
                    # 所有11个统计项
                    field_goals_made=core_stats.get('field_goals_made'),
                    field_goals_attempted=core_stats.get('field_goals_attempted'),
                    field_goal_percentage=core_stats.get('field_goal_percentage'),
                    free_throws_made=core_stats.get('free_throws_made'),
                    free_throws_attempted=core_stats.get('free_throws_attempted'),
                    free_throw_percentage=core_stats.get('free_throw_percentage'),
                    three_pointers_made=core_stats.get('three_pointers_made'),
                    points=core_stats.get('points'),
                    rebounds=core_stats.get('rebounds'),
                    assists=core_stats.get('assists'),
                    steals=core_stats.get('steals'),
                    blocks=core_stats.get('blocks'),
                    turnovers=core_stats.get('turnovers')
                )
                self.session.add(daily_stats)
                self.stats['player_daily_stats'] += 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            print(f"写入球员日期统计失败: {e}")
            self.session.rollback()
            return 0
    
    def transform_player_daily_stats(self, stats_data: Dict) -> Dict:
        """转换球员日统计数据为标准化的11个统计项"""
        core_stats = {}
        
        try:
            # 完整的11个统计项（基于Yahoo stat_categories）
            
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stats_data.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stats_data.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                # 处理百分比格式：.500 或 50.0% 或 0.500
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stats_data.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stats_data.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stats_data.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self._safe_int(stats_data.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self._safe_int(stats_data.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['assists'] = self._safe_int(stats_data.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['steals'] = self._safe_int(stats_data.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self._safe_int(stats_data.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self._safe_int(stats_data.get('19'))
            
        except Exception as e:
            print(f"提取核心日期统计失败: {e}")
        
        return core_stats
    
    # ============================================================================
    # 团队统计数据写入方法
    # ============================================================================
    
    def write_team_stat_values(self, team_key: str, league_key: str, season: str, 
                              coverage_type: str, stats_data: Dict, **kwargs) -> int:
        """
        写入团队周统计值（只处理week数据）
        
        迁移自: archive/database_writer.py write_team_stat_values() 第706行
        """
        try:
            # 只处理周数据
            week = kwargs.get('week')
            if coverage_type != "week" or week is None:
                return 0
            
            # 检查是否已存在
            existing = self.session.query(TeamStatsWeekly).filter_by(
                team_key=team_key,
                season=season,
                week=week
            ).first()
            
            # 提取完整的团队周统计项
            core_stats = self.transform_team_weekly_stats_from_stats_data(stats_data)
            
            if existing:
                # 更新现有记录
                # 更新所有11个统计项
                existing.field_goals_made = core_stats.get('field_goals_made')
                existing.field_goals_attempted = core_stats.get('field_goals_attempted')
                existing.field_goal_percentage = core_stats.get('field_goal_percentage')
                existing.free_throws_made = core_stats.get('free_throws_made')
                existing.free_throws_attempted = core_stats.get('free_throws_attempted')
                existing.free_throw_percentage = core_stats.get('free_throw_percentage')
                existing.three_pointers_made = core_stats.get('three_pointers_made')
                existing.points = core_stats.get('points')
                existing.rebounds = core_stats.get('rebounds')
                existing.assists = core_stats.get('assists')
                existing.steals = core_stats.get('steals')
                existing.blocks = core_stats.get('blocks')
                existing.turnovers = core_stats.get('turnovers')
                existing.updated_at = datetime.utcnow()
                self.stats['team_stats_weekly_updated'] += 1
            else:
                # 创建新记录
                team_stats = TeamStatsWeekly(
                    team_key=team_key,
                    league_key=league_key,
                    season=season,
                    week=week,
                    # 所有11个统计项
                    field_goals_made=core_stats.get('field_goals_made'),
                    field_goals_attempted=core_stats.get('field_goals_attempted'),
                    field_goal_percentage=core_stats.get('field_goal_percentage'),
                    free_throws_made=core_stats.get('free_throws_made'),
                    free_throws_attempted=core_stats.get('free_throws_attempted'),
                    free_throw_percentage=core_stats.get('free_throw_percentage'),
                    three_pointers_made=core_stats.get('three_pointers_made'),
                    points=core_stats.get('points'),
                    rebounds=core_stats.get('rebounds'),
                    assists=core_stats.get('assists'),
                    steals=core_stats.get('steals'),
                    blocks=core_stats.get('blocks'),
                    turnovers=core_stats.get('turnovers')
                )
                self.session.add(team_stats)
                self.stats['team_stats_weekly'] += 1
            
            self.session.commit()
            return 1
            
        except Exception as e:
            self.session.rollback()
            return 0
    
    def transform_team_weekly_stats_from_stats_data(self, stats_data: Dict) -> Dict:
        """从团队周统计数据中提取完整的11个统计项"""
        core_stats = {}
        
        try:
            # 首先构建stat_id到值的映射
            stat_values = {}
            if isinstance(stats_data, dict) and "stats" in stats_data:
                stats_list = stats_data["stats"]
                for stat_item in stats_list:
                    if isinstance(stat_item, dict) and "stat" in stat_item:
                        stat_info = stat_item["stat"]
                        stat_id = str(stat_info.get("stat_id", ""))
                        stat_value = stat_info.get("value", "")
                        if stat_id:
                            stat_values[stat_id] = stat_value
            
            # 提取11个核心统计项
            # 1. stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
            field_goals_data = stat_values.get('9004003', '')
            if isinstance(field_goals_data, str) and '/' in field_goals_data:
                try:
                    made, attempted = field_goals_data.split('/')
                    core_stats['field_goals_made'] = self._safe_int(made.strip())
                    core_stats['field_goals_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['field_goals_made'] = None
                    core_stats['field_goals_attempted'] = None
            else:
                core_stats['field_goals_made'] = None
                core_stats['field_goals_attempted'] = None
            
            # 2. stat_id: 5 - Field Goal Percentage (FG%)
            fg_pct_str = stat_values.get('5', '')
            if fg_pct_str and fg_pct_str != '-':
                core_stats['field_goal_percentage'] = self._parse_percentage(fg_pct_str)
            else:
                core_stats['field_goal_percentage'] = None
            
            # 3. stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
            free_throws_data = stat_values.get('9007006', '')
            if isinstance(free_throws_data, str) and '/' in free_throws_data:
                try:
                    made, attempted = free_throws_data.split('/')
                    core_stats['free_throws_made'] = self._safe_int(made.strip())
                    core_stats['free_throws_attempted'] = self._safe_int(attempted.strip())
                except:
                    core_stats['free_throws_made'] = None
                    core_stats['free_throws_attempted'] = None
            else:
                core_stats['free_throws_made'] = None
                core_stats['free_throws_attempted'] = None
            
            # 4. stat_id: 8 - Free Throw Percentage (FT%)
            ft_pct_str = stat_values.get('8', '')
            if ft_pct_str and ft_pct_str != '-':
                core_stats['free_throw_percentage'] = self._parse_percentage(ft_pct_str)
            else:
                core_stats['free_throw_percentage'] = None
            
            # 5. stat_id: 10 - 3-point Shots Made (3PTM)
            core_stats['three_pointers_made'] = self._safe_int(stat_values.get('10'))
            
            # 6. stat_id: 12 - Points Scored (PTS)
            core_stats['points'] = self._safe_int(stat_values.get('12'))
            
            # 7. stat_id: 15 - Total Rebounds (REB)
            core_stats['rebounds'] = self._safe_int(stat_values.get('15'))
            
            # 8. stat_id: 16 - Assists (AST)
            core_stats['assists'] = self._safe_int(stat_values.get('16'))
            
            # 9. stat_id: 17 - Steals (ST)
            core_stats['steals'] = self._safe_int(stat_values.get('17'))
            
            # 10. stat_id: 18 - Blocked Shots (BLK)
            core_stats['blocks'] = self._safe_int(stat_values.get('18'))
            
            # 11. stat_id: 19 - Turnovers (TO)
            core_stats['turnovers'] = self._safe_int(stat_values.get('19'))
            
        except Exception as e:
            print(f"提取团队周统计失败: {e}")
        
        return core_stats
        
    def write_team_weekly_stats_from_matchup(self, team_key: str, league_key: str, season: str, 
                                           week: int, team_stats_data: Dict) -> bool:
        """
        从matchup数据写入团队周统计（专门用于从team_matchups生成数据）
        
        迁移自: archive/database_writer.py write_team_weekly_stats_from_matchup() 第2021行
        """
        try:
            # 使用现有的write_team_stat_values方法
            return self.write_team_stat_values(
                team_key=team_key,
                league_key=league_key,
                season=season,
                coverage_type="week",
                stats_data=team_stats_data,
                week=week
            ) > 0
            
        except Exception as e:
            print(f"从matchup写入团队周统计失败: {e}")
            return False
    
    # ============================================================================
    # 联盟和排名数据写入方法
    # ============================================================================
    
    def write_league_standings(self, league_key: str, team_key: str, season: str, **kwargs) -> bool:
        """
        写入联盟排名数据
        
        迁移自: archive/database_writer.py write_league_standings() 第791行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(LeagueStandings).filter_by(
                league_key=league_key,
                team_key=team_key,
                season=season
            ).first()
            
            if existing:
                # 更新现有记录
                for key, value in kwargs.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                existing.updated_at = datetime.utcnow()
            else:
                # 创建新记录
                standings = LeagueStandings(
                    league_key=league_key,
                    team_key=team_key,
                    season=season,
                    rank=kwargs.get('rank'),
                    playoff_seed=kwargs.get('playoff_seed'),
                    wins=kwargs.get('wins', 0),
                    losses=kwargs.get('losses', 0),
                    ties=kwargs.get('ties', 0),
                    win_percentage=kwargs.get('win_percentage', 0.0),
                    games_back=kwargs.get('games_back', '-'),
                    divisional_wins=kwargs.get('divisional_wins', 0),
                    divisional_losses=kwargs.get('divisional_losses', 0),
                    divisional_ties=kwargs.get('divisional_ties', 0)
                )
                self.session.add(standings)
                self.stats['league_standings'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入联盟排名失败: {e}")
            self.session.rollback()
            return False
    
    # ============================================================================
    # 对战数据写入方法
    # ============================================================================
    
    def write_team_matchup(self, league_key: str, team_key: str, season: str, week: int, **kwargs) -> bool:
        """
        写入团队对战数据（使用结构化字段替代JSON）
        
        迁移自: archive/database_writer.py write_team_matchup() 第847行
        """
        try:
            # 检查是否已存在
            existing = self.session.query(TeamMatchups).filter_by(
                league_key=league_key,
                team_key=team_key,
                season=season,
                week=week
            ).first()
            
            if existing:
                # 更新现有记录
                for key, value in kwargs.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                existing.updated_at = datetime.utcnow()
            else:
                # 创建新记录
                matchup = TeamMatchups(
                    league_key=league_key,
                    team_key=team_key,
                    season=season,
                    week=week,
                    week_start=kwargs.get('week_start'),
                    week_end=kwargs.get('week_end'),
                    status=kwargs.get('status'),
                    is_playoffs=kwargs.get('is_playoffs', False),
                    is_consolation=kwargs.get('is_consolation', False),
                    is_tied=kwargs.get('is_tied', False),
                    is_winner=kwargs.get('is_winner'),
                    team_points=kwargs.get('team_points'),
                    team_projected_points=kwargs.get('team_projected_points'),
                    opponent_team_key=kwargs.get('opponent_team_key'),
                    opponent_team_name=kwargs.get('opponent_team_name'),
                    opponent_points=kwargs.get('opponent_points'),
                    opponent_projected_points=kwargs.get('opponent_projected_points'),
                    games_played=kwargs.get('games_played'),
                    games_remaining=kwargs.get('games_remaining'),
                    games_in_progress=kwargs.get('games_in_progress'),
                    categories_won=kwargs.get('categories_won', 0),
                    # 各统计类别获胜情况
                    fg_pct_won=kwargs.get('fg_pct_won'),
                    ft_pct_won=kwargs.get('ft_pct_won'),
                    three_ptm_won=kwargs.get('three_ptm_won'),
                    pts_won=kwargs.get('pts_won'),
                    reb_won=kwargs.get('reb_won'),
                    ast_won=kwargs.get('ast_won'),
                    st_won=kwargs.get('st_won'),
                    blk_won=kwargs.get('blk_won'),
                    to_won=kwargs.get('to_won')
                )
                self.session.add(matchup)
                self.stats['team_matchups'] += 1
            
            self.session.commit()
            return True
            
        except Exception as e:
            print(f"写入团队对战失败: {e}")
            self.session.rollback()
            return False
        
    def write_team_matchup_from_data(self, matchup_data: Dict, team_key: str, 
                                    league_key: str, season: str) -> bool:
        """
        从API返回的matchup数据中解析并写入团队对战记录
        
        迁移自: archive/database_writer.py write_team_matchup_from_data() 第960行
        """
        try:
            # 解析matchup基本信息
            week = int(matchup_data.get("week", 0))
            week_start = matchup_data.get("week_start")
            week_end = matchup_data.get("week_end")
            status = matchup_data.get("status")
            is_playoffs = self._safe_bool(matchup_data.get("is_playoffs"))
            is_consolation = self._safe_bool(matchup_data.get("is_consolation"))
            is_tied = self._safe_bool(matchup_data.get("is_tied"))
            
            # 解析teams数据
            teams_data = matchup_data.get("teams", {})
            matchup_details = self._parse_teams_matchup_data(teams_data, team_key)
            
            # 解析stat_winners
            stat_winners = matchup_data.get("stat_winners", [])
            stat_wins = self._parse_stat_winners(stat_winners, team_key)
            
            # 合并所有数据并写入
            return self.write_team_matchup(
                league_key=league_key,
                team_key=team_key,
                season=season,
                week=week,
                week_start=week_start,
                week_end=week_end,
                status=status,
                is_playoffs=is_playoffs,
                is_consolation=is_consolation,
                is_tied=is_tied,
                **matchup_details,
                **stat_wins
            )
            
        except Exception as e:
            print(f"解析matchup数据失败: {e}")
            return False
    
    # ============================================================================
    # 辅助解析方法
    # ============================================================================
    
    def _parse_stat_winners(self, stat_winners: list, team_key: str) -> Dict:
        """
        解析stat_winners，返回该团队在各统计类别中的获胜情况
        
        迁移自: archive/database_writer.py _parse_stat_winners() 第1028行
        """
        stat_wins = {
            'fg_pct_won': None,     # stat_id: 5
            'ft_pct_won': None,     # stat_id: 8
            'three_ptm_won': None,  # stat_id: 10
            'pts_won': None,        # stat_id: 12
            'reb_won': None,        # stat_id: 15
            'ast_won': None,        # stat_id: 16
            'st_won': None,         # stat_id: 17
            'blk_won': None,        # stat_id: 18
            'to_won': None          # stat_id: 19
        }
        
        stat_id_mapping = {
            '5': 'fg_pct_won',
            '8': 'ft_pct_won',
            '10': 'three_ptm_won',
            '12': 'pts_won',
            '15': 'reb_won',
            '16': 'ast_won',
            '17': 'st_won',
            '18': 'blk_won',
            '19': 'to_won'
        }
        
        for stat_winner in stat_winners:
            if isinstance(stat_winner, dict) and "stat_winner" in stat_winner:
                winner_info = stat_winner["stat_winner"]
                stat_id = str(winner_info.get("stat_id", ""))
                winner_team_key = winner_info.get("winner_team_key")
                is_tied = self._safe_bool(winner_info.get("is_tied"))
                
                if stat_id in stat_id_mapping:
                    field_name = stat_id_mapping[stat_id]
                    if is_tied:
                        stat_wins[field_name] = None  # 平局
                    elif winner_team_key == team_key:
                        stat_wins[field_name] = True  # 获胜
                    else:
                        stat_wins[field_name] = False  # 失败
        
        # 计算获胜的类别数
        categories_won = sum(1 for v in stat_wins.values() if v is True)
        stat_wins['categories_won'] = categories_won
        
        return stat_wins
        
    def _parse_teams_matchup_data(self, teams_data: Dict, target_team_key: str) -> Dict:
        """
        解析teams数据，提取对战详情
        
        迁移自: archive/database_writer.py _parse_teams_matchup_data() 第1047行
        """
        matchup_details = {
            'is_winner': None,
            'team_points': None,
            'team_projected_points': None,
            'opponent_team_key': None,
            'opponent_team_name': None,
            'opponent_points': None,
            'opponent_projected_points': None,
            'games_played': None,
            'games_remaining': None,
            'games_in_progress': None
        }
        
        try:
            # 获取团队数量
            team_count = int(teams_data.get("count", 0))
            
            # 遍历团队数据
            for i in range(team_count):
                team_container = teams_data.get(str(i), {})
                team_data = team_container.get("team", [])
                
                if not isinstance(team_data, list) or len(team_data) < 2:
                    continue
                
                # 提取团队基本信息
                team_info = team_data[0][0] if isinstance(team_data[0], list) else {}
                team_key = team_info.get("team_key")
                
                if not team_key:
                    continue
                
                # 查找team_points和其他信息
                team_points_info = None
                for item in team_data[1:]:
                    if isinstance(item, dict):
                        if "team_points" in item:
                            team_points_info = item["team_points"]
                        elif "team_remaining_games" in item:
                            games_info = item["team_remaining_games"]
                            if team_key == target_team_key:
                                matchup_details['games_played'] = self._safe_int(games_info.get("total", {}).get("completed_games"))
                                matchup_details['games_remaining'] = self._safe_int(games_info.get("total", {}).get("remaining_games"))
                                matchup_details['games_in_progress'] = self._safe_int(games_info.get("total", {}).get("live_games"))
                
                if team_key == target_team_key:
                    # 这是目标团队
                    if team_points_info:
                        matchup_details['team_points'] = self._safe_float(team_points_info.get("total"))
                        matchup_details['team_projected_points'] = self._safe_float(team_points_info.get("projected_total"))
                else:
                    # 这是对手团队
                    matchup_details['opponent_team_key'] = team_key
                    matchup_details['opponent_team_name'] = team_info.get("name")
                    if team_points_info:
                        matchup_details['opponent_points'] = self._safe_float(team_points_info.get("total"))
                        matchup_details['opponent_projected_points'] = self._safe_float(team_points_info.get("projected_total"))
            
            # 判断胜负
            if matchup_details['team_points'] is not None and matchup_details['opponent_points'] is not None:
                if matchup_details['team_points'] > matchup_details['opponent_points']:
                    matchup_details['is_winner'] = True
                elif matchup_details['team_points'] < matchup_details['opponent_points']:
                    matchup_details['is_winner'] = False
                # 相等时保持None（平局）
            
        except Exception as e:
            print(f"解析teams matchup数据失败: {e}")
        
        return matchup_details
    
    # ============================================================================
    # 辅助方法
    # ============================================================================
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.strip()
                if not value or value == '-':
                    return None
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.strip()
                if not value or value == '-':
                    return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_bool(self, value) -> bool:
        """安全转换为布尔值"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        if isinstance(value, (int, float)):
            return bool(value)
        return False
    
    def _parse_percentage(self, pct_str) -> Optional[float]:
        """解析百分比字符串，返回百分比值（0-100）"""
        if not pct_str or pct_str == '-':
            return None
        
        try:
            # 移除空格
            pct_str = str(pct_str).strip()
            
            # 移除百分号
            if pct_str.endswith('%'):
                pct_str = pct_str[:-1]
            
            # 转换为浮点数
            value = float(pct_str)
            
            # 判断是否需要乘以100
            # 如果值小于2，认为是小数形式（如0.500），需要乘以100
            # 如果值大于等于2，认为已经是百分比形式（如50.0）
            if value < 2:
                value = value * 100
            
            # 保留3位小数
            return round(value, 3)
            
        except (ValueError, TypeError):
            return None
        
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
    
    def get_stats_summary(self) -> Dict[str, int]:
        """获取写入统计摘要"""
        return self.stats.copy()

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def load_player_stats(session, stats_data: Dict):
    """加载球员统计"""
    # TODO: 需要实现直接的数据库加载
    return None

def load_team_stats(session, stats_data: Dict):
    """加载团队统计"""
    # TODO: 需要实现直接的数据库加载
    return None

def load_weekly_stats_batch(session, stats_data: List[Dict]):
    """批量加载周统计"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_season_stats_batch(session, stats_data: List[Dict]):
    """批量加载赛季统计"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_projected_stats(session, projections_data: Dict):
    """加载预测统计"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_matchup_results(session, results_data: Dict):
    """加载对战结果"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_transaction(session, transaction_data: Dict):
    """加载单个交易"""
    # TODO: 需要实现直接的数据库加载
    return None

def load_waiver_claim(session, claim_data: Dict):
    """加载waiver claim"""
    # TODO: 需要实现直接的数据库加载
    return None

def load_trade(session, trade_data: Dict):
    """加载交易"""
    # TODO: 需要实现直接的数据库加载
    return None

def load_standings(session, standings_data: List[Dict]):
    """加载排名"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_schedule(session, schedule_data: List[Dict]):
    """加载赛程"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_draft_rankings(session, rankings_data: List[Dict]):
    """加载选秀排名"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_ownership_data(session, ownership_data: Dict):
    """加载所有权数据"""
    # TODO: 需要实现直接的数据库加载
    pass

def load_matchup_grades(session, grades_data: Dict):
    """加载对战评分"""
    # TODO: 需要实现直接的数据库加载
    pass