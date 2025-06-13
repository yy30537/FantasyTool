"""
Team Loader - 团队数据加载器

负责将转换后的团队数据写入数据库
"""
from typing import Dict, List, Optional, Union
from datetime import date
import logging

from .base_loader import BaseLoader, LoadResult

logger = logging.getLogger(__name__)


class TeamLoader(BaseLoader):
    """团队数据加载器"""
    
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载团队数据
        
        Args:
            data: 转换后的团队数据，可以是单个结果或列表
            **kwargs: 其他参数，如league_key, season等
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult(success=False)
        
        try:
            # 处理输入数据格式
            if isinstance(data, dict):
                # 单个TransformResult的data部分
                team_data_list = [data]
            elif isinstance(data, list):
                team_data_list = data
            else:
                result.add_error(data, "不支持的数据格式")
                return result
            
            # 批量加载所有团队数据
            for team_data in team_data_list:
                single_result = self._load_single_team(team_data, **kwargs)
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "团队数据加载")
            
        except Exception as e:
            result.add_error(data, f"团队数据加载异常: {str(e)}")
        
        return result
    
    def _load_single_team(self, team_data: Dict, **kwargs) -> LoadResult:
        """加载单个团队的数据"""
        result = LoadResult(success=False)
        
        try:
            # 判断数据类型
            if "teams" in team_data:
                # 批量团队基本信息
                return self._load_teams_batch(team_data["teams"], **kwargs)
            elif "weekly_stats" in team_data:
                # 团队周统计
                return self._load_team_weekly_stats(team_data, **kwargs)
            elif "season_stats" in team_data:
                # 团队赛季统计（联盟排名）
                return self._load_team_season_stats(team_data, **kwargs)
            elif "matchup_info" in team_data:
                # 团队对战信息
                return self._load_team_matchup(team_data, **kwargs)
            elif "team_key" in team_data:
                # 单个团队基本信息
                return self._load_single_team_info(team_data, **kwargs)
            else:
                result.add_error(team_data, "无法识别的团队数据格式")
                
        except Exception as e:
            result.add_error(team_data, f"团队数据处理异常: {str(e)}")
        
        return result
    
    def _load_teams_batch(self, teams: List[Dict], **kwargs) -> LoadResult:
        """批量加载团队基本信息"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(teams, "缺少league_key参数")
                return result
            
            # 使用database_writer的批量写入方法
            count = self.db_writer.write_teams_batch(teams, league_key)
            
            result.records_processed = len(teams)
            result.records_loaded = count
            result.records_failed = len(teams) - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(teams, f"批量加载团队失败: {str(e)}")
        
        return result
    
    def _load_single_team_info(self, team_data: Dict, **kwargs) -> LoadResult:
        """加载单个团队基本信息"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(team_data, "缺少league_key参数")
                return result
            
            # 使用批量方法加载单个团队
            count = self.db_writer.write_teams_batch([team_data], league_key)
            
            result.records_processed = 1
            result.records_loaded = count
            result.records_failed = 1 - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(team_data, f"加载团队信息失败: {str(e)}")
        
        return result
    
    def _load_team_weekly_stats(self, stats_data: Dict, **kwargs) -> LoadResult:
        """加载团队周统计"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["team_key", "week", "weekly_stats"]
            missing_fields = self.validate_record(stats_data, required_fields)
            if missing_fields:
                result.add_error(stats_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([league_key, season]):
                result.add_error(stats_data, f"缺少必需参数: league_key={league_key}, season={season}")
                return result
            
            # 写入周统计
            success = self.db_writer.write_team_weekly_stats_from_matchup(
                team_key=stats_data["team_key"],
                league_key=league_key,
                season=season,
                week=stats_data["week"],
                team_stats_data=stats_data["weekly_stats"]
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(stats_data, f"加载团队周统计失败: {str(e)}")
        
        return result
    
    def _load_team_season_stats(self, stats_data: Dict, **kwargs) -> LoadResult:
        """加载团队赛季统计（联盟排名）"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["team_key", "season_stats"]
            missing_fields = self.validate_record(stats_data, required_fields)
            if missing_fields:
                result.add_error(stats_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([league_key, season]):
                result.add_error(stats_data, f"缺少必需参数: league_key={league_key}, season={season}")
                return result
            
            season_stats = stats_data["season_stats"]
            
            # 写入联盟排名
            success = self.db_writer.write_league_standings(
                league_key=league_key,
                team_key=stats_data["team_key"],
                season=season,
                rank=season_stats.get("rank"),
                playoff_seed=season_stats.get("playoff_seed"),
                wins=season_stats.get("wins", 0),
                losses=season_stats.get("losses", 0),
                ties=season_stats.get("ties", 0),
                win_percentage=season_stats.get("win_percentage", 0.0),
                games_back=season_stats.get("games_back", "-"),
                divisional_wins=season_stats.get("divisional_wins", 0),
                divisional_losses=season_stats.get("divisional_losses", 0),
                divisional_ties=season_stats.get("divisional_ties", 0)
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(stats_data, f"加载团队赛季统计失败: {str(e)}")
        
        return result
    
    def _load_team_matchup(self, matchup_data: Dict, **kwargs) -> LoadResult:
        """加载团队对战信息"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["team_key", "matchup_info"]
            missing_fields = self.validate_record(matchup_data, required_fields)
            if missing_fields:
                result.add_error(matchup_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([league_key, season]):
                result.add_error(matchup_data, f"缺少必需参数: league_key={league_key}, season={season}")
                return result
            
            # 使用database_writer的matchup写入方法
            success = self.db_writer.write_team_matchup_from_data(
                matchup_data=matchup_data["matchup_info"],
                team_key=matchup_data["team_key"],
                league_key=league_key,
                season=season
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(matchup_data, f"加载团队对战信息失败: {str(e)}")
        
        return result
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """加载单条团队记录（实现基类抽象方法）"""
        try:
            single_result = self._load_single_team(record, **kwargs)
            return single_result.success
        except Exception as e:
            self.logger.error(f"加载单条团队记录失败: {e}")
            return False
    
    def load_teams_batch(self, teams_data: List[Dict], league_key: str) -> LoadResult:
        """批量加载团队基本信息的便捷方法"""
        return self.load({"teams": teams_data}, league_key=league_key)
    
    def load_team_weekly_stats(self, team_key: str, week: int, weekly_stats: Dict,
                             league_key: str, season: str) -> LoadResult:
        """加载团队周统计的便捷方法"""
        stats_data = {
            "team_key": team_key,
            "week": week,
            "weekly_stats": weekly_stats
        }
        return self.load(stats_data, league_key=league_key, season=season)
    
    def load_team_season_stats(self, team_key: str, season_stats: Dict,
                             league_key: str, season: str) -> LoadResult:
        """加载团队赛季统计的便捷方法"""
        stats_data = {
            "team_key": team_key,
            "season_stats": season_stats
        }
        return self.load(stats_data, league_key=league_key, season=season)
    
    def load_team_matchup(self, team_key: str, matchup_info: Dict,
                        league_key: str, season: str) -> LoadResult:
        """加载团队对战信息的便捷方法"""
        matchup_data = {
            "team_key": team_key,
            "matchup_info": matchup_info
        }
        return self.load(matchup_data, league_key=league_key, season=season) 