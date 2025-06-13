"""
Player Loader - 球员数据加载器

负责将转换后的球员数据写入数据库
"""
from typing import Dict, List, Optional, Union
from datetime import date
import logging

from .base_loader import BaseLoader, LoadResult

logger = logging.getLogger(__name__)


class PlayerLoader(BaseLoader):
    """球员数据加载器"""
    
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载球员数据
        
        Args:
            data: 转换后的球员数据，可以是单个结果或列表
            **kwargs: 其他参数，如league_key, season等
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult(success=False)
        
        try:
            # 处理输入数据格式
            if isinstance(data, dict):
                # 单个TransformResult的data部分
                player_data_list = [data]
            elif isinstance(data, list):
                player_data_list = data
            else:
                result.add_error(data, "不支持的数据格式")
                return result
            
            # 批量加载所有球员数据
            for player_data in player_data_list:
                single_result = self._load_single_player(player_data, **kwargs)
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "球员数据加载")
            
        except Exception as e:
            result.add_error(data, f"球员数据加载异常: {str(e)}")
        
        return result
    
    def _load_single_player(self, player_data: Dict, **kwargs) -> LoadResult:
        """加载单个球员的数据"""
        result = LoadResult(success=False)
        
        try:
            # 判断数据类型
            if "players" in player_data:
                # 批量球员基本信息
                return self._load_players_batch(player_data["players"], **kwargs)
            elif "season_stats" in player_data:
                # 球员赛季统计
                return self._load_player_season_stats(player_data, **kwargs)
            elif "daily_stats" in player_data:
                # 球员日期统计
                return self._load_player_daily_stats(player_data, **kwargs)
            elif "player_key" in player_data:
                # 单个球员基本信息
                return self._load_single_player_info(player_data, **kwargs)
            else:
                result.add_error(player_data, "无法识别的球员数据格式")
                
        except Exception as e:
            result.add_error(player_data, f"球员数据处理异常: {str(e)}")
        
        return result
    
    def _load_players_batch(self, players: List[Dict], **kwargs) -> LoadResult:
        """批量加载球员基本信息"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(players, "缺少league_key参数")
                return result
            
            # 使用database_writer的批量写入方法
            count = self.db_writer.write_players_batch(players, league_key)
            
            result.records_processed = len(players)
            result.records_loaded = count
            result.records_failed = len(players) - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(players, f"批量加载球员失败: {str(e)}")
        
        return result
    
    def _load_single_player_info(self, player_data: Dict, **kwargs) -> LoadResult:
        """加载单个球员基本信息"""
        result = LoadResult(success=False)
        
        try:
            league_key = kwargs.get("league_key")
            if not league_key:
                result.add_error(player_data, "缺少league_key参数")
                return result
            
            # 使用批量方法加载单个球员
            count = self.db_writer.write_players_batch([player_data], league_key)
            
            result.records_processed = 1
            result.records_loaded = count
            result.records_failed = 1 - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(player_data, f"加载球员信息失败: {str(e)}")
        
        return result
    
    def _load_player_season_stats(self, stats_data: Dict, **kwargs) -> LoadResult:
        """加载球员赛季统计"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["player_key", "editorial_player_key", "season_stats"]
            missing_fields = self.validate_record(stats_data, required_fields)
            if missing_fields:
                result.add_error(stats_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([league_key, season]):
                result.add_error(stats_data, f"缺少必需参数: league_key={league_key}, season={season}")
                return result
            
            # 写入赛季统计
            success = self.db_writer.write_player_season_stat_values(
                player_key=stats_data["player_key"],
                editorial_player_key=stats_data["editorial_player_key"],
                league_key=league_key,
                season=season,
                stats_data=stats_data["season_stats"]
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(stats_data, f"加载球员赛季统计失败: {str(e)}")
        
        return result
    
    def _load_player_daily_stats(self, stats_data: Dict, **kwargs) -> LoadResult:
        """加载球员日期统计"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["player_key", "editorial_player_key", "daily_stats", "stats_date"]
            missing_fields = self.validate_record(stats_data, required_fields)
            if missing_fields:
                result.add_error(stats_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([league_key, season]):
                result.add_error(stats_data, f"缺少必需参数: league_key={league_key}, season={season}")
                return result
            
            # 解析日期
            stats_date = stats_data["stats_date"]
            if not isinstance(stats_date, date):
                result.add_error(stats_data, f"无效的日期格式: {stats_date}")
                return result
            
            # 写入日期统计
            success = self.db_writer.write_player_daily_stat_values(
                player_key=stats_data["player_key"],
                editorial_player_key=stats_data["editorial_player_key"],
                league_key=league_key,
                season=season,
                date_obj=stats_date,
                stats_data=stats_data["daily_stats"],
                week=stats_data.get("week")
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(stats_data, f"加载球员日期统计失败: {str(e)}")
        
        return result
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """加载单条球员记录（实现基类抽象方法）"""
        try:
            single_result = self._load_single_player(record, **kwargs)
            return single_result.success
        except Exception as e:
            self.logger.error(f"加载单条球员记录失败: {e}")
            return False
    
    def load_players_batch(self, players_data: List[Dict], league_key: str) -> LoadResult:
        """批量加载球员基本信息的便捷方法"""
        return self.load({"players": players_data}, league_key=league_key)
    
    def load_player_season_stats(self, player_key: str, editorial_player_key: str,
                               season_stats: Dict, league_key: str, season: str) -> LoadResult:
        """加载球员赛季统计的便捷方法"""
        stats_data = {
            "player_key": player_key,
            "editorial_player_key": editorial_player_key,
            "season_stats": season_stats
        }
        return self.load(stats_data, league_key=league_key, season=season)
    
    def load_player_daily_stats(self, player_key: str, editorial_player_key: str,
                              daily_stats: Dict, stats_date: date, league_key: str, 
                              season: str, week: Optional[int] = None) -> LoadResult:
        """加载球员日期统计的便捷方法"""
        stats_data = {
            "player_key": player_key,
            "editorial_player_key": editorial_player_key,
            "daily_stats": daily_stats,
            "stats_date": stats_date,
            "week": week
        }
        return self.load(stats_data, league_key=league_key, season=season) 