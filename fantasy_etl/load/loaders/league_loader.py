"""
League Loader - 联盟数据加载器

负责将转换后的联盟数据写入数据库
"""
from typing import Dict, List, Optional, Union
import logging

from .base_loader import BaseLoader, LoadResult

logger = logging.getLogger(__name__)


class LeagueLoader(BaseLoader):
    """联盟数据加载器"""
    
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载联盟数据
        
        Args:
            data: 转换后的联盟数据，可以是单个结果或列表
            **kwargs: 其他参数，如league_key等
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult(success=False)
        
        try:
            # 处理输入数据格式
            if isinstance(data, dict):
                # 单个TransformResult的data部分
                league_data_list = [data]
            elif isinstance(data, list):
                league_data_list = data
            else:
                result.add_error(data, "不支持的数据格式")
                return result
            
            # 批量加载所有联盟数据
            for league_data in league_data_list:
                single_result = self._load_single_league(league_data, **kwargs)
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "联盟数据加载")
            
        except Exception as e:
            result.add_error(data, f"联盟数据加载异常: {str(e)}")
        
        return result
    
    def _load_single_league(self, league_data: Dict, **kwargs) -> LoadResult:
        """加载单个联盟的数据"""
        result = LoadResult(success=False)
        
        try:
            # 判断数据类型
            if "leagues" in league_data:
                # 批量联盟基本信息
                return self._load_leagues_batch(league_data["leagues"], **kwargs)
            elif "league_settings" in league_data:
                # 联盟设置
                return self._load_league_settings(league_data, **kwargs)
            elif "standings" in league_data:
                # 联盟排名
                return self._load_league_standings(league_data, **kwargs)
            elif "stat_categories" in league_data:
                # 统计类别
                return self._load_stat_categories(league_data, **kwargs)
            elif "league_key" in league_data:
                # 单个联盟基本信息
                return self._load_single_league_info(league_data, **kwargs)
            else:
                result.add_error(league_data, "无法识别的联盟数据格式")
                
        except Exception as e:
            result.add_error(league_data, f"联盟数据处理异常: {str(e)}")
        
        return result
    
    def _load_leagues_batch(self, leagues: Dict, **kwargs) -> LoadResult:
        """批量加载联盟基本信息"""
        result = LoadResult(success=False)
        
        try:
            # 使用database_writer的联盟写入方法
            count = self.db_writer.write_leagues_data(leagues)
            
            # 计算总记录数
            total_records = sum(len(league_list) for league_list in leagues.values())
            
            result.records_processed = total_records
            result.records_loaded = count
            result.records_failed = total_records - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(leagues, f"批量加载联盟失败: {str(e)}")
        
        return result
    
    def _load_single_league_info(self, league_data: Dict, **kwargs) -> LoadResult:
        """加载单个联盟基本信息"""
        result = LoadResult(success=False)
        
        try:
            # 构造适合write_leagues_data的格式
            game_key = league_data.get("game_key", "unknown")
            leagues_dict = {game_key: [league_data]}
            
            count = self.db_writer.write_leagues_data(leagues_dict)
            
            result.records_processed = 1
            result.records_loaded = count
            result.records_failed = 1 - count
            result.success = count > 0
            
        except Exception as e:
            result.add_error(league_data, f"加载联盟信息失败: {str(e)}")
        
        return result
    
    def _load_league_settings(self, settings_data: Dict, **kwargs) -> LoadResult:
        """加载联盟设置"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["league_key", "league_settings"]
            missing_fields = self.validate_record(settings_data, required_fields)
            if missing_fields:
                result.add_error(settings_data, f"缺少必需字段: {missing_fields}")
                return result
            
            # 写入联盟设置
            success = self.db_writer.write_league_settings(
                league_key=settings_data["league_key"],
                settings_data=settings_data["league_settings"]
            )
            
            result.records_processed = 1
            result.records_loaded = 1 if success else 0
            result.records_failed = 0 if success else 1
            result.success = success
            
        except Exception as e:
            result.add_error(settings_data, f"加载联盟设置失败: {str(e)}")
        
        return result
    
    def _load_league_standings(self, standings_data: Dict, **kwargs) -> LoadResult:
        """加载联盟排名"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["league_key", "standings"]
            missing_fields = self.validate_record(standings_data, required_fields)
            if missing_fields:
                result.add_error(standings_data, f"缺少必需字段: {missing_fields}")
                return result
            
            league_key = standings_data["league_key"]
            season = kwargs.get("season", "2024")
            standings = standings_data["standings"]
            
            # 批量写入排名数据
            loaded_count = 0
            for standing in standings:
                try:
                    success = self.db_writer.write_league_standings(
                        league_key=league_key,
                        team_key=standing["team_key"],
                        season=season,
                        rank=standing.get("rank"),
                        playoff_seed=standing.get("playoff_seed"),
                        wins=standing.get("wins", 0),
                        losses=standing.get("losses", 0),
                        ties=standing.get("ties", 0),
                        win_percentage=standing.get("win_percentage", 0.0),
                        games_back=standing.get("games_back", "-"),
                        divisional_wins=standing.get("divisional_wins", 0),
                        divisional_losses=standing.get("divisional_losses", 0),
                        divisional_ties=standing.get("divisional_ties", 0)
                    )
                    if success:
                        loaded_count += 1
                except Exception as e:
                    result.add_error(standing, f"写入排名记录失败: {str(e)}")
            
            result.records_processed = len(standings)
            result.records_loaded = loaded_count
            result.records_failed = len(standings) - loaded_count
            result.success = loaded_count > 0
            
        except Exception as e:
            result.add_error(standings_data, f"加载联盟排名失败: {str(e)}")
        
        return result
    
    def _load_stat_categories(self, categories_data: Dict, **kwargs) -> LoadResult:
        """加载统计类别"""
        result = LoadResult(success=False)
        
        try:
            # 验证必需字段
            required_fields = ["league_key", "stat_categories"]
            missing_fields = self.validate_record(categories_data, required_fields)
            if missing_fields:
                result.add_error(categories_data, f"缺少必需字段: {missing_fields}")
                return result
            
            # 写入统计类别
            count = self.db_writer.write_stat_categories(
                league_key=categories_data["league_key"],
                stat_categories_data=categories_data["stat_categories"]
            )
            
            result.records_processed = count  # write_stat_categories返回写入的数量
            result.records_loaded = count
            result.records_failed = 0
            result.success = count > 0
            
        except Exception as e:
            result.add_error(categories_data, f"加载统计类别失败: {str(e)}")
        
        return result
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """加载单条联盟记录（实现基类抽象方法）"""
        try:
            single_result = self._load_single_league(record, **kwargs)
            return single_result.success
        except Exception as e:
            self.logger.error(f"加载单条联盟记录失败: {e}")
            return False
    
    def load_leagues_batch(self, leagues_data: Dict) -> LoadResult:
        """批量加载联盟基本信息的便捷方法"""
        return self.load({"leagues": leagues_data})
    
    def load_league_settings(self, league_key: str, settings_data: Dict) -> LoadResult:
        """加载联盟设置的便捷方法"""
        data = {
            "league_key": league_key,
            "league_settings": settings_data
        }
        return self.load(data)
    
    def load_league_standings(self, league_key: str, standings: List[Dict], season: str = "2024") -> LoadResult:
        """加载联盟排名的便捷方法"""
        data = {
            "league_key": league_key,
            "standings": standings
        }
        return self.load(data, season=season)
    
    def load_stat_categories(self, league_key: str, stat_categories_data: Dict) -> LoadResult:
        """加载统计类别的便捷方法"""
        data = {
            "league_key": league_key,
            "stat_categories": stat_categories_data
        }
        return self.load(data) 