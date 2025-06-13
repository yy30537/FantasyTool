"""
Roster Loader - 阵容数据加载器

负责将转换后的阵容数据写入数据库
"""
from typing import Dict, List, Optional, Union
from datetime import date
import logging

from .base_loader import BaseLoader, LoadResult

logger = logging.getLogger(__name__)


class RosterLoader(BaseLoader):
    """阵容数据加载器"""
    
    def load(self, data: Union[Dict, List[Dict]], **kwargs) -> LoadResult:
        """
        加载阵容数据
        
        Args:
            data: 转换后的阵容数据，可以是单个结果或列表
            **kwargs: 其他参数，如league_key, season等
            
        Returns:
            LoadResult: 加载结果
        """
        result = LoadResult(success=False)
        
        try:
            # 处理输入数据格式
            if isinstance(data, dict):
                # 单个TransformResult的data部分
                roster_data_list = [data]
            elif isinstance(data, list):
                roster_data_list = data
            else:
                result.add_error(data, "不支持的数据格式")
                return result
            
            # 批量加载所有阵容数据
            for roster_data in roster_data_list:
                single_result = self._load_single_roster(roster_data, **kwargs)
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "阵容数据加载")
            
        except Exception as e:
            result.add_error(data, f"阵容数据加载异常: {str(e)}")
        
        return result
    
    def _load_single_roster(self, roster_data: Dict, **kwargs) -> LoadResult:
        """加载单个阵容的数据"""
        result = LoadResult(success=False)
        
        try:
            # 验证阵容数据结构
            if "players" not in roster_data:
                result.add_error(roster_data, "缺少players数据")
                return result
            
            players = roster_data["players"]
            if not isinstance(players, list):
                result.add_error(roster_data, "players字段必须是列表")
                return result
            
            # 获取通用参数
            team_key = roster_data.get("team_key") or kwargs.get("team_key")
            league_key = kwargs.get("league_key")
            season = kwargs.get("season")
            
            if not all([team_key, league_key, season]):
                result.add_error(roster_data, f"缺少必需参数: team_key={team_key}, league_key={league_key}, season={season}")
                return result
            
            # 批量处理球员记录
            for player in players:
                result.process_record()
                
                try:
                    if self._load_single_player_roster(player, team_key, league_key, season):
                        result.add_success()
                    else:
                        result.add_error(player, "球员阵容记录写入失败")
                        
                except Exception as e:
                    result.add_error(player, f"球员阵容记录处理异常: {str(e)}")
            
            result.success = result.records_loaded > 0
            
        except Exception as e:
            result.add_error(roster_data, f"阵容数据处理异常: {str(e)}")
        
        return result
    
    def _load_single_player_roster(self, player: Dict, team_key: str, league_key: str, season: str) -> bool:
        """加载单个球员的阵容记录"""
        try:
            # 验证必需字段
            required_fields = ["player_key", "parsed_date"]
            missing_fields = self.validate_record(player, required_fields)
            if missing_fields:
                self.logger.warning(f"球员阵容记录缺少必需字段: {missing_fields}")
                return False
            
            # 解析日期
            roster_date = player["parsed_date"]
            if not isinstance(roster_date, date):
                self.logger.warning(f"无效的日期格式: {roster_date}")
                return False
            
            # 清理记录
            cleaned_player = self.clean_record_for_db(player)
            
            # 写入数据库
            success = self.db_writer.write_roster_daily(
                team_key=team_key,
                player_key=cleaned_player["player_key"],
                league_key=league_key,
                roster_date=roster_date,
                season=season,
                selected_position=cleaned_player.get("selected_position"),
                is_starting=cleaned_player.get("is_starting", False),
                is_bench=cleaned_player.get("is_bench", False),
                is_injured_reserve=cleaned_player.get("is_injured_reserve", False),
                player_status=cleaned_player.get("status"),
                status_full=cleaned_player.get("status_full"),
                injury_note=cleaned_player.get("injury_note"),
                is_keeper=cleaned_player.get("is_keeper", False),
                keeper_cost=cleaned_player.get("keeper_cost"),
                is_prescoring=cleaned_player.get("is_prescoring", False),
                is_editable=cleaned_player.get("is_editable", False)
            )
            
            return success
            
        except Exception as e:
            self.logger.error(f"写入球员阵容记录失败 {player.get('player_key', 'unknown')}: {e}")
            return False
    
    def load_single_record(self, record: Dict, **kwargs) -> bool:
        """加载单条阵容记录（实现基类抽象方法）"""
        try:
            # 如果是单个球员记录
            if "player_key" in record:
                team_key = kwargs.get("team_key")
                league_key = kwargs.get("league_key") 
                season = kwargs.get("season")
                
                if not all([team_key, league_key, season]):
                    return False
                
                return self._load_single_player_roster(record, team_key, league_key, season)
            
            # 如果是完整的阵容数据
            elif "players" in record:
                single_result = self._load_single_roster(record, **kwargs)
                return single_result.success
            
            else:
                self.logger.warning("不支持的记录格式")
                return False
                
        except Exception as e:
            self.logger.error(f"加载单条阵容记录失败: {e}")
            return False
    
    def load_team_roster_for_date(self, team_key: str, roster_data: Dict, league_key: str, season: str) -> LoadResult:
        """为特定团队和日期加载阵容数据的便捷方法"""
        return self.load(
            roster_data,
            team_key=team_key,
            league_key=league_key,
            season=season
        )
    
    def load_multiple_team_rosters(self, rosters_data: List[Dict], league_key: str, season: str) -> LoadResult:
        """批量加载多个团队的阵容数据"""
        result = LoadResult(success=False)
        
        try:
            for roster_data in rosters_data:
                team_key = roster_data.get("team_key")
                if not team_key:
                    result.add_error(roster_data, "缺少team_key")
                    continue
                
                single_result = self.load_team_roster_for_date(
                    team_key=team_key,
                    roster_data=roster_data,
                    league_key=league_key,
                    season=season
                )
                
                # 合并结果
                result.records_processed += single_result.records_processed
                result.records_loaded += single_result.records_loaded
                result.records_failed += single_result.records_failed
                result.errors.extend(single_result.errors)
            
            result.success = result.records_loaded > 0
            self.log_result(result, "多团队阵容数据加载")
            
        except Exception as e:
            result.add_error(rosters_data, f"批量加载阵容数据异常: {str(e)}")
        
        return result 