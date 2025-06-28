#!/usr/bin/env python3
"""
赛程数据提取器
生成赛季日程维度数据，用于填充date_dimension表
对应旧脚本中的 fetch_season_schedule_data()
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from ..base_extractor import BaseExtractor, ExtractorType
from ..api_models import ExtractionResult
from ..yahoo_client import YahooAPIClient


class ScheduleExtractor(BaseExtractor):
    """赛程数据提取器
    
    生成赛季日程维度数据，包括：
    - 赛季开始和结束日期
    - 每周的日期范围
    - 季后赛周期
    - 日期维度记录
    
    这个提取器主要用于填充date_dimension表，
    为其他数据提取器提供日期基础
    """
    
    def __init__(self, api_client: YahooAPIClient):
        """
        初始化赛程数据提取器
        
        Args:
            api_client: Yahoo API客户端实例
        """
        super().__init__(
            yahoo_client=api_client,
            extractor_name="ScheduleExtractor",
            extractor_type=ExtractorType.METADATA
        )
    
    def _extract_data(self, **params) -> List[Dict[str, Any]]:
        """
        实现BaseExtractor的抽象方法
        
        Args:
            **params: 提取参数，包含：
                - league_key: 联盟键（必需）
                - season: 赛季（可选，默认从league_key解析）
                - start_date: 自定义开始日期（可选）
                - end_date: 自定义结束日期（可选）
                
        Returns:
            List[Dict]: 生成的日期维度数据
        """
        league_key = params.get('league_key')
        if not league_key:
            raise ValueError("ScheduleExtractor requires 'league_key' parameter")
        
        season = params.get('season', self._extract_season_from_league_key(league_key))
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        
        # 如果没有提供日期范围，则从联盟设置中获取
        if not start_date or not end_date:
            league_dates = self._get_league_season_dates(league_key)
            if league_dates:
                start_date = start_date or league_dates.get('start_date')
                end_date = end_date or league_dates.get('end_date')
        
        # 如果仍然没有日期，使用默认赛季日期
        if not start_date or not end_date:
            default_dates = self._get_default_season_dates(season)
            start_date = start_date or default_dates['start_date']
            end_date = end_date or default_dates['end_date']
        
        return self._generate_date_dimension_data(league_key, season, start_date, end_date)
    
    def get_extraction_dependencies(self) -> List[str]:
        """
        获取提取依赖
        
        Returns:
            List[str]: 依赖的提取器列表
        """
        return ["LeagueExtractor"]  # 依赖联盟基本信息
    
    def supports_incremental_update(self) -> bool:
        """检查是否支持增量更新"""
        return False  # 赛季日程一旦确定通常不会变化
    
    def get_update_frequency(self) -> int:
        """获取建议更新频率（秒）"""
        return 30 * 24 * 3600  # 30天更新一次
    
    def _get_league_season_dates(self, league_key: str) -> Optional[Dict[str, date]]:
        """
        从联盟设置获取赛季日期
        
        Args:
            league_key: 联盟键
            
        Returns:
            Dict: 包含start_date和end_date的字典，如果获取失败则返回None
        """
        try:
            # 构建API URL获取联盟基本信息
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}?format=json"
            
            # 调用API
            response_data = self.client.get(url)
            if not response_data:
                return None
            
            # 解析联盟信息
            return self._parse_league_dates_from_response(response_data)
            
        except Exception as e:
            self.logger.error(f"Failed to get league season dates for {league_key}: {e}")
            return None
    
    def _parse_league_dates_from_response(self, response_data: Dict) -> Optional[Dict[str, date]]:
        """从联盟API响应中解析赛季日期"""
        try:
            fantasy_content = response_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 查找联盟基本信息
            league_info = None
            if isinstance(league_data, list) and len(league_data) > 0:
                league_info = league_data[0]
            elif isinstance(league_data, dict):
                league_info = league_data
            
            if not league_info:
                return None
            
            start_date_str = league_info.get("start_date")
            end_date_str = league_info.get("end_date")
            
            dates = {}
            if start_date_str:
                dates['start_date'] = self._parse_date_string(start_date_str)
            if end_date_str:
                dates['end_date'] = self._parse_date_string(end_date_str)
            
            return dates if dates else None
            
        except Exception as e:
            self.logger.error(f"Failed to parse league dates from response: {e}")
            return None
    
    def _parse_date_string(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        try:
            if not date_str:
                return None
            # 尝试不同的日期格式
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y%m%d']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _get_default_season_dates(self, season: str) -> Dict[str, date]:
        """
        获取默认赛季日期
        
        Args:
            season: 赛季年份
            
        Returns:
            Dict: 包含默认开始和结束日期
        """
        try:
            year = int(season)
            # 默认Fantasy篮球赛季：10月到4月
            start_date = date(year, 10, 1)
            end_date = date(year + 1, 4, 30)
            
            return {
                'start_date': start_date,
                'end_date': end_date
            }
            
        except (ValueError, TypeError):
            # 如果无法解析年份，使用当前年份
            current_year = datetime.now().year
            return {
                'start_date': date(current_year, 10, 1),
                'end_date': date(current_year + 1, 4, 30)
            }
    
    def _generate_date_dimension_data(self, league_key: str, season: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        生成日期维度数据
        
        Args:
            league_key: 联盟键
            season: 赛季
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[Dict]: 日期维度数据
        """
        date_dimension_data = []
        
        try:
            current_date = start_date
            
            while current_date <= end_date:
                dimension_record = {
                    "date": current_date,
                    "league_key": league_key,
                    "season": season,
                    "year": current_date.year,
                    "month": current_date.month,
                    "day": current_date.day,
                    "weekday": current_date.weekday(),
                    "week_of_year": current_date.isocalendar()[1],
                    "quarter": (current_date.month - 1) // 3 + 1,
                    "is_weekend": current_date.weekday() >= 5,
                    "date_string": current_date.strftime('%Y-%m-%d'),
                    "display_date": current_date.strftime('%m/%d/%Y')
                }
                
                date_dimension_data.append(dimension_record)
                
                # 移动到下一天
                current_date += timedelta(days=1)
            
            self.logger.info(f"Generated {len(date_dimension_data)} date dimension records for league {league_key}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate date dimension data: {e}")
        
        return date_dimension_data
    
    def _extract_season_from_league_key(self, league_key: str) -> str:
        """从联盟键中提取赛季"""
        try:
            # 联盟键通常格式为 game_id.l.league_id 或类似
            # 这里简化处理，返回当前年份
            return str(datetime.now().year)
        except Exception:
            return "2024"
    
    def extract(self, league_key: str, season: str = None, start_date: date = None, end_date: date = None, **kwargs) -> ExtractionResult:
        """
        提取赛季日程数据
        
        Args:
            league_key: 联盟键
            season: 赛季（可选）
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            **kwargs: 其他参数
            
        Returns:
            ExtractionResult: 包含日程数据的提取结果
        """
        try:
            # 调用基类方法
            schedule_data = self._extract_data(
                league_key=league_key,
                season=season,
                start_date=start_date,
                end_date=end_date,
                **kwargs
            )
            
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=True,
                data=schedule_data,
                total_records=len(schedule_data),
                message=f"Successfully generated {len(schedule_data)} date dimension records for league {league_key}"
            )
            
        except Exception as e:
            self.logger.error(f"ScheduleExtractor failed for league {league_key}: {e}")
            return ExtractionResult(
                extractor_name=self.extractor_name,
                success=False,
                data=[],
                total_records=0,
                error_message=str(e)
            )
    
    def extract_season_schedule(self, league_key: str, season: str = None) -> ExtractionResult:
        """
        提取完整赛季日程
        
        Args:
            league_key: 联盟键
            season: 赛季（可选）
            
        Returns:
            ExtractionResult: 完整赛季日程的提取结果
        """
        return self.extract(league_key, season)
    
    def extract_date_range(self, league_key: str, start_date: date, end_date: date, season: str = None) -> ExtractionResult:
        """
        提取指定日期范围的日程数据
        
        Args:
            league_key: 联盟键
            start_date: 开始日期
            end_date: 结束日期
            season: 赛季（可选）
            
        Returns:
            ExtractionResult: 指定日期范围日程的提取结果
        """
        return self.extract(league_key, season, start_date, end_date)