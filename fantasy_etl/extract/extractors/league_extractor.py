"""
League Extractor - 联盟数据提取器

负责提取游戏、联盟和联盟设置相关数据
"""
from typing import Dict, List, Optional
import logging

from .base_extractor import BaseExtractor, ExtractionResult

logger = logging.getLogger(__name__)


class LeagueExtractor(BaseExtractor):
    """联盟数据提取器 - 负责提取联盟相关的原始数据"""
    
    def extract_user_games(self) -> ExtractionResult:
        """提取用户的所有游戏数据
        
        Returns:
            ExtractionResult: 包含games数据的提取结果
        """
        try:
            self.logger.info("开始提取用户游戏数据")
            
            endpoint = "users;use_login=1/games?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error="无法获取用户游戏数据",
                    metadata=self._build_metadata(operation="extract_user_games")
                )
            
            # 提取games数据
            games_data = self._extract_games_from_response(response_data)
            
            self.logger.info(f"成功提取 {len(games_data)} 个游戏")
            
            return ExtractionResult(
                success=True,
                data=games_data,
                total_records=len(games_data),
                metadata=self._build_metadata(
                    operation="extract_user_games",
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取用户游戏数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(operation="extract_user_games")
            )
    
    def extract_user_leagues(self, game_key: str) -> ExtractionResult:
        """提取指定游戏下用户的联盟数据
        
        Args:
            game_key: 游戏键
            
        Returns:
            ExtractionResult: 包含leagues数据的提取结果
        """
        try:
            if not game_key:
                return ExtractionResult(
                    success=False,
                    error="game_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_user_leagues")
                )
            
            self.logger.info(f"开始提取游戏 {game_key} 的联盟数据")
            
            endpoint = f"users;use_login=1/games;game_keys={game_key}/leagues?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取游戏 {game_key} 的联盟数据",
                    metadata=self._build_metadata(
                        operation="extract_user_leagues",
                        game_key=game_key
                    )
                )
            
            # 提取leagues数据
            leagues_data = self._extract_leagues_from_response(response_data, game_key)
            
            self.logger.info(f"成功提取游戏 {game_key} 的 {len(leagues_data)} 个联盟")
            
            return ExtractionResult(
                success=True,
                data=leagues_data,
                total_records=len(leagues_data),
                metadata=self._build_metadata(
                    operation="extract_user_leagues",
                    game_key=game_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取游戏 {game_key} 联盟数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_user_leagues",
                    game_key=game_key
                )
            )
    
    def extract_league_settings(self, league_key: str) -> ExtractionResult:
        """提取联盟设置数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 包含league settings数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_settings")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的设置数据")
            
            endpoint = f"league/{league_key}/settings?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的设置数据",
                    metadata=self._build_metadata(
                        operation="extract_league_settings",
                        league_key=league_key
                    )
                )
            
            # 提取settings数据
            settings_data = self._extract_settings_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的设置数据")
            
            return ExtractionResult(
                success=True,
                data=settings_data,
                total_records=1,
                metadata=self._build_metadata(
                    operation="extract_league_settings",
                    league_key=league_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 设置数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_settings",
                    league_key=league_key
                )
            )
    
    def extract_league_standings(self, league_key: str) -> ExtractionResult:
        """提取联盟排名数据
        
        Args:
            league_key: 联盟键
            
        Returns:
            ExtractionResult: 包含league standings数据的提取结果
        """
        try:
            if not league_key:
                return ExtractionResult(
                    success=False,
                    error="league_key参数不能为空",
                    metadata=self._build_metadata(operation="extract_league_standings")
                )
            
            self.logger.info(f"开始提取联盟 {league_key} 的排名数据")
            
            endpoint = f"league/{league_key}/standings?format=json"
            response_data = self.client.get(endpoint)
            
            if not response_data:
                return ExtractionResult(
                    success=False,
                    error=f"无法获取联盟 {league_key} 的排名数据",
                    metadata=self._build_metadata(
                        operation="extract_league_standings",
                        league_key=league_key
                    )
                )
            
            # 提取standings数据
            standings_data = self._extract_standings_from_response(response_data)
            
            self.logger.info(f"成功提取联盟 {league_key} 的 {len(standings_data)} 条排名数据")
            
            return ExtractionResult(
                success=True,
                data=standings_data,
                total_records=len(standings_data),
                metadata=self._build_metadata(
                    operation="extract_league_standings",
                    league_key=league_key,
                    raw_response=response_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"提取联盟 {league_key} 排名数据时出错: {str(e)}")
            return ExtractionResult(
                success=False,
                error=str(e),
                metadata=self._build_metadata(
                    operation="extract_league_standings",
                    league_key=league_key
                )
            )
    
    def extract(self, operation: str, **kwargs) -> ExtractionResult:
        """统一的提取接口
        
        Args:
            operation: 操作类型 ('user_games', 'user_leagues', 'league_settings', 'league_standings')
            **kwargs: 操作参数
            
        Returns:
            ExtractionResult: 提取结果
        """
        if operation == "user_games":
            return self.extract_user_games()
        elif operation == "user_leagues":
            return self.extract_user_leagues(kwargs.get("game_key"))
        elif operation == "league_settings":
            return self.extract_league_settings(kwargs.get("league_key"))
        elif operation == "league_standings":
            return self.extract_league_standings(kwargs.get("league_key"))
        else:
            return ExtractionResult(
                success=False,
                error=f"不支持的操作类型: {operation}",
                metadata=self._build_metadata(operation=operation)
            )
    
    def _extract_games_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取games数据"""
        games = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            user_data = self._safe_get(fantasy_content, "users", "0", "user")
            
            if not user_data:
                return games
            
            # user_data是一个包含多个元素的列表
            games_container = None
            if isinstance(user_data, list) and len(user_data) > 1:
                for item in user_data:
                    if isinstance(item, dict) and "games" in item:
                        games_container = item["games"]
                        break
            
            if not games_container:
                return games
            
            games_count = int(games_container.get("count", 0))
            
            for i in range(games_count):
                game_data = games_container.get(str(i), {})
                if "game" in game_data:
                    game_info = game_data["game"]
                    if isinstance(game_info, list) and len(game_info) > 0:
                        # 合并game信息
                        merged_game = {}
                        for item in game_info:
                            if isinstance(item, dict):
                                merged_game.update(item)
                        
                        if merged_game.get("game_key"):
                            games.append(merged_game)
        
        except Exception as e:
            self.logger.error(f"提取games数据时出错: {str(e)}")
        
        return games
    
    def _extract_leagues_from_response(self, response_data: Dict, game_key: str) -> List[Dict]:
        """从API响应中提取leagues数据"""
        leagues = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            user_data = self._safe_get(fantasy_content, "users", "0", "user")
            
            if not user_data:
                return leagues
            
            # 查找games容器
            games_container = None
            if isinstance(user_data, list) and len(user_data) > 1:
                for item in user_data:
                    if isinstance(item, dict) and "games" in item:
                        games_container = item["games"]
                        break
            
            if not games_container:
                return leagues
            
            # 查找目标游戏的leagues
            games_count = int(games_container.get("count", 0))
            for i in range(games_count):
                game_data = games_container.get(str(i), {})
                if "game" in game_data:
                    game_info = game_data["game"]
                    
                    # 检查是否是目标游戏
                    current_game_key = None
                    leagues_data = None
                    
                    if isinstance(game_info, list) and len(game_info) > 0:
                        # 第一个元素包含game基本信息
                        game_basic = game_info[0]
                        if isinstance(game_basic, dict):
                            current_game_key = game_basic.get("game_key")
                        
                        # 第二个元素可能包含leagues
                        if len(game_info) > 1 and isinstance(game_info[1], dict):
                            leagues_data = game_info[1].get("leagues")
                    
                    # 如果是目标游戏且有leagues数据
                    if current_game_key == game_key and leagues_data:
                        leagues_count = int(leagues_data.get("count", 0))
                        
                        for j in range(leagues_count):
                            league_data = leagues_data.get(str(j), {})
                            if "league" in league_data:
                                league_info = league_data["league"]
                                
                                # 合并league信息
                                merged_league = {"game_key": game_key}
                                if isinstance(league_info, list):
                                    for item in league_info:
                                        if isinstance(item, dict):
                                            merged_league.update(item)
                                elif isinstance(league_info, dict):
                                    merged_league.update(league_info)
                                
                                if merged_league.get("league_key"):
                                    leagues.append(merged_league)
                        break
        
        except Exception as e:
            self.logger.error(f"提取leagues数据时出错: {str(e)}")
        
        return leagues
    
    def _extract_settings_from_response(self, response_data: Dict) -> Dict:
        """从API响应中提取settings数据"""
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
            if isinstance(league_data, list) and len(league_data) > 1:
                # 查找settings容器
                for item in league_data:
                    if isinstance(item, dict) and "settings" in item:
                        return item["settings"]
            elif isinstance(league_data, dict) and "settings" in league_data:
                return league_data["settings"]
            
            return {}
            
        except Exception as e:
            self.logger.error(f"提取settings数据时出错: {str(e)}")
            return {}
    
    def _extract_standings_from_response(self, response_data: Dict) -> List[Dict]:
        """从API响应中提取standings数据"""
        standings = []
        
        try:
            fantasy_content = response_data.get("fantasy_content", {})
            league_data = fantasy_content.get("league")
            
            # 查找standings容器
            standings_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "standings" in item:
                        standings_container = item["standings"]
                        break
            elif isinstance(league_data, dict) and "standings" in league_data:
                standings_container = league_data["standings"]
            
            if not standings_container:
                return standings
            
            # 查找teams容器
            teams_container = None
            if isinstance(standings_container, list):
                for item in standings_container:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            elif isinstance(standings_container, dict) and "teams" in standings_container:
                teams_container = standings_container["teams"]
            
            if teams_container:
                teams_count = int(teams_container.get("count", 0))
                for i in range(teams_count):
                    team_data = teams_container.get(str(i), {})
                    if "team" in team_data:
                        standings.append(team_data["team"])
        
        except Exception as e:
            self.logger.error(f"提取standings数据时出错: {str(e)}")
        
        return standings 