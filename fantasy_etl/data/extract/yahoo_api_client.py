"""
Yahoo API客户端
重构自archive/yahoo_api_utils.py，提供Yahoo Fantasy Sports API访问功能
"""
import time
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from ...auth import OAuthManager

@dataclass
class APIResponse:
    """API响应数据类"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    status_code: Optional[int] = None

class YahooAPIClient:
    """Yahoo Fantasy Sports API客户端"""
    
    def __init__(self, max_retries: int = 3, delay: float = 1.0):
        self.oauth_manager = OAuthManager()
        self.max_retries = max_retries
        self.delay = delay
        self.base_url = "https://fantasysports.yahooapis.com/fantasy/v2"
    
    def _get_headers(self) -> Optional[Dict[str, str]]:
        """获取API请求头"""
        access_token = self.oauth_manager.get_access_token()
        if not access_token:
            return None
        
        return {
            'Authorization': f"Bearer {access_token}",
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, url: str) -> APIResponse:
        """发起API请求，带重试机制"""
        headers = self._get_headers()
        if not headers:
            return APIResponse(
                success=False,
                error_message="无法获取有效的访问令牌，请先完成认证"
            )
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    return APIResponse(
                        success=True,
                        data=response.json(),
                        status_code=response.status_code
                    )
                elif response.status_code == 401:
                    # 授权问题，尝试刷新令牌
                    print("授权失败，尝试刷新令牌...")
                    refreshed_token = self.oauth_manager.refresh_token_if_needed()
                    if refreshed_token:
                        headers = self._get_headers()
                        if headers:
                            continue
                    
                    return APIResponse(
                        success=False,
                        error_message="令牌刷新失败，无法继续请求",
                        status_code=response.status_code
                    )
                else:
                    error_msg = f"请求失败: {response.status_code} - {response.text}"
                    print(error_msg)
                    
                    # 如果不是最后一次尝试，等待后重试
                    if attempt < self.max_retries - 1:
                        wait_time = (attempt + 1) * self.delay  # 指数退避
                        print(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                    
                    return APIResponse(
                        success=False,
                        error_message=error_msg,
                        status_code=response.status_code
                    )
                        
            except Exception as e:
                error_msg = f"请求时出错: {str(e)}"
                print(error_msg)
                
                # 如果不是最后一次尝试，等待后重试
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * self.delay  # 指数退避
                    print(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                
                return APIResponse(
                    success=False,
                    error_message=error_msg
                )
        
        return APIResponse(
            success=False,
            error_message="达到最大重试次数，请求失败"
        )
    
    def get_user_games(self) -> APIResponse:
        """获取用户的游戏列表"""
        url = f"{self.base_url}/users;use_login=1/games?format=json"
        return self._make_request(url)
    
    def get_user_leagues(self, game_key: Optional[str] = None) -> APIResponse:
        """获取用户的联盟列表"""
        if game_key:
            url = f"{self.base_url}/users;use_login=1/games;game_keys={game_key}/leagues?format=json"
        else:
            url = f"{self.base_url}/users;use_login=1/games/leagues?format=json"
        return self._make_request(url)
    
    def get_league_info(self, league_key: str) -> APIResponse:
        """获取联盟信息"""
        url = f"{self.base_url}/league/{league_key}?format=json"
        return self._make_request(url)
    
    def get_league_settings(self, league_key: str) -> APIResponse:
        """获取联盟设置"""
        url = f"{self.base_url}/league/{league_key}/settings?format=json"
        return self._make_request(url)
    
    def get_league_teams(self, league_key: str) -> APIResponse:
        """获取联盟球队"""
        url = f"{self.base_url}/league/{league_key}/teams?format=json"
        return self._make_request(url)
    
    def get_league_players(self, league_key: str, start: int = 0, count: int = 25) -> APIResponse:
        """获取联盟球员"""
        url = f"{self.base_url}/league/{league_key}/players;start={start};count={count}?format=json"
        return self._make_request(url)
    
    def get_league_standings(self, league_key: str) -> APIResponse:
        """获取联盟排名"""
        url = f"{self.base_url}/league/{league_key}/standings?format=json"
        return self._make_request(url)
    
    def get_league_transactions(self, league_key: str) -> APIResponse:
        """获取联盟交易记录"""
        url = f"{self.base_url}/league/{league_key}/transactions?format=json"
        return self._make_request(url)
    
    def get_team_roster(self, team_key: str, date: Optional[str] = None) -> APIResponse:
        """获取球队阵容"""
        if date:
            url = f"{self.base_url}/team/{team_key}/roster;date={date}?format=json"
        else:
            url = f"{self.base_url}/team/{team_key}/roster?format=json"
        return self._make_request(url)
    
    def get_team_matchups(self, team_key: str, weeks: Optional[str] = None) -> APIResponse:
        """获取球队对战信息"""
        if weeks:
            url = f"{self.base_url}/team/{team_key}/matchups;weeks={weeks}?format=json"
        else:
            url = f"{self.base_url}/team/{team_key}/matchups?format=json"
        return self._make_request(url)
    
    def get_player_stats(self, player_key: str, stat_type: str = "season") -> APIResponse:
        """获取球员统计数据
        
        Args:
            player_key: 球员key
            stat_type: 统计类型，可以是 "season", "average", "lastweek", "lastmonth"
        """
        url = f"{self.base_url}/player/{player_key}/stats;type={stat_type}?format=json"
        return self._make_request(url)
    
    def get_league_scoreboard(self, league_key: str, week: Optional[int] = None) -> APIResponse:
        """获取联盟记分板"""
        if week:
            url = f"{self.base_url}/league/{league_key}/scoreboard;week={week}?format=json"
        else:
            url = f"{self.base_url}/league/{league_key}/scoreboard?format=json"
        return self._make_request(url)
    
    def get_player_stats_for_date(self, player_key: str, date_str: str) -> APIResponse:
        """获取球员指定日期的统计数据"""
        url = f"{self.base_url}/player/{player_key}/stats;type=date;date={date_str}?format=json"
        return self._make_request(url)
    
    def get_players_stats_batch(self, league_key: str, player_keys: List[str], 
                               date_str: str) -> APIResponse:
        """批量获取球员指定日期的统计数据"""
        if not player_keys:
            return APIResponse(success=False, error_message="球员列表为空")
        
        # Yahoo API限制批量查询的球员数量
        if len(player_keys) > 25:
            return APIResponse(success=False, error_message="批量查询球员数量不能超过25个")
        
        player_keys_str = ",".join(player_keys)
        url = f"{self.base_url}/league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}?format=json"
        return self._make_request(url)
    
    def get_players_season_stats_batch(self, league_key: str, player_keys: List[str]) -> APIResponse:
        """批量获取球员赛季统计数据"""
        if not player_keys:
            return APIResponse(success=False, error_message="球员列表为空")
        
        # Yahoo API限制批量查询的球员数量
        if len(player_keys) > 25:
            return APIResponse(success=False, error_message="批量查询球员数量不能超过25个")
        
        player_keys_str = ",".join(player_keys)
        url = f"{self.base_url}/league/{league_key}/players;player_keys={player_keys_str}/stats;type=season?format=json"
        return self._make_request(url)