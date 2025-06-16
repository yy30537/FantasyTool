"""
Yahoo Fantasy API Client - 统一API客户端

负责Yahoo Fantasy API的认证、请求管理和错误处理
"""
import os
import time
import pickle
import pathlib
import requests
from datetime import datetime
from typing import Dict, Optional, Any, List
from dotenv import load_dotenv
import logging

from .rate_limiter import SyncRateLimiter

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)


class TokenManager:
    """令牌管理器 - 处理OAuth令牌的存储、加载和刷新"""
    
    def __init__(self, token_dir: Optional[str] = None):
        """初始化令牌管理器
        
        Args:
            token_dir: 令牌存储目录，默认使用项目根目录下的tokens
        """
        base_dir = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        self.token_dir = pathlib.Path(token_dir) if token_dir else base_dir / "tokens"
        self.token_file = self.token_dir / "yahoo_token.token"
        
        # OAuth配置
        self.client_id = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
        self.client_secret = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
        self.token_url = "https://api.login.yahoo.com/oauth2/get_token"
        
        # 确保令牌目录存在
        self._ensure_token_directory()
    
    def _ensure_token_directory(self) -> None:
        """确保令牌目录存在"""
        if not self.token_dir.exists():
            self.token_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建令牌目录: {self.token_dir}")
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌到文件"""
        try:
            self._ensure_token_directory()
            with open(self.token_file, 'wb') as f:
                pickle.dump(token, f)
            logger.info(f"令牌已保存到: {self.token_file}")
            return True
        except Exception as e:
            logger.error(f"保存令牌时出错: {str(e)}")
            return False
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """从文件加载令牌"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'rb') as f:
                    token = pickle.load(f)
                logger.debug("成功加载令牌")
                return token
            except Exception as e:
                logger.error(f"加载令牌时出错: {str(e)}")
        else:
            logger.warning("令牌文件不存在，请先完成OAuth授权")
        return None
    
    def refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        """刷新访问令牌"""
        try:
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(self.token_url, data=data)
            
            if response.status_code == 200:
                new_token = response.json()
                
                # 设置过期时间
                now = datetime.now().timestamp()
                expires_in = new_token.get('expires_in', 3600)
                new_token['expires_at'] = now + int(expires_in)
                
                # 保留refresh_token（如果新令牌中没有）
                if 'refresh_token' not in new_token and refresh_token:
                    new_token['refresh_token'] = refresh_token
                
                # 保存更新的令牌
                self.save_token(new_token)
                logger.info("令牌刷新成功")
                return new_token
            else:
                logger.error(f"令牌刷新失败: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"刷新令牌时出错: {str(e)}")
        
        return None
    
    def is_token_expired(self, token: Dict[str, Any]) -> bool:
        """检查令牌是否过期"""
        if not token:
            return True
        
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        # 提前60秒判断为过期，给刷新留时间
        return now >= (expires_at - 60)


class CircuitBreaker:
    """熔断器 - 防止连续失败的API请求"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0):
        """初始化熔断器"""
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
    
    def call(self, func, *args, **kwargs):
        """通过熔断器调用函数"""
        current_time = time.time()
        
        if self.state == "open":
            if current_time - self.last_failure_time > self.timeout:
                self.state = "half-open"
                logger.info("熔断器状态: half-open")
            else:
                raise Exception("熔断器开启，请求被拒绝")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """成功回调"""
        self.failure_count = 0
        if self.state == "half-open":
            self.state = "closed"
            logger.info("熔断器状态: closed")
    
    def _on_failure(self):
        """失败回调"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"熔断器状态: open (连续失败 {self.failure_count} 次)")


class YahooFantasyClient:
    """Yahoo Fantasy API客户端 - 统一的API请求接口"""
    
    def __init__(self, requests_per_minute: int = 60, enable_circuit_breaker: bool = True):
        """初始化API客户端"""
        self.token_manager = TokenManager()
        self.rate_limiter = SyncRateLimiter(requests_per_minute)
        self.circuit_breaker = CircuitBreaker() if enable_circuit_breaker else None
        
        # API基础URL
        self.base_url = "https://fantasysports.yahooapis.com/fantasy/v2"
        
        logger.info(f"Yahoo Fantasy API客户端初始化完成 (速率限制: {requests_per_minute} req/min)")
    
    def _get_valid_token(self) -> Optional[Dict[str, Any]]:
        """获取有效的访问令牌"""
        token = self.token_manager.load_token()
        if not token:
            logger.error("未找到令牌，请先完成OAuth授权")
            return None
        
        # 检查是否过期并刷新
        if self.token_manager.is_token_expired(token):
            refresh_token = token.get('refresh_token')
            if refresh_token:
                token = self.token_manager.refresh_token(refresh_token)
                if not token:
                    logger.error("令牌刷新失败")
                    return None
            else:
                logger.error("没有refresh_token，无法刷新")
                return None
        
        return token
    
    def _make_request(self, url: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
        """发起API请求"""
        token = self._get_valid_token()
        if not token:
            return None
        
        headers = {
            'Authorization': f"Bearer {token['access_token']}",
            'Content-Type': 'application/json'
        }
        headers.update(kwargs.pop('headers', {}))
        
        try:
            # 应用速率限制
            self.rate_limiter.acquire()
            
            # 发起请求
            response = requests.request(method, url, headers=headers, **kwargs)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.warning("访问令牌无效，尝试刷新")
                # 令牌可能无效，尝试刷新
                refresh_token = token.get('refresh_token')
                if refresh_token:
                    new_token = self.token_manager.refresh_token(refresh_token)
                    if new_token:
                        # 用新令牌重试
                        headers['Authorization'] = f"Bearer {new_token['access_token']}"
                        response = requests.request(method, url, headers=headers, **kwargs)
                        if response.status_code == 200:
                            return response.json()
                
                logger.error("令牌刷新后仍然无法访问")
                return None
            else:
                logger.error(f"API请求失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"请求时出错: {str(e)}")
            return None
    
    def fetch_with_retry(self, url: str, max_retries: int = 3, **kwargs) -> Optional[Dict[str, Any]]:
        """带重试机制的请求"""
        def _request():
            return self._make_request(url, **kwargs)
        
        # 如果启用了熔断器，通过熔断器调用
        if self.circuit_breaker:
            try:
                return self.circuit_breaker.call(_request)
            except Exception as e:
                logger.error(f"熔断器拒绝请求: {str(e)}")
                return None
        
        # 标准重试逻辑
        for attempt in range(max_retries):
            try:
                result = _request()
                if result:
                    return result
            except Exception as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            
            # 指数退避
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        logger.error(f"请求最终失败: {url}")
        return None
    
    def get(self, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """GET请求"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        return self.fetch_with_retry(url, **kwargs)
    
    def post(self, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """POST请求"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        return self.fetch_with_retry(url, method="POST", **kwargs)
    
    def build_url(self, endpoint: str, format: str = "json") -> str:
        """构建完整的API URL"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        if format and not url.endswith(f"?format={format}"):
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}format={format}"
        return url
    
    # ===== 业务API方法 =====
    
    def fetch_user_games(self) -> Optional[Dict[str, Any]]:
        """获取用户的games数据"""
        endpoint = "users;use_login=1/games"
        return self.get(endpoint)
    
    def fetch_user_leagues(self, game_key: str) -> Optional[Dict[str, Any]]:
        """获取指定game下用户的leagues数据"""
        endpoint = f"users;use_login=1/games;game_keys={game_key}/leagues"
        return self.get(endpoint)
    
    def fetch_league_info(self, league_key: str) -> Optional[Dict[str, Any]]:
        """获取联盟基本信息"""
        endpoint = f"league/{league_key}"
        return self.get(endpoint)
    
    def fetch_league_settings(self, league_key: str) -> Optional[Dict[str, Any]]:
        """获取联盟设置"""
        endpoint = f"league/{league_key}/settings"
        return self.get(endpoint)
    
    def fetch_league_standings(self, league_key: str) -> Optional[Dict[str, Any]]:
        """获取联盟排名"""
        endpoint = f"league/{league_key}/standings"
        return self.get(endpoint)
    
    def fetch_league_teams(self, league_key: str) -> Optional[Dict[str, Any]]:
        """获取联盟团队数据"""
        endpoint = f"league/{league_key}/teams"
        return self.get(endpoint)
    
    def fetch_league_players(self, league_key: str, start: int = 0, count: int = 25) -> Optional[Dict[str, Any]]:
        """获取联盟球员数据"""
        endpoint = f"league/{league_key}/players"
        if start > 0:
            endpoint += f";start={start}"
        if count != 25:
            endpoint += f";count={count}"
        return self.get(endpoint)
    
    def fetch_league_transactions(self, league_key: str, start: int = 0, count: int = 25) -> Optional[Dict[str, Any]]:
        """获取联盟交易数据"""
        endpoint = f"league/{league_key}/transactions"
        params = []
        if start > 0:
            params.append(f"start={start}")
        if count != 25:
            params.append(f"count={count}")
        if params:
            endpoint += f";{';'.join(params)}"
        return self.get(endpoint)
    
    def fetch_team_roster(self, team_key: str, date_str: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取团队阵容"""
        endpoint = f"team/{team_key}/roster"
        if date_str:
            endpoint += f";date={date_str}"
        return self.get(endpoint)
    
    def fetch_team_matchups(self, team_key: str) -> Optional[Dict[str, Any]]:
        """获取团队对战数据"""
        endpoint = f"team/{team_key}/matchups"
        return self.get(endpoint)
    
    def fetch_player_season_stats(self, league_key: str, player_keys: List[str]) -> Optional[Dict[str, Any]]:
        """获取球员赛季统计"""
        player_keys_str = ",".join(player_keys)
        endpoint = f"league/{league_key}/players;player_keys={player_keys_str}/stats;type=season"
        return self.get(endpoint)
    
    def fetch_player_daily_stats(self, league_key: str, player_keys: List[str], date_str: str) -> Optional[Dict[str, Any]]:
        """获取球员日统计"""
        player_keys_str = ",".join(player_keys)
        endpoint = f"league/{league_key}/players;player_keys={player_keys_str}/stats;type=date;date={date_str}"
        return self.get(endpoint) 

# ===== 工具函数 =====

def parse_yahoo_date(date_str: str) -> Optional[datetime]:
    """解析Yahoo日期格式"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def format_league_selection_info(leagues_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """格式化联盟信息用于选择界面
    
    Args:
        leagues_data: 按game_key分组的联盟数据
        
    Returns:
        格式化后的联盟列表，每个联盟包含选择所需的信息
    """
    all_leagues = []
    league_counter = 1
    
    for game_key, leagues in leagues_data.items():
        for league in leagues:
            league_info = {
                'index': league_counter,
                'league_key': league.get('league_key'),
                'name': league.get('name', '未知联盟'),
                'season': league.get('season', '未知赛季'),
                'num_teams': league.get('num_teams', 0),
                'game_code': league.get('game_code', '未知运动'),
                'scoring_type': league.get('scoring_type', '未知'),
                'is_finished': league.get('is_finished', 0) == 1,
                'draft_status': league.get('draft_status', '未知'),
                'league_type': league.get('league_type', '未知')
            }
            all_leagues.append(league_info)
            league_counter += 1
    
    return all_leagues


def print_league_selection_info(leagues_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """打印联盟选择信息到控制台
    
    Args:
        leagues_data: 按game_key分组的联盟数据
        
    Returns:
        格式化后的联盟列表
    """
    print("\n" + "="*80)
    print("可选择的Fantasy联盟")
    print("="*80)
    
    all_leagues = format_league_selection_info(leagues_data)
    
    if not all_leagues:
        print("没有找到任何联盟")
        return []
    
    for league_info in all_leagues:
        # 打印联盟信息
        status = "已结束" if league_info['is_finished'] else "进行中"
        print(f"{league_info['index']:2d}. {league_info['name']}")
        print(f"    联盟ID: {league_info['league_key']}")
        print(f"    运动类型: {league_info['game_code'].upper()} | 赛季: {league_info['season']} | 状态: {status}")
        print(f"    球队数量: {league_info['num_teams']} | 计分方式: {league_info['scoring_type']}")
        print(f"    选秀状态: {league_info['draft_status']} | 联盟类型: {league_info['league_type']}")
        print()
    
    print("="*80)
    return all_leagues


def select_league_interactively(leagues_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """交互式选择联盟
    
    Args:
        leagues_data: 按game_key分组的联盟数据
        
    Returns:
        选中的联盟信息，如果取消选择则返回None
    """
    all_leagues = print_league_selection_info(leagues_data)
    
    if not all_leagues:
        print("没有找到任何联盟")
        return None
    
    while True:
        try:
            choice = input(f"请选择联盟 (1-{len(all_leagues)}, 输入 'q' 退出): ").strip()
            
            if choice.lower() == 'q':
                print("取消选择")
                return None
            
            if not choice:
                continue
                
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(all_leagues):
                selected_league = all_leagues[choice_num - 1]
                print(f"\n✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                return selected_league
            else:
                print(f"请输入1到{len(all_leagues)}之间的数字")
                
        except ValueError:
            print("请输入有效的数字或 'q' 退出")
        except KeyboardInterrupt:
            print("\n用户取消选择")
            return None


def extract_game_keys_from_data(games_data: Dict[str, Any]) -> List[str]:
    """从games数据中提取游戏键（只包含type='full'的游戏）
    
    Args:
        games_data: Yahoo API返回的games数据
        
    Returns:
        游戏键列表
    """
    game_keys = []
    
    try:
        fantasy_content = games_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        games_count = int(games_container.get("count", 0))
        
        for i in range(games_count):
            str_index = str(i)
            if str_index not in games_container:
                continue
                
            game_container = games_container[str_index]
            game_data = game_container["game"]
            
            if isinstance(game_data, list) and len(game_data) > 0:
                game_info = game_data[0]
                game_key = game_info.get("game_key")
                game_type = game_info.get("type")
                
                if game_key and game_type == "full":
                    game_keys.append(game_key)
        
    except Exception as e:
        logger.error(f"提取游戏键失败: {str(e)}")
    
    return game_keys


def extract_leagues_from_api_data(api_data: Dict[str, Any], game_key: str) -> List[Dict[str, Any]]:
    """从API返回的数据中提取联盟信息
    
    Args:
        api_data: Yahoo API返回的leagues数据
        game_key: 游戏键
        
    Returns:
        联盟信息列表
    """
    leagues = []
    
    try:
        if "error" in api_data:
            return leagues
        
        fantasy_content = api_data["fantasy_content"]
        user_data = fantasy_content["users"]["0"]["user"]
        games_container = user_data[1]["games"]
        
        for i in range(int(games_container.get("count", 0))):
            str_index = str(i)
            if str_index not in games_container:
                continue
            
            game_container = games_container[str_index]
            game_data = game_container["game"]
            
            current_game_key = None
            if isinstance(game_data, list) and len(game_data) > 0:
                current_game_key = game_data[0].get("game_key")
            
            if current_game_key != game_key:
                continue
            
            if len(game_data) > 1 and "leagues" in game_data[1]:
                leagues_container = game_data[1]["leagues"]
                leagues_count = int(leagues_container.get("count", 0))
                
                for j in range(leagues_count):
                    str_league_index = str(j)
                    if str_league_index not in leagues_container:
                        continue
                    
                    league_container = leagues_container[str_league_index]
                    league_data = league_container["league"]
                    
                    league_info = {}
                    if isinstance(league_data, list):
                        for item in league_data:
                            if isinstance(item, dict):
                                league_info.update(item)
                    
                    # 确保联盟信息包含game_key
                    league_info["game_key"] = game_key
                    leagues.append(league_info)
            break
    
    except Exception as e:
        logger.error(f"提取联盟信息失败: {str(e)}")
    
    return leagues