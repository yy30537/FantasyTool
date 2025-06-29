"""
API Data Models
===============

Yahoo Fantasy API响应数据模型和标准化数据结构定义

主要功能：
- API响应数据的类型定义
- 提取结果的标准化格式
- 数据验证和序列化
- 错误状态和状态码定义

设计原则：
- 类型安全：使用类型提示确保数据类型正确
- 可扩展性：易于添加新的数据模型
- 一致性：统一的数据格式和命名规范
- 可测试性：支持数据验证和模拟

使用示例：
```python
from fantasy_etl.extract.api_models import ExtractResult, GameData

result = ExtractResult(
    status=ExtractStatus.SUCCESS,
    data=[GameData(game_key="410.l.123", name="My League")]
)
```
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from enum import Enum
import json


class ExtractStatus(Enum):
    """数据提取状态枚举"""
    SUCCESS = "success"
    FAILED = "failed"
    INVALID_PARAMETERS = "invalid_parameters"
    AUTH_ERROR = "auth_error"
    RATE_LIMITED = "rate_limited"
    NETWORK_ERROR = "network_error"
    DATA_VALIDATION_ERROR = "data_validation_error"
    EXTRACTION_ERROR = "extraction_error"
    PARTIAL_SUCCESS = "partial_success"


@dataclass
class ExtractResult:
    """
    标准化的数据提取结果
    
    所有提取器都应返回此类型的结果
    """
    status: ExtractStatus
    data: Optional[Any] = None
    raw_data: Optional[Any] = None
    error_message: Optional[str] = None
    context: Optional['ExtractionContext'] = None
    execution_time: float = 0.0
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_success(self) -> bool:
        """检查提取是否成功"""
        return self.status == ExtractStatus.SUCCESS
    
    @property
    def has_data(self) -> bool:
        """检查是否有数据"""
        return self.data is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'status': self.status.value,
            'has_data': self.has_data,
            'error_message': self.error_message,
            'execution_time': self.execution_time,
            'retry_count': self.retry_count,
            'metadata': self.metadata
        }


# ================================
# Yahoo Fantasy API数据模型
# ================================

@dataclass
class GameData:
    """游戏数据模型"""
    game_key: str
    game_id: str
    name: str
    code: str
    type: str
    url: str
    season: str
    is_registration_over: bool = False
    is_game_over: bool = False
    is_offseason: bool = False
    editorial_season: Optional[str] = None
    picks_status: Optional[str] = None
    contest_group_id: Optional[str] = None
    scenario_generator: Optional[bool] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'GameData':
        """从API响应创建GameData实例"""
        return cls(
            game_key=data.get('game_key', ''),
            game_id=data.get('game_id', ''),
            name=data.get('name', ''),
            code=data.get('code', ''),
            type=data.get('type', ''),
            url=data.get('url', ''),
            season=data.get('season', ''),
            is_registration_over=bool(data.get('is_registration_over', False)),
            is_game_over=bool(data.get('is_game_over', False)),
            is_offseason=bool(data.get('is_offseason', False)),
            editorial_season=data.get('editorial_season'),
            picks_status=data.get('picks_status'),
            contest_group_id=data.get('contest_group_id'),
            scenario_generator=bool(data.get('scenario_generator', False)) if data.get('scenario_generator') is not None else None
        )


@dataclass
class LeagueData:
    """联盟数据模型"""
    league_key: str
    league_id: str
    game_key: str
    name: str
    url: str
    logo_url: Optional[str] = None
    password: Optional[str] = None
    draft_status: Optional[str] = None
    num_teams: Optional[int] = None
    edit_key: Optional[str] = None
    weekly_deadline: Optional[str] = None
    league_update_timestamp: Optional[str] = None
    scoring_type: Optional[str] = None
    league_type: Optional[str] = None
    renew: Optional[str] = None
    renewed: Optional[str] = None
    felo_tier: Optional[str] = None
    iris_group_chat_id: Optional[str] = None
    short_invitation_url: Optional[str] = None
    allow_add_to_dl_extra_pos: Optional[bool] = None
    is_pro_league: Optional[bool] = None
    is_cash_league: Optional[bool] = None
    current_week: Optional[int] = None
    start_week: Optional[int] = None
    start_date: Optional[str] = None
    end_week: Optional[int] = None
    end_date: Optional[str] = None
    is_finished: Optional[bool] = None
    is_plus_league: Optional[bool] = None
    game_code: Optional[str] = None
    season: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'LeagueData':
        """从API响应创建LeagueData实例"""
        return cls(
            league_key=data.get('league_key', ''),
            league_id=data.get('league_id', ''),
            game_key=data.get('game_key', ''),
            name=data.get('name', ''),
            url=data.get('url', ''),
            logo_url=data.get('logo_url'),
            password=data.get('password'),
            draft_status=data.get('draft_status'),
            num_teams=data.get('num_teams'),
            edit_key=data.get('edit_key'),
            weekly_deadline=data.get('weekly_deadline'),
            league_update_timestamp=data.get('league_update_timestamp'),
            scoring_type=data.get('scoring_type'),
            league_type=data.get('league_type'),
            renew=data.get('renew'),
            renewed=data.get('renewed'),
            felo_tier=data.get('felo_tier'),
            iris_group_chat_id=data.get('iris_group_chat_id'),
            short_invitation_url=data.get('short_invitation_url'),
            allow_add_to_dl_extra_pos=data.get('allow_add_to_dl_extra_pos'),
            is_pro_league=data.get('is_pro_league'),
            is_cash_league=data.get('is_cash_league'),
            current_week=data.get('current_week'),
            start_week=data.get('start_week'),
            start_date=data.get('start_date'),
            end_week=data.get('end_week'),
            end_date=data.get('end_date'),
            is_finished=data.get('is_finished'),
            is_plus_league=data.get('is_plus_league'),
            game_code=data.get('game_code'),
            season=data.get('season')
        )


@dataclass
class TeamData:
    """团队数据模型"""
    team_key: str
    team_id: str
    league_key: str
    name: str
    url: Optional[str] = None
    team_logo_url: Optional[str] = None
    division_id: Optional[str] = None
    waiver_priority: Optional[int] = None
    faab_balance: Optional[str] = None
    number_of_moves: int = 0
    number_of_trades: int = 0
    roster_adds_week: Optional[str] = None
    roster_adds_value: Optional[str] = None
    clinched_playoffs: bool = False
    has_draft_grade: bool = False
    managers: List['ManagerData'] = field(default_factory=list)
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], league_key: str) -> 'TeamData':
        """从API响应创建TeamData实例"""
        return cls(
            team_key=data.get('team_key', ''),
            team_id=data.get('team_id', ''),
            league_key=league_key,
            name=data.get('name', ''),
            url=data.get('url'),
            team_logo_url=data.get('team_logo_url'),
            division_id=data.get('division_id'),
            waiver_priority=data.get('waiver_priority'),
            faab_balance=data.get('faab_balance'),
            number_of_moves=data.get('number_of_moves', 0),
            number_of_trades=data.get('number_of_trades', 0),
            roster_adds_week=data.get('roster_adds_week'),
            roster_adds_value=data.get('roster_adds_value'),
            clinched_playoffs=bool(data.get('clinched_playoffs', False)),
            has_draft_grade=bool(data.get('has_draft_grade', False))
        )


@dataclass
class ManagerData:
    """管理员数据模型"""
    manager_id: str
    nickname: str
    guid: Optional[str] = None
    is_commissioner: bool = False
    is_current_login: bool = False
    email: Optional[str] = None
    image_url: Optional[str] = None
    felo_score: Optional[str] = None
    felo_tier: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'ManagerData':
        """从API响应创建ManagerData实例"""
        return cls(
            manager_id=data.get('manager_id', ''),
            nickname=data.get('nickname', ''),
            guid=data.get('guid'),
            is_commissioner=bool(data.get('is_commissioner', False)),
            is_current_login=bool(data.get('is_current_login', False)),
            email=data.get('email'),
            image_url=data.get('image_url'),
            felo_score=data.get('felo_score'),
            felo_tier=data.get('felo_tier')
        )


@dataclass
class PlayerData:
    """球员数据模型"""
    player_key: str
    player_id: str
    league_key: str
    editorial_player_key: Optional[str] = None
    editorial_team_key: Optional[str] = None
    editorial_team_full_name: Optional[str] = None
    editorial_team_abbr: Optional[str] = None
    bye_weeks: Optional[str] = None
    uniform_number: Optional[str] = None
    display_position: Optional[str] = None
    headshot_url: Optional[str] = None
    image_url: Optional[str] = None
    is_undroppable: bool = False
    position_type: Optional[str] = None
    primary_position: Optional[str] = None
    eligible_positions: List[str] = field(default_factory=list)
    has_player_notes: bool = False
    has_recent_player_notes: bool = False
    
    # 球员基本信息
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    ascii_first: Optional[str] = None
    ascii_last: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], league_key: str) -> 'PlayerData':
        """从API响应创建PlayerData实例"""
        name_data = data.get('name', {})
        
        return cls(
            player_key=data.get('player_key', ''),
            player_id=data.get('player_id', ''),
            league_key=league_key,
            editorial_player_key=data.get('editorial_player_key'),
            editorial_team_key=data.get('editorial_team_key'),
            editorial_team_full_name=data.get('editorial_team_full_name'),
            editorial_team_abbr=data.get('editorial_team_abbr'),
            bye_weeks=data.get('bye_weeks'),
            uniform_number=data.get('uniform_number'),
            display_position=data.get('display_position'),
            headshot_url=data.get('headshot_url'),
            image_url=data.get('image_url'),
            is_undroppable=bool(data.get('is_undroppable', False)),
            position_type=data.get('position_type'),
            primary_position=data.get('primary_position'),
            has_player_notes=bool(data.get('has_player_notes', False)),
            has_recent_player_notes=bool(data.get('has_recent_player_notes', False)),
            first_name=name_data.get('first'),
            last_name=name_data.get('last'),
            full_name=name_data.get('full'),
            ascii_first=name_data.get('ascii_first'),
            ascii_last=name_data.get('ascii_last')
        )


@dataclass
class RosterData:
    """阵容数据模型"""
    team_key: str
    player_key: str
    league_key: str
    coverage_date: str
    selected_position: Optional[str] = None
    is_starting: bool = False
    is_bench: bool = False
    is_injured_reserve: bool = False
    player_status: Optional[str] = None
    status_full: Optional[str] = None
    injury_note: Optional[str] = None
    is_keeper: bool = False
    keeper_cost: Optional[str] = None
    is_prescoring: bool = False
    is_editable: bool = False
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], team_key: str, league_key: str) -> 'RosterData':
        """从API响应创建RosterData实例"""
        return cls(
            team_key=team_key,
            player_key=data.get('player_key', ''),
            league_key=league_key,
            coverage_date=data.get('coverage_date', ''),
            selected_position=data.get('selected_position'),
            player_status=data.get('status'),
            status_full=data.get('status_full'),
            injury_note=data.get('injury_note'),
            is_keeper=bool(data.get('is_keeper', False)),
            keeper_cost=data.get('keeper_cost'),
            is_prescoring=bool(data.get('is_prescoring', False)),
            is_editable=bool(data.get('is_editable', False))
        )


@dataclass
class TransactionData:
    """交易数据模型"""
    transaction_key: str
    transaction_id: str
    league_key: str
    type: str
    status: str
    timestamp: Optional[str] = None
    faab_bid: Optional[str] = None
    trader_team_key: Optional[str] = None
    tradee_team_key: Optional[str] = None
    players: List['TransactionPlayerData'] = field(default_factory=list)
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], league_key: str) -> 'TransactionData':
        """从API响应创建TransactionData实例"""
        return cls(
            transaction_key=data.get('transaction_key', ''),
            transaction_id=data.get('transaction_id', ''),
            league_key=league_key,
            type=data.get('type', ''),
            status=data.get('status', ''),
            timestamp=data.get('timestamp'),
            faab_bid=data.get('faab_bid'),
            trader_team_key=data.get('trader_team_key'),
            tradee_team_key=data.get('tradee_team_key')
        )


@dataclass
class TransactionPlayerData:
    """交易球员数据模型"""
    transaction_key: str
    player_key: str
    league_key: str
    type: str
    source_team_key: Optional[str] = None
    destination_team_key: Optional[str] = None
    source_type: Optional[str] = None
    destination_type: Optional[str] = None
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], transaction_key: str, league_key: str) -> 'TransactionPlayerData':
        """从API响应创建TransactionPlayerData实例"""
        return cls(
            transaction_key=transaction_key,
            player_key=data.get('player_key', ''),
            league_key=league_key,
            type=data.get('type', ''),
            source_team_key=data.get('source_team_key'),
            destination_team_key=data.get('destination_team_key'),
            source_type=data.get('source_type'),
            destination_type=data.get('destination_type')
        )


@dataclass
class MatchupData:
    """对战数据模型"""
    league_key: str
    team_key: str
    season: str
    week: int
    week_start: Optional[str] = None
    week_end: Optional[str] = None
    status: Optional[str] = None
    opponent_team_key: Optional[str] = None
    is_winner: Optional[bool] = None
    is_tied: bool = False
    team_points: int = 0
    opponent_points: int = 0
    winner_team_key: Optional[str] = None
    is_playoffs: bool = False
    is_consolation: bool = False
    is_matchup_of_week: bool = False
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], team_key: str, league_key: str, season: str) -> 'MatchupData':
        """从API响应创建MatchupData实例"""
        return cls(
            league_key=league_key,
            team_key=team_key,
            season=season,
            week=data.get('week', 0),
            week_start=data.get('week_start'),
            week_end=data.get('week_end'),
            status=data.get('status'),
            opponent_team_key=data.get('opponent_team_key'),
            is_winner=data.get('is_winner'),
            is_tied=bool(data.get('is_tied', False)),
            team_points=data.get('team_points', 0),
            opponent_points=data.get('opponent_points', 0),
            winner_team_key=data.get('winner_team_key'),
            is_playoffs=bool(data.get('is_playoffs', False)),
            is_consolation=bool(data.get('is_consolation', False)),
            is_matchup_of_week=bool(data.get('is_matchup_of_week', False))
        )


@dataclass
class PlayerStatsData:
    """球员统计数据模型"""
    player_key: str
    editorial_player_key: str
    league_key: str
    season: str
    date: Optional[str] = None
    week: Optional[int] = None
    stats: Dict[str, Union[str, int, float]] = field(default_factory=dict)
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], league_key: str, season: str) -> 'PlayerStatsData':
        """从API响应创建PlayerStatsData实例"""
        return cls(
            player_key=data.get('player_key', ''),
            editorial_player_key=data.get('editorial_player_key', ''),
            league_key=league_key,
            season=season,
            date=data.get('date'),
            week=data.get('week'),
            stats=data.get('stats', {})
        )


@dataclass
class LeagueSettingsData:
    """联盟设置数据模型"""
    league_key: str
    draft_type: Optional[str] = None
    is_auction_draft: bool = False
    persistent_url: Optional[str] = None
    uses_playoff: bool = True
    has_playoff_consolation_games: bool = False
    playoff_start_week: Optional[str] = None
    uses_playoff_reseeding: bool = False
    uses_lock_eliminated_teams: bool = False
    num_playoff_teams: Optional[int] = None
    num_playoff_consolation_teams: int = 0
    has_multiweek_championship: bool = False
    waiver_type: Optional[str] = None
    waiver_rule: Optional[str] = None
    uses_faab: bool = False
    draft_time: Optional[str] = None
    draft_pick_time: Optional[str] = None
    post_draft_players: Optional[str] = None
    max_teams: Optional[int] = None
    waiver_time: Optional[str] = None
    trade_end_date: Optional[str] = None
    trade_ratify_type: Optional[str] = None
    trade_reject_time: Optional[str] = None
    player_pool: Optional[str] = None
    cant_cut_list: Optional[str] = None
    draft_together: bool = False
    is_publicly_viewable: bool = True
    can_trade_draft_picks: bool = False
    sendbird_channel_url: Optional[str] = None
    roster_positions: List[Dict[str, Any]] = field(default_factory=list)
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any], league_key: str) -> 'LeagueSettingsData':
        """从API响应创建LeagueSettingsData实例"""
        return cls(
            league_key=league_key,
            draft_type=data.get('draft_type'),
            is_auction_draft=bool(data.get('is_auction_draft', False)),
            persistent_url=data.get('persistent_url'),
            uses_playoff=bool(data.get('uses_playoff', True)),
            has_playoff_consolation_games=bool(data.get('has_playoff_consolation_games', False)),
            playoff_start_week=data.get('playoff_start_week'),
            uses_playoff_reseeding=bool(data.get('uses_playoff_reseeding', False)),
            uses_lock_eliminated_teams=bool(data.get('uses_lock_eliminated_teams', False)),
            num_playoff_teams=data.get('num_playoff_teams'),
            num_playoff_consolation_teams=data.get('num_playoff_consolation_teams', 0),
            has_multiweek_championship=bool(data.get('has_multiweek_championship', False)),
            waiver_type=data.get('waiver_type'),
            waiver_rule=data.get('waiver_rule'),
            uses_faab=bool(data.get('uses_faab', False)),
            draft_time=data.get('draft_time'),
            draft_pick_time=data.get('draft_pick_time'),
            post_draft_players=data.get('post_draft_players'),
            max_teams=data.get('max_teams'),
            waiver_time=data.get('waiver_time'),
            trade_end_date=data.get('trade_end_date'),
            trade_ratify_type=data.get('trade_ratify_type'),
            trade_reject_time=data.get('trade_reject_time'),
            player_pool=data.get('player_pool'),
            cant_cut_list=data.get('cant_cut_list'),
            draft_together=bool(data.get('draft_together', False)),
            is_publicly_viewable=bool(data.get('is_publicly_viewable', True)),
            can_trade_draft_picks=bool(data.get('can_trade_draft_picks', False)),
            sendbird_channel_url=data.get('sendbird_channel_url'),
            roster_positions=data.get('roster_positions', [])
        )


# 为提取器创建专用的结果类，匹配提取器的期望字段
@dataclass
class ExtractionResult:
    """
    提取器专用的数据提取结果
    
    所有提取器都应返回此类型的结果
    """
    success: bool
    data: List[Any] = field(default_factory=list)
    error_message: Optional[str] = None
    total_count: int = 0
    extraction_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def has_data(self) -> bool:
        """检查是否有数据"""
        return len(self.data) > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'success': self.success,
            'has_data': self.has_data,
            'total_count': self.total_count,
            'error_message': self.error_message,
            'extraction_time': self.extraction_time.isoformat() if self.extraction_time else None,
            'metadata': self.metadata
        }


# ================================
# 工具函数
# ================================

def validate_api_response(response: Dict[str, Any]) -> bool:
    """
    验证API响应格式
    
    Args:
        response: API响应数据
        
    Returns:
        bool: 响应是否有效
    """
    if not isinstance(response, dict):
        return False
    
    # 检查是否包含fantasy_content
    if 'fantasy_content' in response:
        return isinstance(response['fantasy_content'], dict)
    
    # 某些端点可能不包含fantasy_content
    return True


def extract_fantasy_content(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    从API响应中提取fantasy_content
    
    Args:
        response: API响应数据
        
    Returns:
        fantasy_content数据，如果不存在则返回None
    """
    return response.get('fantasy_content') if isinstance(response, dict) else None


def safe_get_nested(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    安全地获取嵌套字典值
    
    Args:
        data: 数据字典
        path: 路径，用点分隔 (如 "user.profile.name")
        default: 默认值
        
    Returns:
        找到的值或默认值
    """
    try:
        keys = path.split('.')
        result = data
        for key in keys:
            result = result[key]
        return result
    except (KeyError, TypeError, AttributeError):
        return default


def convert_to_json_serializable(obj: Any) -> Any:
    """
    将对象转换为JSON可序列化格式
    
    Args:
        obj: 要转换的对象
        
    Returns:
        JSON可序列化的对象
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    else:
        return obj
