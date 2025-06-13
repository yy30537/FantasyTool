from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON, ForeignKey, Index, Date, text, Float, ForeignKeyConstraint, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

Base = declarative_base()

class Game(Base):
    """游戏基本信息表"""
    __tablename__ = 'games'
    
    game_key = Column(String(20), primary_key=True)
    game_id = Column(String(20), nullable=False)
    name = Column(String(100), nullable=False)
    code = Column(String(20), nullable=False)  # nba, yahoops等
    type = Column(String(50))  # full, pickem-team-list等
    url = Column(String(500))
    season = Column(String(10), nullable=False)
    is_registration_over = Column(Boolean, default=False)
    is_game_over = Column(Boolean, default=False)
    is_offseason = Column(Boolean, default=False)
    editorial_season = Column(String(10))
    picks_status = Column(String(50))
    contest_group_id = Column(String(20))
    scenario_generator = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    leagues = relationship("League", back_populates="game")
    
    # 索引
    __table_args__ = (Index('idx_game_code_season', 'code', 'season'),)

class League(Base):
    """联盟信息表"""
    __tablename__ = 'leagues'
    
    league_key = Column(String(50), primary_key=True)
    league_id = Column(String(20), nullable=False)
    game_key = Column(String(20), ForeignKey('games.game_key'), nullable=False)
    name = Column(String(200), nullable=False)
    url = Column(String(500))
    logo_url = Column(String(500))
    password = Column(String(100))
    draft_status = Column(String(50))  # predraft, postdraft等
    num_teams = Column(Integer, nullable=False)
    edit_key = Column(String(50))
    weekly_deadline = Column(String(50))
    league_update_timestamp = Column(String(50))
    scoring_type = Column(String(50))  # head等
    league_type = Column(String(50))  # private, public等
    renew = Column(String(50))  # 上一季league key
    renewed = Column(String(50))  # 下一季league key
    felo_tier = Column(String(50))  # gold, silver, platinum等
    iris_group_chat_id = Column(String(100))
    short_invitation_url = Column(String(500))
    allow_add_to_dl_extra_pos = Column(Boolean, default=False)
    is_pro_league = Column(Boolean, default=False)
    is_cash_league = Column(Boolean, default=False)
    current_week = Column(String(10))
    start_week = Column(String(10))
    start_date = Column(String(20))
    end_week = Column(String(10))
    end_date = Column(String(20))
    is_finished = Column(Boolean, default=False)
    is_plus_league = Column(Boolean, default=False)
    game_code = Column(String(20))
    season = Column(String(10), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    game = relationship("Game", back_populates="leagues")
    teams = relationship("Team", back_populates="league")
    league_settings = relationship("LeagueSettings", back_populates="league", uselist=False)
    players = relationship("Player", back_populates="league")
    transactions = relationship("Transaction", back_populates="league")
    stat_categories = relationship("StatCategory", back_populates="league")
    league_standings = relationship("LeagueStandings", back_populates="league")
    team_matchups = relationship("TeamMatchups", back_populates="league")
    roster_positions = relationship("LeagueRosterPosition", back_populates="league")
    
    # 索引
    __table_args__ = (
        Index('idx_league_game_season', 'game_key', 'season'),
        Index('idx_league_status', 'draft_status', 'is_finished'),
    )

class LeagueSettings(Base):
    """联盟设置表"""
    __tablename__ = 'league_settings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False, unique=True)
    draft_type = Column(String(50))  # live等
    is_auction_draft = Column(Boolean, default=False)
    persistent_url = Column(String(500))
    uses_playoff = Column(Boolean, default=True)
    has_playoff_consolation_games = Column(Boolean, default=False)
    playoff_start_week = Column(String(10))
    uses_playoff_reseeding = Column(Boolean, default=False)
    uses_lock_eliminated_teams = Column(Boolean, default=False)
    num_playoff_teams = Column(Integer)
    num_playoff_consolation_teams = Column(Integer, default=0)
    has_multiweek_championship = Column(Boolean, default=False)
    waiver_type = Column(String(20))  # FR等
    waiver_rule = Column(String(50))  # all等
    uses_faab = Column(Boolean, default=False)
    draft_time = Column(String(50))
    draft_pick_time = Column(String(10))
    post_draft_players = Column(String(10))  # W等
    max_teams = Column(Integer)
    waiver_time = Column(String(10))
    trade_end_date = Column(String(20))
    trade_ratify_type = Column(String(50))  # commish等
    trade_reject_time = Column(String(10))
    player_pool = Column(String(20))  # ALL等
    cant_cut_list = Column(String(50))  # none等
    draft_together = Column(Boolean, default=False)
    is_publicly_viewable = Column(Boolean, default=True)
    can_trade_draft_picks = Column(Boolean, default=False)
    sendbird_channel_url = Column(String(200))
    roster_positions = Column(JSON)  # 保留：存储阵容位置配置
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="league_settings")

class StatCategory(Base):
    """统计类别定义表 - 标记核心统计项"""
    __tablename__ = 'stat_categories'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    stat_id = Column(Integer, nullable=False)
    name = Column(String(200), nullable=False)
    display_name = Column(String(100), nullable=False)
    abbr = Column(String(20), nullable=False)
    group_name = Column(String(50))
    sort_order = Column(Integer)
    position_type = Column(String(10))
    is_enabled = Column(Boolean, default=True)
    is_only_display_stat = Column(Boolean, default=False)
    
    # 标记是否为核心统计项（用于提取到独立列）
    is_core_stat = Column(Boolean, default=False)
    core_stat_column = Column(String(50))  # 对应的列名
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="stat_categories")
    
    # 索引
    __table_args__ = (
        Index('idx_stat_category_unique', 'league_key', 'stat_id', unique=True),
        Index('idx_stat_category_league', 'league_key'),
        Index('idx_stat_category_core', 'is_core_stat', 'league_key'),
    )

class Team(Base):
    """团队信息表"""
    __tablename__ = 'teams'
    
    team_key = Column(String(50), primary_key=True)
    team_id = Column(String(20), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    name = Column(String(200), nullable=False)
    url = Column(String(500))
    team_logo_url = Column(String(500))
    division_id = Column(String(10))
    waiver_priority = Column(Integer)
    faab_balance = Column(String(20))
    number_of_moves = Column(Integer, default=0)
    number_of_trades = Column(Integer, default=0)
    roster_adds_week = Column(String(10))  # 当前周
    roster_adds_value = Column(String(10))  # 当前周添加次数
    clinched_playoffs = Column(Boolean, default=False)
    has_draft_grade = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="teams")
    managers = relationship("Manager", back_populates="team")
    roster_daily = relationship("RosterDaily", back_populates="team")
    league_standings = relationship("LeagueStandings", back_populates="team")
    team_matchups = relationship("TeamMatchups", back_populates="team", foreign_keys="TeamMatchups.team_key")
    
    # 索引
    __table_args__ = (Index('idx_team_league', 'league_key'),)

class Manager(Base):
    """团队管理员表"""
    __tablename__ = 'managers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    manager_id = Column(String(20), nullable=False)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    nickname = Column(String(100), nullable=False)
    guid = Column(String(100), nullable=False)
    is_commissioner = Column(Boolean, default=False)
    email = Column(String(200))
    image_url = Column(String(500))
    felo_score = Column(String(20))
    felo_tier = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="managers")

class Player(Base):
    """球员信息表（移除eligible_positions JSON列）"""
    __tablename__ = 'players'
    
    player_key = Column(String(50), primary_key=True)
    player_id = Column(String(20), nullable=False)
    editorial_player_key = Column(String(50), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    
    # 静态信息
    full_name = Column(String(200), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    
    # 动态信息
    current_team_key = Column(String(20))
    current_team_name = Column(String(100))
    current_team_abbr = Column(String(10))
    display_position = Column(String(50))
    primary_position = Column(String(10))
    position_type = Column(String(10))
    uniform_number = Column(String(10))
    status = Column(String(20))  # INJ等
    image_url = Column(String(500))
    headshot_url = Column(String(500))
    is_undroppable = Column(Boolean, default=False)
    season = Column(String(10), nullable=False)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="players")
    roster_daily = relationship("RosterDaily", back_populates="player")
    eligible_positions = relationship("PlayerEligiblePosition", back_populates="player")
    
    # 索引
    __table_args__ = (
        Index('idx_player_league', 'league_key'),
        Index('idx_player_editorial_key', 'editorial_player_key'),
        Index('idx_player_name', 'full_name'),
        Index('idx_player_position', 'display_position'),
    )

class PlayerEligiblePosition(Base):
    """球员合适位置表"""
    __tablename__ = 'player_eligible_positions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_key = Column(String(50), ForeignKey('players.player_key'), nullable=False)
    position = Column(String(10), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 关系
    player = relationship("Player", back_populates="eligible_positions")
    
    # 索引
    __table_args__ = (
        Index('idx_player_position_unique', 'player_key', 'position', unique=True),
        Index('idx_player_position_pos', 'position'),
    )

class RosterDaily(Base):
    """每日名单表 - 记录每天的球员分配情况"""
    __tablename__ = 'roster_daily'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    player_key = Column(String(50), ForeignKey('players.player_key'), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    
    # 时间维度 - 统一为date
    date = Column(Date, nullable=False)  # 名单日期
    season = Column(String(10), nullable=False)  # 赛季
    week = Column(Integer)  # 周数（NFL/NBA等）
    
    # 名单位置信息
    selected_position = Column(String(20))  # 当前选择的位置
    is_starting = Column(Boolean, default=False)  # 是否首发
    is_bench = Column(Boolean, default=False)  # 是否替补
    is_injured_reserve = Column(Boolean, default=False)  # 是否伤病名单
    
    # 球员状态信息
    player_status = Column(String(20))  # INJ等
    status_full = Column(String(100))  # 完整状态描述
    injury_note = Column(String(200))  # 伤病说明
    
    # Fantasy相关
    is_keeper = Column(Boolean, default=False)  # 是否keeper
    keeper_cost = Column(String(20))  # keeper成本
    is_prescoring = Column(Boolean, default=False)  # 是否预评分
    is_editable = Column(Boolean, default=False)  # 是否可编辑
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="roster_daily")
    player = relationship("Player", back_populates="roster_daily")
    
    # 索引
    __table_args__ = (
        Index('idx_roster_daily_unique', 'team_key', 'player_key', 'date', unique=True),
        Index('idx_roster_daily_date', 'date', 'league_key'),
        Index('idx_roster_daily_team_date', 'team_key', 'date'),
        Index('idx_roster_daily_player_date', 'player_key', 'date'),
        Index('idx_roster_daily_season', 'league_key', 'season'),
    )

class Transaction(Base):
    """交易记录表"""
    __tablename__ = 'transactions'
    
    transaction_key = Column(String(50), primary_key=True)
    transaction_id = Column(String(20), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    type = Column(String(50), nullable=False)  # add/drop, trade等
    status = Column(String(50), nullable=False)  # successful等
    timestamp = Column(String(50), nullable=False)
    
    # 两队交易的额外字段
    trader_team_key = Column(String(50))  # 交易发起方团队key
    trader_team_name = Column(String(200))  # 交易发起方团队名称
    tradee_team_key = Column(String(50))  # 交易接受方团队key
    tradee_team_name = Column(String(200))  # 交易接受方团队名称
    picks_data = Column(JSON)  # 存储draft picks交易数据
    
    players_data = Column(JSON)  # 保留：存储完整的球员交易数据
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="transactions")
    
    # 索引
    __table_args__ = (
        Index('idx_transaction_league', 'league_key'),
        Index('idx_transaction_type', 'type'),
        Index('idx_transaction_timestamp', 'timestamp'),
    )

class TransactionPlayer(Base):
    """交易球员详情表"""
    __tablename__ = 'transaction_players'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_key = Column(String(50), ForeignKey('transactions.transaction_key'), nullable=False)
    player_key = Column(String(50), nullable=False)
    player_id = Column(String(20), nullable=False)
    player_name = Column(String(200), nullable=False)
    editorial_team_abbr = Column(String(10))
    display_position = Column(String(50))
    position_type = Column(String(10))
    
    # 交易数据
    transaction_type = Column(String(20), nullable=False)  # add, drop, trade
    source_type = Column(String(50))  # freeagents, team, waivers等
    source_team_key = Column(String(50))
    source_team_name = Column(String(200))
    destination_type = Column(String(50))  # team, waivers等
    destination_team_key = Column(String(50))
    destination_team_name = Column(String(200))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 索引
    __table_args__ = (Index('idx_transaction_player_unique', 'transaction_key', 'player_key', unique=True),)

class DateDimension(Base):
    """日期维度表 - 用于管理赛季日程"""
    __tablename__ = 'date_dimension'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)  # 统一为date
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    season = Column(String(10), nullable=False)
    
    # 索引
    __table_args__ = (
        Index('idx_date_unique', 'date', 'league_key', unique=True),
        Index('idx_date_season', 'league_key', 'season'),
    )

class PlayerDailyStats(Base):
    """球员日统计表 - 只存储标准化核心统计数据"""
    __tablename__ = 'player_daily_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_key = Column(String(50), nullable=False)
    editorial_player_key = Column(String(50), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    season = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    week = Column(Integer)
    
    # 完整的11个统计项为独立列（基于Yahoo stat_categories）
    # stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
    field_goals_made = Column(Integer)      # 从 "9004003" 中提取的made部分
    field_goals_attempted = Column(Integer) # 从 "9004003" 中提取的attempted部分
    
    # stat_id: 5 - Field Goal Percentage (FG%)
    field_goal_percentage = Column(Numeric(6, 3))   # stat_id=5, 保留三位小数
    
    # stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
    free_throws_made = Column(Integer)      # 从 "9007006" 中提取的made部分
    free_throws_attempted = Column(Integer) # 从 "9007006" 中提取的attempted部分
    
    # stat_id: 8 - Free Throw Percentage (FT%)
    free_throw_percentage = Column(Numeric(6, 3))   # stat_id=8, 保留三位小数
    
    # stat_id: 10 - 3-point Shots Made (3PTM)
    three_pointers_made = Column(Integer)   # stat_id=10
    
    # stat_id: 12 - Points Scored (PTS)
    points = Column(Integer)                # stat_id=12
    
    # stat_id: 15 - Total Rebounds (REB)
    rebounds = Column(Integer)              # stat_id=15
    
    # stat_id: 16 - Assists (AST)
    assists = Column(Integer)               # stat_id=16
    
    # stat_id: 17 - Steals (ST)
    steals = Column(Integer)                # stat_id=17
    
    # stat_id: 18 - Blocked Shots (BLK)
    blocks = Column(Integer)                # stat_id=18
    
    # stat_id: 19 - Turnovers (TO)
    turnovers = Column(Integer)             # stat_id=19
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_player_daily_unique', 'player_key', 'date', unique=True),
        Index('idx_player_daily_league_date', 'league_key', 'date'),
        Index('idx_player_daily_points', 'points', 'date'),  # 按得分查询
        Index('idx_player_daily_season', 'season', 'date'),
    )

class PlayerSeasonStats(Base):
    """球员赛季统计表 - 只存储标准化核心统计数据"""
    __tablename__ = 'player_season_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_key = Column(String(50), nullable=False)
    editorial_player_key = Column(String(50), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    season = Column(String(10), nullable=False)
    
    # 完整的11个统计项为独立列（基于Yahoo stat_categories）
    # stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
    field_goals_made = Column(Integer)      # 从 "9004003" 中提取的made部分
    field_goals_attempted = Column(Integer) # 从 "9004003" 中提取的attempted部分
    
    # stat_id: 5 - Field Goal Percentage (FG%)
    field_goal_percentage = Column(Numeric(6, 3))   # stat_id=5
    
    # stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
    free_throws_made = Column(Integer)      # 从 "9007006" 中提取的made部分
    free_throws_attempted = Column(Integer) # 从 "9007006" 中提取的attempted部分
    
    # stat_id: 8 - Free Throw Percentage (FT%)
    free_throw_percentage = Column(Numeric(6, 3))   # stat_id=8
    
    # stat_id: 10 - 3-point Shots Made (3PTM)
    three_pointers_made = Column(Integer)   # stat_id=10
    
    # stat_id: 12 - Points Scored (PTS)
    total_points = Column(Integer)          # stat_id=12 (赛季累计)
    
    # stat_id: 15 - Total Rebounds (REB)
    total_rebounds = Column(Integer)        # stat_id=15
    
    # stat_id: 16 - Assists (AST)
    total_assists = Column(Integer)         # stat_id=16
    
    # stat_id: 17 - Steals (ST)
    total_steals = Column(Integer)          # stat_id=17
    
    # stat_id: 18 - Blocked Shots (BLK)
    total_blocks = Column(Integer)          # stat_id=18
    
    # stat_id: 19 - Turnovers (TO)
    total_turnovers = Column(Integer)       # stat_id=19
    
    # 派生统计项
    games_played = Column(Integer)          # 比赛场次
    avg_points = Column(Float)              # 平均分
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_player_season_unique', 'player_key', 'season', unique=True),
        Index('idx_player_season_league', 'league_key', 'season'),
        Index('idx_player_season_points', 'total_points', 'season'),
        Index('idx_player_season_avg', 'avg_points', 'season'),
    )

class TeamStatsWeekly(Base):
    """团队周统计表 - 存储每周统计数据"""
    __tablename__ = 'team_stats_weekly'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    season = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)  # 只存储周数据
    
    # 完整的11个团队周统计项（基于Yahoo stat_categories）
    # stat_id: 9004003 - Field Goals Made / Attempted (FGM/A)
    field_goals_made = Column(Integer)      # 从 "9004003" 中提取的made部分
    field_goals_attempted = Column(Integer) # 从 "9004003" 中提取的attempted部分
    
    # stat_id: 5 - Field Goal Percentage (FG%)
    field_goal_percentage = Column(Numeric(6, 3))   # stat_id=5
    
    # stat_id: 9007006 - Free Throws Made / Attempted (FTM/A)
    free_throws_made = Column(Integer)      # 从 "9007006" 中提取的made部分
    free_throws_attempted = Column(Integer) # 从 "9007006" 中提取的attempted部分
    
    # stat_id: 8 - Free Throw Percentage (FT%)
    free_throw_percentage = Column(Numeric(6, 3))   # stat_id=8
    
    # stat_id: 10 - 3-point Shots Made (3PTM)
    three_pointers_made = Column(Integer)   # stat_id=10
    
    # stat_id: 12 - Points Scored (PTS)
    points = Column(Integer)                # stat_id=12
    
    # stat_id: 15 - Total Rebounds (REB)
    rebounds = Column(Integer)              # stat_id=15
    
    # stat_id: 16 - Assists (AST)
    assists = Column(Integer)               # stat_id=16
    
    # stat_id: 17 - Steals (ST)
    steals = Column(Integer)                # stat_id=17
    
    # stat_id: 18 - Blocked Shots (BLK)
    blocks = Column(Integer)                # stat_id=18
    
    # stat_id: 19 - Turnovers (TO)
    turnovers = Column(Integer)             # stat_id=19
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 索引
    __table_args__ = (
        Index('idx_team_stat_weekly_unique', 'team_key', 'season', 'week', unique=True),
        Index('idx_team_stat_weekly_league', 'league_key', 'season', 'week'),
    )

class LeagueStandings(Base):
    """联盟排名表 - 存储联盟排名和团队总体表现"""
    __tablename__ = 'league_standings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    season = Column(String(10), nullable=False)
    
    # 排名信息
    rank = Column(Integer, nullable=False)
    playoff_seed = Column(String(10))  # 季后赛种子
    
    # 胜负记录
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    ties = Column(Integer, default=0)
    win_percentage = Column(Float)
    games_back = Column(String(10))  # 落后场次，"-" 表示第一名
    
    # 分区记录
    divisional_wins = Column(Integer, default=0)
    divisional_losses = Column(Integer, default=0)
    divisional_ties = Column(Integer, default=0)
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="league_standings")
    league = relationship("League", back_populates="league_standings")
    
    # 索引
    __table_args__ = (
        Index('idx_league_standings_unique', 'league_key', 'team_key', 'season', unique=True),
        Index('idx_league_standings_rank', 'league_key', 'season', 'rank'),
        Index('idx_league_standings_playoff', 'playoff_seed', 'season'),
    )

class TeamMatchups(Base):
    """团队对战表 - 存储每周的对战信息和结果（移除JSON，使用结构化字段）"""
    __tablename__ = 'team_matchups'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    season = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)
    
    # 对战基本信息
    week_start = Column(String(20))  # 周开始日期
    week_end = Column(String(20))    # 周结束日期
    status = Column(String(20))      # postevent, live等
    opponent_team_key = Column(String(50), ForeignKey('teams.team_key'))
    
    # 对战结果
    is_winner = Column(Boolean)      # 该团队是否获胜
    is_tied = Column(Boolean, default=False)
    team_points = Column(Integer)    # 该团队获得的点数(类别获胜数)
    opponent_points = Column(Integer)  # 对手获得的点数
    winner_team_key = Column(String(50))  # 获胜团队key（冗余但便于查询）
    
    # 特殊标记
    is_playoffs = Column(Boolean, default=False)
    is_consolation = Column(Boolean, default=False)
    is_matchup_of_week = Column(Boolean, default=False)
    
    # 统计类别获胜详情（结构化存储，替代JSON）
    wins_field_goal_pct = Column(Boolean, default=False)      # stat_id: 5 - FG%
    wins_free_throw_pct = Column(Boolean, default=False)      # stat_id: 8 - FT%
    wins_three_pointers = Column(Boolean, default=False)      # stat_id: 10 - 3PTM
    wins_points = Column(Boolean, default=False)              # stat_id: 12 - PTS
    wins_rebounds = Column(Boolean, default=False)            # stat_id: 15 - REB
    wins_assists = Column(Boolean, default=False)             # stat_id: 16 - AST
    wins_steals = Column(Boolean, default=False)              # stat_id: 17 - ST
    wins_blocks = Column(Boolean, default=False)              # stat_id: 18 - BLK
    wins_turnovers = Column(Boolean, default=False)           # stat_id: 19 - TO
    
    # 团队比赛场次信息
    completed_games = Column(Integer)     # 已完成比赛数
    remaining_games = Column(Integer)     # 剩余比赛数
    live_games = Column(Integer)          # 进行中比赛数
    
    # 对手比赛场次信息
    opponent_completed_games = Column(Integer)
    opponent_remaining_games = Column(Integer)
    opponent_live_games = Column(Integer)
    
    # 元数据
    fetched_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系 - 通过外键关联到 TeamStatsWeekly
    team = relationship("Team", back_populates="team_matchups", foreign_keys=[team_key])
    league = relationship("League", back_populates="team_matchups")
    
    # 索引
    __table_args__ = (
        Index('idx_team_matchup_unique', 'team_key', 'season', 'week', unique=True),
        Index('idx_team_matchup_league', 'league_key', 'season', 'week'),
        Index('idx_team_matchup_opponent', 'opponent_team_key', 'week'),
        Index('idx_team_matchup_playoffs', 'is_playoffs', 'season'),
        Index('idx_team_matchup_winner', 'winner_team_key', 'week'),
        Index('idx_team_matchup_stats_reference', 'team_key', 'season', 'week'),  # 关联TeamStatsWeekly的索引
    )

class LeagueRosterPosition(Base):
    """联盟阵容位置表（解析 league_settings.roster_positions）"""
    __tablename__ = 'league_roster_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    position = Column(String(20), nullable=False)
    position_type = Column(String(10))
    count = Column(Integer, default=0)
    is_starting_position = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    league = relationship("League", back_populates="roster_positions")

    __table_args__ = (
        Index('idx_roster_position_league', 'league_key'),
        Index('idx_roster_position_unique', 'league_key', 'position', unique=True),
    )

# 数据库连接配置
def get_database_url():
    """获取数据库连接URL"""
    db_user = os.getenv('DB_USER', 'fantasy_user')
    db_password = os.getenv('DB_PASSWORD', 'fantasyPassword')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'fantasy_db')
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def create_database_engine():
    """创建数据库引擎"""
    database_url = get_database_url()
    engine = create_engine(database_url, echo=False)  # 关闭详细日志
    return engine

def create_tables(engine):
    """创建所有表"""
    Base.metadata.create_all(engine)

def recreate_tables(engine):
    """重新创建所有表（先删除再创建）"""
    print("🔄 重新创建数据库表...")
    
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # 首先查询数据库中的所有表
                result = conn.execute(text("""
                    SELECT tablename FROM pg_tables 
                    WHERE schemaname = 'public' 
                    ORDER BY tablename;
                """))
                existing_tables = [row[0] for row in result.fetchall()]
                
                if existing_tables:
                    print(f"发现 {len(existing_tables)} 个现有表")
                    
                    # 删除所有现有表，使用CASCADE处理依赖
                    for table_name in existing_tables:
                        try:
                            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                            print(f"✓ 删除表 {table_name}")
                        except Exception as e:
                            print(f"删除表 {table_name} 时出错: {e}")
                            
                    # 确保删除可能遗留的旧表
                    legacy_tables = ['rosters', 'roster_history', 'player_stats_history', 'player_season_stats', 'player_daily_stats', 'team_stats']
                    for table_name in legacy_tables:
                        try:
                            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                            print(f"✓ 删除遗留表 {table_name}")
                        except Exception as e:
                            print(f"删除遗留表 {table_name} 时出错（可能不存在）: {e}")
                else:
                    print("✓ 数据库中没有现有表")
                
                trans.commit()
                print("✓ 所有表删除完成")
                
            except Exception as e:
                trans.rollback()
                raise e
    
    except Exception as e:
        print(f"删除表时出错: {e}")
        print("尝试使用SQLAlchemy标准方法...")
        try:
            # 如果CASCADE删除失败，尝试标准删除
            Base.metadata.drop_all(engine)
            print("✓ 使用标准方法删除表成功")
        except Exception as e2:
            print(f"标准删除也失败: {e2}")
            print("⚠️ 无法自动删除表，请手动执行以下SQL:")
            print("DROP SCHEMA public CASCADE;")
            print("CREATE SCHEMA public;")
            print("然后重新运行程序")
            return False
    
    # 重新创建所有表
    try:
        Base.metadata.create_all(engine)
        print("✅ 数据库表重新创建完成")
        return True
    except Exception as e:
        print(f"创建表失败: {e}")
        return False

def get_session(engine):
    """获取数据库会话"""
    Session = sessionmaker(bind=engine)
    return Session()

if __name__ == "__main__":
    # 创建数据库引擎
    engine = create_database_engine()
    
    # 创建所有表
    create_tables(engine)
    
    print("数据库模型创建完成！")
    print("表包括：")
    for table_name in Base.metadata.tables.keys():
        print(f"  - {table_name}") 