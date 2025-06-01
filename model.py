from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text, JSON, Float, ForeignKey, BigInteger
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
    roster_positions = Column(JSON)  # 存储阵容位置配置
    stat_categories = Column(JSON)  # 存储统计类别配置
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="league_settings")

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
    rosters = relationship("Roster", back_populates="team")

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
    """球员信息表（合并静态和动态信息）"""
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
    eligible_positions = Column(JSON)  # 存储合适的位置列表
    season = Column(String(10), nullable=False)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="players")
    player_stats = relationship("PlayerStats", back_populates="player")
    rosters = relationship("Roster", back_populates="player")

class PlayerStats(Base):
    """球员统计数据表"""
    __tablename__ = 'player_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_key = Column(String(50), ForeignKey('players.player_key'), nullable=False)
    stat_id = Column(String(20), nullable=False)  # 统计项ID（如 9004003, 5, 8等）
    value = Column(String(100), nullable=False)  # 统计值
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    player = relationship("Player", back_populates="player_stats")

class Roster(Base):
    """团队名单表"""
    __tablename__ = 'rosters'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_key = Column(String(50), ForeignKey('teams.team_key'), nullable=False)
    player_key = Column(String(50), ForeignKey('players.player_key'), nullable=False)
    coverage_date = Column(String(20))  # 名单日期
    is_prescoring = Column(Boolean, default=False)
    is_editable = Column(Boolean, default=False)
    
    # 球员当前状态信息
    status = Column(String(20))  # INJ等
    status_full = Column(String(100))
    injury_note = Column(String(200))
    is_keeper = Column(Boolean, default=False)
    keeper_cost = Column(String(20))
    kept = Column(Boolean, default=False)
    
    # 位置信息
    selected_position = Column(String(20))  # 当前选择的位置
    eligible_positions_to_add = Column(JSON)  # 可以添加的位置
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="rosters")
    player = relationship("Player", back_populates="rosters")

class Transaction(Base):
    """交易记录表"""
    __tablename__ = 'transactions'
    
    transaction_key = Column(String(50), primary_key=True)
    transaction_id = Column(String(20), nullable=False)
    league_key = Column(String(50), ForeignKey('leagues.league_key'), nullable=False)
    type = Column(String(50), nullable=False)  # add/drop, trade等
    status = Column(String(50), nullable=False)  # successful等
    timestamp = Column(String(50), nullable=False)
    players_data = Column(JSON)  # 存储完整的球员交易数据
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="transactions")

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
    engine = create_engine(database_url, echo=True)
    return engine

def create_tables(engine):
    """创建所有表"""
    Base.metadata.create_all(engine)

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