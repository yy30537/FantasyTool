from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, Table, DateTime, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import os
from dotenv import load_dotenv
from datetime import datetime

# 加载环境变量
load_dotenv()

# 数据库配置
DB_USER = os.getenv('DB_USER', 'fantasy_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'fantasyPassword')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'fantasy_db')

# 创建数据库连接
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 用户表
class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    yahoo_guid = Column(String(255), unique=True, index=True)
    name = Column(String(255))
    nickname = Column(String(255))
    email = Column(String(255))
    felo_score = Column(Integer, nullable=True)
    felo_tier = Column(String(50), nullable=True)
    image_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    games = relationship("UserGame", back_populates="user")
    teams = relationship("Team", back_populates="manager")

# 游戏表
class Game(Base):
    __tablename__ = "games"
    
    id = Column(Integer, primary_key=True, index=True)
    game_key = Column(String(50), unique=True, index=True)
    game_id = Column(String(50), unique=True)
    name = Column(String(255))
    code = Column(String(50))
    type = Column(String(50))
    url = Column(String(255))
    season = Column(String(50))
    is_registration_over = Column(Boolean, default=False)
    is_game_over = Column(Boolean, default=False)
    is_offseason = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    leagues = relationship("League", back_populates="game")
    users = relationship("UserGame", back_populates="game")

# 用户游戏关联表
class UserGame(Base):
    __tablename__ = "user_games"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    game_id = Column(Integer, ForeignKey("games.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 关系
    user = relationship("User", back_populates="games")
    game = relationship("Game", back_populates="users")

# 联盟表
class League(Base):
    __tablename__ = "leagues"
    
    id = Column(Integer, primary_key=True, index=True)
    league_key = Column(String(255), unique=True, index=True)
    league_id = Column(String(255))
    name = Column(String(255))
    url = Column(String(255))
    logo_url = Column(String(255), nullable=True)
    draft_status = Column(String(50))
    num_teams = Column(Integer, default=0)
    edit_key = Column(String(255), nullable=True)
    weekly_deadline = Column(String(255), nullable=True)
    league_update_timestamp = Column(String(255), nullable=True)
    scoring_type = Column(String(50))
    league_type = Column(String(50), default="unknown")
    renew = Column(String(255), nullable=True)
    renewed = Column(String(255), nullable=True)
    felo_tier = Column(String(50), nullable=True)
    iris_group_chat_id = Column(String(255), nullable=True)
    allow_add_to_dl_extra_pos = Column(Integer, default=0)
    is_pro_league = Column(String(10), default="0")
    is_cash_league = Column(String(10), default="0")
    current_week = Column(Integer, nullable=True)
    start_week = Column(Integer, nullable=True)
    end_week = Column(Integer, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    is_finished = Column(Boolean, default=False)
    is_plus_league = Column(String(10), default="0")
    game_code = Column(String(50), nullable=True)
    season = Column(String(50), nullable=True)
    game_id = Column(Integer, ForeignKey("games.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    game = relationship("Game", back_populates="leagues")
    teams = relationship("Team", back_populates="league")
    settings = relationship("LeagueSetting", back_populates="league", uselist=False)
    scoreboards = relationship("Scoreboard", back_populates="league")

# 联盟设置表
class LeagueSetting(Base):
    __tablename__ = "league_settings"
    
    id = Column(Integer, primary_key=True, index=True)
    league_id = Column(Integer, ForeignKey("leagues.id"), unique=True)
    
    # 基本设置
    draft_type = Column(String(50), nullable=True)
    is_auction_draft = Column(Boolean, default=False)
    scoring_type = Column(String(50), nullable=True)
    uses_playoff = Column(Boolean, default=False)
    has_playoff_consolation_games = Column(Boolean, default=False)
    playoff_start_week = Column(Integer, nullable=True)
    uses_playoff_reseeding = Column(Boolean, default=False)
    uses_lock_eliminated_teams = Column(Boolean, default=False)
    num_playoff_teams = Column(Integer, nullable=True)
    num_playoff_consolation_teams = Column(Integer, nullable=True)
    has_multiweek_championship = Column(Boolean, default=False)
    waiver_type = Column(String(10), nullable=True)
    waiver_rule = Column(String(50), nullable=True)
    uses_faab = Column(Boolean, default=False)
    draft_time = Column(String(50), nullable=True)
    draft_pick_time = Column(Integer, nullable=True)
    post_draft_players = Column(String(10), nullable=True)
    max_teams = Column(Integer, nullable=True)
    waiver_time = Column(Integer, nullable=True)
    trade_end_date = Column(String(50), nullable=True)
    trade_ratify_type = Column(String(50), nullable=True)
    trade_reject_time = Column(Integer, nullable=True)
    player_pool = Column(String(50), nullable=True)
    cant_cut_list = Column(String(50), nullable=True)
    draft_together = Column(Boolean, default=False)
    max_weekly_adds = Column(Integer, nullable=True)
    uses_median_score = Column(Boolean, default=False)
    
    # 复杂数据存储为JSON
    roster_positions = Column(JSON, nullable=True)  # 阵容位置配置
    stat_categories = Column(JSON, nullable=True)   # 统计类别配置
    league_premium_features = Column(JSON, nullable=True)  # 高级功能
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="settings")

# 队伍表
class Team(Base):
    __tablename__ = "teams"
    
    id = Column(Integer, primary_key=True, index=True)
    team_key = Column(String(255), unique=True, index=True)
    team_id = Column(String(255))
    name = Column(String(255))
    is_owned_by_current_user = Column(Boolean, default=False)
    url = Column(String(255))
    team_logo = Column(String(255), nullable=True)
    waiver_priority = Column(Integer, nullable=True)
    number_of_moves = Column(Integer, default=0)
    number_of_trades = Column(Integer, default=0)
    clinched_playoffs = Column(Boolean, default=False)
    has_draft_grade = Column(Boolean, default=False)
    league_scoring_type = Column(String(50), nullable=True)
    league_id = Column(Integer, ForeignKey("leagues.id"))
    manager_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="teams")
    manager = relationship("User", back_populates="teams")
    roster = relationship("TeamPlayer", back_populates="team")
    standings = relationship("TeamStanding", back_populates="team", uselist=False)
    season_stats = relationship("TeamSeasonStats", back_populates="team", uselist=False)
    matchup_performances = relationship("MatchupTeam", back_populates="team")

# 队伍赛季统计表（从standings数据中提取）
class TeamSeasonStats(Base):
    __tablename__ = "team_season_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), unique=True)
    season = Column(String(50))
    
    # 投篮统计
    field_goals_made = Column(Integer, nullable=True)
    field_goals_attempted = Column(Integer, nullable=True)
    field_goal_percentage = Column(Float, nullable=True)
    
    # 罚球统计
    free_throws_made = Column(Integer, nullable=True)
    free_throws_attempted = Column(Integer, nullable=True)
    free_throw_percentage = Column(Float, nullable=True)
    
    # 三分球统计
    three_pointers_made = Column(Integer, nullable=True)
    
    # 基本统计
    points = Column(Integer, nullable=True)
    rebounds = Column(Integer, nullable=True)
    assists = Column(Integer, nullable=True)
    steals = Column(Integer, nullable=True)
    blocks = Column(Integer, nullable=True)
    turnovers = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="season_stats")

# 队伍排名表（简化版，只存储排名相关信息）
class TeamStanding(Base):
    __tablename__ = "team_standings"
    
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), unique=True)
    
    # 排名信息
    rank = Column(Integer, nullable=True)
    playoff_seed = Column(String(50), nullable=True)
    
    # 胜负记录
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    ties = Column(Integer, default=0)
    percentage = Column(Float, nullable=True)
    games_back = Column(String(50), nullable=True)
    
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="standings")

# 球员表
class Player(Base):
    __tablename__ = "players"
    
    id = Column(Integer, primary_key=True, index=True)
    player_key = Column(String(255), unique=True, index=True)
    player_id = Column(String(255))
    name = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    editorial_player_key = Column(String(255), nullable=True)
    editorial_team_key = Column(String(255), nullable=True)
    editorial_team_full_name = Column(String(255), nullable=True)
    editorial_team_abbr = Column(String(50), nullable=True)
    uniform_number = Column(String(20), nullable=True)
    display_position = Column(String(50))
    headshot_url = Column(String(255), nullable=True)
    is_undroppable = Column(Boolean, default=False)
    position_type = Column(String(50))
    eligible_positions = Column(String(255))  # 存储逗号分隔的位置列表
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    teams = relationship("TeamPlayer", back_populates="player")
    stats = relationship("PlayerStat", back_populates="player")

# 队伍球员关联表
class TeamPlayer(Base):
    __tablename__ = "team_players"
    
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"))
    player_id = Column(Integer, ForeignKey("players.id"))
    position = Column(String(50))
    selected_position = Column(String(50))
    is_starting = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    team = relationship("Team", back_populates="roster")
    player = relationship("Player", back_populates="teams")

# 球员统计数据表
class PlayerStat(Base):
    __tablename__ = "player_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"))
    game_id = Column(Integer, ForeignKey("games.id"))
    season = Column(String(50))
    week = Column(Integer, nullable=True)
    stats_data = Column(JSON)  # 存储JSON格式的统计数据
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    player = relationship("Player", back_populates="stats")
    game = relationship("Game")

# 记分板表（重构：更清晰的结构）
class Scoreboard(Base):
    __tablename__ = "scoreboards"
    
    id = Column(Integer, primary_key=True, index=True)
    league_id = Column(Integer, ForeignKey("leagues.id"))
    week = Column(Integer)
    week_start = Column(String(50), nullable=True)
    week_end = Column(String(50), nullable=True)
    status = Column(String(50))
    is_playoffs = Column(Boolean, default=False)
    is_consolation = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    league = relationship("League", back_populates="scoreboards")
    matchups = relationship("Matchup", back_populates="scoreboard")

# 比赛对决表（重构：简化结构）
class Matchup(Base):
    __tablename__ = "matchups"
    
    id = Column(Integer, primary_key=True, index=True)
    scoreboard_id = Column(Integer, ForeignKey("scoreboards.id"))
    week = Column(Integer)
    week_start = Column(String(50), nullable=True)
    week_end = Column(String(50), nullable=True)
    status = Column(String(50))
    is_playoffs = Column(Boolean, default=False)
    is_consolation = Column(Boolean, default=False)
    is_tied = Column(Boolean, default=False)
    winner_team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    scoreboard = relationship("Scoreboard", back_populates="matchups")
    winner_team = relationship("Team", foreign_keys=[winner_team_id])
    teams = relationship("MatchupTeam", back_populates="matchup")

# 对决团队表（重构：添加更多统计信息）
class MatchupTeam(Base):
    __tablename__ = "matchup_teams"
    
    id = Column(Integer, primary_key=True, index=True)
    matchup_id = Column(Integer, ForeignKey("matchups.id"))
    team_id = Column(Integer, ForeignKey("teams.id"))
    
    # 得分信息
    points = Column(Float, nullable=True)
    projected_points = Column(Float, nullable=True)
    
    # 详细统计（如果可用）
    field_goals_made = Column(Integer, nullable=True)
    field_goals_attempted = Column(Integer, nullable=True)
    field_goal_percentage = Column(Float, nullable=True)
    free_throws_made = Column(Integer, nullable=True)
    free_throws_attempted = Column(Integer, nullable=True)
    free_throw_percentage = Column(Float, nullable=True)
    three_pointers_made = Column(Integer, nullable=True)
    total_points = Column(Integer, nullable=True)
    rebounds = Column(Integer, nullable=True)
    assists = Column(Integer, nullable=True)
    steals = Column(Integer, nullable=True)
    blocks = Column(Integer, nullable=True)
    turnovers = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关系
    matchup = relationship("Matchup", back_populates="teams")
    team = relationship("Team", back_populates="matchup_performances")

# 创建所有表
def create_tables():
    Base.metadata.create_all(bind=engine)

# 获取数据库会话
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 