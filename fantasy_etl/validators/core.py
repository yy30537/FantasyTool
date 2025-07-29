"""
核心验证器
包含所有 verify_* 函数，从 archive/yahoo_api_data.py 迁移
"""

from typing import Optional, Dict
from sqlalchemy.orm import Session


class CoreValidators:
    """核心验证器"""
    
    def __init__(self, session: Session = None, selected_league: Dict = None):
        """
        初始化验证器
        
        Args:
            session: 数据库会话（可选）
            selected_league: 当前选择的联盟信息（可选）
        """
        self.session = session
        self.selected_league = selected_league
    
    # ============================================================================
    # 从 archive/yahoo_api_data.py 迁移的 verify_* 函数
    # ============================================================================
    
    def verify_league_exists_in_db(self) -> bool:
        """
        确保当前选择的联盟存在于数据库中
        
        迁移自: archive/yahoo_api_data.py verify_league_exists_in_db() 第270行
        
        Returns:
            联盟是否存在于数据库中
        """
        if not self.selected_league:
            return False
        
        if not self.session:
            pass
            return False
        
        league_key = self.selected_league['league_key']
        
        try:
            # 检查联盟是否已存在于数据库中
            from fantasy_etl.database import League
            existing_league = self.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if existing_league:
                return True
            
            # 联盟不存在，说明数据库中缺少完整数据，建议重新获取
            pass
            return False
                
        except Exception as e:
            pass
            return False
        
    def verify_league_selected(self) -> bool:
        """
        确保已选择联盟
        
        迁移自: archive/yahoo_api_data.py verify_league_selected() 第1397行
        
        Returns:
            是否已选择联盟
        """
        if not self.selected_league:
            pass
            return False
        return True
        
    # ============================================================================
    # 扩展验证方法 (新增)
    # ============================================================================
    
    def verify_database_connection(self) -> bool:
        """
        验证数据库连接状态
        
        新增方法，用于系统启动时的基础验证
        
        Returns:
            数据库连接是否正常
        """
        if not self.session:
            pass
            return False
        
        try:
            # 尝试执行简单查询
            self.session.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
        
    def verify_api_credentials(self) -> bool:
        """
        验证Yahoo API凭据
        
        新增方法，用于API调用前的凭据验证
        
        Returns:
            API凭据是否有效
        """
        import os
        
        # 检查必要的环境变量
        client_id = os.getenv('YAHOO_CLIENT_ID')
        client_secret = os.getenv('YAHOO_CLIENT_SECRET')
        
        if not client_id or not client_secret:
            print("❌ Yahoo API凭据未设置")
            print("请在.env文件中设置YAHOO_CLIENT_ID和YAHOO_CLIENT_SECRET")
            return False
        
        # 检查令牌文件
        import os.path
        token_file = os.path.join('tokens', 'yahoo_token.pkl')
        
        if not os.path.exists(token_file):
            print("⚠️ OAuth令牌文件不存在，需要进行授权")
            return False
        
        # 检查令牌有效性
        try:
            import pickle
            with open(token_file, 'rb') as f:
                token = pickle.load(f)
            
            # 检查必要字段
            if not token.get('access_token') or not token.get('refresh_token'):
                print("❌ 令牌文件格式无效")
                return False
            
            # 检查是否过期
            import time
            expires_at = token.get('expires_at', 0)
            if expires_at < time.time():
                print("⚠️ 令牌已过期，需要刷新")
                # 这里不返回False，因为可以自动刷新
            
            return True
            
        except Exception as e:
            print(f"❌ 读取令牌文件失败: {e}")
            return False
        
    def verify_season_data_integrity(self, league_key: str, season: str) -> bool:
        """
        验证赛季数据完整性
        
        新增方法，用于数据完整性检查
        
        Args:
            league_key: 联盟键
            season: 赛季
            
        Returns:
            数据是否完整
        """
        if not self.session:
            pass
            return False
        
        try:
            from fantasy_etl.database import (
                League, Team, Player, DateDimension
            )
            
            # 检查联盟是否存在
            league = self.session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if not league:
                print(f"❌ 联盟 {league_key} 不存在")
                return False
            
            # 检查是否有团队数据
            team_count = self.session.query(Team).filter_by(
                league_key=league_key
            ).count()
            
            if team_count == 0:
                print("❌ 缺少团队数据")
                return False
            
            # 检查是否有球员数据
            player_count = self.session.query(Player).count()
            
            if player_count == 0:
                print("❌ 缺少球员数据")
                return False
            
            # 检查是否有日期维度数据
            date_count = self.session.query(DateDimension).filter_by(
                league_key=league_key,
                season=season
            ).count()
            
            if date_count == 0:
                print("❌ 缺少日期维度数据")
                return False
            
            print("✅ 数据完整性检查通过")
            return True
            
        except Exception as e:
            print(f"❌ 数据完整性检查失败: {e}")
            return False
    
    def verify_table_exists(self, table_name: str) -> bool:
        """
        验证数据库表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        if not self.session:
            return False
        
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.session.bind)
            return table_name in inspector.get_table_names()
        except Exception:
            return False
    
    def verify_required_tables(self) -> bool:
        """
        验证所有必需的表是否存在
        
        Returns:
            所有必需表是否都存在
        """
        required_tables = [
            'games', 'leagues', 'league_settings', 'stat_categories',
            'teams', 'managers', 'players', 'player_eligible_positions',
            'roster_daily', 'player_daily_stats', 'player_season_stats',
            'team_stats_weekly', 'league_standings', 'team_matchups',
            'transactions', 'transaction_players', 'date_dimensions',
            'league_roster_positions'
        ]
        
        missing_tables = []
        for table in required_tables:
            if not self.verify_table_exists(table):
                missing_tables.append(table)
        
        if missing_tables:
            print(f"❌ 缺少以下数据库表: {', '.join(missing_tables)}")
            return False
        
        return True
    
    def set_selected_league(self, league: Dict):
        """设置当前选择的联盟"""
        self.selected_league = league
    
    def set_session(self, session: Session):
        """设置数据库会话"""
        self.session = session

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def verify_league_data(league_data: Dict) -> bool:
    """验证联盟数据"""
    validator = CoreValidators()
    return validator.verify_league_data(league_data)

def verify_team_data(team_data: Dict) -> bool:
    """验证团队数据"""
    validator = CoreValidators()
    return validator.verify_team_data(team_data)

def verify_player_data(player_data: Dict) -> bool:
    """验证球员数据"""
    validator = CoreValidators()
    return validator.verify_player_data(player_data)

def verify_transaction_data(transaction_data: Dict) -> bool:
    """验证交易数据"""
    validator = CoreValidators()
    return validator.verify_transaction_data(transaction_data)

def verify_stats_data(stats_data: Dict) -> bool:
    """验证统计数据"""
    validator = CoreValidators()
    return validator.verify_stats_data(stats_data)