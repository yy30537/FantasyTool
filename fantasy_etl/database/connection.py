"""
数据库连接管理
从 archive/model.py 迁移数据库连接功能
"""

import os
from typing import Optional
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    create_engine = None
    text = None
    sessionmaker = None
    declarative_base = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 创建Base - 这将在后续的模型模块中使用
Base = declarative_base() if declarative_base else None


class DatabaseConnection:
    """数据库连接管理器"""
    
    def __init__(self):
        """初始化数据库连接"""
        self.engine = None
        self.session_factory = None
        self.session = None
        
    def get_database_url(self) -> str:
        """
        获取数据库连接URL
        
        迁移自: archive/model.py get_database_url() 第707行
        """
        db_user = os.getenv('DB_USER', 'fantasy_user')
        db_password = os.getenv('DB_PASSWORD', 'fantasyPassword')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'fantasy_db')
        
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
    def create_database_engine(self):
        """
        创建数据库引擎
        
        迁移自: archive/model.py create_database_engine() 第717行
        """
        database_url = self.get_database_url()
        self.engine = create_engine(database_url, echo=False)  # 关闭详细日志
        return self.engine
        
    def get_session(self):
        """
        获取数据库会话
        
        迁移自: archive/model.py get_session() 第796行
        """
        if not self.engine:
            self.create_database_engine()
            
        if not self.session_factory:
            self.session_factory = sessionmaker(bind=self.engine)
            
        if not self.session:
            self.session = self.session_factory()
            
        return self.session
        
    def create_tables(self):
        """
        创建所有表
        
        迁移自: archive/model.py create_tables() 第723行
        """
        if not self.engine:
            self.create_database_engine()
            
        Base.metadata.create_all(self.engine)
        
    def recreate_tables(self) -> bool:
        """
        重新创建所有表（先删除再创建）
        
        迁移自: archive/model.py recreate_tables() 第727行
        """
        if not self.engine:
            self.create_database_engine()
            
        print("🔄 重新创建数据库表...")
        
        try:
            with self.engine.connect() as conn:
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
                Base.metadata.drop_all(self.engine)
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
            Base.metadata.create_all(self.engine)
            print("✅ 数据库表重新创建完成")
            return True
        except Exception as e:
            print(f"创建表失败: {e}")
            return False
        
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
            self.session = None
            
    def __enter__(self):
        """上下文管理器入口"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


# 便捷函数（保持与原始代码的兼容性）
def get_database_url() -> str:
    """获取数据库连接URL - 便捷函数"""
    db_connection = DatabaseConnection()
    return db_connection.get_database_url()


def create_database_engine():
    """创建数据库引擎 - 便捷函数"""
    db_connection = DatabaseConnection()
    return db_connection.create_database_engine()


def get_session(engine):
    """获取数据库会话 - 便捷函数"""
    Session = sessionmaker(bind=engine)
    return Session()


def create_tables(engine):
    """创建所有表 - 便捷函数"""
    Base.metadata.create_all(engine)


def recreate_tables(engine) -> bool:
    """重新创建所有表 - 便捷函数"""
    db_connection = DatabaseConnection()
    db_connection.engine = engine
    return db_connection.recreate_tables()