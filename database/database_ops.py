import os
from typing import Dict

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker



from database.model import (
    Base, Game, League, LeagueSettings, Team, Manager, Player, StatCategory,
    PlayerEligiblePosition, PlayerSeasonStats, PlayerDailyStats,
    TeamStatsWeekly, LeagueStandings, TeamMatchups,
    RosterDaily, Transaction, TransactionPlayer, DateDimension,
    LeagueRosterPosition
)

class DatabaseOps:

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
        database_url = DatabaseOps.get_database_url()
        engine = create_engine(database_url, echo=False)  # 关闭详细日志
        return engine

    def create_tables(engine):
        """创建所有表"""
        Base.metadata.create_all(engine)

    def recreate_tables(engine):
        """重新创建所有表（先删除再创建）"""
        print("🔄 重新创建数据库表...")

        try:
            print("删除所有表...")
            Base.metadata.drop_all(engine)
            print("✓ 成功删除所有表")
        except Exception as e:
            print(f"SQLAlchemy标准删除失败: {e}")
            print("尝试手动删除表...")
            
            # 如果标准方法失败，尝试手动删除
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
                            print("开始删除现有表...")
                            for i, table_name in enumerate(existing_tables):
                                try:
                                    print(f"正在删除表 {table_name} ({i+1}/{len(existing_tables)})...")
                                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                                    print(f"✓ 删除表 {table_name}")
                                except Exception as e:
                                    print(f"删除表 {table_name} 时出错: {e}")
                                    # 继续删除其他表，不要因为一个表失败就停止
                        else:
                            print("✓ 数据库中没有现有表")

                        trans.commit()
                        print("✓ 所有表删除完成")

                    except Exception as e:
                        trans.rollback()
                        raise e

            except Exception as e2:
                print(f"手动删除表也失败: {e2}")
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

    def clear_database(confirm: bool = False) -> bool:
        """清空并重建数据库"""
        if not confirm:
            print("⚠️ 此操作将删除所有数据！")
        print("🔄 重新创建数据库表...")
        try:
            # 使用recreate_tables重建所有表
            engine = DatabaseOps.create_database_engine()

            success = DatabaseOps.recreate_tables(engine)
            if success:
                print("✅ 数据库清空并重建成功")
                return True
            else:
                print("❌ 数据库重建失败")
                return False

        except Exception as e:
            print(f"❌ 清空数据库失败: {e}")
            return False

    def show_database_summary():
        """显示数据库摘要"""
        try:

            print("\n📊 数据库摘要:")
            print("-" * 60)
            
            # 统计各表数据量
            tables = [
                ("Game", Game),
                ("League", League), 
                ("LeagueSettings", LeagueSettings),
                ("StatCategory", StatCategory),
                ("Team", Team),
                ("Manager", Manager),
                ("Player", Player),
                ("PlayerEligiblePosition", PlayerEligiblePosition),
                ("Transaction", Transaction),
                ("TransactionPlayer", TransactionPlayer),
                ("RosterDaily", RosterDaily),
                ("PlayerSeasonStats", PlayerSeasonStats),
                ("PlayerDailyStats", PlayerDailyStats),
                ("TeamStatsWeekly", TeamStatsWeekly),
                ("LeagueStandings", LeagueStandings),
                ("TeamMatchups", TeamMatchups),
                ("DateDimension", DateDimension)
            ]

            engine = DatabaseOps.create_database_engine()
            session = DatabaseOps.get_session(engine)

            for name, model in tables:
                try:
                    count = session.query(model).count()
                    
                    print(f"{count:6d} {name:12}")
                except Exception as e:
                    print(f"{name:12}: Query Faileds ({e})")
            
            print("-" * 60)
            
            
        except Exception as e:
            print(f"显示数据库摘要失败: {e}")
        finally:
            session.close()
            engine.dispose()
