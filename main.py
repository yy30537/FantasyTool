#!/usr/bin/env python3
"""
Yahoo Fantasy Sports ETL 工具 - 统一主入口
提供Web界面OAuth认证和数据获取功能的统一访问点
"""

import sys
import os
import argparse
from pathlib import Path

# 确保可以正确导入模块
current_dir = os.path.dirname(os.path.abspath(__file__))
archive_dir = os.path.join(current_dir, 'archive')
sys.path.insert(0, current_dir)
sys.path.insert(0, archive_dir)

def run_web_app():
    """启动Web应用进行OAuth认证"""
    try:
        # 检查令牌状态
        from fantasy_etl.api.client import YahooFantasyAPIClient
        from datetime import datetime
        
        client = YahooFantasyAPIClient()
        DEFAULT_TOKEN_FILE = client.token_file_path
        
        token_exists = DEFAULT_TOKEN_FILE.exists()
        token_valid = check_token_status_silent()
        
        if token_exists and not token_valid:
            print("⚠️ 令牌已过期")
            confirm = input("重新认证? (y/n): ").strip().lower()
            if confirm != 'y':
                return
        elif token_valid:
            print("✅ 令牌有效")
            confirm = input("重新认证? (y/n): ").strip().lower()
            if confirm != 'y':
                return
        else:
            print("🔐 需要OAuth认证")
        
        # 导入OAuth认证器
        from fantasy_etl.api.oauth_authenticator import start_oauth_server
        
        # 启动OAuth认证服务器
        start_oauth_server(host='localhost', port=8000, debug=False, use_https=True)
        
    except KeyboardInterrupt:
        print("\n🛑 认证服务已停止")
    except Exception as e:
        print(f"❌ 认证失败: {str(e)}")

def check_token_status_silent():
    """静默检查OAuth令牌状态（不打印信息），包含自动刷新功能"""
    try:
        from fantasy_etl.api.client import YahooFantasyAPIClient
        from datetime import datetime
        
        client = YahooFantasyAPIClient()
        DEFAULT_TOKEN_FILE = client.token_file_path
        
        if not DEFAULT_TOKEN_FILE.exists():
            return False
        
        token = client.load_token()
        if not token:
            return False
        
        # 检查令牌是否有必要的字段
        required_fields = ['access_token', 'expires_at']
        if any(field not in token for field in required_fields):
            return False
        
        # 尝试刷新令牌（如果需要）
        refreshed_token = client.refresh_token_if_needed(token)
        if not refreshed_token:
            return False
        
        # 检查刷新后的令牌是否仍然有效
        now = datetime.now().timestamp()
        expires_at = refreshed_token.get('expires_at', 0)
        
        return now < expires_at
        
    except Exception:
        return False

def run_data_fetcher():
    """运行数据获取器交互式菜单"""
    try:
        # 使用新的模块化ETL系统
        from fantasy_etl.api.fetchers import YahooFantasyFetcher
        from fantasy_etl.loaders.batch import BatchLoaders
        from fantasy_etl.transformers.core import CoreTransformers
        from fantasy_etl.validators.core import CoreValidators
        from fantasy_etl.database import create_database_engine, get_session
        
        print("🚀 启动模块化ETL工具...")
        
        # 创建组件实例
        fetcher = YahooFantasyFetcher()
        
        # 尝试创建数据库会话
        engine = create_database_engine()
        session = get_session(engine)
        if session:
            loader = BatchLoaders(session)
            print("✅ 数据库会话已创建")
        else:
            print("⚠️ 数据库会话创建失败，使用占位符")
            loader = None
            
        transformer = CoreTransformers()
        validator = CoreValidators()
        
        print("ETL组件:")
        print("1. 完整联盟数据获取")
        print("2. 查看数据库状态") 
        print("3. 选择联盟进行详细处理")
        print("0. 返回")
        
        while True:
            choice = input("选择 (0-3): ").strip()
            
            if choice == "0":
                break
            elif choice == "1":
                print("📄 完整联盟数据获取")
                try:
                    # 使用已迁移的完整功能
                    print("🚀 获取基础联盟数据...")
                    leagues_data = fetcher.fetch_and_select_league()
                    
                    if leagues_data:
                        games_data = leagues_data.get('games_data')
                        leagues_data_dict = leagues_data.get('leagues_data', {})
                        
                        # 写入基础数据
                        if games_data:
                            games_count = loader.write_games_data(games_data)
                            print(f"✅ 写入 {games_count} 个游戏记录")
                        
                        total_leagues = 0
                        for game_key, leagues_list in leagues_data_dict.items():
                            leagues_count = loader.write_leagues_data({game_key: leagues_list})
                            total_leagues += leagues_count
                            
                            # 获取联盟详细设置
                            for league_info in leagues_list:
                                league_key = league_info.get('league_key')
                                if league_key:
                                    settings_data = fetcher.fetch_league_settings(league_key)
                                    if settings_data:
                                        loader.write_league_settings(league_key, settings_data)
                        
                        print(f"✅ 共处理 {total_leagues} 个联盟")
                    else:
                        print("❌ 未获取到联盟数据")
                        
                except Exception as e:
                    print(f"❌ 联盟数据获取失败: {str(e)}")
                
                input("按Enter继续...")
                
            elif choice == "2":
                print("📊 数据库状态")
                try:
                    from fantasy_etl.database import League, Team, Player
                    
                    league_count = session.query(League).count()
                    team_count = session.query(Team).count()
                    player_count = session.query(Player).count()
                    
                    print(f"✅ 联盟数量: {league_count}")
                    print(f"✅ 团队数量: {team_count}")
                    print(f"✅ 球员数量: {player_count}")
                    
                    if league_count > 0:
                        print("\n📋 可用联盟:")
                        leagues = session.query(League).all()
                        for league in leagues:
                            print(f"  - {league.name} ({league.league_key})")
                    
                except Exception as e:
                    print(f"❌ 数据库查询失败: {str(e)}")
                
                input("按Enter继续...")
                
            elif choice == "3":
                print("🎯 选择联盟进行详细处理")
                try:
                    from fantasy_etl.database import League
                    leagues = session.query(League).all()
                    
                    if not leagues:
                        print("❌ 未找到联盟数据，请先执行选项1")
                        input("按Enter继续...")
                        continue
                    
                    print("📋 可用联盟:")
                    for i, league in enumerate(leagues):
                        print(f"{i+1}. {league.name} ({league.league_key})")
                    
                    try:
                        choice_idx = int(input("选择联盟 (输入数字): ")) - 1
                        if 0 <= choice_idx < len(leagues):
                            selected_league = leagues[choice_idx]
                            league_key = selected_league.league_key
                            season = selected_league.season
                            
                            print(f"🚀 处理联盟: {selected_league.name}")
                            
                            # 使用已迁移的完整联盟数据获取功能
                            complete_data = fetcher.fetch_complete_league_data(league_key, season)
                            
                            # 写入各类数据
                            if complete_data.get('players_data'):
                                players_count = loader.write_players_batch(complete_data['players_data'], league_key)
                                print(f"✅ 球员数据: {players_count} 条记录")
                            
                            if complete_data.get('teams_data'):
                                # 暂时跳过团队数据批量写入，需要进一步开发转换逻辑
                                print("⚠️ 团队数据转换功能需要进一步开发")
                            
                            if complete_data.get('transactions_data'):
                                trans_count = loader.write_transactions_batch(complete_data['transactions_data'], league_key)
                                print(f"✅ 交易数据: {trans_count} 条记录")
                            
                            print("✅ 联盟数据处理完成")
                        else:
                            print("❌ 无效选择")
                    except ValueError:
                        print("❌ 请输入有效数字")
                        
                except Exception as e:
                    print(f"❌ 联盟处理失败: {str(e)}")
                
                input("按Enter继续...")
            else:
                print("无效选择")
            
    except Exception as e:
        print(f"❌ ETL工具启动失败: {str(e)}")
        raise

def run_sample_fetcher():
    """运行样本数据获取器"""
    try:
        from sample_data.fetch_sample_data import YahooFantasySampleFetcher
        
        print("\n🔬 启动样本数据获取工具...")
        
        # 创建样本数据获取器
        fetcher = YahooFantasySampleFetcher()
        
        # 运行交互式菜单
        fetcher.run_interactive_menu()
        
    except Exception as e:
        print(f"❌ 样本数据获取器启动失败: {str(e)}")
        raise

def setup_database():
    """初始化数据库"""
    try:
        from fantasy_etl.database import create_database_engine, create_tables
        
        print("🗄️ 初始化数据库...")
        engine = create_database_engine()
        create_tables(engine)
        print("✅ 完成")
        
    except Exception as e:
        print(f"❌ 数据库初始化失败: {str(e)}")
        raise

def check_token_status():
    """检查OAuth令牌状态（带详细输出和自动刷新）"""
    try:
        from fantasy_etl.api.client import YahooFantasyAPIClient
        from datetime import datetime
        
        client = YahooFantasyAPIClient()
        DEFAULT_TOKEN_FILE = client.token_file_path
        
        print("🔍 检查令牌状态...")
        
        if not DEFAULT_TOKEN_FILE.exists():
            print("❌ 未找到令牌")
            print("💡 请先运行OAuth授权")
            return False
        
        token = client.load_token()
        if not token:
            print("❌ 无法加载令牌")
            return False
        
        # 检查令牌字段
        required_fields = ['access_token', 'expires_at']
        missing_fields = [field for field in required_fields if field not in token]
        
        if missing_fields:
            print(f"❌ 令牌字段缺失: {missing_fields}")
            return False
        
        # 检查过期状态
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        if now >= (expires_at - 60):
            print("⚠️ 令牌过期，尝试刷新...")
            refreshed_token = client.refresh_token_if_needed(token)
            
            if refreshed_token:
                new_expires_at = refreshed_token.get('expires_at', 0)
                remaining_time = int((new_expires_at - now) / 3600)
                print(f"✅ 刷新成功，剩余 {remaining_time}h")
                return True
            else:
                print("❌ 刷新失败")
                return False
        else:
            remaining_time = int((expires_at - now) / 3600)
            print(f"✅ 令牌有效，剩余 {remaining_time}h")
            return True
        
    except Exception as e:
        print(f"❌ 检查令牌状态失败: {str(e)}")
        return False

def show_menu():
    """显示主菜单"""
    print("\n🚀 Yahoo Fantasy ETL")
    print("1. OAuth认证")
    print("2. 数据获取")
    print("3. 样本数据") 
    print("4. 初始化数据库")
    print("5. 检查令牌")
    print("6. 帮助")
    print("0. 退出")

def show_help():
    """显示帮助信息"""
    print("""
🔧 Yahoo Fantasy Sports ETL 工具使用指南

📋 主要功能:
  --web           启动Web界面进行OAuth认证
  --data          启动数据获取工具
  --sample        启动样本数据获取工具
  --db-init       初始化数据库
  --check-token   检查OAuth令牌状态
  --help          显示此帮助信息

🚀 快速开始:
  1. 首次使用: python main.py --db-init
  2. OAuth授权: python main.py --web
  3. 获取数据: python main.py --data

💡 提示:
  - 首次使用需要先完成OAuth授权
  - 可以随时使用 --check-token 检查令牌状态
  - 详细的API功能请查看各个子模块

📂 项目结构:
  - app.py: Flask Web应用，OAuth认证
  - yahoo_api_data.py: 数据获取主模块
  - yahoo_api_utils.py: API工具和令牌管理
  - model.py: 数据库模型定义
  - database_writer.py: 数据库写入器
  - fetch_sample_data.py: 样本数据获取器
""")

def main():
    """主函数 - 统一入口点"""
    parser = argparse.ArgumentParser(
        description="Yahoo Fantasy Sports ETL 工具",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--web", action="store_true", 
                       help="启动Web界面进行OAuth认证")
    parser.add_argument("--data", action="store_true", 
                       help="启动数据获取工具")
    parser.add_argument("--sample", action="store_true", 
                       help="启动样本数据获取工具")
    parser.add_argument("--db-init", action="store_true", 
                       help="初始化数据库")
    parser.add_argument("--check-token", action="store_true", 
                       help="检查OAuth令牌状态")
    parser.add_argument("--help-detailed", action="store_true", 
                       help="显示详细帮助信息")
    
    args = parser.parse_args()
    
    # 检查是否有任何参数
    has_args = any([args.web, args.data, args.sample, args.db_init, 
                   args.check_token, args.help_detailed])
    
    if not has_args:
        # 没有参数，运行交互式菜单
        show_menu()
        
        while True:
            choice = input("选择: ").strip()
            
            if choice == "0":
                print("退出")
                break
            elif choice == "1":
                run_web_app()
                show_menu()
            elif choice == "2":
                if not check_token_status_silent():
                    print("❌ 需要先认证")
                    continue
                run_data_fetcher()
                show_menu()
            elif choice == "3":
                if not check_token_status_silent():
                    print("❌ 需要先认证")
                    continue
                run_sample_fetcher()
                show_menu()
            elif choice == "4":
                setup_database()
                input("按Enter继续...")
            elif choice == "5":
                check_token_status()
                input("按Enter继续...")
            elif choice == "6":
                show_help()
                input("按Enter继续...")
            else:
                print("无效选择")
    else:
        # 有参数，执行对应功能
        if args.help_detailed:
            show_help()
        
        if args.db_init:
            setup_database()
        
        if args.check_token:
            check_token_status()
        
        if args.web:
            run_web_app()
        
        if args.data:
            run_data_fetcher()
        
        if args.sample:
            run_sample_fetcher()


if __name__ == "__main__":
    main()