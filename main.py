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
        # 先检查令牌状态
        from archive.yahoo_api_utils import load_token, DEFAULT_TOKEN_FILE
        from datetime import datetime
        
        token_exists = DEFAULT_TOKEN_FILE.exists()
        token_valid = check_token_status_silent()
        
        if token_exists and not token_valid:
            # 令牌存在但已过期
            print("\n⚠️ 检测到过期的OAuth令牌")
            print("选择操作:")
            print("1. 重新进行OAuth认证")
            print("2. 查看当前令牌状态")
            print("3. 返回主菜单")
            
            while True:
                choice = input("\n请选择 (1-3): ").strip()
                if choice == "1":
                    print("🔄 将重新进行OAuth认证...")
                    break
                elif choice == "2":
                    check_token_status()
                    print("\n按任意键继续...")
                    input()
                    print("\n选择操作:")
                    print("1. 重新进行OAuth认证")
                    print("2. 查看当前令牌状态")
                    print("3. 返回主菜单")
                elif choice == "3":
                    print("💡 返回主菜单，需要有效令牌才能使用数据获取功能")
                    return
                else:
                    print("❌ 无效选择，请重试")
                    
        elif token_valid:
            # 令牌有效
            print("\n✅ 检测到有效的OAuth令牌")
            print("选择操作:")
            print("1. 重新进行OAuth认证")
            print("2. 查看当前令牌状态")
            print("3. 返回主菜单 (默认)")
            
            while True:
                choice = input("\n请选择 (1-3, 直接按Enter返回主菜单): ").strip()
                if choice == "1":
                    print("🔄 将重新进行OAuth认证...")
                    break
                elif choice == "2":
                    check_token_status()
                    print("\n按任意键继续...")
                    input()
                    print("\n选择操作:")
                    print("1. 重新进行OAuth认证")
                    print("2. 查看当前令牌状态")
                    print("3. 返回主菜单 (默认)")
                elif choice == "3" or choice == "":
                    print("💡 返回主菜单，可以直接使用数据获取功能")
                    return
                else:
                    print("❌ 无效选择，请重试")
        else:
            # 没有令牌文件，直接进行OAuth认证
            print("\n🔐 首次使用，需要进行OAuth认证")
        
        # 导入OAuth认证器
        from oauth_authenticator import start_oauth_server
        
        # 启动OAuth认证服务器
        start_oauth_server(host='localhost', port=8000, debug=True, use_https=True)
        
    except KeyboardInterrupt:
        print("\n\n🛑 OAuth认证服务已停止")
        print("💡 如果已完成授权，现在可以使用数据获取功能")
    except Exception as e:
        print(f"\n❌ OAuth认证服务启动失败: {str(e)}")
        print("💡 请检查端口8000是否被占用")

def check_token_status_silent():
    """静默检查OAuth令牌状态（不打印信息），包含自动刷新功能"""
    try:
        from archive.yahoo_api_utils import load_token, refresh_token_if_needed, DEFAULT_TOKEN_FILE
        
        if not DEFAULT_TOKEN_FILE.exists():
            return False
        
        token = load_token()
        if not token:
            return False
        
        # 检查令牌是否有必要的字段
        required_fields = ['access_token', 'expires_at']
        if any(field not in token for field in required_fields):
            return False
        
        # 尝试刷新令牌（如果需要）
        refreshed_token = refresh_token_if_needed(token)
        if not refreshed_token:
            return False
        
        # 检查刷新后的令牌是否仍然有效
        from datetime import datetime
        now = datetime.now().timestamp()
        expires_at = refreshed_token.get('expires_at', 0)
        
        return now < expires_at
        
    except Exception:
        return False

def run_data_fetcher():
    """运行数据获取器交互式菜单"""
    try:
        # 导入数据获取器
        from yahoo_api_data import YahooFantasyDataFetcher
        
        print("\n🚀 启动Yahoo Fantasy数据获取工具...")
        
        # 创建数据获取器
        fetcher = YahooFantasyDataFetcher(delay=2, batch_size=100)
        
        try:
            # 运行交互式菜单
            fetcher.run_interactive_menu()
        finally:
            # 确保清理资源
            fetcher.close()
            
    except Exception as e:
        print(f"❌ 数据获取器启动失败: {str(e)}")
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
        from model import create_database_engine, create_tables
        
        print("\n🗄️ 初始化数据库...")
        
        # 创建数据库引擎
        engine = create_database_engine()
        
        # 创建所有表
        create_tables(engine)
        
        print("✅ 数据库初始化完成！")
        
    except Exception as e:
        print(f"❌ 数据库初始化失败: {str(e)}")
        raise

def check_token_status():
    """检查OAuth令牌状态（带详细输出和自动刷新）"""
    try:
        from archive.yahoo_api_utils import load_token, refresh_token_if_needed, DEFAULT_TOKEN_FILE
        
        print("\n🔍 检查OAuth令牌状态...")
        
        if not DEFAULT_TOKEN_FILE.exists():
            print("❌ 未找到令牌文件")
            print(f"   令牌文件路径: {DEFAULT_TOKEN_FILE}")
            print("💡 请先运行 'python main.py --web' 完成OAuth授权")
            return False
        
        token = load_token()
        if not token:
            print("❌ 令牌文件存在但无法加载")
            return False
        
        # 检查令牌是否有必要的字段
        required_fields = ['access_token', 'expires_at']
        missing_fields = [field for field in required_fields if field not in token]
        
        if missing_fields:
            print(f"❌ 令牌缺少必要字段: {missing_fields}")
            return False
        
        # 检查令牌是否过期并尝试刷新
        from datetime import datetime
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        if now >= (expires_at - 60):  # 提前60秒检查
            print("⚠️ 令牌已过期或即将过期，尝试自动刷新...")
            refreshed_token = refresh_token_if_needed(token)
            
            if refreshed_token:
                # 刷新成功，检查新令牌状态
                new_expires_at = refreshed_token.get('expires_at', 0)
                remaining_time = int((new_expires_at - now) / 3600)
                print(f"✅ 令牌刷新成功，剩余时间: {remaining_time} 小时")
                return True
            else:
                print("❌ 令牌刷新失败")
                print("💡 请重新运行OAuth授权: python main.py --web")
                return False
        else:
            remaining_time = int((expires_at - now) / 3600)
            print(f"✅ 令牌有效，剩余时间: {remaining_time} 小时")
            return True
        
    except Exception as e:
        print(f"❌ 检查令牌状态失败: {str(e)}")
        return False

def show_menu():
    """显示主菜单"""
    print("\n" + "="*60)
    print("🚀 Yahoo Fantasy Sports ETL 工具")
    print("="*60)
    print("请选择操作:")
    print("1. 启动Web界面 (OAuth认证)")
    print("2. 启动数据获取工具")
    print("3. 启动样本数据获取工具") 
    print("4. 初始化数据库")
    print("5. 检查令牌状态")
    print("6. 显示详细帮助")
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
            choice = input("\n请选择 (0-6): ").strip()
            
            if choice == "0":
                print("👋 退出程序")
                break
            elif choice == "1":
                run_web_app()
                show_menu()
            elif choice == "2":
                # 运行数据获取前先检查令牌
                if not check_token_status_silent():
                    print("\n❌ 未找到有效的OAuth令牌")
                    print("💡 请先选择选项1完成OAuth认证")
                    continue
                run_data_fetcher()
                show_menu()
            elif choice == "3":
                # 运行样本数据获取前先检查令牌
                if not check_token_status_silent():
                    print("\n❌ 未找到有效的OAuth令牌")
                    print("💡 请先选择选项1完成OAuth认证")
                    continue
                run_sample_fetcher()
                show_menu()
            elif choice == "4":
                setup_database()
                print("\n按任意键继续...")
                input()
            elif choice == "5":
                check_token_status()
                print("\n按任意键继续...")
                input()
            elif choice == "6":
                show_help()
                print("\n按任意键继续...")
                input()
            else:
                print("❌ 无效选择，请重试")
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
    