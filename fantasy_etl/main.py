"""
Fantasy ETL 主程序 - 交互式命令行工具

提供完整的数据提取和处理功能的命令行界面
"""
import argparse
import sys
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .extract.yahoo_client import (
    YahooFantasyClient, 
    print_league_selection_info, 
    select_league_interactively,
    extract_game_keys_from_data,
    extract_leagues_from_api_data
)
from .auth.oauth_helper import OAuthHelper

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FantasyETLCLI:
    """Fantasy ETL 命令行界面"""
    
    def __init__(self):
        """初始化CLI"""
        self.client = YahooFantasyClient()
        self.oauth_helper = OAuthHelper()
        self.selected_league = None
    
    def check_authentication(self) -> bool:
        """检查认证状态"""
        token_status = self.oauth_helper.check_token_status()
        
        if token_status['is_valid']:
            print("✅ 认证状态: 已授权")
            return True
        else:
            print(f"❌ 认证状态: {token_status['message']}")
            return False
    
    def authenticate(self) -> bool:
        """执行认证流程"""
        print("\n🔐 开始认证流程...")
        return self.oauth_helper.complete_oauth_flow()
    
    def fetch_and_select_league(self) -> bool:
        """获取并选择联盟"""
        print("\n🚀 获取联盟数据...")
        
        # 1. 获取games数据
        games_data = self.client.fetch_user_games()
        if not games_data:
            print("❌ 无法获取games数据")
            return False
        
        # 2. 提取game keys
        game_keys = extract_game_keys_from_data(games_data)
        if not game_keys:
            print("❌ 未找到可用的游戏")
            return False
        
        print(f"✓ 找到 {len(game_keys)} 个游戏")
        
        # 3. 获取所有联盟数据
        all_leagues = {}
        for game_key in game_keys:
            leagues_data = self.client.fetch_user_leagues(game_key)
            if leagues_data:
                extracted_leagues = extract_leagues_from_api_data(leagues_data, game_key)
                if extracted_leagues:
                    all_leagues[game_key] = extracted_leagues
        
        if not all_leagues:
            print("❌ 未找到任何联盟")
            return False
        
        # 4. 交互式选择联盟
        selected_league = select_league_interactively(all_leagues)
        if not selected_league:
            print("❌ 未选择联盟")
            return False
        
        self.selected_league = selected_league
        print(f"✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
        return True
    
    def show_main_menu(self) -> None:
        """显示主菜单"""
        print("\n" + "="*60)
        print("🏀 Yahoo Fantasy ETL 数据工具")
        print("="*60)
        
        # 显示状态信息
        auth_status = "✅ 已认证" if self.check_authentication() else "❌ 未认证"
        league_status = f"✅ {self.selected_league['name']}" if self.selected_league else "❌ 未选择"
        
        print(f"认证状态: {auth_status}")
        print(f"当前联盟: {league_status}")
        print()
        
        # 显示菜单选项
        print("📋 可用操作:")
        print("1. 🔐 OAuth认证")
        print("2. 🏆 选择联盟")
        print("3. 📊 基础数据提取")
        print("4. 📈 时间序列数据提取")
        print("5. 🔍 数据分析")
        print("6. ⚙️  配置管理")
        print("7. 📝 查看日志")
        print("0. 🚪 退出")
        print("="*60)
    
    def handle_auth_menu(self) -> None:
        """处理认证菜单"""
        while True:
            print("\n🔐 认证管理")
            print("1. 开始OAuth授权")
            print("2. 检查令牌状态")
            print("3. 撤销令牌")
            print("0. 返回主菜单")
            
            choice = input("\n请选择操作 (0-3): ").strip()
            
            if choice == "0":
                break
            elif choice == "1":
                success = self.oauth_helper.complete_oauth_flow()
                if success:
                    print("✅ 认证完成")
                else:
                    print("❌ 认证失败")
            elif choice == "2":
                status = self.oauth_helper.check_token_status()
                print(f"\n📊 令牌状态: {status['message']}")
                if status.get('expires_at'):
                    expires_at = datetime.fromtimestamp(status['expires_at'])
                    print(f"🕐 过期时间: {expires_at}")
            elif choice == "3":
                confirm = input("确认撤销令牌？(y/N): ").strip().lower()
                if confirm == 'y':
                    if self.oauth_helper.revoke_token():
                        print("✅ 令牌已撤销")
                    else:
                        print("❌ 撤销失败")
            else:
                print("❌ 无效选择")
    
    def handle_league_menu(self) -> None:
        """处理联盟选择菜单"""
        if not self.check_authentication():
            print("❌ 请先完成认证")
            return
        
        print("\n🏆 联盟管理")
        if self.fetch_and_select_league():
            print("✅ 联盟选择完成")
        else:
            print("❌ 联盟选择失败")
    
    def handle_basic_extraction_menu(self) -> None:
        """处理基础数据提取菜单"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return
        
        print("\n📊 基础数据提取")
        print("1. 提取联盟配置数据")
        print("2. 提取团队和球员数据")
        print("3. 提取交易数据")
        print("4. 提取排名和对战数据")
        print("5. 提取所有基础数据")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作 (0-5): ").strip()
        
        if choice == "0":
            return
        elif choice == "1":
            print("🚧 联盟配置数据提取功能开发中...")
        elif choice == "2":
            print("🚧 团队和球员数据提取功能开发中...")
        elif choice == "3":
            print("🚧 交易数据提取功能开发中...")
        elif choice == "4":
            print("🚧 排名和对战数据提取功能开发中...")
        elif choice == "5":
            print("🚧 完整基础数据提取功能开发中...")
        else:
            print("❌ 无效选择")
    
    def handle_time_series_menu(self) -> None:
        """处理时间序列数据提取菜单"""
        if not self.selected_league:
            print("❌ 请先选择联盟")
            return
        
        print("\n📈 时间序列数据提取")
        print("1. 提取阵容历史数据")
        print("2. 提取球员统计数据")
        print("3. 提取团队统计数据")
        print("4. 自定义时间范围提取")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作 (0-4): ").strip()
        
        if choice == "0":
            return
        elif choice == "1":
            print("🚧 阵容历史数据提取功能开发中...")
        elif choice == "2":
            print("🚧 球员统计数据提取功能开发中...")
        elif choice == "3":
            print("🚧 团队统计数据提取功能开发中...")
        elif choice == "4":
            print("🚧 自定义时间范围提取功能开发中...")
        else:
            print("❌ 无效选择")
    
    def run_interactive_mode(self) -> None:
        """运行交互式模式"""
        print("🎉 欢迎使用 Yahoo Fantasy ETL 工具!")
        
        try:
            while True:
                self.show_main_menu()
                choice = input("请选择操作 (0-7): ").strip()
                
                if choice == "0":
                    print("👋 再见!")
                    break
                elif choice == "1":
                    self.handle_auth_menu()
                elif choice == "2":
                    self.handle_league_menu()
                elif choice == "3":
                    self.handle_basic_extraction_menu()
                elif choice == "4":
                    self.handle_time_series_menu()
                elif choice == "5":
                    print("🚧 数据分析功能开发中...")
                elif choice == "6":
                    print("🚧 配置管理功能开发中...")
                elif choice == "7":
                    print("🚧 日志查看功能开发中...")
                else:
                    print("❌ 无效选择，请重试")
        
        except KeyboardInterrupt:
            print("\n👋 用户中断，再见!")
        except Exception as e:
            logger.error(f"程序运行出错: {str(e)}")
            print(f"❌ 程序出错: {str(e)}")


def main():
    """主函数 - 命令行入口点"""
    parser = argparse.ArgumentParser(
        description="Yahoo Fantasy ETL 数据工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python -m fantasy_etl.main              # 交互式模式
  python -m fantasy_etl.main --auth       # 仅执行OAuth认证
  python -m fantasy_etl.main --status     # 检查认证状态
        """
    )
    
    parser.add_argument(
        '--auth', 
        action='store_true', 
        help='仅执行OAuth认证'
    )
    parser.add_argument(
        '--status', 
        action='store_true', 
        help='检查认证状态'
    )
    parser.add_argument(
        '--interactive', '-i',
        action='store_true', 
        help='运行交互式模式（默认）'
    )
    parser.add_argument(
        '--league-key',
        type=str,
        help='指定联盟键，跳过联盟选择'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='显示详细日志'
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    cli = FantasyETLCLI()
    
    try:
        if args.auth:
            # 仅执行认证
            success = cli.authenticate()
            sys.exit(0 if success else 1)
        
        elif args.status:
            # 检查状态
            is_auth = cli.check_authentication()
            sys.exit(0 if is_auth else 1)
        
        else:
            # 默认运行交互式模式
            cli.run_interactive_mode()
    
    except KeyboardInterrupt:
        print("\n👋 用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序异常: {str(e)}")
        print(f"❌ 程序异常: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()