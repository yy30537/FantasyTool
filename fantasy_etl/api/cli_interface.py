"""
命令行界面
重构自archive/yahoo_api_data.py的交互式菜单部分
"""
import os
import sys
from datetime import datetime, date, timedelta
from typing import Optional, Tuple

from .fantasy_data_service import FantasyDataService
from ..auth import WebAuthServer

class CLIInterface:
    """命令行界面"""
    
    def __init__(self):
        self.data_service = FantasyDataService()
        self.auth_server = None
    
    def run(self):
        """运行主菜单"""
        print("🏀 Welcome to Fantasy Tool - Yahoo Fantasy Sports分析工具")
        print("=" * 60)
        
        while True:
            self._display_menu()
            choice = input("\\n请选择操作: ").strip()
            
            if choice == '0':
                print("👋 感谢使用！")
                break
            elif choice == 'a':
                self._handle_authentication()
            elif choice == '1':
                self._handle_league_selection()
            elif choice == '2':
                self._handle_league_data_fetch()
            elif choice == '3':
                self._handle_roster_history()
            elif choice == '4':
                self._handle_player_daily_stats()
            elif choice == '5':
                self._handle_player_season_stats()
            elif choice == '6':
                self._handle_database_summary()
            elif choice == '7':
                self._handle_database_cleanup()
            elif choice == '8':
                self._handle_team_weekly_data()
            elif choice == '9':
                self._handle_team_season_data()
            else:
                print("❌ 无效选择，请重试")
    
    def _display_menu(self):
        """显示主菜单"""
        # 检查认证状态
        auth_status = "✅ 已认证" if self.data_service.authenticate_user() else "❌ 未认证"
        
        # 检查联盟选择状态
        league_status = f"✅ {self.data_service.selected_league['name']}" if self.data_service.selected_league else "❌ 未选择"
        
        print(f"\\n📊 当前状态: 认证({auth_status}) | 联盟({league_status})")
        print("=" * 60)
        print("a. 登录认证          - OAuth authentication management")
        print("1. 选择联盟          - League selection from user's Yahoo account")
        print("2. 获取联盟数据      - Complete league data extraction")
        print("3. 获取阵容历史数据  - Historical roster data with date ranges")
        print("4. 获取球员日统计数据 - Player daily statistics")
        print("5. 获取球员赛季统计数据 - Player season statistics")
        print("6. 数据库摘要        - Database summary and statistics")
        print("7. 清空数据库        - Database cleanup operations")
        print("8. 获取团队每周数据  - Team weekly matchup data")
        print("9. 获取团队赛季数据  - Team season data")
        print("0. 退出             - Exit program")
        print("=" * 60)
    
    def _handle_authentication(self):
        """处理认证"""
        print("\\n🔐 开始OAuth认证流程...")
        
        if self.data_service.authenticate_user():
            print("✅ 您已经认证成功！")
            return
        
        print("启动Web认证服务器...")
        print("🌐 请在浏览器中完成认证流程")
        
        try:
            self.auth_server = WebAuthServer()
            self.auth_server.run(debug=False)
        except KeyboardInterrupt:
            print("\\n⚠️ 认证流程被用户中断")
        except Exception as e:
            print(f"❌ 认证服务器启动失败: {e}")
            print("💡 请检查环境变量配置：YAHOO_CLIENT_ID, YAHOO_CLIENT_SECRET")
    
    def _handle_league_selection(self):
        """处理联盟选择"""
        print("\\n🏆 联盟选择")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        use_existing = input("是否使用数据库中的联盟数据? (y/N): ").strip().lower() == 'y'
        
        success = self.data_service.fetch_and_select_league(use_existing_data=use_existing)
        if success:
            print("✅ 联盟选择成功！")
        else:
            print("❌ 联盟选择失败")
    
    def _handle_league_data_fetch(self):
        """处理联盟数据获取"""
        print("\\n📊 获取联盟完整数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        success = self.data_service.fetch_league_complete_data()
        if success:
            print("✅ 联盟数据获取成功！")
        else:
            print("❌ 联盟数据获取失败")
    
    def _handle_roster_history(self):
        """处理阵容历史数据"""
        print("\\n👥 获取阵容历史数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        # 获取时间范围
        date_range = self._get_time_selection_interactive("阵容历史")
        if not date_range:
            return
        
        start_date, end_date = date_range
        print(f"📅 准备获取 {start_date} 到 {end_date} 的阵容数据...")
        
        confirm = input("确认开始获取? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        success = self.data_service.fetch_roster_history_data(start_date, end_date)
        if success:
            print("✅ 阵容历史数据获取成功！")
            self._handle_database_summary()
        else:
            print("❌ 阵容历史数据获取失败")
        
        input("按回车键继续...")
    
    def _handle_player_daily_stats(self):
        """处理球员日统计数据"""
        print("\\n📈 获取球员日统计数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        # 获取时间范围
        date_range = self._get_time_selection_interactive("球员日统计")
        if not date_range:
            return
        
        start_date, end_date = date_range
        print(f"📅 准备获取 {start_date} 到 {end_date} 的球员日统计数据...")
        
        # 警告大数据量
        total_days = (end_date - start_date).days + 1
        if total_days > 30:
            print(f"⚠️ 将获取 {total_days} 天的数据，可能需要较长时间")
        
        confirm = input("确认开始获取? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        success = self.data_service.fetch_player_daily_stats_data(start_date, end_date)
        if success:
            print("✅ 球员日统计数据获取成功！")
            self._handle_database_summary()
        else:
            print("❌ 球员日统计数据获取失败")
        
        input("按回车键继续...")
    
    def _handle_player_season_stats(self):
        """处理球员赛季统计数据"""
        print("\\n📊 获取球员赛季统计数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        print("📊 准备获取当前联盟所有球员的赛季统计数据...")
        
        confirm = input("确认开始获取? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        success = self.data_service.fetch_player_season_stats_data()
        if success:
            print("✅ 球员赛季统计数据获取成功！")
            self._handle_database_summary()
        else:
            print("❌ 球员赛季统计数据获取失败")
        
        input("按回车键继续...")
    
    def _handle_database_summary(self):
        """处理数据库摘要"""
        print("\\n📋 数据库摘要")
        
        summary = self.data_service.get_database_summary()
        if summary:
            print("=" * 40)
            print(f"🎮 游戏: {summary.get('games', 0)}")
            print(f"🏆 联盟: {summary.get('leagues', 0)}")
            print(f"🏀 球队: {summary.get('teams', 0)}")
            print(f"👤 管理员: {summary.get('managers', 0)}")
            print(f"🏃 球员: {summary.get('players', 0)}")
            print(f"📊 球员赛季统计: {summary.get('player_season_stats', 0)}")
            print(f"📈 球员日统计: {summary.get('player_daily_stats', 0)}")
            print(f"🔄 交易记录: {summary.get('transactions', 0)}")
            print(f"👥 每日阵容: {summary.get('roster_daily', 0)}")
            print("=" * 40)
        else:
            print("❌ 无法获取数据库摘要")
        
        input("按回车键继续...")
    
    def _handle_database_cleanup(self):
        """处理数据库清理"""
        print("\\n🧹 数据库清理")
        print("⚠️ 此操作将删除所有数据！")
        
        confirm = input("确认要清空数据库吗? 输入 'DELETE' 确认: ").strip()
        if confirm == 'DELETE':
            print("🚧 数据库清理功能正在开发中...")
        else:
            print("❌ 操作已取消")
        
        input("按回车键继续...")
    
    def _handle_team_weekly_data(self):
        """处理团队每周数据"""
        print("\\n📅 获取团队每周数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        print("📊 准备获取当前联盟所有团队的每周对战和统计数据...")
        
        confirm = input("确认开始获取? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        success = self.data_service.fetch_team_weekly_data()
        if success:
            print("✅ 团队每周数据获取成功！")
            self._handle_database_summary()
        else:
            print("❌ 团队每周数据获取失败")
        
        input("按回车键继续...")
    
    def _handle_team_season_data(self):
        """处理团队赛季数据"""
        print("\\n🏆 获取团队赛季数据")
        
        if not self.data_service.authenticate_user():
            print("❌ 请先完成认证 (选择 'a')")
            return
        
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟 (选择 '1')")
            return
        
        print("📊 准备获取当前联盟所有团队的赛季统计数据...")
        
        confirm = input("确认开始获取? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        success = self.data_service.fetch_team_season_data()
        if success:
            print("✅ 团队赛季数据获取成功！")
            self._handle_database_summary()
        else:
            print("❌ 团队赛季数据获取失败")
        
        input("按回车键继续...")
    
    def _get_time_selection_interactive(self, data_type: str) -> Optional[Tuple[date, date]]:
        """交互式时间范围选择"""
        print(f"\\n📅 选择{data_type}获取的时间范围:")
        print("1. 最近7天")
        print("2. 最近30天")
        print("3. 指定日期")
        print("4. 完整赛季")
        print("0. 取消")
        
        choice = input("\\n请选择时间范围 (0-4): ").strip()
        
        if choice == '0':
            return None
        elif choice == '1':
            # 最近7天
            end_date = date.today()
            start_date = end_date - timedelta(days=6)
            print(f"📅 选择时间范围: {start_date} 到 {end_date}")
            return (start_date, end_date)
        elif choice == '2':
            # 最近30天
            end_date = date.today()
            start_date = end_date - timedelta(days=29)
            print(f"📅 选择时间范围: {start_date} 到 {end_date}")
            return (start_date, end_date)
        elif choice == '3':
            # 指定日期
            return self._get_custom_date_range()
        elif choice == '4':
            # 完整赛季
            return self._get_season_date_range()
        else:
            print("❌ 无效选择")
            return None
    
    def _get_custom_date_range(self) -> Optional[Tuple[date, date]]:
        """获取自定义日期范围"""
        try:
            start_str = input("输入开始日期 (YYYY-MM-DD): ").strip()
            end_str = input("输入结束日期 (YYYY-MM-DD): ").strip()
            
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
            
            if start_date > end_date:
                print("❌ 开始日期不能晚于结束日期")
                return None
            
            if (end_date - start_date).days > 90:
                print("⚠️ 时间范围超过90天，建议缩短范围以避免API限制")
                confirm = input("是否继续? (y/N): ").strip().lower()
                if confirm != 'y':
                    return None
            
            print(f"📅 选择时间范围: {start_date} 到 {end_date}")
            return (start_date, end_date)
            
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
            return None
    
    def _get_season_date_range(self) -> Optional[Tuple[date, date]]:
        """获取赛季日期范围"""
        if not self.data_service.selected_league:
            print("❌ 请先选择联盟")
            return None
        
        # 从联盟信息获取赛季日期
        season_info = self.data_service.get_season_date_info()
        if not season_info:
            print("❌ 无法获取赛季日期信息")
            return None
        
        start_date = season_info["start_date"]
        end_date = season_info["latest_date"]
        
        print(f"📅 赛季时间范围: {start_date} 到 {end_date}")
        print(f"📊 赛季状态: {season_info['season_status']}")
        
        if (end_date - start_date).days > 180:
            print("⚠️ 完整赛季数据量较大，可能需要较长时间")
            confirm = input("是否继续? (y/N): ").strip().lower()
            if confirm != 'y':
                return None
        
        return (start_date, end_date)

def main():
    """主函数"""
    try:
        cli = CLIInterface()
        cli.run()
    except KeyboardInterrupt:
        print("\\n\\n👋 程序被用户中断，再见！")
    except Exception as e:
        print(f"\\n❌ 程序出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()