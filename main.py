"""
Fantasy ETL 主应用入口
===================

【主要职责】
1. ETL应用的统一入口点
2. 用户交互和界面逻辑
3. 各个ETL模块的协调
4. 业务逻辑的整合

【功能模块】
- LeagueSelector: 联盟选择交互
- DataFetcher: 数据获取协调
- ETLPipeline: ETL流程管理
"""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

from fantasy_etl.auth.oauth_manager import OAuthManager
from fantasy_etl.auth.web_auth_server import WebAuthServer
from fantasy_etl.auth.token_storage import TokenStorage


class LeagueSelector:
    """联盟选择器 - 处理用户交互和联盟选择逻辑"""
    
    @staticmethod
    def parse_yahoo_date(date_str: str) -> Optional[datetime]:
        """解析Yahoo日期格式"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            return None
    
    @staticmethod
    def print_league_selection_info(leagues_data: Dict) -> list:
        """打印联盟选择信息"""
        print("\n" + "="*80)
        print("可选择的Fantasy联盟")
        print("="*80)
        
        all_leagues = []
        league_counter = 1
        
        for game_key, leagues in leagues_data.items():
            for league in leagues:
                league_info = {
                    'index': league_counter,
                    'league_key': league.get('league_key'),
                    'name': league.get('name', '未知联盟'),
                    'season': league.get('season', '未知赛季'),
                    'num_teams': league.get('num_teams', 0),
                    'game_code': league.get('game_code', '未知运动'),
                    'scoring_type': league.get('scoring_type', '未知'),
                    'is_finished': league.get('is_finished', 0) == 1
                }
                all_leagues.append(league_info)
                
                # 打印联盟信息
                status = "已结束" if league_info['is_finished'] else "进行中"
                print(f"{league_counter:2d}. {league_info['name']}")
                print(f"    联盟ID: {league_info['league_key']}")
                print(f"    运动类型: {league_info['game_code'].upper()} | 赛季: {league_info['season']} | 状态: {status}")
                print(f"    球队数量: {league_info['num_teams']} | 计分方式: {league_info['scoring_type']}")
                print()
                
                league_counter += 1
        
        print("="*80)
        return all_leagues
    
    @staticmethod
    def select_league_interactively(leagues_data: Dict) -> Optional[Dict[str, Any]]:
        """交互式选择联盟"""
        all_leagues = LeagueSelector.print_league_selection_info(leagues_data)
        
        if not all_leagues:
            print("没有找到任何联盟")
            return None
        
        while True:
            try:
                choice = input(f"请选择联盟 (1-{len(all_leagues)}): ").strip()
                
                if not choice:
                    continue
                    
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(all_leagues):
                    selected_league = all_leagues[choice_num - 1]
                    print(f"\n✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                    return selected_league
                else:
                    print(f"请输入1到{len(all_leagues)}之间的数字")
                    
            except ValueError:
                print("请输入有效的数字")
            except KeyboardInterrupt:
                print("\n用户取消选择")
                return None


class FantasyETLApp:
    """Fantasy ETL 主应用类"""
    
    def __init__(self):
        """初始化ETL应用"""
        self.token_storage = TokenStorage()
        self.oauth_manager = OAuthManager(token_storage=self.token_storage)
        self.web_auth_server = WebAuthServer(oauth_manager=self.oauth_manager)
        self.league_selector = LeagueSelector()
    
    def check_authentication(self) -> bool:
        """检查认证状态"""
        token = self.token_storage.load_token()
        if not token:
            print("❌ 未找到认证令牌")
            return False
        
        if self.oauth_manager.validate_token(token):
            print("✅ 认证令牌有效")
            return True
        else:
            print("⚠️ 认证令牌已过期，尝试刷新...")
            refreshed_token = self.oauth_manager.refresh_token(token)
            if refreshed_token:
                print("✅ 令牌刷新成功")
                return True
            else:
                print("❌ 令牌刷新失败")
                return False
    
    def start_auth_server(self, host: str = 'localhost', port: int = 8000, 
                         debug: bool = True, ssl_context: str = 'adhoc'):
        """启动Web认证服务器"""
        print("🚀 启动Web认证服务器...")
        self.web_auth_server.start(host=host, port=port, debug=debug, ssl_context=ssl_context)
    
    def fetch_leagues(self) -> Optional[Dict]:
        """获取用户联盟数据"""
        if not self.check_authentication():
            return None
        
        print("📡 获取联盟数据...")
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        return self.oauth_manager.make_authenticated_request(url)
    
    def select_league(self, leagues_data: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
        """选择联盟"""
        if not leagues_data:
            leagues_data = self.fetch_leagues()
        
        if not leagues_data:
            print("❌ 无法获取联盟数据")
            return None
        
        # 这里需要解析leagues_data的具体格式
        # TODO: 实现leagues_data的解析逻辑
        parsed_leagues = self._parse_leagues_data(leagues_data)
        
        return self.league_selector.select_league_interactively(parsed_leagues)
    
    def _parse_leagues_data(self, raw_data: Dict) -> Dict:
        """解析原始联盟数据"""
        # TODO: 实现具体的数据解析逻辑
        # 这里需要根据Yahoo API的实际响应格式来解析
        return {}
    
    def run_interactive_menu(self):
        """运行交互式主菜单"""
        while True:
            print("\n" + "="*60)
            print("🏈 Fantasy ETL 数据管道")
            print("="*60)
            
            print("1. 检查认证状态")
            print("2. 启动Web认证服务器")
            print("3. 获取联盟数据")
            print("4. 选择联盟")
            print("5. 运行ETL流程")
            print("0. 退出")
            
            choice = input("\n请选择操作 (0-5): ").strip()
            
            if choice == "0":
                print("👋 再见！")
                break
            elif choice == "1":
                self.check_authentication()
            elif choice == "2":
                self.start_auth_server()
            elif choice == "3":
                leagues_data = self.fetch_leagues()
                if leagues_data:
                    print("✅ 联盟数据获取成功")
                    print(json.dumps(leagues_data, indent=2, ensure_ascii=False)[:500] + "...")
            elif choice == "4":
                selected_league = self.select_league()
                if selected_league:
                    print(f"✅ 已选择联盟: {selected_league}")
            elif choice == "5":
                print("🚧 ETL流程功能开发中...")
            else:
                print("❌ 无效选择，请重试")


def main():
    """主程序入口"""
    app = FantasyETLApp()
    app.run_interactive_menu()


if __name__ == "__main__":
    main()
