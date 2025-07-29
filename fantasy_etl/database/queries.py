"""
数据库查询操作
包含所有 get_* 函数，从多个源文件迁移
"""

from typing import Optional, Dict, Tuple, List
from datetime import datetime, date, timedelta
from .connection import DatabaseConnection
from .model import (
    League, Team, Player, Manager, Game,
    LeagueSettings, StatCategory, PlayerEligiblePosition,
    RosterDaily, PlayerDailyStats, PlayerSeasonStats,
    TeamStatsWeekly, LeagueStandings, TeamMatchups,
    Transaction, TransactionPlayer, DateDimension,
    LeagueRosterPosition
)


class DatabaseQueries:
    """数据库查询操作类"""
    
    def __init__(self):
        """初始化查询器"""
        self.db_connection = DatabaseConnection()
        self.session = None
        
    def _get_session(self):
        """获取数据库会话"""
        if not self.session:
            self.session = self.db_connection.get_session()
        return self.session
        
    # ============================================================================
    # 从 archive/yahoo_api_data.py 迁移的查询函数
    # ============================================================================
    
    def get_leagues_from_database(self) -> Optional[Dict]:
        """
        从数据库获取联盟数据
        
        迁移自: archive/yahoo_api_data.py _get_leagues_from_database() 第78行
        """
        try:
            session = self._get_session()
            leagues = session.query(League).all()
            if not leagues:
                return None
            
            # 按game_key分组
            leagues_data = {}
            for league in leagues:
                game_key = league.game_key
                if game_key not in leagues_data:
                    leagues_data[game_key] = []
                
                # 转换为字典格式
                league_dict = {
                    'league_key': league.league_key,
                    'league_id': league.league_id,
                    'game_key': league.game_key,
                    'name': league.name,
                    'url': league.url,
                    'logo_url': league.logo_url,
                    'password': league.password,
                    'draft_status': league.draft_status,
                    'num_teams': league.num_teams,
                    'edit_key': league.edit_key,
                    'weekly_deadline': league.weekly_deadline,
                    'league_update_timestamp': league.league_update_timestamp,
                    'scoring_type': league.scoring_type,
                    'league_type': league.league_type,
                    'renew': league.renew,
                    'renewed': league.renewed,
                    'felo_tier': league.felo_tier,
                    'iris_group_chat_id': league.iris_group_chat_id,
                    'short_invitation_url': league.short_invitation_url,
                    'allow_add_to_dl_extra_pos': league.allow_add_to_dl_extra_pos,
                    'is_pro_league': league.is_pro_league,
                    'is_cash_league': league.is_cash_league,
                    'current_week': league.current_week,
                    'start_week': league.start_week,
                    'start_date': league.start_date,
                    'end_week': league.end_week,
                    'end_date': league.end_date,
                    'is_finished': league.is_finished,
                    'is_plus_league': league.is_plus_league,
                    'game_code': league.game_code,
                    'season': league.season
                }
                leagues_data[game_key].append(league_dict)
            
            return leagues_data
            
        except Exception as e:
            return None
        
    def get_season_date_info(self, league_key: str) -> Dict:
        """
        获取赛季日期信息和状态
        
        迁移自: archive/yahoo_api_data.py get_season_date_info() 第1221行
        """
        if not league_key:
            return {}
        
        try:
            session = self._get_session()
            league_db = session.query(League).filter_by(
                league_key=league_key
            ).first()
            
            if not league_db:
                return {}
            
            start_date_str = league_db.start_date
            end_date_str = league_db.end_date
            is_finished = league_db.is_finished
            
            if not start_date_str or not end_date_str:
                return {}
            
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            today = date.today()
            
            # 判断赛季状态 - 主要基于数据库的is_finished字段
            if is_finished:
                season_status = "finished"
                latest_date = end_date  # 如果赛季已结束，使用结束日期
            elif today > end_date:
                season_status = "finished"  # 根据日期判断已结束
                latest_date = end_date
            elif today < start_date:
                season_status = "not_started"
                latest_date = start_date
            else:
                season_status = "ongoing"
                latest_date = min(today, end_date)  # 进行中的赛季使用今天和结束日期的较小值
            
            return {
                "start_date": start_date,
                "end_date": end_date,
                "latest_date": latest_date,
                "season_status": season_status,
                "is_finished": is_finished,
                "today": today
            }
            
        except Exception as e:
            print(f"获取赛季日期信息失败: {e}")
            return {}
        
    def get_teams_data_from_db(self, league_key: str) -> Optional[Dict]:
        """
        从数据库获取团队数据并转换为API格式
        
        迁移自: archive/yahoo_api_data.py _get_teams_data_from_db() 第2638行
        """
        try:
            session = self._get_session()
            teams = session.query(Team).filter_by(
                league_key=league_key
            ).all()
            
            if not teams:
                return None
            
            # 模拟API返回的团队数据格式
            teams_data = {
                "fantasy_content": {
                    "league": [
                        {},  # 其他联盟信息
                        {
                            "teams": {
                                "count": len(teams)
                            }
                        }
                    ]
                }
            }
            
            # 添加团队数据
            teams_container = teams_data["fantasy_content"]["league"][1]["teams"]
            for i, team in enumerate(teams):
                teams_container[str(i)] = {
                    "team": [
                        [
                            {
                                "team_key": team.team_key,
                                "team_id": team.team_id,
                                "name": team.name,
                                "url": team.url,
                                "team_logo_url": team.team_logo_url,
                                "waiver_priority": team.waiver_priority,
                                "number_of_moves": team.number_of_moves,
                                "number_of_trades": team.number_of_trades,
                                "roster_adds": {
                                    "coverage_value": team.roster_adds_week,
                                    "value": team.roster_adds_value
                                },
                                "clinched_playoffs": team.clinched_playoffs,
                                "has_draft_grade": team.has_draft_grade,
                                "managers": []  # 管理员数据需要时可以从Manager表获取
                            }
                        ]
                    ]
                }
            
            return teams_data
            
        except Exception as e:
            return None
        
    def get_time_selection_interactive(self, data_type: str, season_info: Dict) -> Optional[Tuple]:
        """
        交互式时间选择界面
        
        迁移自: archive/yahoo_api_data.py get_time_selection_interactive() 第2272行
        注意：这个函数包含用户交互，可能需要重新设计
        """
        print(f"\n=== {data_type}数据时间选择 ===")
        print("1. 指定日期 (YYYY-MM-DD)")
        print("2. 指定时间段 (start: YYYY-MM-DD, end: YYYY-MM-DD)")
        print("3. 天数回溯")
        print("0. 返回")
        
        choice = input("\n请选择时间模式 (0-3): ").strip()
        
        if choice == "0":
            return None
        elif choice == "1":
            target_date = input("请输入日期 (YYYY-MM-DD): ").strip()
            if not target_date:
                print("❌ 日期不能为空")
                return None
            return self._calculate_date_range("specific", season_info, target_date=target_date)
        elif choice == "2":
            start_date = input("请输入开始日期 (YYYY-MM-DD): ").strip()
            end_date = input("请输入结束日期 (YYYY-MM-DD): ").strip()
            if not start_date or not end_date:
                print("❌ 开始和结束日期不能为空")
                return None
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                if start_dt > end_dt:
                    print("❌ 开始日期不能晚于结束日期")
                    return None
                return (start_dt, end_dt)
            except ValueError:
                print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                return None
        elif choice == "3":
            days_input = input("请输入回溯天数: ").strip()
            try:
                days_back = int(days_input)
                if days_back <= 0:
                    print("❌ 天数必须大于0")
                    return None
                return self._calculate_date_range("days_back", season_info, days_back=days_back)
            except ValueError:
                print("❌ 天数必须是有效数字")
                return None
        else:
            print("❌ 无效选择")
            return None
    
    def _calculate_date_range(self, mode: str, season_info: Dict, days_back: int = None, 
                           target_date: str = None) -> Optional[tuple]:
        """计算日期范围"""
        if not season_info:
            print("❌ 无法获取赛季信息")
            return None
        
        if mode == "specific":
            if not target_date:
                print("❌ 指定日期模式需要提供target_date")
                return None
            try:
                target = datetime.strptime(target_date, '%Y-%m-%d').date()
                # 检查日期是否在赛季范围内
                if target < season_info["start_date"] or target > season_info["end_date"]:
                    print(f"⚠️ 指定日期 {target_date} 不在赛季范围内 ({season_info['start_date']} 到 {season_info['end_date']})")
                return (target, target)
            except ValueError:
                print(f"❌ 日期格式错误: {target_date}")
                return None
        
        elif mode == "days_back":
            if days_back is None:
                print("❌ 天数回溯模式需要提供days_back")
                return None
            
            # 从最新日期向前回溯
            end_date = season_info["latest_date"]
            start_date = end_date - timedelta(days=days_back)
            
            # 确保不超出赛季范围
            start_date = max(start_date, season_info["start_date"])
            
            print(f"📅 天数回溯模式: 从 {start_date} 到 {end_date} (回溯{days_back}天，赛季状态: {season_info['season_status']})")
            return (start_date, end_date)
        
        else:
            print(f"❌ 不支持的模式: {mode}")
            return None
    
    # ============================================================================
    # 从 archive/database_writer.py 迁移的查询函数
    # ============================================================================
    
    def get_stats_summary(self, stats_dict: Dict[str, int]) -> str:
        """
        获取统计摘要
        
        迁移自: archive/database_writer.py get_stats_summary() 第128行
        """
        return (f"统计: 游戏({stats_dict.get('games', 0)}) 联盟({stats_dict.get('leagues', 0)}) "
                f"团队({stats_dict.get('teams', 0)}) 球员({stats_dict.get('players', 0)}) "
                f"交易({stats_dict.get('transactions', 0)}) 交易球员({stats_dict.get('transaction_players', 0)}) "
                f"名单({stats_dict.get('roster_daily', 0)}) 赛季统计({stats_dict.get('player_season_stats', 0)}) "
                f"日期统计({stats_dict.get('player_daily_stats', 0)}) 团队周统计({stats_dict.get('team_stats_weekly', 0)}) "
                f"团队赛季统计({stats_dict.get('team_stats_season', 0)})")
        
    def get_stat_category_info(self, league_key: str, stat_id: int) -> Optional[Dict]:
        """
        获取统计类别信息
        
        迁移自: archive/database_writer.py get_stat_category_info() 第400行
        """
        try:
            session = self._get_session()
            stat_cat = session.query(StatCategory).filter_by(
                league_key=league_key,
                stat_id=stat_id
            ).first()
            
            if stat_cat:
                return {
                    'name': stat_cat.name,
                    'display_name': stat_cat.display_name,
                    'abbr': stat_cat.abbr,
                    'group': stat_cat.group_name
                }
            return None
            
        except Exception as e:
            print(f"获取统计类别信息失败 {stat_id}: {e}")
            return None
        
    def get_database_summary(self) -> Dict[str, int]:
        """
        获取数据库摘要信息
        
        迁移自: archive/database_writer.py get_database_summary() 第1784行
        """
        summary = {}
        
        # 定义所有表和对应的模型类
        tables = {
            'games': Game,
            'leagues': League,
            'league_settings': LeagueSettings,
            'stat_categories': StatCategory,
            'teams': Team,
            'managers': Manager,
            'players': Player,
            'player_eligible_positions': PlayerEligiblePosition,
            'player_season_stats': PlayerSeasonStats,        # 更新为新的混合存储表
            'player_daily_stats': PlayerDailyStats,          # 更新为新的混合存储表
            'team_stats_weekly': TeamStatsWeekly,            # 更新为新的团队周统计表
            'league_standings': LeagueStandings,
            'team_matchups': TeamMatchups,
            'roster_daily': RosterDaily,
            'transactions': Transaction,
            'transaction_players': TransactionPlayer,
            'date_dimension': DateDimension
        }
        
        session = self._get_session()
        for table_name, model_class in tables.items():
            try:
                count = session.query(model_class).count()
                summary[table_name] = count
            except Exception as e:
                print(f"查询 {table_name} 表时出错: {e}")
                summary[table_name] = -1  # 表示查询失败
        
        return summary
        
    def close(self):
        """关闭数据库连接"""
        self.db_connection.close()
        if self.session:
            self.session = None