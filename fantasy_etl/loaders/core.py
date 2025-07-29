"""
核心数据加载器
包含基础的 load_* 函数，从 archive/yahoo_api_data.py 迁移
"""

from typing import List, Dict, Optional
from datetime import datetime


class CoreLoaders:
    """核心数据加载器"""
    
    def __init__(self, db_writer):
        """
        初始化加载器
        
        Args:
            db_writer: 数据库写入器实例
        """
        self.db_writer = db_writer
    
    # ============================================================================
    # 从 archive/yahoo_api_data.py 迁移的 load_* 函数
    # ============================================================================
    
    def load_roster_data(self, roster_list: List[Dict], selected_league: Dict) -> bool:
        """
        加载roster数据到数据库 (纯加载，不转换数据)
        
        迁移自: archive/yahoo_api_data.py load_roster_data() 第611行
        
        Args:
            roster_list: 已转换的roster数据列表
            selected_league: 当前选择的联盟信息
            
        Returns:
            成功加载返回True，否则返回False
        """
        if not roster_list:
            return False
            
        count = 0
        for roster_entry in roster_list:
            try:
                # 解析日期 - 如果无法解析则跳过该记录，不使用当前日期
                roster_date_str = roster_entry["coverage_date"]
                if not roster_date_str:
                    continue
                
                try:
                    roster_date = datetime.strptime(roster_date_str, '%Y-%m-%d').date()
                except Exception as e:
                    continue
                
                # 判断是否首发
                selected_position = roster_entry["selected_position"]
                is_starting = selected_position not in ['BN', 'IL', 'IR'] if selected_position else False
                is_bench = selected_position == 'BN' if selected_position else False
                is_injured_reserve = selected_position in ['IL', 'IR'] if selected_position else False
                
                # 使用新的write_roster_daily方法
                if self.db_writer.write_roster_daily(
                    team_key=roster_entry["team_key"],
                    player_key=roster_entry["player_key"],
                    league_key=selected_league['league_key'],
                    roster_date=roster_date,
                    season=selected_league.get('season', '2024'),
                    selected_position=selected_position,
                    is_starting=is_starting,
                    is_bench=is_bench,
                    is_injured_reserve=is_injured_reserve,
                    player_status=roster_entry["status"],
                    status_full=roster_entry["status_full"],
                    injury_note=roster_entry["injury_note"],
                    is_keeper=roster_entry.get("is_keeper", False),
                    keeper_cost=roster_entry.get("keeper_cost"),
                    is_prescoring=roster_entry["is_prescoring"],
                    is_editable=roster_entry["is_editable"]
                ):
                    count += 1
                    
            except Exception as e:
                continue
        
        return count > 0
        
    def load_team_matchups(self, transformed_matchups: List[Dict], league_key: str, season: str) -> bool:
        """
        加载团队matchups数据到数据库 (纯加载，不转换数据)
        
        迁移自: archive/yahoo_api_data.py load_team_matchups() 第1601行
        
        Args:
            transformed_matchups: 已转换的matchups数据列表
            league_key: 联盟标识符
            season: 赛季
            
        Returns:
            成功加载返回True，否则返回False
        """
        if not transformed_matchups:
            return False
            
        success_count = 0
        for matchup_item in transformed_matchups:
            matchup_info = matchup_item['matchup_info']
            team_key = matchup_item['team_key']
            
            # 使用新的方法直接从matchup数据写入数据库
            if self.db_writer.write_team_matchup_from_data(matchup_info, team_key, league_key, season):
                success_count += 1
                
                # 同时写入TeamStatsWeekly数据 - 需要先transform再load
                # 注意：这里需要调用transform_team_weekly_stats和load_team_weekly_stats
                # 由于这些函数可能在其他模块，这里暂时注释
                # weekly_stats = self.transform_team_weekly_stats(matchup_info, team_key)
                # if weekly_stats:
                #     self.load_team_weekly_stats(league_key, season, weekly_stats)
        
        return success_count > 0
        
    def load_teams_to_db(self, teams_data: Dict, league_key: str) -> int:
        """
        将团队数据写入数据库
        
        迁移自: archive/yahoo_api_data.py load_teams_to_db() 第373行
        
        Args:
            teams_data: API返回的团队数据
            league_key: 联盟标识符
            
        Returns:
            成功写入的团队数量
        """
        teams_list = []
        
        try:
            fantasy_content = teams_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            teams_container = None
            if isinstance(league_data, list) and len(league_data) > 1:
                for item in league_data:
                    if isinstance(item, dict) and "teams" in item:
                        teams_container = item["teams"]
                        break
            
            if not teams_container:
                return 0
            
            teams_count = int(teams_container.get("count", 0))
            for i in range(teams_count):
                str_index = str(i)
                if str_index not in teams_container:
                    continue
                
                team_container = teams_container[str_index]
                team_data = team_container["team"]
                
                # 处理团队数据 - 需要transform_team_data_from_api函数
                # 这个函数应该在transformers模块中
                # team_dict = self.transform_team_data_from_api(team_data)
                # if team_dict:
                #     teams_list.append(team_dict)
                
                # 暂时直接处理
                team_dict = self._transform_team_data_from_api(team_data)
                if team_dict:
                    teams_list.append(team_dict)
        
        except Exception as e:
            return 0
        
        # 批量写入数据库
        if teams_list:
            return self.db_writer.write_teams_batch(teams_list, league_key)
        
        return 0
    
    def _transform_team_data_from_api(self, team_data: List) -> Optional[Dict]:
        """临时的转换函数，应该移到transformers模块"""
        try:
            if not isinstance(team_data, list) or len(team_data) == 0:
                return None
            
            team_info_list = team_data[0]
            if not isinstance(team_info_list, list):
                return None
            
            # 提取团队基本信息
            team_dict = {}
            managers_data = []
            
            for item in team_info_list:
                if isinstance(item, dict):
                    if "managers" in item:
                        managers_data = item["managers"]
                    elif "team_logos" in item and item["team_logos"]:
                        # 处理team logo
                        if len(item["team_logos"]) > 0 and "team_logo" in item["team_logos"][0]:
                            team_dict["team_logo_url"] = item["team_logos"][0]["team_logo"].get("url")
                    elif "roster_adds" in item:
                        # 处理roster adds
                        roster_adds = item["roster_adds"]
                        team_dict["roster_adds_week"] = roster_adds.get("coverage_value")
                        team_dict["roster_adds_value"] = roster_adds.get("value")
                    elif "clinched_playoffs" in item:
                        team_dict["clinched_playoffs"] = bool(item["clinched_playoffs"])
                    elif "has_draft_grade" in item:
                        team_dict["has_draft_grade"] = bool(item["has_draft_grade"])
                    elif "number_of_trades" in item:
                        # 处理可能是字符串的数字字段
                        try:
                            team_dict["number_of_trades"] = int(item["number_of_trades"])
                        except (ValueError, TypeError):
                            team_dict["number_of_trades"] = 0
                    else:
                        # 直接更新其他字段
                        team_dict.update(item)
            
            # 添加managers数据
            team_dict["managers"] = managers_data
            
            # 验证必要字段
            if not team_dict.get("team_key"):
                return None
            
            return team_dict
            
        except Exception as e:
            return None
        
    def load_transactions_to_db(self, transactions: List[Dict], league_key: str) -> int:
        """
        将交易数据写入数据库
        
        迁移自: archive/yahoo_api_data.py load_transactions_to_db() 第985行
        
        Args:
            transactions: 交易数据列表
            league_key: 联盟标识符
            
        Returns:
            成功写入的交易数量
        """
        if not transactions:
            return 0
        
        return self.db_writer.write_transactions_batch(transactions, league_key)
        
    def load_league_standings_to_db(self, team_info: Dict, league_key: str, season: str) -> bool:
        """
        将联盟排名数据写入数据库
        
        迁移自: archive/yahoo_api_data.py load_league_standings_to_db() 第1142行
        
        Args:
            team_info: 团队信息（包含排名数据）
            league_key: 联盟标识符
            season: 赛季
            
        Returns:
            成功返回True，否则返回False
        """
        try:
            team_key = team_info["team_key"]
            team_standings = team_info.get("team_standings", {})
            team_points = team_info.get("team_points", {})
            
            # 提取standings数据
            rank = None
            wins = 0
            losses = 0
            ties = 0
            win_percentage = 0.0
            games_back = "-"
            playoff_seed = None
            
            if isinstance(team_standings, dict):
                rank = team_standings.get("rank")
                wins = int(team_standings.get("outcome_totals", {}).get("wins", 0))
                losses = int(team_standings.get("outcome_totals", {}).get("losses", 0))
                ties = int(team_standings.get("outcome_totals", {}).get("ties", 0))
                win_percentage = float(team_standings.get("outcome_totals", {}).get("percentage", 0))
                games_back = team_standings.get("games_back", "-")
                playoff_seed = team_standings.get("playoff_seed")
                
                # 分区记录
                divisional_outcome = team_standings.get("divisional_outcome_totals", {})
                divisional_wins = int(divisional_outcome.get("wins", 0))
                divisional_losses = int(divisional_outcome.get("losses", 0))
                divisional_ties = int(divisional_outcome.get("ties", 0))
            else:
                divisional_wins = 0
                divisional_losses = 0
                divisional_ties = 0
            
            # 写入数据库（不再需要存储 JSON 数据，所有字段已结构化）
            return self.db_writer.write_league_standings(
                league_key=league_key,
                team_key=team_key,
                season=season,
                rank=rank,
                playoff_seed=playoff_seed,
                wins=wins,
                losses=losses,
                ties=ties,
                win_percentage=win_percentage,
                games_back=games_back,
                divisional_wins=divisional_wins,
                divisional_losses=divisional_losses,
                divisional_ties=divisional_ties
            )
            
        except Exception as e:
            return False
        
    def load_team_weekly_stats(self, league_key: str, season: str, weekly_stats_data: Dict) -> bool:
        """
        加载团队周统计数据到数据库 (纯加载，不转换数据)
        
        迁移自: archive/yahoo_api_data.py load_team_weekly_stats() 第1654行
        
        Args:
            league_key: 联盟标识符
            season: 赛季
            weekly_stats_data: 已转换的周统计数据
            
        Returns:
            成功返回True，否则返回False
        """
        try:
            if not weekly_stats_data:
                return False
            
            return self.db_writer.write_team_weekly_stats_from_matchup(
                team_key=weekly_stats_data['team_key'],
                league_key=league_key,
                season=season,
                week=weekly_stats_data['week'],
                team_stats_data=weekly_stats_data['stats']
            )
            
        except Exception as e:
            print(f"加载团队周统计失败: {e}")
            return False
        
    def close(self):
        """关闭数据库连接"""
        if hasattr(self.db_writer, 'close'):
            self.db_writer.close()