"""
核心数据转换器
包含通用的 transform_* 函数
"""

from typing import Optional, Dict, List
from datetime import date, datetime, timedelta


class CoreTransformers:
    """核心数据转换器"""
    
    # ============================================================================
    # 位置和基础转换
    # ============================================================================
    
    def transform_position_string(self, position_data) -> Optional[str]:
        """
        从位置数据中提取位置字符串
        
        迁移自: archive/yahoo_api_data.py transform_position_string() 第1533行
        """
        if not position_data:
            return None
            
        # 如果是字符串，直接返回
        if isinstance(position_data, str):
            return position_data
            
        # 如果是字典，查找position键
        if isinstance(position_data, dict):
            return position_data.get('position')
            
        # 如果是列表，尝试获取第一个元素
        if isinstance(position_data, list) and len(position_data) > 0:
            first_item = position_data[0]
            if isinstance(first_item, str):
                return first_item
            elif isinstance(first_item, dict):
                return first_item.get('position')
                
        return None
    
    # ============================================================================
    # 游戏和联盟数据转换
    # ============================================================================
    
    def transform_game_keys(self, games_data: Dict) -> List[str]:
        """
        从游戏数据中提取游戏键
        
        迁移自: archive/yahoo_api_data.py _extract_game_keys() 第181行
        """
        game_keys = []
        
        try:
            fantasy_content = games_data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_container = user_data[1]["games"]
            games_count = int(games_container.get("count", 0))
            
            for i in range(games_count):
                str_index = str(i)
                if str_index not in games_container:
                    continue
                    
                game_container = games_container[str_index]
                game_data = game_container["game"]
                
                if isinstance(game_data, list) and len(game_data) > 0:
                    game_info = game_data[0]
                    game_key = game_info.get("game_key")
                    game_type = game_info.get("type")
                    
                    # 只包含type='full'的游戏
                    if game_key and game_type == "full":
                        game_keys.append(game_key)
                
        except Exception as e:
            print(f"提取游戏键时出错: {e}")
        
        return game_keys
        
    def transform_leagues_from_data(self, data: Dict, game_key: str) -> List[Dict]:
        """
        从API返回数据中提取联盟信息
        
        迁移自: archive/yahoo_api_data.py _extract_leagues_from_data() 第212行
        """
        leagues = []
        
        try:
            fantasy_content = data["fantasy_content"]
            user_data = fantasy_content["users"]["0"]["user"]
            games_data = user_data[1]["games"]
            
            # 找到对应的游戏
            games_count = int(games_data.get("count", 0))
            target_game = None
            
            for i in range(games_count):
                str_index = str(i)
                if str_index in games_data:
                    game_container = games_data[str_index]
                    if "game" in game_container:
                        game_data = game_container["game"]
                        if isinstance(game_data, list) and len(game_data) > 1:
                            game_info = game_data[0]
                            if game_info.get("game_key") == game_key:
                                target_game = game_data[1]
                                break
            
            if not target_game or "leagues" not in target_game:
                return leagues
            
            leagues_container = target_game["leagues"]
            leagues_count = int(leagues_container.get("count", 0))
            
            for i in range(leagues_count):
                str_index = str(i)
                if str_index in leagues_container:
                    league_container = leagues_container[str_index]
                    if "league" in league_container:
                        league_data = league_container["league"]
                        if isinstance(league_data, list) and len(league_data) > 0:
                            league_info = league_data[0]
                            # 添加game_key到联盟信息中
                            league_info["game_key"] = game_key
                            leagues.append(league_info)
                            
        except Exception as e:
            print(f"提取联盟信息时出错: {e}")
        
        return leagues
    
    # ============================================================================
    # 交易数据转换
    # ============================================================================
    
    def transform_transactions_from_data(self, transactions_data: Dict) -> List[Dict]:
        """
        从API返回数据中提取交易信息
        
        迁移自: archive/yahoo_api_data.py _extract_transactions_from_data() 第936行
        """
        transactions = []
        
        try:
            fantasy_content = transactions_data["fantasy_content"]
            league_data = fantasy_content["league"]
            
            # 找到transactions容器
            transactions_container = None
            if isinstance(league_data, list):
                for item in league_data:
                    if isinstance(item, dict) and "transactions" in item:
                        transactions_container = item["transactions"]
                        break
            
            if not transactions_container:
                return transactions
            
            transactions_count = int(transactions_container.get("count", 0))
            
            for i in range(transactions_count):
                str_index = str(i)
                if str_index not in transactions_container:
                    continue
                
                transaction_container = transactions_container[str_index]
                if "transaction" not in transaction_container:
                    continue
                
                transaction_info = transaction_container["transaction"]
                
                # 处理不同格式的transaction数据
                if isinstance(transaction_info, list):
                    # 合并列表中的所有字典
                    merged_transaction = {}
                    for item in transaction_info:
                        if isinstance(item, dict):
                            merged_transaction.update(item)
                    if merged_transaction:
                        transactions.append(merged_transaction)
                elif isinstance(transaction_info, dict):
                    transactions.append(transaction_info)
        
        except Exception as e:
            print(f"提取交易信息时出错: {e}")
        
        return transactions
    
    # ============================================================================
    # 日期和时间处理
    # ============================================================================
    
    def calculate_date_range(self, mode: str, season_info: Dict, days_back: int = None, 
                           target_date: str = None) -> Optional[tuple]:
        """
        计算日期范围
        
        迁移自: archive/yahoo_api_data.py calculate_date_range() 第1257行
        """
        if not season_info:
            print("❌ 无法获取赛季信息")
            return None
        
        if mode == "specific":
            # 指定日期模式
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
            # 天数回溯模式
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
        
        elif mode == "full_season":
            # 完整赛季模式
            start_date = season_info["start_date"]
            end_date = season_info["end_date"]
            
            print(f"📅 完整赛季模式: 从 {start_date} 到 {end_date}")
            return (start_date, end_date)
        
        else:
            print(f"❌ 不支持的模式: {mode}")
            return None

# ============================================================================
# 独立函数接口 - 为了保持与文档的一致性
# ============================================================================

def transform_league_data(raw_data: Dict) -> Dict:
    """转换联盟数据"""
    transformer = CoreTransformers()
    return transformer.transform_league_data(raw_data)

def transform_game_data(raw_data: Dict) -> Dict:
    """转换游戏数据"""
    transformer = CoreTransformers()
    # 这个函数在类中不存在，需要实现
    return raw_data

def transform_settings_data(raw_data: Dict) -> Dict:
    """转换设置数据"""
    transformer = CoreTransformers()
    return transformer.transform_league_settings(raw_data)

def transform_standings_data(raw_data: List[Dict]) -> List[Dict]:
    """转换排名数据"""
    transformer = CoreTransformers()
    return [transformer.transform_team_standings_info(team) for team in raw_data]

def transform_draft_data(raw_data: List[Dict]) -> List[Dict]:
    """转换选秀数据"""
    transformer = CoreTransformers()
    return transformer.transform_draft_results(raw_data)

def transform_transaction_data(raw_data: Dict) -> Dict:
    """转换交易数据"""
    transformer = CoreTransformers()
    return transformer.transform_transaction_data(raw_data)

def transform_waiver_data(raw_data: Dict) -> Dict:
    """转换waiver数据"""
    transformer = CoreTransformers()
    return transformer.transform_waiver_claim(raw_data)