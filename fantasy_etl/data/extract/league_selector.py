"""
联盟选择器
从archive/yahoo_api_utils.py重构而来，提供交互式联盟选择功能
"""
from typing import Dict, List, Optional, Any
from datetime import datetime

def print_league_selection_info(leagues_data: Dict[str, List[Dict]]) -> List[Dict]:
    """打印联盟选择信息"""
    print("\\n" + "="*80)
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

def select_league_interactively(leagues_data: Dict[str, List[Dict]]) -> Optional[Dict]:
    """交互式选择联盟"""
    all_leagues = print_league_selection_info(leagues_data)
    
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
                print(f"\\n✓ 已选择联盟: {selected_league['name']} ({selected_league['league_key']})")
                return selected_league
            else:
                print(f"请输入1到{len(all_leagues)}之间的数字")
                
        except ValueError:
            print("请输入有效的数字")
        except KeyboardInterrupt:
            print("\\n用户取消选择")
            return None

def parse_yahoo_date(date_str: str) -> Optional[datetime]:
    """解析Yahoo日期格式"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None