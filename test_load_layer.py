"""
Load Layer Test Script
=====================

测试Yahoo Fantasy ETL load层的完整功能：
1. 数据库连接测试
2. 数据模型创建
3. 加载器功能测试
4. 完整ETL管道测试

使用方法:
python test_load_layer.py [--recreate-tables] [--test-data]

参数:
--recreate-tables: 重新创建数据库表
--test-data: 使用测试数据进行完整测试
"""

import sys
import os
import argparse
import logging
from datetime import datetime, date
from typing import Dict, List, Any

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath('.'))

from fantasy_etl.load.database.connection_manager import ConnectionManager
from fantasy_etl.load.database.session_manager import SessionManager
from fantasy_etl.load.database.models import create_database_engine, create_tables, recreate_tables
from fantasy_etl.load.loader_manager import LoaderManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_load_layer.log')
    ]
)
logger = logging.getLogger(__name__)


def test_database_connection():
    """测试数据库连接"""
    logger.info("🔄 测试数据库连接...")
    
    try:
        conn_manager = ConnectionManager()
        
        # 测试连接
        if conn_manager.test_connection():
            logger.info("✅ 数据库连接成功")
            return True
        else:
            logger.error("❌ 数据库连接失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 数据库连接测试异常: {e}")
        return False


def test_database_creation(recreate: bool = False):
    """测试数据库表创建"""
    logger.info("🔄 测试数据库表创建...")
    
    try:
        engine = create_database_engine()
        
        if recreate:
            logger.info("正在重新创建数据库表...")
            success = recreate_tables(engine)
            if not success:
                logger.error("❌ 重新创建数据库表失败")
                return False
        else:
            logger.info("正在创建数据库表...")
            create_tables(engine)
        
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库表创建失败: {e}")
        return False


def generate_test_data() -> Dict[str, Any]:
    """生成测试数据"""
    logger.info("🔄 生成测试数据...")
    
    test_data = {
        'games': [
            {
                'game_key': '428',
                'game_id': '428',
                'name': 'Basketball',
                'code': 'nba',
                'type': 'full',
                'season': '2023',
                'is_registration_over': False,
                'is_game_over': False,
                'is_offseason': False
            }
        ],
        'leagues': [
            {
                'league_key': '428.l.1234',
                'league_id': '1234',
                'game_key': '428',
                'name': 'Test League',
                'num_teams': 10,
                'season': '2023',
                'draft_status': 'postdraft',
                'league_type': 'private',
                'scoring_type': 'head',
                'start_date': '2023-10-17',
                'end_date': '2024-04-12',
                'current_week': '1',
                'settings': {
                    'league_key': '428.l.1234',
                    'draft_type': 'live',
                    'uses_playoff': True,
                    'playoff_start_week': '18',
                    'num_playoff_teams': 6,
                    'waiver_type': 'FR',
                    'waiver_rule': 'all',
                    'trade_end_date': '2024-03-15',
                    'trade_ratify_type': 'commish',
                    'player_pool': 'ALL'
                },
                'stat_categories': [
                    {
                        'league_key': '428.l.1234',
                        'stat_id': 5,
                        'name': 'Field Goal Percentage',
                        'display_name': 'FG%',
                        'abbr': 'FG%',
                        'position_type': 'B',
                        'is_enabled': True,
                        'is_core_stat': True,
                        'core_stat_column': 'field_goal_percentage'
                    },
                    {
                        'league_key': '428.l.1234',
                        'stat_id': 12,
                        'name': 'Points',
                        'display_name': 'PTS',
                        'abbr': 'PTS',
                        'position_type': 'B',
                        'is_enabled': True,
                        'is_core_stat': True,
                        'core_stat_column': 'points'
                    }
                ]
            }
        ],
        'teams': [
            {
                'team_key': '428.l.1234.t.1',
                'team_id': '1',
                'league_key': '428.l.1234',
                'name': 'Test Team 1',
                'waiver_priority': 5,
                'number_of_moves': 10,
                'number_of_trades': 2,
                'managers': [
                    {
                        'manager_id': 'manager1',
                        'team_key': '428.l.1234.t.1',
                        'nickname': 'TestManager1',
                        'guid': 'ABCDEF123456',
                        'is_commissioner': True,
                        'email': 'test1@example.com'
                    }
                ]
            },
            {
                'team_key': '428.l.1234.t.2',
                'team_id': '2',
                'league_key': '428.l.1234',
                'name': 'Test Team 2',
                'waiver_priority': 3,
                'number_of_moves': 8,
                'number_of_trades': 1,
                'managers': [
                    {
                        'manager_id': 'manager2',
                        'team_key': '428.l.1234.t.2',
                        'nickname': 'TestManager2',
                        'guid': 'GHIJKL789012',
                        'is_commissioner': False,
                        'email': 'test2@example.com'
                    }
                ]
            }
        ],
        'players': [
            {
                'player_key': '428.p.6020',
                'player_id': '6020',
                'editorial_player_key': '6020',
                'league_key': '428.l.1234',
                'full_name': 'LeBron James',
                'first_name': 'LeBron',
                'last_name': 'James',
                'current_team_key': 'LAL',
                'current_team_name': 'Los Angeles Lakers',
                'current_team_abbr': 'LAL',
                'display_position': 'SF,PF',
                'primary_position': 'SF',
                'position_type': 'P',
                'uniform_number': '6',
                'status': 'A',
                'season': '2023',
                'eligible_positions': ['SF', 'PF']
            },
            {
                'player_key': '428.p.5479',
                'player_id': '5479',
                'editorial_player_key': '5479',
                'league_key': '428.l.1234',
                'full_name': 'Stephen Curry',
                'first_name': 'Stephen',
                'last_name': 'Curry',
                'current_team_key': 'GSW',
                'current_team_name': 'Golden State Warriors',
                'current_team_abbr': 'GSW',
                'display_position': 'PG',
                'primary_position': 'PG',
                'position_type': 'P',
                'uniform_number': '30',
                'status': 'A',
                'season': '2023',
                'eligible_positions': ['PG']
            }
        ],
        'dates': [
            {
                'date': date(2023, 10, 17),
                'league_key': '428.l.1234',
                'season': '2023'
            },
            {
                'date': date(2023, 10, 18),
                'league_key': '428.l.1234',
                'season': '2023'
            }
        ],
        'rosters': [
            {
                'team_key': '428.l.1234.t.1',
                'player_key': '428.p.6020',
                'league_key': '428.l.1234',
                'date': date(2023, 10, 17),
                'season': '2023',
                'week': 1,
                'selected_position': 'SF',
                'is_starting': True,
                'is_bench': False,
                'player_status': 'A'
            },
            {
                'team_key': '428.l.1234.t.2',
                'player_key': '428.p.5479',
                'league_key': '428.l.1234',
                'date': date(2023, 10, 17),
                'season': '2023',
                'week': 1,
                'selected_position': 'PG',
                'is_starting': True,
                'is_bench': False,
                'player_status': 'A'
            }
        ],
        'player_stats': {
            'daily': [
                {
                    'player_key': '428.p.6020',
                    'editorial_player_key': '6020',
                    'league_key': '428.l.1234',
                    'season': '2023',
                    'date': date(2023, 10, 17),
                    'week': 1,
                    'field_goals_made': 12,
                    'field_goals_attempted': 22,
                    'field_goal_percentage': 0.545,
                    'free_throws_made': 8,
                    'free_throws_attempted': 10,
                    'free_throw_percentage': 0.800,
                    'three_pointers_made': 3,
                    'points': 35,
                    'rebounds': 10,
                    'assists': 8,
                    'steals': 2,
                    'blocks': 1,
                    'turnovers': 4
                }
            ],
            'season': [
                {
                    'player_key': '428.p.6020',
                    'editorial_player_key': '6020',
                    'league_key': '428.l.1234',
                    'season': '2023',
                    'field_goals_made': 780,
                    'field_goals_attempted': 1500,
                    'field_goal_percentage': 0.520,
                    'free_throws_made': 450,
                    'free_throws_attempted': 600,
                    'free_throw_percentage': 0.750,
                    'three_pointers_made': 120,
                    'total_points': 2130,
                    'total_rebounds': 650,
                    'total_assists': 520,
                    'total_steals': 95,
                    'total_blocks': 45,
                    'total_turnovers': 280
                }
            ]
        },
        'team_stats': [
            {
                'team_key': '428.l.1234.t.1',
                'league_key': '428.l.1234',
                'season': '2023',
                'week': 1,
                'field_goals_made': 45,
                'field_goals_attempted': 95,
                'field_goal_percentage': 0.474,
                'free_throws_made': 20,
                'free_throws_attempted': 25,
                'free_throw_percentage': 0.800,
                'three_pointers_made': 12,
                'points': 122,
                'rebounds': 48,
                'assists': 25,
                'steals': 8,
                'blocks': 6,
                'turnovers': 15
            }
        ],
        'standings': [
            {
                'league_key': '428.l.1234',
                'team_key': '428.l.1234.t.1',
                'season': '2023',
                'rank': 1,
                'wins': 5,
                'losses': 2,
                'ties': 0,
                'win_percentage': 0.714,
                'games_back': '-'
            },
            {
                'league_key': '428.l.1234',
                'team_key': '428.l.1234.t.2',
                'season': '2023',
                'rank': 2,
                'wins': 4,
                'losses': 3,
                'ties': 0,
                'win_percentage': 0.571,
                'games_back': '1.0'
            }
        ],
        'matchups': [
            {
                'league_key': '428.l.1234',
                'team_key': '428.l.1234.t.1',
                'season': '2023',
                'week': 1,
                'status': 'postevent',
                'opponent_team_key': '428.l.1234.t.2',
                'is_winner': True,
                'team_points': 7,
                'opponent_points': 4,
                'winner_team_key': '428.l.1234.t.1',
                'wins_field_goal_pct': True,
                'wins_free_throw_pct': False,
                'wins_three_pointers': True,
                'wins_points': True,
                'wins_rebounds': True,
                'wins_assists': False,
                'wins_steals': True,
                'wins_blocks': True,
                'wins_turnovers': False,
                'completed_games': 10,
                'remaining_games': 0,
                'live_games': 0
            }
        ],
        'transactions': [
            {
                'transaction_key': '428.l.1234.tr.1',
                'transaction_id': '1',
                'league_key': '428.l.1234',
                'type': 'add/drop',
                'status': 'successful',
                'timestamp': '1697500800',
                'players': [
                    {
                        'transaction_key': '428.l.1234.tr.1',
                        'player_key': '428.p.6020',
                        'player_id': '6020',
                        'player_name': 'LeBron James',
                        'editorial_team_abbr': 'LAL',
                        'display_position': 'SF,PF',
                        'transaction_type': 'add',
                        'source_type': 'freeagents',
                        'destination_type': 'team',
                        'destination_team_key': '428.l.1234.t.1',
                        'destination_team_name': 'Test Team 1'
                    }
                ]
            }
        ]
    }
    
    logger.info("✅ 测试数据生成完成")
    return test_data


def test_individual_loaders(test_data: Dict[str, Any]):
    """测试各个加载器"""
    logger.info("🔄 测试各个数据加载器...")
    
    try:
        loader_manager = LoaderManager()
        results = {}
        
        # 测试游戏加载器
        logger.info("测试游戏加载器...")
        if 'games' in test_data:
            game_result = loader_manager.game_loader.load(test_data['games'])
            results['games'] = game_result
            logger.info(f"游戏数据: {game_result.records_loaded} 条记录已加载")
        
        # 测试日期维度加载器
        logger.info("测试日期维度加载器...")
        if 'dates' in test_data:
            date_result = loader_manager.date_loader.load(test_data['dates'])
            results['dates'] = date_result
            logger.info(f"日期数据: {date_result.records_loaded} 条记录已加载")
        
        # 测试联盟加载器
        logger.info("测试联盟加载器...")
        if 'leagues' in test_data:
            league_result = loader_manager._load_league_data(test_data['leagues'])
            results['leagues'] = league_result
            logger.info(f"联盟数据: {league_result.records_loaded} 条记录已加载")
        
        # 测试团队加载器
        logger.info("测试团队加载器...")
        if 'teams' in test_data:
            team_result = loader_manager._load_team_data(test_data['teams'])
            results['teams'] = team_result
            logger.info(f"团队数据: {team_result.records_loaded} 条记录已加载")
        
        # 测试球员加载器
        logger.info("测试球员加载器...")
        if 'players' in test_data:
            player_result = loader_manager.player_loader.load(test_data['players'])
            results['players'] = player_result
            logger.info(f"球员数据: {player_result.records_loaded} 条记录已加载")
        
        logger.info("✅ 各个加载器测试完成")
        return results
        
    except Exception as e:
        logger.error(f"❌ 加载器测试失败: {e}")
        raise


def test_complete_etl_pipeline(test_data: Dict[str, Any]):
    """测试完整ETL管道"""
    logger.info("🔄 测试完整ETL管道...")
    
    try:
        loader_manager = LoaderManager()
        
        # 执行完整数据加载
        results = loader_manager.load_complete_fantasy_data(test_data)
        
        # 生成加载总结
        summary = loader_manager.get_load_summary(results)
        
        logger.info("📊 ETL管道执行结果:")
        logger.info(f"  总记录数: {summary['total_records_loaded']}")
        logger.info(f"  更新记录数: {summary['total_records_updated']}")
        logger.info(f"  失败记录数: {summary['total_records_failed']}")
        logger.info(f"  数据类型数: {summary['total_data_types']}")
        logger.info(f"  成功率: {summary['success_rate']:.2f}%")
        
        if summary['errors']:
            logger.warning("⚠️ 发现错误:")
            for error in summary['errors']:
                logger.warning(f"  - {error}")
        
        logger.info("✅ 完整ETL管道测试完成")
        return results
        
    except Exception as e:
        logger.error(f"❌ ETL管道测试失败: {e}")
        raise


def test_data_validation():
    """测试数据验证功能"""
    logger.info("🔄 测试数据验证功能...")
    
    try:
        loader_manager = LoaderManager()
        
        # 验证测试联盟数据
        validation_results = loader_manager.validate_data_consistency('428.l.1234')
        
        logger.info("📋 数据验证结果:")
        logger.info(f"  验证状态: {'✅ 通过' if validation_results['is_valid'] else '❌ 失败'}")
        logger.info(f"  检查项目: {len(validation_results['checks_performed'])}")
        
        if validation_results['warnings']:
            logger.warning("⚠️ 验证警告:")
            for warning in validation_results['warnings']:
                logger.warning(f"  - {warning}")
        
        if validation_results['errors']:
            logger.error("❌ 验证错误:")
            for error in validation_results['errors']:
                logger.error(f"  - {error}")
        
        logger.info("✅ 数据验证测试完成")
        return validation_results
        
    except Exception as e:
        logger.error(f"❌ 数据验证测试失败: {e}")
        raise


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Test Yahoo Fantasy ETL Load Layer')
    parser.add_argument('--recreate-tables', action='store_true', 
                       help='重新创建数据库表')
    parser.add_argument('--test-data', action='store_true',
                       help='使用测试数据进行完整测试')
    
    args = parser.parse_args()
    
    logger.info("🚀 开始Yahoo Fantasy ETL Load层测试")
    logger.info("=" * 60)
    
    try:
        # 1. 测试数据库连接
        if not test_database_connection():
            logger.error("数据库连接失败，请检查配置")
            return False
        
        # 2. 测试数据库表创建
        if not test_database_creation(recreate=args.recreate_tables):
            logger.error("数据库表创建失败")
            return False
        
        # 如果指定了测试数据，进行完整测试
        if args.test_data:
            # 3. 生成测试数据
            test_data = generate_test_data()
            
            # 4. 测试各个加载器
            loader_results = test_individual_loaders(test_data)
            
            # 5. 测试完整ETL管道
            etl_results = test_complete_etl_pipeline(test_data)
            
            # 6. 测试数据验证
            validation_results = test_data_validation()
            
            logger.info("🎉 所有测试完成!")
            
        else:
            logger.info("✅ 基础数据库测试完成")
            logger.info("💡 使用 --test-data 参数进行完整功能测试")
        
        logger.info("=" * 60)
        logger.info("🏁 Yahoo Fantasy ETL Load层测试结束")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试过程中出现异常: {e}")
        logger.exception("详细错误信息:")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 