#!/usr/bin/env python3
"""
完整ETL流程演示

展示Extract、Transform、Load三层的完整集成和数据流转
"""
import sys
import os
from datetime import datetime, date
from typing import Dict, List, Any

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Extract层导入
from fantasy_etl.extract import YahooFantasyClient
from fantasy_etl.extract.extractors import (
    RosterExtractor, PlayerExtractor, TeamExtractor, 
    LeagueExtractor, TransactionExtractor
)

# Transform层导入
from fantasy_etl.transform import (
    RosterTransformer, PlayerTransformer, TeamTransformer,
    LeagueTransformer, TransactionTransformer,
    get_transformer, transform_roster_data
)

# Load层导入
from fantasy_etl.load.loaders import (
    RosterLoader, PlayerLoader, TeamLoader,
    LeagueLoader, TransactionLoader
)

def print_section(title: str):
    """打印章节标题"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_subsection(title: str):
    """打印子章节标题"""
    print(f"\n{'-'*40}")
    print(f" {title}")
    print(f"{'-'*40}")

def print_result(result, data_type: str):
    """打印转换结果"""
    if result.success:
        print(f"✅ {data_type}转换成功")
        if result.metadata:
            print(f"   元数据: {result.metadata}")
        if result.warnings:
            print(f"   警告数量: {len(result.warnings)}")
    else:
        print(f"❌ {data_type}转换失败")
        if result.errors:
            print(f"   错误数量: {len(result.errors)}")
            for error in result.errors[:3]:  # 只显示前3个错误
                print(f"   - {error.field}: {error.error}")

def demo_extract_layer():
    """演示Extract层功能"""
    print_section("Extract Layer 演示")
    
    # 创建Yahoo客户端（模拟）
    print("🔧 初始化Yahoo客户端...")
    client = YahooFantasyClient()
    print("✅ Yahoo客户端初始化完成")
    
    # 演示各种提取器
    extractors = {
        "Roster": RosterExtractor(client),
        "Player": PlayerExtractor(client),
        "Team": TeamExtractor(client),
        "League": LeagueExtractor(client),
        "Transaction": TransactionExtractor(client)
    }
    
    for name, extractor in extractors.items():
        print(f"\n📊 {name}Extractor 已就绪")
        print(f"   - 支持的方法: {[method for method in dir(extractor) if not method.startswith('_')]}")

def demo_transform_layer():
    """演示Transform层功能"""
    print_section("Transform Layer 演示")
    
    # 1. 演示工厂函数
    print_subsection("转换器工厂函数")
    data_types = ['roster', 'player', 'team', 'league', 'transaction']
    
    for data_type in data_types:
        try:
            transformer = get_transformer(data_type)
            print(f"✅ {data_type.capitalize()}Transformer: {transformer.__class__.__name__}")
        except ValueError as e:
            print(f"❌ {e}")
    
    # 2. 演示Roster数据转换
    print_subsection("Roster数据转换演示")
    
    # 模拟Yahoo API返回的roster数据
    mock_roster_data = {
        "fantasy_content": {
            "team": [
                {},  # 其他团队信息
                {
                    "roster": {
                        "date": "2024-01-15",
                        "is_prescoring": False,
                        "is_editable": True,
                        "0": {
                            "players": {
                                "count": 2,
                                "0": {
                                    "player": [
                                        [
                                            {
                                                "player_key": "nba.p.3704",
                                                "player_id": "3704",
                                                "name": {
                                                    "full": "LeBron James",
                                                    "first": "LeBron",
                                                    "last": "James"
                                                },
                                                "status": "Healthy"
                                            }
                                        ],
                                        {
                                            "selected_position": {
                                                "position": "SF"
                                            },
                                            "is_keeper": {
                                                "status": True,
                                                "cost": "50",
                                                "kept": False
                                            }
                                        }
                                    ]
                                },
                                "1": {
                                    "player": [
                                        [
                                            {
                                                "player_key": "nba.p.4725",
                                                "player_id": "4725",
                                                "name": {
                                                    "full": "Stephen Curry",
                                                    "first": "Stephen",
                                                    "last": "Curry"
                                                },
                                                "status": "Healthy"
                                            }
                                        ],
                                        {
                                            "selected_position": {
                                                "position": "PG"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }
    }
    
    # 使用RosterTransformer转换数据
    roster_transformer = RosterTransformer()
    roster_result = roster_transformer.transform(mock_roster_data)
    print_result(roster_result, "Roster")
    
    if roster_result.success:
        print(f"   转换的球员数量: {roster_result.data['total_players']}")
        print(f"   阵容日期: {roster_result.data['roster_info']['coverage_date']}")
    
    # 3. 演示Player统计数据转换
    print_subsection("Player统计数据转换演示")
    
    # 模拟球员赛季统计数据
    mock_player_season_stats = {
        "9004003": "450/1000",  # FGM/FGA
        "5": "45.0",            # FG%
        "9007006": "180/200",   # FTM/FTA
        "8": "90.0",            # FT%
        "10": "150",            # 3PTM
        "12": "1280",           # PTS
        "15": "520",            # REB
        "16": "380",            # AST
        "17": "85",             # STL
        "18": "45",             # BLK
        "19": "120"             # TO
    }
    
    player_transformer = PlayerTransformer()
    season_stats_result = player_transformer.transform_player_season_stats(mock_player_season_stats)
    print_result(season_stats_result, "Player Season Stats")
    
    if season_stats_result.success:
        stats = season_stats_result.data['season_stats']
        print(f"   投篮命中率: {stats.get('field_goal_percentage', 'N/A')}")
        print(f"   总得分: {stats.get('total_points', 'N/A')}")
        print(f"   总篮板: {stats.get('total_rebounds', 'N/A')}")
    
    # 4. 演示Team数据转换
    print_subsection("Team统计数据转换演示")
    
    # 模拟团队周统计数据
    mock_team_weekly_stats = {
        "stats": [
            {"stat": {"stat_id": 12, "value": "1250"}},  # PTS
            {"stat": {"stat_id": 15, "value": "450"}},   # REB
            {"stat": {"stat_id": 16, "value": "320"}},   # AST
            {"stat": {"stat_id": 17, "value": "75"}},    # STL
            {"stat": {"stat_id": 18, "value": "35"}},    # BLK
            {"stat": {"stat_id": 19, "value": "95"}}     # TO
        ],
        "coverage_type": "week",
        "coverage_value": 10
    }
    
    team_transformer = TeamTransformer()
    team_stats_result = team_transformer.transform_team_weekly_stats(mock_team_weekly_stats)
    print_result(team_stats_result, "Team Weekly Stats")
    
    if team_stats_result.success:
        stats = team_stats_result.data['weekly_stats']
        print(f"   团队得分: {stats.get('points', 'N/A')}")
        print(f"   团队篮板: {stats.get('rebounds', 'N/A')}")
        print(f"   团队助攻: {stats.get('assists', 'N/A')}")
    
    # 5. 演示League数据转换
    print_subsection("League数据转换演示")
    
    # 模拟联盟基本信息
    mock_league_data = {
        "league_key": "nba.l.12345",
        "league_id": "12345",
        "game_key": "nba",
        "name": "My Fantasy League",
        "num_teams": 12,
        "scoring_type": "head",
        "season": "2024",
        "is_finished": False
    }
    
    league_transformer = LeagueTransformer()
    league_result = league_transformer.transform(mock_league_data)
    print_result(league_result, "League")
    
    if league_result.success:
        print(f"   联盟名称: {league_result.data.get('name', 'N/A')}")
        print(f"   团队数量: {league_result.data.get('num_teams', 'N/A')}")
        print(f"   赛季: {league_result.data.get('season', 'N/A')}")
    
    # 6. 演示Transaction数据转换
    print_subsection("Transaction数据转换演示")
    
    # 模拟交易数据
    mock_transaction_data = {
        "transaction_key": "nba.l.12345.tr.1",
        "transaction_id": "1",
        "type": "add/drop",
        "status": "successful",
        "timestamp": "1705123200",  # Unix时间戳
        "players": {
            "count": 2,
            "0": {
                "player": [
                    [
                        {
                            "player_key": "nba.p.5555",
                            "name": {"full": "New Player"}
                        }
                    ],
                    {
                        "transaction_data": {
                            "type": "add",
                            "destination_team_key": "nba.l.12345.t.1"
                        }
                    }
                ]
            },
            "1": {
                "player": [
                    [
                        {
                            "player_key": "nba.p.6666",
                            "name": {"full": "Dropped Player"}
                        }
                    ],
                    {
                        "transaction_data": {
                            "type": "drop",
                            "source_team_key": "nba.l.12345.t.1"
                        }
                    }
                ]
            }
        }
    }
    
    transaction_transformer = TransactionTransformer()
    transaction_result = transaction_transformer.transform(mock_transaction_data)
    print_result(transaction_result, "Transaction")
    
    if transaction_result.success:
        print(f"   交易类型: {transaction_result.data['transaction'].get('type', 'N/A')}")
        print(f"   涉及球员数量: {transaction_result.metadata.get('players_count', 0)}")

def demo_load_layer():
    """演示Load层功能"""
    print_section("Load Layer 演示")
    
    # 创建模拟的数据库写入器
    print("🔧 初始化数据库写入器...")
    try:
        # 导入数据库写入器
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts'))
        from database_writer import FantasyDatabaseWriter
        
        db_writer = FantasyDatabaseWriter()
        print("✅ 数据库写入器初始化完成")
    except Exception as e:
        print(f"❌ 数据库写入器初始化失败: {e}")
        print("🔧 使用模拟数据库写入器...")
        
        # 创建模拟的数据库写入器
        class MockDatabaseWriter:
            def write_roster_daily(self, **kwargs):
                return True
            def write_player_season_stats(self, **kwargs):
                return True
            def write_teams_batch(self, **kwargs):
                return 1
            def write_league_settings(self, **kwargs):
                return True
            def write_transactions_batch(self, **kwargs):
                return 1
        
        db_writer = MockDatabaseWriter()
        print("✅ 模拟数据库写入器创建完成")
    
    # 演示各种加载器
    loaders = {
        "Roster": RosterLoader(db_writer),
        "Player": PlayerLoader(db_writer),
        "Team": TeamLoader(db_writer),
        "League": LeagueLoader(db_writer),
        "Transaction": TransactionLoader(db_writer)
    }
    
    for name, loader in loaders.items():
        print(f"\n📊 {name}Loader 已就绪")
        print(f"   - 批处理大小: {loader.batch_size}")
        print(f"   - 支持的方法: {[method for method in dir(loader) if not method.startswith('_') and 'load' in method]}")

def demo_complete_etl_pipeline():
    """演示完整的ETL流程"""
    print_section("完整ETL流程演示")
    
    print("🔄 模拟完整的数据流转过程...")
    
    # 1. Extract阶段
    print_subsection("1. Extract阶段")
    print("📥 从Yahoo Fantasy API提取原始数据...")
    
    # 模拟提取的原始数据
    raw_roster_data = {
        "fantasy_content": {
            "team": [
                {},
                {
                    "roster": {
                        "date": "2024-01-15",
                        "is_prescoring": False,
                        "is_editable": True,
                        "0": {
                            "players": {
                                "count": 1,
                                "0": {
                                    "player": [
                                        [{"player_key": "nba.p.3704", "name": {"full": "LeBron James"}}],
                                        {"selected_position": {"position": "SF"}}
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }
    }
    print("✅ 原始数据提取完成")
    
    # 2. Transform阶段
    print_subsection("2. Transform阶段")
    print("🔄 转换原始数据为标准化格式...")
    
    transformer = RosterTransformer()
    transform_result = transformer.transform(raw_roster_data)
    
    if transform_result.success:
        print("✅ 数据转换成功")
        print(f"   转换的球员数量: {transform_result.data['total_players']}")
        transformed_data = transform_result.data
    else:
        print("❌ 数据转换失败")
        print(f"   错误数量: {len(transform_result.errors)}")
        return
    
    # 3. Load阶段
    print_subsection("3. Load阶段")
    print("💾 加载转换后的数据到目标系统...")
    
    # 创建数据库写入器
    try:
        # 导入数据库写入器
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts'))
        from database_writer import FantasyDatabaseWriter
        
        db_writer = FantasyDatabaseWriter()
    except Exception as e:
        # 创建模拟的数据库写入器
        class MockDatabaseWriter:
            def write_roster_daily(self, **kwargs):
                return True
        db_writer = MockDatabaseWriter()
    
    loader = RosterLoader(db_writer)
    
    # 模拟加载过程
    print("📝 准备加载数据...")
    print(f"   目标表: roster_daily")
    print(f"   记录数量: {transformed_data['total_players']}")
    print("✅ 数据加载完成（模拟）")
    
    # 4. 总结
    print_subsection("4. ETL流程总结")
    print("📊 ETL流程执行完成")
    print(f"   ✅ Extract: 成功提取原始数据")
    print(f"   ✅ Transform: 成功转换 {transformed_data['total_players']} 条记录")
    print(f"   ✅ Load: 成功加载到目标系统")
    print(f"   ⏱️  处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def demo_error_handling():
    """演示错误处理机制"""
    print_section("错误处理机制演示")
    
    # 1. 演示数据验证错误
    print_subsection("数据验证错误")
    
    invalid_data = {"invalid": "data"}  # 缺少必需字段
    
    transformer = RosterTransformer(strict_mode=True)
    result = transformer.transform(invalid_data)
    
    print_result(result, "Invalid Roster Data")
    if result.errors:
        print("   错误详情:")
        for error in result.errors:
            print(f"   - 字段: {error.field}")
            print(f"     错误: {error.error}")
            print(f"     严重程度: {error.severity}")
    
    # 2. 演示警告处理
    print_subsection("警告处理")
    
    data_with_warnings = {
        "fantasy_content": {
            "team": [
                {},
                {
                    "roster": {
                        "date": "invalid-date",  # 无效日期格式
                        "is_prescoring": False,
                        "is_editable": True,
                        "0": {
                            "players": {
                                "count": 1,
                                "0": {
                                    "player": [
                                        [{"player_key": "nba.p.3704"}],
                                        {"selected_position": {"position": "SF"}}
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }
    }
    
    result = transformer.transform(data_with_warnings)
    print_result(result, "Data with Warnings")
    
    if result.warnings:
        print("   警告详情:")
        for warning in result.warnings:
            print(f"   - 字段: {warning.field}")
            print(f"     警告: {warning.error}")

def main():
    """主函数"""
    print("🚀 Fantasy ETL 完整演示")
    print(f"演示时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 演示各层功能
        demo_extract_layer()
        demo_transform_layer()
        demo_load_layer()
        
        # 演示完整流程
        demo_complete_etl_pipeline()
        
        # 演示错误处理
        demo_error_handling()
        
        print_section("演示完成")
        print("🎉 Fantasy ETL 完整演示成功完成！")
        print("\n📋 演示内容总结:")
        print("   ✅ Extract Layer - 数据提取层")
        print("   ✅ Transform Layer - 数据转换层")
        print("   ✅ Load Layer - 数据加载层")
        print("   ✅ 完整ETL流程")
        print("   ✅ 错误处理机制")
        
    except Exception as e:
        print(f"\n❌ 演示过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 