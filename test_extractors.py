#!/usr/bin/env python3
"""
提取器稳定性测试脚本
==================

测试已实现的提取器功能，确保它们稳定可用
"""

import sys
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from fantasy_etl.config.settings import Settings
from fantasy_etl.auth.oauth_manager import OAuthManager
from fantasy_etl.auth.token_storage import FileTokenStorage
from fantasy_etl.extract.yahoo_client import YahooAPIClient
from fantasy_etl.extract.rate_limiter import RateLimiter

# 导入提取器
from fantasy_etl.extract.extractors.game_extractor import GameExtractor
from fantasy_etl.extract.extractors.league_extractor import LeagueExtractor
from fantasy_etl.extract.extractors.team_extractor import TeamExtractor
from fantasy_etl.extract.extractors.player_extractor import PlayerExtractor
from fantasy_etl.extract.extractors.settings_extractor import SettingsExtractor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExtractorTester:
    """提取器测试器"""
    
    def __init__(self):
        """初始化测试器"""
        self.settings = Settings()
        self.token_storage = FileTokenStorage(self.settings.get_token_file_path())
        self.oauth_manager = OAuthManager(
            client_id=self.settings.get_api_config().yahoo_client_id,
            client_secret=self.settings.get_api_config().yahoo_client_secret,
            token_storage=self.token_storage
        )
        self.rate_limiter = RateLimiter(requests_per_second=1.0)
        self.api_client = YahooAPIClient(
            oauth_manager=self.oauth_manager,
            rate_limiter=self.rate_limiter
        )
        
        # 初始化提取器
        self.game_extractor = GameExtractor(self.api_client)
        self.league_extractor = LeagueExtractor(self.api_client)
        self.team_extractor = TeamExtractor(self.api_client)
        self.player_extractor = PlayerExtractor(self.api_client)
        self.settings_extractor = SettingsExtractor(self.api_client)
        
        # 测试数据
        self.test_data = {}
    
    def test_game_extractor(self) -> bool:
        """测试游戏提取器"""
        try:
            logger.info("=== 测试游戏提取器 ===")
            
            result = self.game_extractor.extract()
            
            if result.success:
                logger.info(f"✅ 游戏提取器测试通过 - 提取到 {result.total_count} 个游戏")
                if result.data:
                    first_game = result.data[0]
                    logger.info(f"首个游戏: {first_game.game_key} - {first_game.name}")
                    self.test_data['game_keys'] = [game.game_key for game in result.data[:3]]  # 保存前3个游戏键用于后续测试
                return True
            else:
                logger.error(f"❌ 游戏提取器测试失败: {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 游戏提取器测试异常: {e}")
            return False
    
    def test_league_extractor(self) -> bool:
        """测试联盟提取器"""
        try:
            logger.info("=== 测试联盟提取器 ===")
            
            # 使用测试游戏键
            game_keys = self.test_data.get('game_keys', None)
            result = self.league_extractor.extract(game_keys=game_keys)
            
            if result.success:
                logger.info(f"✅ 联盟提取器测试通过 - 提取到 {result.total_count} 个联盟")
                if result.data:
                    first_league = result.data[0]
                    logger.info(f"首个联盟: {first_league.league_key} - {first_league.name}")
                    self.test_data['league_keys'] = [league.league_key for league in result.data[:2]]  # 保存前2个联盟键
                return True
            else:
                logger.error(f"❌ 联盟提取器测试失败: {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 联盟提取器测试异常: {e}")
            return False
    
    def test_team_extractor(self) -> bool:
        """测试团队提取器"""
        try:
            logger.info("=== 测试团队提取器 ===")
            
            league_keys = self.test_data.get('league_keys', [])
            if not league_keys:
                logger.warning("⚠️ 没有可用的联盟键，跳过团队提取器测试")
                return True
            
            # 测试第一个联盟
            league_key = league_keys[0]
            result = self.team_extractor.extract(league_key=league_key)
            
            if result.success:
                logger.info(f"✅ 团队提取器测试通过 - 联盟 {league_key} 提取到 {result.total_count} 个团队")
                if result.data:
                    first_team = result.data[0]
                    logger.info(f"首个团队: {first_team.team_key} - {first_team.name}")
                    self.test_data['team_keys'] = [team.team_key for team in result.data[:2]]  # 保存前2个团队键
                return True
            else:
                logger.error(f"❌ 团队提取器测试失败: {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 团队提取器测试异常: {e}")
            return False
    
    def test_settings_extractor(self) -> bool:
        """测试联盟设置提取器"""
        try:
            logger.info("=== 测试联盟设置提取器 ===")
            
            league_keys = self.test_data.get('league_keys', [])
            if not league_keys:
                logger.warning("⚠️ 没有可用的联盟键，跳过联盟设置提取器测试")
                return True
            
            # 测试第一个联盟
            league_key = league_keys[0]
            result = self.settings_extractor.extract(league_key=league_key)
            
            if result.success:
                logger.info(f"✅ 联盟设置提取器测试通过 - 联盟 {league_key}")
                if result.data:
                    settings = result.data[0]
                    logger.info(f"联盟设置: 草稿类型={settings.draft_type}, 季后赛={settings.uses_playoff}")
                return True
            else:
                logger.error(f"❌ 联盟设置提取器测试失败: {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 联盟设置提取器测试异常: {e}")
            return False
    
    def test_player_extractor(self) -> bool:
        """测试球员提取器（限制数据量）"""
        try:
            logger.info("=== 测试球员提取器 ===")
            
            league_keys = self.test_data.get('league_keys', [])
            if not league_keys:
                logger.warning("⚠️ 没有可用的联盟键，跳过球员提取器测试")
                return True
            
            # 测试第一个联盟，但由于球员数据量大，只做基本功能测试
            league_key = league_keys[0]
            
            # 暂时修改分页大小以限制测试数据量
            original_page_size = self.player_extractor.page_size
            self.player_extractor.page_size = 5  # 只获取5个球员进行测试
            
            result = self.player_extractor.extract(league_key=league_key)
            
            # 恢复原始分页大小
            self.player_extractor.page_size = original_page_size
            
            if result.success:
                logger.info(f"✅ 球员提取器测试通过 - 联盟 {league_key} 测试提取到 {result.total_count} 个球员")
                if result.data:
                    first_player = result.data[0]
                    logger.info(f"首个球员: {first_player.player_key} - {first_player.full_name}")
                return True
            else:
                logger.error(f"❌ 球员提取器测试失败: {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 球员提取器测试异常: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """运行所有测试"""
        logger.info("开始提取器稳定性测试...")
        
        tests = [
            ("游戏提取器", self.test_game_extractor),
            ("联盟提取器", self.test_league_extractor),
            ("团队提取器", self.test_team_extractor),
            ("联盟设置提取器", self.test_settings_extractor),
            ("球员提取器", self.test_player_extractor),
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            logger.info(f"\n--- 开始测试: {test_name} ---")
            if test_func():
                passed += 1
            else:
                logger.error(f"测试失败: {test_name}")
        
        logger.info(f"\n=== 测试结果汇总 ===")
        logger.info(f"通过: {passed}/{total}")
        
        if passed == total:
            logger.info("🎉 所有提取器测试通过！")
            return True
        else:
            logger.error(f"❌ {total - passed} 个提取器测试失败")
            return False


def main():
    """主函数"""
    try:
        tester = ExtractorTester()
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"测试运行异常: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 