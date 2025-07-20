#!/usr/bin/env python3
"""
Fantasy Tool 主入口点
支持主应用和认证服务器启动
"""
import sys
import argparse
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fantasy_etl.api import main as run_main_app
from fantasy_etl.auth import WebAuthServer
from fantasy_etl.core.config import settings


def check_environment():
    """检查环境配置"""
    print("🔍 检查环境配置...")
    
    errors = settings.validate()
    if errors:
        print("❌ 配置错误:")
        for error in errors:
            print(f"  - {error}")
        print("\n💡 请检查 .env 文件或环境变量配置")
        return False
    
    print("✅ 环境配置检查通过")
    return True


def run_auth_server():
    """启动认证服务器"""
    print("🔐 Fantasy Tool OAuth认证服务器")
    print("=" * 40)
    
    # 检查Yahoo API配置
    if not settings.yahoo_api.is_valid:
        print("❌ Yahoo API配置无效")
        print("请设置以下环境变量:")
        print("  - YAHOO_CLIENT_ID")
        print("  - YAHOO_CLIENT_SECRET") 
        sys.exit(1)
    
    print(f"🌐 启动认证服务器: {settings.app.web_server_host}:{settings.app.web_server_port}")
    
    try:
        server = WebAuthServer(
            host=settings.app.web_server_host,
            port=settings.app.web_server_port
        )
        server.run(debug=settings.app.debug)
    except KeyboardInterrupt:
        print("\n👋 认证服务器已关闭")
    except Exception as e:
        print(f"\n❌ 服务器启动失败: {e}")
        sys.exit(1)


def run_main_application():
    """启动主应用"""
    print("🏀 Fantasy Tool - Yahoo Fantasy Sports分析工具")
    print("=" * 60)
    
    # 显示配置摘要
    settings.print_summary()
    
    # 检查环境
    if not check_environment():
        sys.exit(1)
    
    print("\n🚀 启动应用...")
    
    try:
        # 运行主程序
        run_main_app()
    except KeyboardInterrupt:
        print("\n👋 感谢使用 Fantasy Tool!")
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        sys.exit(1)


def main():
    """主入口点"""
    parser = argparse.ArgumentParser(
        description="Fantasy Tool - Yahoo Fantasy Sports分析工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
启动方式:
  python -m fantasy_etl              # 启动主应用
  python -m fantasy_etl auth-server  # 启动认证服务器
  python main.py                     # 启动主应用 (替代方式)
"""
    )
    
    parser.add_argument(
        'command', 
        nargs='?', 
        default='app',
        choices=['app', 'auth-server'],
        help='要启动的服务 (默认: app)'
    )
    
    args = parser.parse_args()
    
    if args.command == 'auth-server':
        run_auth_server()
    else:
        run_main_application()


if __name__ == "__main__":
    main()