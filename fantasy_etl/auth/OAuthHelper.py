"""
OAuth授权助手 - 独立的命令行OAuth授权工具

替代原来app.py的功能，提供命令行形式的OAuth授权流程
"""
import os
import webbrowser
from urllib.parse import urlencode, parse_qs, urlparse
from typing import Dict, Any, Optional
import requests
from dotenv import load_dotenv
import logging

from ..extract.yahoo_client import TokenManager

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)


class OAuthHelper:
    """OAuth授权助手 - 处理完整的OAuth授权流程"""
    
    def __init__(self):
        """初始化OAuth助手"""
        self.client_id = os.getenv("YAHOO_CLIENT_ID", "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk")
        self.client_secret = os.getenv("YAHOO_CLIENT_SECRET", "a5b3a6e1ff6a3e982036ec873a78f6fa46431508")
        self.redirect_uri = os.getenv("REDIRECT_URI", "oob")  # 使用out-of-band默认值
        self.scope = ["fspt-w"]  # Fantasy Sports读写权限
        
        # API端点
        self.authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
        self.token_url = "https://api.login.yahoo.com/oauth2/get_token"
        
        # 令牌管理器
        self.token_manager = TokenManager()
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self) -> None:
        """验证OAuth配置"""
        if not self.client_id or not self.client_secret:
            raise ValueError("缺少必要的OAuth配置：YAHOO_CLIENT_ID 和 YAHOO_CLIENT_SECRET")
        
        logger.info("🔍 OAuth配置检查:")
        logger.info(f"CLIENT_ID: {'✓设置' if self.client_id else '❌未设置'}")
        logger.info(f"CLIENT_SECRET: {'✓设置' if self.client_secret else '❌未设置'}")
        logger.info(f"REDIRECT_URI: {self.redirect_uri}")
    
    def get_authorization_url(self) -> str:
        """生成授权URL"""
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'scope': ' '.join(self.scope)
        }
        
        url = f"{self.authorization_base_url}?{urlencode(params)}"
        logger.info(f"生成授权URL: {url}")
        return url
    
    def start_oauth_flow(self, auto_open_browser: bool = True) -> str:
        """开始OAuth授权流程
        
        Args:
            auto_open_browser: 是否自动打开浏览器
            
        Returns:
            授权URL
        """
        print("\n🚀 开始Yahoo Fantasy OAuth授权流程...")
        
        # 检查是否已有有效令牌
        existing_token = self.token_manager.load_token()
        if existing_token and not self.token_manager.is_token_expired(existing_token):
            print("✅ 检测到有效的访问令牌，无需重新授权")
            return ""
        
        # 生成授权URL
        auth_url = self.get_authorization_url()
        
        print(f"\n📋 请按照以下步骤完成授权:")
        print(f"1. 访问以下URL (浏览器将自动打开):")
        print(f"   {auth_url}")
        print(f"2. 登录Yahoo账号并授权应用访问")
        print(f"3. 复制授权码并粘贴到下方")
        print()
        
        # 自动打开浏览器
        if auto_open_browser:
            try:
                webbrowser.open(auth_url)
                print("✓ 已在默认浏览器中打开授权页面")
            except Exception as e:
                print(f"⚠️ 无法自动打开浏览器: {e}")
                print("请手动复制上述URL到浏览器中打开")
        
        return auth_url
    
    def exchange_code_for_token(self, authorization_code: str) -> Optional[Dict[str, Any]]:
        """使用授权码换取访问令牌
        
        Args:
            authorization_code: OAuth授权码
            
        Returns:
            访问令牌信息，失败时返回None
        """
        try:
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'redirect_uri': self.redirect_uri,
                'code': authorization_code,
                'grant_type': 'authorization_code'
            }
            
            logger.info("正在用授权码换取访问令牌...")
            response = requests.post(self.token_url, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                
                # 设置过期时间
                import time
                expires_in = token_data.get('expires_in', 3600)
                token_data['expires_at'] = time.time() + int(expires_in)
                
                # 保存令牌
                if self.token_manager.save_token(token_data):
                    logger.info("✅ 访问令牌获取并保存成功")
                    return token_data
                else:
                    logger.error("❌ 令牌保存失败")
                    return None
            else:
                logger.error(f"❌ 令牌获取失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 令牌交换过程出错: {str(e)}")
            return None
    
    def complete_oauth_flow(self, auto_open_browser: bool = True) -> bool:
        """完成完整的OAuth授权流程
        
        Args:
            auto_open_browser: 是否自动打开浏览器
            
        Returns:
            授权是否成功
        """
        try:
            # 启动授权流程
            auth_url = self.start_oauth_flow(auto_open_browser)
            
            # 如果已有有效令牌，直接返回成功
            if not auth_url:
                return True
            
            # 等待用户输入授权码
            while True:
                try:
                    auth_code = input("\n请输入授权码 (输入 'q' 取消): ").strip()
                    
                    if auth_code.lower() == 'q':
                        print("❌ 用户取消授权")
                        return False
                    
                    if not auth_code:
                        print("❌ 授权码不能为空，请重新输入")
                        continue
                    
                    # 交换令牌
                    token = self.exchange_code_for_token(auth_code)
                    if token:
                        print("✅ OAuth授权完成！")
                        print(f"✓ 访问令牌已保存，有效期: {token.get('expires_in', 3600)} 秒")
                        return True
                    else:
                        print("❌ 授权码无效，请重新输入")
                        continue
                        
                except KeyboardInterrupt:
                    print("\n❌ 用户中断授权")
                    return False
                except Exception as e:
                    print(f"❌ 处理授权码时出错: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"❌ OAuth流程失败: {str(e)}")
            return False
    
    def check_token_status(self) -> Dict[str, Any]:
        """检查当前令牌状态
        
        Returns:
            令牌状态信息
        """
        token = self.token_manager.load_token()
        
        if not token:
            return {
                'has_token': False,
                'is_valid': False,
                'message': '未找到访问令牌'
            }
        
        is_expired = self.token_manager.is_token_expired(token)
        
        if is_expired:
            # 尝试刷新令牌
            refresh_token = token.get('refresh_token')
            if refresh_token:
                new_token = self.token_manager.refresh_token(refresh_token)
                if new_token:
                    return {
                        'has_token': True,
                        'is_valid': True,
                        'message': '令牌已刷新',
                        'expires_at': new_token.get('expires_at', 0)
                    }
                else:
                    return {
                        'has_token': True,
                        'is_valid': False,
                        'message': '令牌已过期且刷新失败'
                    }
            else:
                return {
                    'has_token': True,
                    'is_valid': False,
                    'message': '令牌已过期且无法刷新'
                }
        else:
            return {
                'has_token': True,
                'is_valid': True,
                'message': '令牌有效',
                'expires_at': token.get('expires_at', 0)
            }
    
    def revoke_token(self) -> bool:
        """撤销（删除）当前令牌"""
        try:
            token_file = self.token_manager.token_file
            if token_file.exists():
                token_file.unlink()
                logger.info("✅ 访问令牌已删除")
                return True
            else:
                logger.info("ℹ️ 未找到令牌文件")
                return True
        except Exception as e:
            logger.error(f"❌ 删除令牌失败: {str(e)}")
            return False


def main():
    """OAuth助手的命令行入口点"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Yahoo Fantasy OAuth授权助手")
    parser.add_argument('--auth', action='store_true', help='开始OAuth授权流程')
    parser.add_argument('--status', action='store_true', help='检查令牌状态')
    parser.add_argument('--revoke', action='store_true', help='撤销当前令牌')
    parser.add_argument('--no-browser', action='store_true', help='不自动打开浏览器')
    
    args = parser.parse_args()
    
    oauth_helper = OAuthHelper()
    
    if args.auth:
        success = oauth_helper.complete_oauth_flow(auto_open_browser=not args.no_browser)
        exit(0 if success else 1)
    
    elif args.status:
        status = oauth_helper.check_token_status()
        print(f"\n📊 令牌状态: {status['message']}")
        if status.get('expires_at'):
            import time
            from datetime import datetime
            expires_at = datetime.fromtimestamp(status['expires_at'])
            print(f"🕐 过期时间: {expires_at}")
        exit(0 if status['is_valid'] else 1)
    
    elif args.revoke:
        success = oauth_helper.revoke_token()
        exit(0 if success else 1)
    
    else:
        parser.print_help()
        exit(1)


if __name__ == "__main__":
    main()