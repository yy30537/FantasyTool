"""
OAuth认证管理器
重构自archive/app.py，提供Yahoo Fantasy Sports OAuth认证功能
"""
import os
import time
import json
import pickle
import pathlib
import requests
from datetime import datetime
from dotenv import load_dotenv
from requests_oauthlib import OAuth2Session
from typing import Optional, Dict, Any

# 加载环境变量
load_dotenv()

class OAuthManager:
    """Yahoo Fantasy Sports OAuth认证管理器"""
    
    def __init__(self):
        # 项目根目录 (确保路径正确)
        self.project_root = pathlib.Path(__file__).parent.parent.parent
        
        # OAuth配置
        self.client_id = os.getenv("YAHOO_CLIENT_ID")
        self.client_secret = os.getenv("YAHOO_CLIENT_SECRET")
        self.redirect_uri = os.getenv("YAHOO_REDIRECT_URI", "http://localhost:8000/auth/callback")
        self.authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
        self.token_url = "https://api.login.yahoo.com/oauth2/get_token"
        self.scope = ["fspt-w"]  # Fantasy Sports读写权限
        
        # 令牌文件路径 (使用项目根目录下的tokens)
        self.tokens_dir = self.project_root / "tokens"
        self.tokens_dir.mkdir(exist_ok=True)
        self.token_file = self.tokens_dir / "yahoo_token.token"
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self) -> None:
        """验证OAuth配置"""
        if not self.client_id or not self.client_secret:
            raise ValueError("缺少YAHOO_CLIENT_ID或YAHOO_CLIENT_SECRET环境变量")
        
        print(f"🔧 OAuth配置:")
        print(f"   Client ID: {self.client_id[:10]}...")
        print(f"   Redirect URI: {self.redirect_uri}")
        print(f"   Token路径: {self.token_file}")
    
    def get_oauth_session(self) -> OAuth2Session:
        """创建OAuth2Session"""
        return OAuth2Session(
            self.client_id, 
            redirect_uri=self.redirect_uri, 
            scope=self.scope
        )
    
    def get_authorization_url(self) -> tuple[str, str]:
        """获取授权URL"""
        oauth = self.get_oauth_session()
        authorization_url, state = oauth.authorization_url(self.authorization_base_url)
        print(f"🔗 授权URL: {authorization_url}")
        return authorization_url, state
    
    def fetch_token(self, authorization_response: str) -> Dict[str, Any]:
        """获取访问令牌"""
        try:
            oauth = self.get_oauth_session()
            
            print(f"🔄 处理OAuth回调: {authorization_response}")
            
            token = oauth.fetch_token(
                self.token_url,
                client_secret=self.client_secret,
                authorization_response=authorization_response
            )
            
            # 设置令牌过期时间
            expires_in = token.get('expires_in', 3600)
            token['expires_at'] = time.time() + int(expires_in)
            
            print(f"✅ 获取token成功，有效期: {expires_in}秒")
            
            # 保存令牌
            if self.save_token(token):
                print("✅ Token已保存到文件")
            else:
                print("⚠️ Token保存失败")
            
            return token
            
        except Exception as e:
            print(f"❌ 获取token失败: {str(e)}")
            raise
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌到文件"""
        try:
            # 确保目录存在
            self.tokens_dir.mkdir(exist_ok=True)
            
            with open(self.token_file, 'wb') as f:
                pickle.dump(token, f)
            
            print(f"💾 令牌已保存到: {self.token_file}")
            return True
            
        except Exception as e:
            print(f"❌ 保存令牌时出错: {str(e)}")
            return False
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """从文件加载令牌"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'rb') as f:
                    token = pickle.load(f)
                
                # 检查token基本有效性
                if 'access_token' in token:
                    print(f"📖 从 {self.token_file} 加载令牌成功")
                    return token
                else:
                    print("⚠️ 令牌文件格式无效")
                    return None
                    
            except Exception as e:
                print(f"❌ 加载令牌时出错: {str(e)}")
                return None
        else:
            print(f"📁 令牌文件不存在: {self.token_file}")
            return None
    
    def refresh_token_if_needed(self, token: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """检查并刷新令牌（如果已过期）"""
        if not token:
            token = self.load_token()
        
        if not token:
            print("❌ 无可用令牌")
            return None
        
        # 检查令牌是否过期
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        # 如果令牌已过期或即将过期（提前60秒刷新）
        if now >= (expires_at - 60):
            try:
                print("🔄 令牌即将过期，尝试刷新...")
                refresh_token = token.get('refresh_token')
                
                if not refresh_token:
                    print("❌ 缺少refresh_token，需要重新认证")
                    return None
                
                data = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': refresh_token,
                    'grant_type': 'refresh_token'
                }
                
                response = requests.post(self.token_url, data=data)
                
                if response.status_code == 200:
                    new_token = response.json()
                    # 设置过期时间
                    expires_in = new_token.get('expires_in', 3600)
                    new_token['expires_at'] = now + int(expires_in)
                    # 保留refresh_token（如果新令牌中没有）
                    if 'refresh_token' not in new_token and refresh_token:
                        new_token['refresh_token'] = refresh_token
                    
                    # 保存更新的令牌
                    self.save_token(new_token)
                    
                    print("✅ 令牌刷新成功")
                    return new_token
                else:
                    print(f"❌ 令牌刷新失败: {response.status_code} - {response.text}")
                    return None
                    
            except Exception as e:
                print(f"❌ 刷新令牌时出错: {str(e)}")
                return None
        else:
            remaining_time = expires_at - now
            print(f"✅ 令牌有效，剩余时间: {int(remaining_time)}秒")
            return token
    
    def get_access_token(self) -> Optional[str]:
        """获取有效的访问令牌"""
        token = self.refresh_token_if_needed()
        if token:
            return token.get('access_token')
        return None
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        access_token = self.get_access_token()
        is_auth = access_token is not None
        print(f"🔐 认证状态: {'✅ 已认证' if is_auth else '❌ 未认证'}")
        return is_auth
    
    def clear_token(self) -> bool:
        """清除保存的令牌"""
        try:
            if self.token_file.exists():
                self.token_file.unlink()
                print(f"🗑️ 已删除令牌文件: {self.token_file}")
            return True
        except Exception as e:
            print(f"❌ 删除令牌文件时出错: {str(e)}")
            return False