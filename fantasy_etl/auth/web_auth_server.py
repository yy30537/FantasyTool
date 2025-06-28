"""
Web授权服务器 (Web Authentication Server)
=======================================

专门的Web OAuth授权服务，提供完整的Yahoo OAuth2.0 Web授权流程。

【主要职责】
1. Flask Web服务器OAuth流程
2. Yahoo OAuth2.0授权码流程
3. 用户交互界面
4. 授权回调处理

【路由支持】
- GET /: 主页面，显示状态和操作链接
- GET /auth: 启动OAuth授权流程
- GET /auth/callback: 处理Yahoo回调
- GET /success: 显示授权成功页面
- GET /fetch: 测试API访问
- GET /config_check: 检查配置状态
- GET /logout: 清除会话
- GET /run_time_series: 显示时间序列命令帮助

【功能特性】
- Flask会话管理
- OAuth状态验证
- 令牌自动刷新
- 错误处理和用户反馈
- SSL/HTTPS支持
"""

from flask import Flask, redirect, request, session
import requests
from requests_oauthlib import OAuth2Session
import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# 导入认证模块
from .oauth_manager import OAuthManager, CLIENT_ID, CLIENT_SECRET, TOKEN_URL
from .token_storage import TokenStorage

# 加载环境变量
load_dotenv()

# OAuth配置
client_id = CLIENT_ID
client_secret = CLIENT_SECRET
authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
token_url = TOKEN_URL
redirect_uri = os.getenv("REDIRECT_URI", "oob")
scope = ["fspt-w"]  # Fantasy Sports读写权限


class WebAuthServer:
    """Web授权服务器"""
    
    def __init__(self, oauth_manager: Optional[OAuthManager] = None):
        """
        初始化Web授权服务器
        
        Args:
            oauth_manager: OAuth管理器实例
        """
        self.oauth_manager = oauth_manager or OAuthManager()
        self.app = None
        self.setup_app()
    
    def setup_app(self) -> Flask:
        """设置Flask应用"""
        app = Flask(__name__)
        app.secret_key = os.urandom(24)
        
        # 注册路由
        self.register_routes(app)
        self.app = app
        return app
    
    def register_routes(self, app: Flask) -> None:
        """注册所有路由"""
        
        @app.route('/')
        def index():
            return self.show_index()
        
        @app.route('/config_check')
        def config_check():
            return self.show_config_check()
        
        @app.route('/auth')
        def auth():
            return self.initiate_auth()
        
        @app.route('/auth/callback')
        def callback():
            return self.handle_callback()
        
        @app.route('/success')
        def success():
            return self.show_success()
        
        @app.route('/fetch')
        def fetch():
            return self.test_api()
        
        @app.route('/run_time_series')
        def run_time_series():
            return self.show_time_series_help()
        
        @app.route('/logout')
        def logout():
            return self.handle_logout()
    
    def get_oauth_session(self) -> OAuth2Session:
        """创建OAuth2Session"""
        return OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
    
    def refresh_token_if_expired(self) -> bool:
        """检查并刷新令牌（如果已过期）"""
        if 'oauth_token' not in session:
            token_storage = TokenStorage()
            token = token_storage.load_token()
            if token:
                session['oauth_token'] = token
            else:
                return False
        
        token = session['oauth_token']
        # 检查令牌是否过期
        now = datetime.now().timestamp()
        expires_at = token.get('expires_at', 0)
        
        # 如果令牌已过期或即将过期（提前60秒刷新）
        if now >= (expires_at - 60):
            try:
                refresh_token = token.get('refresh_token')
                
                data = {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'refresh_token': refresh_token,
                    'grant_type': 'refresh_token'
                }
                
                response = requests.post(token_url, data=data)
                
                if response.status_code == 200:
                    new_token = response.json()
                    # 设置过期时间
                    expires_in = new_token.get('expires_in', 3600)
                    new_token['expires_at'] = now + int(expires_in)
                    # 保留refresh_token（如果新令牌中没有）
                    if 'refresh_token' not in new_token and refresh_token:
                        new_token['refresh_token'] = refresh_token
                    
                    # 更新会话中的令牌
                    session['oauth_token'] = new_token
                    # 保存令牌到文件
                    token_storage = TokenStorage()
                    token_storage.save_token(new_token)
                    
                    return True
                else:
                    return False
            except Exception as e:
                return False
        
        return True
    
    def show_index(self) -> str:
        """显示主页"""
        # 检查配置状态
        config_status = "✅ 配置完整" if client_id and client_secret else "❌ 配置不完整"
        token_storage = TokenStorage()
        token_status = "✅ 有令牌" if token_storage.load_token() else "❌ 无令牌"
        
        return f"""
        <h1>Yahoo Fantasy工具</h1>
        <p>欢迎使用Yahoo Fantasy工具！</p>
        
        <h2>状态检查</h2>
        <p>OAuth配置: {config_status}</p>
        <p>访问令牌: {token_status}</p>
        
        <h2>操作</h2>
        <a href="/auth">使用Yahoo帐号登录</a><br/>
        <a href="/config_check">检查配置</a><br/>
        <a href="/fetch">获取数据（需要先登录）</a>
        
        <h2>帮助</h2>
        <p>如果遇到问题，请查看配置文档</p>
        """
    
    def show_config_check(self) -> str:
        """显示配置检查页面"""
        config_info = {
            "CLIENT_ID": client_id[:10] + "..." if client_id else "未设置",
            "CLIENT_SECRET": "已设置" if client_secret else "未设置", 
            "REDIRECT_URI": redirect_uri,
            "SCOPE": scope
        }
        
        return f"""
        <h1>OAuth配置检查</h1>
        <pre>{json.dumps(config_info, indent=2, ensure_ascii=False)}</pre>
        <p><a href="/">返回首页</a></p>
        """
    
    def initiate_auth(self):
        """启动OAuth流程"""
        if not client_id or not client_secret:
            return """
            <h1>配置错误</h1>
            <p>❌ 缺少OAuth配置</p>
            <p>请设置以下环境变量：</p>
            <ul>
                <li>CLIENT_ID</li>
                <li>CLIENT_SECRET</li>
                <li>REDIRECT_URI (可选，默认为oob)</li>
            </ul>
            <p><a href="/">返回首页</a></p>
            """, 400
        
        try:
            oauth = self.get_oauth_session()
            authorization_url, state = oauth.authorization_url(authorization_base_url)
            session['oauth_state'] = state
            
            return redirect(authorization_url)
        except Exception as e:
            return f"创建授权URL失败: {str(e)}", 400
    
    def handle_callback(self):
        """OAuth回调处理"""
        try:
            oauth = self.get_oauth_session()
            token = oauth.fetch_token(
                token_url,
                client_secret=client_secret,
                authorization_response=request.url
            )
            
            # 设置令牌过期时间
            expires_in = token.get('expires_in', 3600)
            token['expires_at'] = time.time() + int(expires_in)
            
            # 保存令牌到会话
            session['oauth_token'] = token
            
            # 保存令牌到文件
            token_storage = TokenStorage()
            token_storage.save_token(token)
            
            # 创建简单的用户信息对象
            user_info = {
                "token_obtained": True,
                "token_obtained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "access_token_expires_in": expires_in
            }
            
            session['user_info'] = user_info
            
            return redirect('/success')
        
        except Exception as e:
            return f"""
            <h1>OAuth认证失败</h1>
            <p>错误: {str(e)}</p>
            <p>请求URL: {request.url}</p>
            <p><a href="/">返回首页重试</a></p>
            """, 400
    
    def show_success(self) -> str:
        """显示授权成功页面"""
        if 'oauth_token' not in session:
            return redirect('/')
        
        user_info = session.get('user_info', {})
        return f"""
        <h1>授权成功</h1>
        <p>✅ 您已成功授权Yahoo Fantasy工具访问您的Fantasy Sports数据。</p>
        <h2>授权信息</h2>
        <pre>{json.dumps(user_info, indent=2, ensure_ascii=False)}</pre>
        
        <h2>下一步操作</h2>
        <a href='/fetch'>获取数据</a><br/>
        <a href='/run_time_series'>运行时间序列数据获取</a><br/>
        <a href='/logout'>退出登录</a>
        """
    
    def test_api(self):
        """测试API访问"""
        if 'oauth_token' not in session:
            token_storage = TokenStorage()
            token = token_storage.load_token()
            if token:
                session['oauth_token'] = token
            else:
                return redirect('/')
        
        # 刷新令牌（如果需要）
        if not self.refresh_token_if_expired():
            return redirect('/')
        
        token = session['oauth_token']
        headers = {'Authorization': f"Bearer {token['access_token']}"}
        
        # 获取用户的games数据
        url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                return f"""
                <h1>Fantasy数据</h1>
                <p>✅ 数据获取成功</p>
                <pre>{json.dumps(data, indent=2, ensure_ascii=False)}</pre>
                <p><a href="/">返回首页</a></p>
                """
            else:
                return f"""
                <h1>数据获取失败</h1>
                <p>状态码: {response.status_code}</p>
                <p>错误信息: {response.text}</p>
                <p><a href="/">返回首页</a></p>
                """, 400
        except Exception as e:
            return f"API请求异常: {str(e)}", 400
    
    def show_time_series_help(self) -> str:
        """显示时间序列帮助"""
        if 'oauth_token' not in session:
            return redirect('/')
        
        return """
        <h1>时间序列数据获取</h1>
        <p>✅ OAuth认证已完成，可以使用以下命令行工具：</p>
        
        <h2>命令示例：</h2>
        <pre>
# 基础历史数据获取
python time_series_fetcher.py --historical --weeks-back=3

# 自定义参数
python time_series_fetcher.py --historical --weeks-back=5 --delay=1
        </pre>
        
        <p><strong>注意</strong>: 请在终端中运行这些命令</p>
        <p><a href="/">返回首页</a></p>
        """
    
    def handle_logout(self):
        """处理退出登录"""
        session.clear()
        return redirect('/')
    
    def start(self, host: str = 'localhost', port: int = 8000, 
             debug: bool = True, ssl_context: str = 'adhoc') -> None:
        """
        启动Web服务器
        
        Args:
            host: 服务器主机地址
            port: 服务器端口
            debug: 是否启用调试模式
            ssl_context: SSL上下文配置
        """
        try:
            print(f"\n🚀 启动Yahoo Fantasy工具...")
            print(f"📍 访问地址: https://{host}:{port}")
            self.app.run(host=host, port=port, debug=debug, ssl_context=ssl_context) 
        except Exception as e:
            if "cryptography" in str(e).lower():
                print("⚠️  警告: 缺少cryptography库，将使用HTTP模式运行...")
                print(f"📍 HTTP访问地址: http://{host}:{port}")
                self.app.run(host=host, port=port, debug=debug) 
            else:
                print(f"❌ 启动失败: {str(e)}")
                raise


class AuthRouteHandler:
    """授权路由处理器"""
    
    def __init__(self, oauth_manager: OAuthManager):
        self.oauth_manager = oauth_manager
    
    def handle_auth_request(self) -> Dict[str, Any]:
        """处理认证请求"""
        # TODO: 实现更高级的认证处理逻辑
        pass
    
    def handle_api_test(self) -> Dict[str, Any]:
        """处理API测试请求"""
        # TODO: 实现API测试逻辑
        pass


class OAuthFlowManager:
    """OAuth流程管理器"""
    
    def __init__(self, oauth_manager: OAuthManager):
        self.oauth_manager = oauth_manager
        self.state_store = {}  # 临时状态存储
    
    def generate_auth_url(self) -> tuple[str, str]:
        """生成授权URL和状态码"""
        # TODO: 实现授权URL生成逻辑
        pass
    
    def handle_callback(self, callback_url: str, state: str) -> Optional[Dict[str, Any]]:
        """处理OAuth回调"""
        # TODO: 实现回调处理逻辑
    pass 