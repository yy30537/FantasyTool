"""
Web认证服务器
基于Flask的OAuth认证服务器，重构自archive/app.py
"""
import os
import json
import time
from flask import Flask, redirect, request, session, render_template_string
from typing import Optional

from .oauth_manager import OAuthManager

class WebAuthServer:
    """Web认证服务器"""
    
    def __init__(self, host: str = 'localhost', port: int = 8000):
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self.app.secret_key = os.urandom(24)
        
        try:
            self.oauth_manager = OAuthManager()
        except ValueError as e:
            print(f"❌ OAuth配置错误: {e}")
            self.oauth_manager = None
        
        # 注册路由
        self._register_routes()
    
    def _register_routes(self):
        """注册所有路由"""
        
        @self.app.route('/')
        def index():
            """主页"""
            if not self.oauth_manager:
                return self._render_error_page(
                    "配置错误",
                    "缺少Yahoo API配置",
                    ["YAHOO_CLIENT_ID", "YAHOO_CLIENT_SECRET"]
                )
            
            # 检查配置状态
            config_status = "✅ 配置完整" if (self.oauth_manager.client_id and self.oauth_manager.client_secret) else "❌ 配置不完整"
            
            # 检查令牌状态
            token = self.oauth_manager.load_token()
            if token:
                # 检查令牌是否有效
                is_valid = self.oauth_manager.is_authenticated()
                token_status = "✅ 有效令牌" if is_valid else "⚠️ 令牌已过期"
            else:
                token_status = "❌ 无令牌"
            
            return self._render_index_page(config_status, token_status)
        
        @self.app.route('/config_check')
        def config_check():
            """配置检查页面"""
            if not self.oauth_manager:
                return self._render_error_page("配置错误", "OAuth管理器初始化失败")
            
            config_info = {
                "CLIENT_ID": f"{self.oauth_manager.client_id[:10]}..." if self.oauth_manager.client_id else "未设置",
                "CLIENT_SECRET": "已设置" if self.oauth_manager.client_secret else "未设置", 
                "REDIRECT_URI": self.oauth_manager.redirect_uri,
                "SCOPE": self.oauth_manager.scope,
                "TOKEN_PATH": str(self.oauth_manager.token_file)
            }
            
            return self._render_config_page(config_info)
        
        @self.app.route('/auth')
        def auth():
            """启动OAuth流程"""
            if not self.oauth_manager:
                return redirect('/')
            
            try:
                authorization_url, state = self.oauth_manager.get_authorization_url()
                session['oauth_state'] = state
                
                print(f"🔗 重定向到Yahoo授权页面")
                return redirect(authorization_url)
                
            except Exception as e:
                self.app.logger.error(f"创建授权URL失败: {str(e)}")
                return self._render_error_page(
                    "认证错误",
                    f"创建授权URL失败: {str(e)}"
                )
        
        @self.app.route('/auth/callback')
        def callback():
            """OAuth回调处理"""
            if not self.oauth_manager:
                return redirect('/')
            
            try:
                print(f"📥 收到OAuth回调")
                self.app.logger.info(f"回调URL: {request.url}")
                
                # 检查是否有错误参数
                if 'error' in request.args:
                    error = request.args.get('error')
                    error_description = request.args.get('error_description', '')
                    return self._render_error_page(
                        "授权失败",
                        f"Yahoo拒绝了授权请求: {error}",
                        [error_description] if error_description else []
                    )
                
                # 获取授权码
                if 'code' not in request.args:
                    return self._render_error_page(
                        "授权失败",
                        "未收到授权码",
                        ["请重新尝试授权流程"]
                    )
                
                # 获取访问令牌
                token = self.oauth_manager.fetch_token(request.url)
                
                # 保存令牌到会话
                session['oauth_token'] = token
                
                # 创建用户信息
                user_info = {
                    "token_obtained": True,
                    "token_obtained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "access_token_expires_in": token.get('expires_in', 3600),
                    "has_refresh_token": 'refresh_token' in token
                }
                
                session['user_info'] = user_info
                
                print("✅ OAuth认证成功完成")
                return redirect('/success')
            
            except Exception as e:
                self.app.logger.error(f"OAuth回调处理失败: {str(e)}")
                return self._render_error_page(
                    "认证失败",
                    f"处理授权回调时出错: {str(e)}",
                    ["请重新尝试授权流程", f"回调URL: {request.url}"]
                )
        
        @self.app.route('/success')
        def success():
            """授权成功页面"""
            if 'oauth_token' not in session:
                return redirect('/')
            
            user_info = session.get('user_info', {})
            return self._render_success_page(user_info)
        
        @self.app.route('/test_api')
        def test_api():
            """测试API访问"""
            if not self.oauth_manager:
                return redirect('/')
            
            access_token = self.oauth_manager.get_access_token()
            if not access_token:
                return self._render_error_page(
                    "测试失败",
                    "无有效访问令牌",
                    ["请先完成认证流程"]
                )
            
            import requests
            headers = {'Authorization': f"Bearer {access_token}"}
            
            # 测试API调用
            url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json"
            
            try:
                print("🧪 测试Yahoo API访问...")
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._render_api_test_page(True, data)
                else:
                    return self._render_api_test_page(False, {
                        "status_code": response.status_code,
                        "error": response.text
                    })
                    
            except Exception as e:
                return self._render_api_test_page(False, {"error": str(e)})
        
        @self.app.route('/clear_token')
        def clear_token():
            """清除令牌"""
            if self.oauth_manager:
                self.oauth_manager.clear_token()
            
            session.clear()
            print("🗑️ 已清除所有令牌和会话")
            return redirect('/')
        
        @self.app.route('/logout')
        def logout():
            """退出登录"""
            session.clear()
            return redirect('/')
    
    def _render_index_page(self, config_status: str, token_status: str) -> str:
        """渲染主页"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Yahoo Fantasy工具 - OAuth认证</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
                .success { background-color: #d4edda; border: 1px solid #c3e6cb; }
                .warning { background-color: #fff3cd; border: 1px solid #ffeaa7; }
                .error { background-color: #f8d7da; border: 1px solid #f5c6cb; }
                .action { margin: 20px 0; }
                .btn { padding: 10px 20px; margin: 5px; text-decoration: none; border-radius: 5px; display: inline-block; }
                .btn-primary { background-color: #007bff; color: white; }
                .btn-secondary { background-color: #6c757d; color: white; }
                .btn-danger { background-color: #dc3545; color: white; }
            </style>
        </head>
        <body>
            <h1>🏀 Yahoo Fantasy工具</h1>
            <p>欢迎使用Yahoo Fantasy Sports分析工具！</p>
            
            <h2>📊 状态检查</h2>
            <div class="status {{ 'success' if '✅' in config_status else 'error' }}">
                OAuth配置: {{ config_status }}
            </div>
            <div class="status {{ 'success' if '✅' in token_status else ('warning' if '⚠️' in token_status else 'error') }}">
                访问令牌: {{ token_status }}
            </div>
            
            <h2>🚀 操作</h2>
            <div class="action">
                <a href="/auth" class="btn btn-primary">🔐 使用Yahoo账号登录</a>
                <a href="/test_api" class="btn btn-secondary">🧪 测试API访问</a>
                <a href="/config_check" class="btn btn-secondary">🔧 检查配置</a>
            </div>
            
            <div class="action">
                <a href="/clear_token" class="btn btn-danger">🗑️ 清除令牌</a>
            </div>
            
            <h2>📖 帮助</h2>
            <p>如果遇到问题，请：</p>
            <ul>
                <li>检查 .env 文件中的Yahoo API配置</li>
                <li>确认 YAHOO_REDIRECT_URI 设置正确</li>
                <li>查看控制台输出的详细错误信息</li>
            </ul>
        </body>
        </html>
        """
        return render_template_string(template, 
                                    config_status=config_status, 
                                    token_status=token_status)
    
    def _render_success_page(self, user_info: dict) -> str:
        """渲染成功页面"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>认证成功</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .success { background-color: #d4edda; padding: 20px; border-radius: 5px; }
                .info { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }
                .btn { padding: 10px 20px; margin: 5px; text-decoration: none; border-radius: 5px; display: inline-block; }
                .btn-primary { background-color: #007bff; color: white; }
                .btn-success { background-color: #28a745; color: white; }
            </style>
        </head>
        <body>
            <div class="success">
                <h1>✅ 授权成功！</h1>
                <p>您已成功授权Yahoo Fantasy工具访问您的Fantasy Sports数据。</p>
            </div>
            
            <h2>📋 授权信息</h2>
            <div class="info">
                <pre>{{ user_info | tojson(indent=2) }}</pre>
            </div>
            
            <h2>🎯 下一步</h2>
            <p>现在您可以：</p>
            <a href="/test_api" class="btn btn-success">🧪 测试API访问</a>
            <a href="/" class="btn btn-primary">🏠 返回首页</a>
            
            <h2>💡 使用应用</h2>
            <p>认证完成后，您可以关闭浏览器并在命令行中运行：</p>
            <code>python -m fantasy_etl</code>
        </body>
        </html>
        """
        return render_template_string(template, user_info=user_info)
    
    def _render_error_page(self, title: str, error: str, details: list = None) -> str:
        """渲染错误页面"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{{ title }}</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .error { background-color: #f8d7da; padding: 20px; border-radius: 5px; border: 1px solid #f5c6cb; }
                .btn { padding: 10px 20px; margin: 10px 5px 0 0; text-decoration: none; border-radius: 5px; display: inline-block; }
                .btn-primary { background-color: #007bff; color: white; }
                .details { background-color: #f8f9fa; padding: 15px; margin: 15px 0; border-radius: 5px; }
            </style>
        </head>
        <body>
            <div class="error">
                <h1>❌ {{ title }}</h1>
                <p>{{ error }}</p>
            </div>
            
            {% if details %}
            <div class="details">
                <h3>详细信息：</h3>
                <ul>
                {% for detail in details %}
                    <li>{{ detail }}</li>
                {% endfor %}
                </ul>
            </div>
            {% endif %}
            
            <a href="/" class="btn btn-primary">🏠 返回首页</a>
        </body>
        </html>
        """
        return render_template_string(template, title=title, error=error, details=details or [])
    
    def _render_config_page(self, config_info: dict) -> str:
        """渲染配置页面"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>配置检查</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .config { background-color: #f8f9fa; padding: 20px; border-radius: 5px; }
                .btn { padding: 10px 20px; margin: 10px 5px 0 0; text-decoration: none; border-radius: 5px; display: inline-block; }
                .btn-primary { background-color: #007bff; color: white; }
            </style>
        </head>
        <body>
            <h1>🔧 OAuth配置检查</h1>
            <div class="config">
                <pre>{{ config_info | tojson(indent=2) }}</pre>
            </div>
            <a href="/" class="btn btn-primary">🏠 返回首页</a>
        </body>
        </html>
        """
        return render_template_string(template, config_info=config_info)
    
    def _render_api_test_page(self, success: bool, data: dict) -> str:
        """渲染API测试页面"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>API测试结果</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .result { padding: 20px; border-radius: 5px; margin: 20px 0; }
                .success { background-color: #d4edda; border: 1px solid #c3e6cb; }
                .error { background-color: #f8d7da; border: 1px solid #f5c6cb; }
                .data { background-color: #f8f9fa; padding: 15px; border-radius: 5px; max-height: 400px; overflow-y: auto; }
                .btn { padding: 10px 20px; margin: 10px 5px 0 0; text-decoration: none; border-radius: 5px; display: inline-block; }
                .btn-primary { background-color: #007bff; color: white; }
            </style>
        </head>
        <body>
            <h1>🧪 Yahoo API测试结果</h1>
            
            <div class="result {{ 'success' if success else 'error' }}">
                <h2>{{ '✅ 测试成功' if success else '❌ 测试失败' }}</h2>
                <p>{{ 'API访问正常，可以获取Fantasy数据' if success else 'API访问失败，请检查令牌状态' }}</p>
            </div>
            
            <h3>📊 响应数据：</h3>
            <div class="data">
                <pre>{{ data | tojson(indent=2) }}</pre>
            </div>
            
            <a href="/" class="btn btn-primary">🏠 返回首页</a>
        </body>
        </html>
        """
        return render_template_string(template, success=success, data=data)
    
    def run(self, debug: bool = True, ssl_context: Optional[str] = None):
        """启动服务器"""
        try:
            print(f"\n🚀 启动Yahoo Fantasy OAuth认证服务器...")
            print(f"📍 访问地址: http://{self.host}:{self.port}")
            print(f"🔧 调试模式: {'开启' if debug else '关闭'}")
            print(f"💡 在浏览器中访问上述地址完成认证")
            
            self.app.run(host=self.host, port=self.port, debug=debug)
            
        except Exception as e:
            print(f"❌ 服务器启动失败: {str(e)}")
            raise