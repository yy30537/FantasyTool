#!/usr/bin/env python3
"""
Yahoo Fantasy Sports OAuth 认证器
专门负责OAuth认证流程的独立模块
"""

try:
    from flask import Flask, redirect, request, session
    import requests
    from requests_oauthlib import OAuth2Session
    _FLASK_AVAILABLE = True
except ImportError:
    _FLASK_AVAILABLE = False
    Flask = redirect = request = session = None
    requests = OAuth2Session = None
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import json
import time
from datetime import datetime

# 导入API客户端基础设施
from .client import YahooFantasyAPIClient


class YahooOAuthAuthenticator(YahooFantasyAPIClient):
    """Yahoo Fantasy Sports OAuth认证器 - 继承自API客户端"""
    
    def __init__(self):
        """初始化OAuth认证器"""
        if not _FLASK_AVAILABLE:
            print("❌ Flask未安装，OAuth Web服务不可用")
            print("请安装: pip install flask requests-oauthlib")
            super().__init__()
            return
            
        super().__init__()  # 初始化API客户端
        
        self.app = Flask(__name__)
        self.app.secret_key = os.urandom(24)
        
        # OAuth Web应用特定配置
        self.authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
        self.redirect_uri = os.getenv("REDIRECT_URI", "oob")
        self.scope = ["fspt-w"]
        
        # 注册路由
        self._register_routes()
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self):
        """验证OAuth配置"""
        if not self.client_id or not self.client_secret:
            print("❌ 缺少OAuth配置")
            print("请设置CLIENT_ID和CLIENT_SECRET环境变量")
    
    def get_oauth_session(self):
        """创建OAuth2Session"""
        return OAuth2Session(self.client_id, redirect_uri=self.redirect_uri, scope=self.scope)
    
    def refresh_token_if_expired(self):
        """检查并刷新令牌（如果已过期）"""
        if 'oauth_token' not in session:
            token = load_token()
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
                self.app.logger.info("刷新令牌...")
                refresh_token = token.get('refresh_token')
                
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
                    
                    # 更新会话中的令牌
                    session['oauth_token'] = new_token
                    # 保存令牌到文件
                    self.save_token(new_token)
                    
                    self.app.logger.info("令牌刷新成功")
                    return True
                else:
                    self.app.logger.error(f"令牌刷新失败: {response.status_code} - {response.text}")
                    return False
            except Exception as e:
                self.app.logger.error(f"刷新令牌时出错: {str(e)}")
                return False
        
        return True
    
    def _register_routes(self):
        """注册Flask路由"""
        
        @self.app.route('/')
        def index():
            # 检查配置状态
            config_status = "✅ 配置完整" if self.client_id and self.client_secret else "❌ 配置不完整"
            
            # 使用继承的令牌管理方法
            token = self.load_token()
            token_valid = False
            
            if token:
                # 检查令牌是否有效（包括自动刷新）
                try:
                    refreshed_token = self.refresh_token_if_needed(token)
                    if refreshed_token:
                        now = datetime.now().timestamp()
                        expires_at = refreshed_token.get('expires_at', 0)
                        token_valid = now < expires_at
                except:
                    token_valid = False
            
            token_status = "✅ 有效令牌" if token_valid else ("⚠️ 过期令牌" if token else "❌ 无令牌")
            
            # 如果已有有效令牌，显示不同的界面
            if token_valid:
                return f"""
                <!DOCTYPE html>
                <html><head><title>Yahoo Fantasy工具</title></head><body>
                <h1>🎉 Yahoo Fantasy工具</h1>
                <div style="background-color: #d4edda; border: 1px solid #c3e6cb; padding: 15px; border-radius: 5px; margin: 10px 0;">
                    <p><strong>✅ 您已成功授权，可以使用Yahoo Fantasy API！</strong></p>
                    <p>OAuth配置: {config_status}</p>
                    <p>访问令牌: {token_status}</p>
                </div>
                
                <h2>🚀 可用操作</h2>
                <p><a href="/fetch" style="display: inline-block; padding: 8px 16px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px;">测试API调用</a></p>
                <p><a href="/config_check">查看配置详情</a></p>
                
                <h2>🔄 重新授权</h2>
                <p>如果需要重新获取令牌：</p>
                <p><a href="/logout">清除当前令牌并重新授权</a></p>
                </body></html>
                """
            
            # 没有令牌时，显示简化的授权界面
            oauth = self.get_oauth_session()
            authorization_url, state = oauth.authorization_url(self.authorization_base_url)
            
            return f"""
            <!DOCTYPE html>
            <html><head><title>Yahoo Fantasy工具 - OAuth授权</title></head><body>
            <h1>🔐 Yahoo Fantasy工具 - OAuth授权</h1>
            
            <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <p>OAuth配置: {config_status}</p>
                <p>访问令牌: {token_status}</p>
            </div>
            
            <h2>📋 授权步骤（简单2步）</h2>
            <div style="background-color: #e3f2fd; border: 1px solid #bbdefb; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <p><strong>步骤1：</strong> <a href="{authorization_url}" target="_blank" style="color: #1976d2; font-weight: bold;">点击这里获取Yahoo授权码</a> （在新窗口打开）</p>
                <p><strong>步骤2：</strong> 将显示的授权码粘贴到下方框中并提交</p>
            </div>
            
            <h2>🔑 输入授权码</h2>
            <form method="post" action="/auth_code" style="background-color: #fff; border: 1px solid #ddd; padding: 20px; border-radius: 5px;">
                <p>
                    <label for="auth_code" style="font-weight: bold;">授权码:</label><br/>
                    <input type="text" id="auth_code" name="auth_code" size="50" 
                           placeholder="例如: xfmt8ng" required 
                           style="padding: 8px; border: 1px solid #ccc; border-radius: 4px; font-family: monospace;">
                </p>
                <p>
                    <input type="submit" value="提交授权码" 
                           style="padding: 10px 20px; background-color: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">
                </p>
            </form>
            
            <h2>ℹ️ 说明</h2>
            <ul>
                <li>点击授权链接后，Yahoo会要求您登录并授权应用</li>
                <li>授权成功后，页面会显示一个授权码（如：xfmt8ng）</li>
                <li>复制这个授权码，粘贴到上方的输入框中</li>
                <li>提交后即可完成授权，开始使用Fantasy API</li>
            </ul>
            
            <p><a href="/config_check">查看详细配置信息</a></p>
            </body></html>
            """
        
        @self.app.route('/config_check')
        def config_check():
            """配置检查页面"""
            config_info = {
                "CLIENT_ID": self.client_id[:10] + "..." if self.client_id else "未设置",
                "CLIENT_SECRET": "已设置" if self.client_secret else "未设置", 
                "REDIRECT_URI": self.redirect_uri,
                "SCOPE": self.scope
            }
            
            return f"""
            <h1>OAuth配置检查</h1>
            <pre>{json.dumps(config_info, indent=2, ensure_ascii=False)}</pre>
            <p><a href="/">返回首页</a></p>
            """
        
        @self.app.route('/auth_code', methods=['POST'])
        def auth_code():
            """处理授权码提交"""
            try:
                auth_code = request.form.get('auth_code')
                if not auth_code:
                    return """
                    <h1>错误</h1>
                    <p>❌ 授权码不能为空</p>
                    <p><a href="/">返回重试</a></p>
                    """, 400
                
                # 创建OAuth会话
                oauth = OAuth2Session(self.client_id, redirect_uri="oob", scope=self.scope)
                
                # 获取令牌
                token = oauth.fetch_token(
                    self.token_url,
                    client_secret=self.client_secret,
                    code=auth_code.strip()
                )
                
                # 设置令牌过期时间
                expires_in = token.get('expires_in', 3600)
                token['expires_at'] = time.time() + int(expires_in)
                
                # 保存令牌到会话
                session['oauth_token'] = token
                
                # 保存令牌到文件
                self.save_token(token)
                
                # 创建用户信息对象
                user_info = {
                    "token_obtained": True,
                    "token_obtained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "access_token_expires_in": expires_in,
                    "auth_method": "authorization_code"
                }
                
                session['user_info'] = user_info
                
                self.app.logger.info("授权码认证成功")
                return redirect('/success')
                
            except Exception as e:
                self.app.logger.error(f"授权码处理失败: {str(e)}")
                return f"""
                <h1>授权失败</h1>
                <p>❌ 错误: {str(e)}</p>
                <p>请检查授权码是否正确</p>
                <p><a href="/">返回重试</a></p>
                """, 400
        
        @self.app.route('/success')
        def success():
            """授权成功页面"""
            if 'oauth_token' not in session:
                return redirect('/')
            
            user_info = session.get('user_info', {})
            
            return f"""
            <!DOCTYPE html>
            <html><head><title>授权成功</title></head><body>
            <h1>🎉 OAuth授权成功！</h1>
            <div style="background-color: #d4edda; border: 1px solid #c3e6cb; padding: 20px; border-radius: 5px; margin: 10px 0;">
                <p><strong>✅ 您已成功授权Yahoo Fantasy工具！</strong></p>
                <p>令牌获取时间: {user_info.get('token_obtained_at', '未知')}</p>
                <p>令牌有效期: {user_info.get('access_token_expires_in', 3600)} 秒</p>
            </div>
            
            <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 20px; border-radius: 5px; margin: 10px 0;">
                <h2>🚀 下一步操作</h2>
                <p><strong>请按 <kbd>Ctrl+C</kbd> 停止Web服务器</strong></p>
                <p>然后返回终端主菜单，选择：</p>
                <ul>
                    <li><strong>选项2</strong> - 启动数据获取工具</li>
                    <li><strong>选项3</strong> - 启动样本数据获取工具</li>
                </ul>
                <p>现在您可以开始获取Yahoo Fantasy数据了！</p>
            </div>
            
            <p>
                <a href='/fetch' style="display: inline-block; padding: 8px 16px; background-color: #007bff; color: white; text-decoration: none; border-radius: 4px; margin: 5px;">测试API调用</a>
                <a href='/' style="display: inline-block; padding: 8px 16px; background-color: #6c757d; color: white; text-decoration: none; border-radius: 4px; margin: 5px;">返回首页</a>
            </p>
            </body></html>
            """
        
        @self.app.route('/fetch')
        def fetch():
            """获取Fantasy数据"""
            if 'oauth_token' not in session:
                token = self.load_token()
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
                    self.app.logger.error(f"API请求失败: {response.status_code} - {response.text}")
                    return f"""
                    <h1>数据获取失败</h1>
                    <p>状态码: {response.status_code}</p>
                    <p>错误信息: {response.text}</p>
                    <p><a href="/">返回首页</a></p>
                    """, 400
            except Exception as e:
                self.app.logger.error(f"API请求异常: {str(e)}")
                return f"API请求异常: {str(e)}", 400
        
        @self.app.route('/logout')
        def logout():
            """退出登录"""
            session.clear()
            return redirect('/')
    
    def start_server(self, host='localhost', port=8000, debug=True, use_https=True):
        """启动OAuth认证服务器"""
        print(f"🚀 OAuth服务: http://{host}:{port}")
        print("在浏览器中打开上述地址完成认证")
        
        try:
            if use_https:
                # 优先尝试HTTPS，禁用reloader避免重启问题
                self.app.run(host=host, port=port, debug=debug, use_reloader=False, ssl_context='adhoc')
            else:
                self.app.run(host=host, port=port, debug=debug, use_reloader=False)
        except Exception as ssl_error:
            if use_https and "cryptography" in str(ssl_error).lower():
                print("切换到HTTP模式...")
                self.app.run(host=host, port=port, debug=debug, use_reloader=False)
            else:
                raise ssl_error


# 便捷函数用于向后兼容
def create_oauth_app():
    """创建OAuth认证应用"""
    authenticator = YahooOAuthAuthenticator()
    return authenticator.app


def start_oauth_server(host='localhost', port=8000, debug=True, use_https=True):
    """启动OAuth认证服务器"""
    if not _FLASK_AVAILABLE:
        print("❌ Flask未安装，OAuth Web服务不可用")
        print("请安装: pip install flask requests-oauthlib")
        return
        
    authenticator = YahooOAuthAuthenticator()
    try:
        authenticator.start_server(host=host, port=port, debug=debug, use_https=use_https)
    except KeyboardInterrupt:
        print("\n🛑 服务已停止")
    except Exception as e:
        print(f"❌ 启动失败: {str(e)}")


if __name__ == "__main__":
    # 直接运行此脚本时启动OAuth认证服务器
    start_oauth_server()