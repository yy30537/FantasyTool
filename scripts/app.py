from flask import Flask, redirect, request, render_template, session, url_for, flash
import requests
from requests_oauthlib import OAuth2Session
import os
from dotenv import load_dotenv
import json
import time
import pickle
import pathlib
from datetime import datetime

# 加载环境变量
load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Yahoo OAuth配置
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
token_url = "https://api.login.yahoo.com/oauth2/get_token"
redirect_uri = os.getenv("REDIRECT_URI", "oob")  # 默认使用oob
scope = ["fspt-w"]  # Fantasy Sports读写权限

# 验证配置
print("🔍 OAuth配置检查:")
print(f"CLIENT_ID: {'✓设置' if client_id else '❌未设置'}")
print(f"CLIENT_SECRET: {'✓设置' if client_secret else '❌未设置'}")
print(f"REDIRECT_URI: {redirect_uri}")

if not client_id or not client_secret:
    print("❌ 错误: 缺少CLIENT_ID或CLIENT_SECRET环境变量")
    print("请参考 oauth_setup_guide.md 进行配置")

# 创建令牌存储目录
TOKENS_DIR = pathlib.Path("tokens")
TOKENS_DIR.mkdir(exist_ok=True)
DEFAULT_TOKEN_FILE = TOKENS_DIR / "yahoo_token.token"

# 创建OAuth2Session
def get_oauth_session():
    return OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)

# 保存令牌到文件
def save_token(token):
    with open(DEFAULT_TOKEN_FILE, 'wb') as f:
        pickle.dump(token, f)

# 从文件加载令牌
def load_token():
    if DEFAULT_TOKEN_FILE.exists():
        try:
            with open(DEFAULT_TOKEN_FILE, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"加载令牌时出错: {str(e)}")
    
    return None

def refresh_token_if_expired():
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
            app.logger.info("刷新令牌...")
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
                save_token(new_token)
                
                app.logger.info("令牌刷新成功")
                return True
            else:
                app.logger.error(f"令牌刷新失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            app.logger.error(f"刷新令牌时出错: {str(e)}")
            return False
    
    return True

@app.route('/')
def index():
    # 检查配置状态
    config_status = "✅ 配置完整" if client_id and client_secret else "❌ 配置不完整"
    token_status = "✅ 有令牌" if load_token() else "❌ 无令牌"
    
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
    <p>如果遇到问题，请查看 <code>oauth_setup_guide.md</code></p>
    """

@app.route('/config_check')
def config_check():
    """配置检查页面"""
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

@app.route('/auth')
def auth():
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
        oauth = get_oauth_session()
        authorization_url, state = oauth.authorization_url(authorization_base_url)
        session['oauth_state'] = state
        
        app.logger.info(f"重定向到授权URL: {authorization_url}")
        return redirect(authorization_url)
    except Exception as e:
        app.logger.error(f"创建授权URL失败: {str(e)}")
        return f"创建授权URL失败: {str(e)}", 400

@app.route('/auth/callback')
def callback():
    """OAuth回调"""
    try:
        app.logger.info(f"收到回调请求: {request.url}")
        app.logger.info(f"请求参数: {request.args}")
        
        oauth = get_oauth_session()
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
        save_token(token)
        
        # 创建简单的用户信息对象
        user_info = {
            "token_obtained": True,
            "token_obtained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "access_token_expires_in": expires_in
        }
        
        session['user_info'] = user_info
        
        app.logger.info("OAuth认证成功")
        return redirect('/success')
    
    except Exception as e:
        app.logger.error(f"OAuth回调处理失败: {str(e)}")
        return f"""
        <h1>OAuth认证失败</h1>
        <p>错误: {str(e)}</p>
        <p>请求URL: {request.url}</p>
        <p><a href="/">返回首页重试</a></p>
        """, 400

@app.route('/success')
def success():
    """授权成功页面"""
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

@app.route('/fetch')
def fetch():
    """获取Fantasy数据"""
    if 'oauth_token' not in session:
        token = load_token()
        if token:
            session['oauth_token'] = token
        else:
            return redirect('/')
    
    # 刷新令牌（如果需要）
    if not refresh_token_if_expired():
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
            app.logger.error(f"API请求失败: {response.status_code} - {response.text}")
            return f"""
            <h1>数据获取失败</h1>
            <p>状态码: {response.status_code}</p>
            <p>错误信息: {response.text}</p>
            <p><a href="/">返回首页</a></p>
            """, 400
    except Exception as e:
        app.logger.error(f"API请求异常: {str(e)}")
        return f"API请求异常: {str(e)}", 400

@app.route('/run_time_series')
def run_time_series():
    """运行时间序列数据获取"""
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

@app.route('/logout')
def logout():
    """退出登录"""
    session.clear()
    return redirect('/')

if __name__ == '__main__':
    # 使用更安全的启动方式
    try:
        print("\n🚀 启动Yahoo Fantasy工具...")
        print(f"📍 访问地址: https://localhost:8000")
        print(f"🔧 调试模式: 开启")
        app.run(host='localhost', port=8000, debug=True, ssl_context='adhoc') 
    except Exception as e:
        if "cryptography" in str(e).lower():
            print("⚠️  警告: 缺少cryptography库，将使用HTTP模式运行...")
            print("💡 建议安装: pip install cryptography")
            print("📍 HTTP访问地址: http://localhost:8000")
            app.run(host='localhost', port=8000, debug=True) 
        else:
            print(f"❌ 启动失败: {str(e)}")
            raise 