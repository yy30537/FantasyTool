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
redirect_uri = os.getenv("REDIRECT_URI")
scope = ["fspt-w"]  # Fantasy Sports读写权限

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
    return """
    <h1>Yahoo Fantasy工具</h1>
    <p>欢迎使用Yahoo Fantasy工具！</p>
    <a href="/auth">使用Yahoo帐号登录</a>
    """

@app.route('/auth')
def auth():
    """
    启动OAuth流程
    """
    oauth = get_oauth_session()
    authorization_url, state = oauth.authorization_url(authorization_base_url)
    session['oauth_state'] = state
    return redirect(authorization_url)

@app.route('/auth/callback')
def callback():
    """
    OAuth回调
    """
    oauth = get_oauth_session()
    try:
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
        
        return redirect('/success')
    
    except Exception as e:
        return f"获取令牌时出错: {str(e)}", 400

@app.route('/success')
def success():
    """
    授权成功页面
    """
    if 'oauth_token' not in session:
        return redirect('/')
    
    user_info = session.get('user_info', {})
    return f"""
    <h1>授权成功</h1>
    <p>您已成功授权Yahoo Fantasy工具访问您的Fantasy Sports数据。</p>
    <h2>授权信息</h2>
    <pre>{json.dumps(user_info, indent=2)}</pre>
    <a href='/fetch'>获取数据</a>
    <br/>
    <a href='/logout'>退出登录</a>
    """

@app.route('/fetch')
def fetch():
    """
    获取Fantasy数据
    """
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
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return f"""
        <h1>Fantasy数据</h1>
        <pre>{json.dumps(data, indent=2)}</pre>
        <a href='/'>返回首页</a>
        """
    else:
        return f"获取数据失败: {response.status_code} - {response.text}", 400

@app.route('/logout')
def logout():
    """
    退出登录
    """
    session.clear()
    return redirect('/')

if __name__ == '__main__':
    # 使用非adhoc SSL上下文选项，避免cryptography依赖问题
    try:
        app.run(host='localhost', port=8000, debug=True, ssl_context='adhoc') 
    except TypeError as e:
        if "requires the cryptography library" in str(e):
            print("警告: 缺少cryptography库，将使用非安全模式运行...")
            print("请运行: pip install cryptography")
            print("现在将以HTTP模式运行，但Yahoo OAuth可能会要求HTTPS...")
            app.run(host='localhost', port=8000, debug=True) 