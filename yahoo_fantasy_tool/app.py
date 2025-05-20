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
client_id = "dj0yJmk9U0NqTDRYdXd0NW9yJmQ9WVdrOVRGaGhkRUZLTmxnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTFk"
client_secret = "a5b3a6e1ff6a3e982036ec873a78f6fa46431508"
authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
token_url = "https://api.login.yahoo.com/oauth2/get_token"
redirect_uri = "https://localhost:8000/auth/callback"
scope = ["fspt-w"]  # Fantasy Sports读写权限

# 创建令牌存储目录
TOKENS_DIR = pathlib.Path("tokens")
TOKENS_DIR.mkdir(exist_ok=True)

# 创建OAuth2Session
def get_oauth_session():
    return OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)

# 保存令牌到文件
def save_token(token, user_guid):
    token_file = TOKENS_DIR / f"{user_guid}.token"
    with open(token_file, 'wb') as f:
        pickle.dump(token, f)
    
    # 保存最新使用的用户GUID到session
    session['user_guid'] = user_guid

# 从文件加载令牌
def load_token(user_guid=None):
    # 如果没有指定用户GUID，尝试从session获取
    if not user_guid and 'user_guid' in session:
        user_guid = session['user_guid']
    
    if not user_guid:
        return None
    
    token_file = TOKENS_DIR / f"{user_guid}.token"
    if token_file.exists():
        try:
            with open(token_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"加载令牌时出错: {str(e)}")
    
    return None

def refresh_token_if_expired():
    """检查并刷新令牌（如果已过期）"""
    if 'oauth_token' not in session:
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
                # 保存令牌到文件（如果有用户ID）
                if 'user_id' in session:
                    save_token(new_token, session['user_id'])
                
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
def home():
    # 检查是否已有保存的令牌
    if 'user_guid' in session:
        token = load_token(session['user_guid'])
        if token:
            # 检查并刷新令牌
            if refresh_token_if_expired():
                return redirect(url_for('leagues'))  # 直接跳转到联盟页面
            else:
                # 令牌刷新失败，清除会话并重新登录
                session.pop('oauth_token', None)
                session.pop('user_guid', None)
    
    return render_template('index.html')

@app.route('/login')
def login():
    try:
        # 创建OAuth会话
        yahoo = get_oauth_session()
        
        # 获取授权URL
        authorization_url, state = yahoo.authorization_url(authorization_base_url)
        
        # 保存状态供后续验证
        session['oauth_state'] = state
        
        # 跳转到Yahoo授权页面
        return redirect(authorization_url)
    except Exception as e:
        print(f"获取授权URL时发生错误: {str(e)}")
        return render_template('error.html', error=str(e))

@app.route('/auth/callback')
def callback():
    try:
        # 获取授权码
        code = request.args.get('code')
        
        if not code:
            error = request.args.get('error', '未知错误')
            error_description = request.args.get('error_description', '未提供错误详情')
            return render_template('error.html', error=f"{error}: {error_description}")
        
        # 使用授权码获取访问令牌
        yahoo = get_oauth_session()
        
        # 按照Yahoo OAuth2.0文档进行令牌交换
        token = yahoo.fetch_token(
            token_url,
            code=code,
            client_secret=client_secret,
            include_client_id=True
        )
        
        # 保存令牌和过期时间
        token['expires_at'] = int(time.time()) + token.get('expires_in', 3600)
        session['oauth_token'] = token
        
        # 保存令牌到持久存储
        if 'xoauth_yahoo_guid' in token:
            user_guid = token['xoauth_yahoo_guid']
            save_token(token, user_guid)
            flash('登录成功！您的会话已保存。', 'success')
        
        # 重定向到联盟选择页面
        return redirect(url_for('leagues'))
    except Exception as e:
        print(f"回调处理时发生错误: {str(e)}")
        return render_template('error.html', error=str(e))

@app.route('/leagues')
def leagues():
    """显示用户的所有联盟（只加载基本信息）"""
    if 'oauth_token' not in session:
        flash('请先登录以查看您的联盟', 'error')
        return redirect(url_for('home'))
    if not refresh_token_if_expired():
        flash('您的登录已过期，请重新登录', 'error')
        return redirect(url_for('home'))
    try:
        headers = {
            'Authorization': f"Bearer {session['oauth_token']['access_token']}"
        }
        
        # 直接获取所有用户的NBA联盟
        leagues_url = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_codes=nba/leagues?format=json"
        response = requests.get(leagues_url, headers=headers)
        leagues_data = response.json()
        
        # 添加调试信息
        app.logger.debug(f"API响应: {json.dumps(leagues_data, indent=2)}")
        
        # 提取所有联盟信息
        all_leagues = []
        
        if 'fantasy_content' in leagues_data and 'users' in leagues_data['fantasy_content']:
            users = leagues_data['fantasy_content']['users']
            if users.get('count', 0) > 0 and '0' in users:
                user_data = users['0']['user']
                
                # 查找games数组
                for item in user_data:
                    if isinstance(item, dict) and 'games' in item:
                        games = item['games']
                        # 遍历所有游戏
                        for game_key, game_data in games.items():
                            if game_key != 'count':  # 跳过count键
                                if 'game' in game_data and isinstance(game_data['game'], list):
                                    # 遍历game数组中的每个元素
                                    for game_item in game_data['game']:
                                        # 如果元素是字典并且包含leagues键
                                        if isinstance(game_item, dict) and 'leagues' in game_item:
                                            leagues_container = game_item['leagues']
                                            
                                            # 遍历所有联盟
                                            for league_key, league in leagues_container.items():
                                                if league_key != 'count':  # 跳过count键
                                                    if 'league' in league and isinstance(league['league'], list):
                                                        league_info = league['league'][0]  # 获取联盟基本信息
                                                        
                                                        # 提取季节信息
                                                        season = ''
                                                        if 'season' in league_info:
                                                            try:
                                                                season = str(league_info['season'])
                                                            except (ValueError, TypeError):
                                                                season = ''
                                                        
                                                        # 提取最后更新时间
                                                        last_update = 0
                                                        if 'league_update_timestamp' in league_info and league_info['league_update_timestamp'] is not None:
                                                            try:
                                                                last_update = int(league_info['league_update_timestamp'])
                                                            except (ValueError, TypeError):
                                                                last_update = 0
                                                        
                                                        # 创建联盟记录
                                                        league_record = {
                                                            'league_key': league_info.get('league_key', ''),
                                                            'league_id': league_info.get('league_id', ''),
                                                            'name': league_info.get('name', '未命名联盟'),
                                                            'url': league_info.get('url', ''),
                                                            'logo_url': league_info.get('logo_url', ''),
                                                            'draft_status': league_info.get('draft_status', ''),
                                                            'num_teams': league_info.get('num_teams', 0),
                                                            'current_week': league_info.get('current_week', 0),
                                                            'last_update': last_update,
                                                            'season': season,
                                                            'game_code': league_info.get('game_code', 'nba'),
                                                            'is_finished': league_info.get('is_finished', 0)
                                                        }
                                                        
                                                        all_leagues.append(league_record)
        
        # 如果没有联盟信息，添加一个警告
        if not all_leagues:
            app.logger.warning("未找到任何联盟信息")
            flash("未找到任何联盟信息", "warning")
        
        # 按最后更新时间和赛季排序（最新的在前面）
        def safe_int(val):
            if val is None:
                return 0
            try:
                return int(val)
            except (TypeError, ValueError):
                return 0
        
        # 使用更安全的排序方式
        all_leagues.sort(key=lambda x: (-safe_int(x.get('last_update', 0)), -safe_int(x.get('season', 0))))
        
        return render_template('leagues.html', all_leagues=all_leagues)
    except Exception as e:
        app.logger.error(f"获取联盟时出错: {str(e)}")
        flash(f"获取联盟时出错: {str(e)}", 'error')
        return render_template('error.html', error_message=str(e))

@app.route('/league/<league_key>')
def league_overview(league_key):
    """显示联盟详细信息（基本信息、settings、standings、scoreboard）"""
    if 'oauth_token' not in session:
        flash('请先登录以查看联盟详情', 'error')
        return redirect(url_for('home'))
    if not refresh_token_if_expired():
        flash('您的登录已过期，请重新登录', 'error')
        return redirect(url_for('home'))
    try:
        headers = {
            'Authorization': f"Bearer {session['oauth_token']['access_token']}"
        }
        
        # 1. 获取联盟基本信息
        league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}?format=json"
        response = requests.get(league_url, headers=headers)
        league_data = response.json()
        
        # 调试输出
        app.logger.debug(f"联盟基本信息: {json.dumps(league_data, indent=2)}")
        
        # 2. 获取联盟设置
        settings_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings?format=json"
        settings_response = requests.get(settings_url, headers=headers)
        settings_data = settings_response.json()
        
        # 调试输出
        app.logger.debug(f"联盟设置: {json.dumps(settings_data, indent=2)}")
        
        # 3. 获取联盟排名
        standings_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/standings?format=json"
        standings_response = requests.get(standings_url, headers=headers)
        standings_data = standings_response.json()
        
        # 调试输出
        app.logger.debug(f"联盟排名: {json.dumps(standings_data, indent=2)}")
        
        # 4. 获取联盟积分板
        scoreboard_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard?format=json"
        scoreboard_response = requests.get(scoreboard_url, headers=headers)
        scoreboard_data = scoreboard_response.json()
        
        # 调试输出
        app.logger.debug(f"联盟积分板: {json.dumps(scoreboard_data, indent=2)}")
        
        # 处理联盟基本信息
        league_info = {}
        if 'fantasy_content' in league_data and 'league' in league_data['fantasy_content']:
            league_content = league_data['fantasy_content']['league']
            # 处理league_content可能是列表或字典的情况
            if isinstance(league_content, list):
                league_info = league_content[0]
            else:
                league_info = league_content
        
        # 处理联盟设置
        settings_info = {}
        if 'fantasy_content' in settings_data and 'league' in settings_data['fantasy_content']:
            league_content = settings_data['fantasy_content']['league']
            if 'settings' in league_content:
                settings_info = league_content['settings']
        
        # 处理联盟排名
        standings_info = {}
        if 'fantasy_content' in standings_data and 'league' in standings_data['fantasy_content']:
            league_content = standings_data['fantasy_content']['league']
            if 'standings' in league_content:
                standings_info = league_content['standings']
        
        # 处理联盟积分板
        scoreboard_info = {}
        if 'fantasy_content' in scoreboard_data and 'league' in scoreboard_data['fantasy_content']:
            league_content = scoreboard_data['fantasy_content']['league']
            if 'scoreboard' in league_content:
                scoreboard_info = league_content['scoreboard']
        
        return render_template(
            'league_overview.html',
            league=league_info,
            settings=settings_info,
            standings=standings_info,
            scoreboard=scoreboard_info
        )
    except Exception as e:
        app.logger.error(f"获取联盟详情时出错: {str(e)}")
        flash(f"获取联盟详情时出错: {str(e)}", 'error')
        return render_template('error.html', error_message=str(e))

@app.route('/teams/<league_key>')
def teams(league_key=None):
    """显示特定联盟中的所有队伍"""
    if 'oauth_token' not in session:
        flash('请先登录以查看队伍', 'error')
        return redirect(url_for('home'))
    
    if not league_key:
        flash('未指定联盟', 'error')
        return redirect(url_for('leagues'))
    
    if not refresh_token_if_expired():
        flash('您的登录已过期，请重新登录', 'error')
        return redirect(url_for('home'))
    
    try:
        # 获取联盟信息
        league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}?format=json"
        headers = {
            'Authorization': f"Bearer {session['oauth_token']['access_token']}"
        }
        
        response = requests.get(league_url, headers=headers)
        league_data = response.json()
        
        # 调试输出API响应
        app.logger.debug(f"League API Response: {json.dumps(league_data, indent=2)}")
        
        # 从响应中提取联盟信息
        league_info = {}
        if 'fantasy_content' in league_data and 'league' in league_data['fantasy_content']:
            league = None
            
            # 处理league数据可能是列表或字典的情况
            if isinstance(league_data['fantasy_content']['league'], list):
                league = league_data['fantasy_content']['league'][0]
            elif isinstance(league_data['fantasy_content']['league'], dict):
                league = league_data['fantasy_content']['league']
            
            if not league:
                flash('无法解析联盟数据', 'error')
                return redirect(url_for('leagues'))
            
            # 处理游戏信息
            game_key = league_key.split('.')[0]
            game_id = None
            game_info = None
            
            # 查找game_code，可能在不同位置
            game_code = None
            if 'game_code' in league:
                game_code = league['game_code']
            
            if game_code:
                game_name = "Yahoo Fantasy " + game_code.upper()
                season = league.get('season', '')
                is_active = True
                game_info = {
                    'game_key': game_key,
                    'game_id': game_id,
                    'code': game_code,
                    'name': game_name,
                    'season': season,
                    'is_active': is_active
                }
            
            # 构建联盟信息
            league_info = {
                'league_key': league.get('league_key'),
                'league_id': league.get('league_id'),
                'name': league.get('name'),
                'url': league.get('url'),
                'logo_url': league.get('logo_url'),
                'draft_status': league.get('draft_status'),
                'num_teams': league.get('num_teams'),
                'current_week': league.get('current_week'),
                'start_week': league.get('start_week'),
                'end_week': league.get('end_week'),
                'game_info': game_info
            }
        
        # 获取联盟中的队伍
        teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/teams?format=json"
        response = requests.get(teams_url, headers=headers)
        teams_data = response.json()
        
        # 调试输出API响应
        app.logger.debug(f"Teams API Response: {json.dumps(teams_data, indent=2)}")
        
        # 处理响应数据
        teams = []
        if 'fantasy_content' in teams_data and 'league' in teams_data['fantasy_content']:
            league_teams_container = None
            
            # 处理league数据可能是列表或字典的情况
            if isinstance(teams_data['fantasy_content']['league'], list) and len(teams_data['fantasy_content']['league']) > 1:
                league_teams_container = teams_data['fantasy_content']['league'][1]
            elif isinstance(teams_data['fantasy_content']['league'], dict):
                league_teams_container = teams_data['fantasy_content']['league']
            
            # 从league_teams_container中提取teams
            if league_teams_container and 'teams' in league_teams_container:
                league_teams = league_teams_container['teams']
                
                # 遍历队伍，注意跳过计数键
                for key, team in league_teams.items():
                    if key.isdigit():  # 忽略非数字键（如"count"）
                        team_info = process_team_data(team)
                        if team_info:  # 只有处理成功的队伍数据才添加
                            teams.append(team_info)
        
        return render_template('teams.html', teams=teams, league_info=league_info, data=json.dumps(teams_data, indent=2))
    
    except Exception as e:
        app.logger.error(f"获取联盟队伍时出错: {str(e)}")
        flash(f"获取联盟队伍时出错: {str(e)}", 'error')
        return render_template('error.html', error_message=str(e))

@app.route('/logout')
def logout():
    """用户登出，清除会话信息"""
    # 清除会话中的令牌和用户信息
    session.pop('oauth_token', None)
    session.pop('user_guid', None)
    session.pop('oauth_state', None)
    
    # 显示成功消息
    flash('您已成功登出', 'success')
    
    # 重定向到首页
    return redirect(url_for('home'))

if __name__ == '__main__':
    # 创建模板目录
    if not os.path.exists('templates'):
        os.makedirs('templates')
        
    # 使用非adhoc SSL上下文选项，避免cryptography依赖问题
    try:
        app.run(host='localhost', port=8000, debug=True, ssl_context='adhoc') 
    except TypeError as e:
        if "requires the cryptography library" in str(e):
            print("警告: 缺少cryptography库，将使用非安全模式运行...")
            print("请运行: pip install cryptography")
            print("现在将以HTTP模式运行，但Yahoo OAuth可能会要求HTTPS...")
            app.run(host='localhost', port=8000, debug=True) 