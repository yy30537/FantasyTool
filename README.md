# Yahoo Fantasy Sports ETL 工具

基于Yahoo Fantasy Sports API的ETL数据管道，支持：
- 游戏、联盟、团队、球员数据提取
- 统计数据、对战记录、交易历史分析
- 完整的数据提取、转换、加载流程

## 🚀 环境设置

### 1. Python环境配置

```powershell
# Windows PowerShell - 创建虚拟环境
python -m venv venv

# 激活虚拟环境
.\venv\Scripts\Activate

# 安装依赖包
pip install -r requirements.txt
```

### 2. 数据库配置

创建 `.env` 文件：
```env
# PostgreSQL数据库配置
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db

# Yahoo API配置
YAHOO_CLIENT_ID=your_client_id
YAHOO_CLIENT_SECRET=your_client_secret
YAHOO_REDIRECT_URI=http://localhost:5000/oauth/callback
```

### 3. Yahoo API授权设置

#### 3.1 获取Yahoo API凭据
1. 访问 [Yahoo Developer Console](https://developer.yahoo.com/apps/)
2. 创建新应用，选择Fantasy Sports API权限
3. 获取Client ID和Client Secret
4. 设置重定向URI为 `http://localhost:5000/oauth/callback`

#### 3.2 启动OAuth授权流程

```powershell
# 使用新ETL架构启动授权服务器
python -c "
from fantasy_etl.auth.web_auth_server import WebAuthServer
from fantasy_etl.config.settings import Settings

settings = Settings()
auth_server = WebAuthServer(settings.api_config)
auth_server.run(host='localhost', port=5000, debug=True)
"
```

访问 `http://localhost:5000`，完成Yahoo账号授权，获取API访问令牌。

### 4. 验证ETL环境

```powershell
# 测试配置加载
python -c "
from fantasy_etl.config.settings import Settings
settings = Settings()
print('✅ 配置加载成功')
print(f'数据库: {settings.database_config.database}')
print(f'API客户端: {settings.api_config.client_id[:8]}...')
"

# 测试提取器导入
python -c "
from fantasy_etl.extract.extractors.game_extractor import GameExtractor
from fantasy_etl.extract.yahoo_client import YahooAPIClient
from fantasy_etl.config.settings import Settings

print('✅ ETL模块导入成功')
print('可用提取器: GameExtractor, LeagueExtractor, PlayerExtractor 等11个')
"
```

## 📂 项目结构

```
FantasyTool/
├── fantasy_etl/                    # ETL管道核心
│   ├── auth/                       # OAuth认证模块
│   ├── config/                     # 配置管理
│   └── extract/                    # 数据提取层
│       ├── extractors/             # 11个数据提取器
│       ├── yahoo_client.py         # Yahoo API客户端
│       ├── rate_limiter.py         # API速率控制
│       └── base_extractor.py       # 提取器基类
├── tokens/                         # OAuth令牌存储
├── .env                           # 环境变量配置
├── requirements.txt               # Python依赖
└── README.md                      # 项目说明
```

## 🔧 基本使用

### 提取游戏数据示例

```python
from fantasy_etl.config.settings import Settings
from fantasy_etl.extract.yahoo_client import YahooAPIClient
from fantasy_etl.extract.extractors.game_extractor import GameExtractor

# 初始化配置和客户端
settings = Settings()
api_client = YahooAPIClient(settings.api_config)

# 创建提取器并获取数据
game_extractor = GameExtractor(api_client)
result = game_extractor.extract()

if result.is_success:
    print(f"成功提取 {result.total_records} 个游戏")
    for game in result.data:
        print(f"游戏: {game['name']} ({game['game_key']})")
else:
    print(f"提取失败: {result.error_message}")
```

## 📋 下一步

1. **配置数据库** - 创建PostgreSQL数据库并运行表结构
2. **完成OAuth认证** - 获取有效的Yahoo API令牌  
3. **开始数据提取** - 使用11个提取器获取Fantasy数据
4. **实现Transform层** - 数据清洗和标准化
5. **实现Load层** - 数据入库和批量处理

## 🆘 故障排除

### 常见问题

**问题**: `ModuleNotFoundError: No module named 'fantasy_etl'`
**解决**: 确保在项目根目录运行，并激活虚拟环境

**问题**: OAuth认证失败
**解决**: 检查.env文件中的Yahoo API凭据，确认重定向URI配置

**问题**: 数据库连接失败  
**解决**: 检查PostgreSQL服务状态和.env中的数据库配置

