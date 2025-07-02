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


访问 `http://localhost:5000`，完成Yahoo账号授权，获取API访问令牌。


