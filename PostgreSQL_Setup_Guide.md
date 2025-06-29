# Windows PostgreSQL 设置指南

## 1. PostgreSQL服务器配置

由于你已经安装了pgAdmin，现在需要配置PostgreSQL服务器：

### 启动PostgreSQL服务
1. 打开 **服务管理器** (services.msc)
2. 找到 `postgresql-x64-XX` 服务（XX是版本号）
3. 右键 -> **启动** (如果未运行)

### 设置数据库连接
1. 打开 **pgAdmin 4**
2. 连接到本地PostgreSQL服务器 (通常是 localhost:5432)
3. 如果首次使用，创建密码

## 2. 创建Fantasy数据库

在pgAdmin中执行以下SQL：

```sql
-- 创建数据库
CREATE DATABASE fantasy_db
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- 创建用户
CREATE USER fantasy_user WITH PASSWORD 'fantasyPassword';

-- 授予权限
GRANT ALL PRIVILEGES ON DATABASE fantasy_db TO fantasy_user;
GRANT ALL ON SCHEMA public TO fantasy_user;
```

## 3. 环境变量配置

创建或更新 `.env` 文件：

```bash
# PostgreSQL数据库配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword

# Yahoo OAuth配置 (如已有可保持不变)
YAHOO_CLIENT_ID=your_client_id
YAHOO_CLIENT_SECRET=your_client_secret
REDIRECT_URI=oob
```

## 4. 安装Python依赖

确保安装了PostgreSQL相关的Python包：

```bash
# 激活虚拟环境
venv\Scripts\activate

# 安装PostgreSQL适配器
pip install psycopg2-binary

# 安装SQLAlchemy (如果还没有)
pip install sqlalchemy

# 安装其他可能需要的包
pip install python-dotenv
```

## 5. 测试连接

可以使用以下Python代码测试连接：

```python
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'fantasy_db'),
        user=os.getenv('DB_USER', 'fantasy_user'),
        password=os.getenv('DB_PASSWORD', 'fantasyPassword')
    )
    print("✅ PostgreSQL连接成功!")
    conn.close()
except Exception as e:
    print(f"❌ 连接失败: {e}")
```

## 6. 常见问题解决

### 连接被拒绝
- 确保PostgreSQL服务正在运行
- 检查 `pg_hba.conf` 配置文件（通常在PostgreSQL安装目录下的data文件夹）
- 确保该文件包含：`host all all 127.0.0.1/32 md5`

### 权限问题
```sql
-- 在pgAdmin中以postgres用户身份执行
ALTER USER fantasy_user CREATEDB;
GRANT CREATE ON SCHEMA public TO fantasy_user;
```

### 端口占用
- 默认端口5432可能被占用，可以在PostgreSQL配置中更改
- 或在.env文件中使用不同端口

## 7. 验证设置

运行以下命令验证所有设置正确：

```bash
# 在项目根目录下
python -c "
from fantasy_etl.load.database.connection_manager import ConnectionManager
cm = ConnectionManager()
if cm.test_connection():
    print('✅ 数据库设置完成!')
else:
    print('❌ 数据库连接失败')
"
```

完成以上步骤后，你的PostgreSQL就可以与Fantasy ETL工具集成了！ 