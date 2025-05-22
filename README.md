
```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境 (Linux/macOS)
source venv/bin/activate

#安装所需依赖：
pip install -r requirements.txt

# 确保安装cryptography库（用于HTTPS支持）
pip install cryptography

# 使用Prisma作为ORM工具，方便地查询和管理数据。
npm init -y
npm install prisma typescript ts-node @prisma/client
npm install -D @types/node

# 初始化Prisma
npx prisma init

```

- 创建Prisma的schema文件 @schema.prisma ，连接PostgreSQL数据库
- 创建.env文件存储数据库连接信息
- 创建TypeScript脚本 @query-data.ts 来查询数据
- 创建 @package.json 文件，安装依赖
- 创建 @tsconfig.json配置文件

```bash

# 安装依赖
npm install

# 生成Prisma客户端
npx prisma generate

# 运行查询脚本查看数据
npm run query



```

Web界面 http://localhost:3000 
- Express服务器(web/server.ts)
- 主页视图(index.ejs)显示游戏和用户信息
- 游戏详情页(game.ejs)展示游戏和关联联盟


获取JSON数据的API端点
- `/api/games`
- `/api/leagues`
- `/api/teams`


```bash
# 初始化数据库
python3 load_db.py --init-db

# 更新游戏数据
python3 load_db.py --games

# 更新联盟数据
python3 load_db.py --leagues

# 重新创建数据库表并加载数据
python3 load_db.py --recreate-db

# 获取数据流程
python3 fetch_games.py # 获取游戏数据
python3 fetch_leagues.py # 获取联盟数据
python3 fetch_data.py --game-key <game_key> # 获取特定游戏的所有相关数据（包括团队）

# 加载数据到数据库

# 如果是首次加载或需要重建表结构：
python3 load_db.py --recreate-db #删除并重新创建所有表

# 加载数据：
python3 load_db.py # 加载所有数据

# 使用 --games, --leagues, --teams 参数只加载特定类型的数据



```


