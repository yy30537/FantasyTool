# Yahoo Fantasy Sports 工具

- team analysis 
- matchup analysis
- trade analysis 
- trade target suggestion (based on team analysis)
- add/drop suggestion (based on matchup analysis)
- streaming plan (based on matchup analysis)

## 🚀 快速开始

### 1. 环境设置

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境 (Linux/macOS)
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 数据库配置

创建 `.env` 文件：
```env
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db
```

### 3. Yahoo API授权

```bash
# 启动OAuth授权流程
python3 app.py
```

访问 `http://localhost:5000`，完成Yahoo账号授权，获取API访问令牌。

## 📊 数据获取



## 🗄️ 数据库模型

### 创建数据库表

```bash
# 使用model.py创建所有数据库表
python3 model.py
```


## 📈 数据查询示例





## 📁 项目结构

```
FantasyTool/

```

