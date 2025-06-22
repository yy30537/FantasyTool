
## 🏗️ 新架构设计

```markdown

📁 FANTASYTOOL/             # root
│
├── 📁 fantasy_etl/
│   │
│   ├── main.py
│   │
│   ├── 📁 auth/
│   │   └── OAuthHelper.py
│   │
│   ├── 📁 extract/              # 提取层
│   │   ├── yahoo_client.py      # API客户端 + 认证
│   │   ├── rate_limiter.py      # 速率控制
│   │   └── extractors/          # 数据提取器
│   │       ├── game_extractor.py 
│   │       ├── league_extractor.py
│   │       ├── matchup_extractor.py
│   │       ├── player_extractor.py
│   │       ├── player_stats_extractor.py
│   │       ├── roster_extractor.py
│   │       ├── schedule_extractor.py
│   │       ├── team_extractor.py
│   │       ├── team_stats_extractor.py
│   │       └── transaction_extractor.py
│   │
│   ├── 📁 transform/            # 转换层  
│   │   ├── validators.py        # 数据验证
│   │   ├── cleaners.py          # 数据清洗
│   │   ├── quality_checks.py    # 业务转换
│   │   └── transformers/...
│   │
│   ├── 📁 load/                # 加载层
│   │   ├── incremental.py      # 增量更新
│   │   ├── change_detector.py  # 变更检测
│   │   └── loaders/...
│   │
│   ├── 📁 config/              # 配置管理
│   │   └── settings.py         # 配置定义
│   │
│   ├── 📁 monitoring/          # 监控
│   │   ├── metrics.py          # 指标收集
│   │   └── logging.py          # 日志管理
│   │
│   ├── 📁 analytics/           # 分析层
│   │   ├── aggregations.py     # 聚合计算
│   │   ├── feature_engineering.py
│   │   └── views.sql           # 分析视图
│   │
│   └── 📁 services/            # 业务服务
│       ├── team_analyzer.py
│       ├── matchup_analyzer.py
│       └── trade_analyzer.py
│
├── 📁 sample_data/...
│
├── 📁 scripts/                 # 旧脚本
│   ├── app.py
│   ├── database_writer.py
│   ├── fetch_sample_data.py
│   ├── model.py
│   ├── yahoo_api_data.py
│   └── yahoo_api_utils.py
│
├── 📁 tokens/
│   └── yahoo_token.token
│
├── 📁 venv/...
│
├── 📁 doc/
│   ├── 📁 yahoo-fantasy-sports-API-docs/...
│   ├── database.md
│   ├── etl_refactor_plan.md
│   └── project-structure.md
│
├── .env
│
├── .gitattributes
│
├── README.md
│
├── requirements.txt


```

## 🛠️ 技术栈

### 核心技术
- **Python 3.11+** with **asyncio**
- **PostgreSQL 15+** 
- **SQLAlchemy 2.0** (async)
- **pydantic** (数据验证)

### ETL组件
- **aiohttp** (异步HTTP)
- **asyncpg** (PostgreSQL驱动)
- **pandas** (数据处理)
- **structlog** (结构化日志)

### 监控运维
- **prometheus-client** (指标)
- **pytest** (测试)

## 🎯 执行里程碑

| Week | 阶段 | 主要交付物 |
|------|------|-----------|
| 1-2  | 架构分层 | 分层代码结构, Extract/Transform/Load分离 |
| 2-3  | 数据质量 | 验证框架, 质量检查, 清洗规则 |
| 3-4  | 增量更新 | 变更检测, 智能更新策略 |
| 4-5  | 监控配置 | ETL监控, 配置管理, 日志系统 |
| 5-6  | 分析优化 | 物化视图, Feature Engineering |
| 6-7  | 服务实现 | 团队分析, 对战分析API |
