

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
│   │   └── database_loader.py
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