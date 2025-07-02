# Fantasy Tool Project Structure

## 📁 项目架构概览

```
📁 FANTASYTOOL/                    # 项目根目录
│
├── 📁 fantasy_etl/                # 新ETL管道架构
│   │
│   ├── main.py                    # ETL主程序入口
│   │
│   ├── 📁 auth/                   # 认证模块 ✅ 已完成迁移
│   │   ├── __init__.py            # 模块初始化
│   │   ├── oauth_manager.py       # OAuth认证管理 (迁移自 yahoo_api_utils.py)
│   │   ├── token_storage.py       # 令牌存储管理 (迁移自 yahoo_api_utils.py)
│   │   └── web_auth_server.py     # Web授权服务器 (迁移自 app.py)
│   │
│   ├── 📁 extract/                # 提取层 ✅ 基础设施已完成，核心提取器开发中
│   │   ├── yahoo_client.py        # Yahoo API客户端 ✅ 已实现 (340行)
│   │   ├── rate_limiter.py        # API速率限制器 ✅ 已实现 (370行) 
│   │   ├── base_extractor.py      # 基础提取器类 ✅ 已实现 (500行)
│   │   ├── api_models.py          # API响应数据模型 ✅ 已实现 (620行)
│   │   └── extractors/            # 数据提取器集合
│   │       ├── game_extractor.py        # 游戏数据提取 ✅ 已实现稳定 (250行)
│   │       ├── league_extractor.py      # 联盟数据提取 ✅ 已实现稳定 (390行)  
│   │       ├── team_extractor.py        # 团队数据提取 ✅ 已实现稳定 (350行)
│   │       ├── player_extractor.py      # 球员数据提取 ✅ 已实现稳定 (480行)
│   │       ├── settings_extractor.py    # 联盟设置提取 ✅ 已实现稳定 (310行)
│   │       ├── stat_categories_extractor.py  # 统计类别提取 ✅ 已实现稳定 (290行)
│   │       ├── player_stats_extractor.py # 球员统计提取 ✅ 已实现稳定 (420行)
│   │       ├── roster_extractor.py      # 阵容数据提取 ✅ 已实现稳定 (380行)
│   │       ├── transaction_extractor.py # 交易数据提取 ✅ 已实现稳定 (450行)
│   │       ├── matchup_extractor.py     # 对战数据提取 ✅ 已实现稳定 (520行)
│   │       └── schedule_extractor.py    # 赛季日程提取 ✅ 已实现稳定 (310行)
│   │
│   ├── 📁 transform/              # 转换层 ✅ 已完成迁移
│   │   ├── 📁 parsers/            # 数据解析器 ✅ 7个解析器全部完成
│   │   │   ├── game_parser.py           # 游戏数据解析 ✅ 已实现 (480行)
│   │   │   ├── league_parser.py         # 联盟数据解析 ✅ 已实现 (680行)
│   │   │   ├── team_parser.py           # 团队数据解析 ✅ 已实现 (520行)
│   │   │   ├── player_parser.py         # 球员数据解析 ✅ 已实现 (650行)
│   │   │   ├── transaction_parser.py    # 交易数据解析 ✅ 已实现 (622行)
│   │   │   ├── matchup_parser.py        # 对战数据解析 ✅ 已实现 (766行)
│   │   │   └── standings_parser.py      # 排名数据解析 ✅ 已实现 (741行)
│   │   │
│   │   ├── 📁 normalizers/        # 数据标准化器 ✅ 3个标准化器全部完成
│   │   │   ├── player_normalizer.py     # 球员信息标准化 ✅ 已实现 (380行)
│   │   │   ├── stats_normalizer.py      # 统计数据标准化 ✅ 已实现 (620行)
│   │   │   └── position_normalizer.py   # 位置数据标准化 ✅ 已实现 (350行)
│   │   │
│   │   ├── 📁 stats/              # 统计数据转换器 ✅ 4个转换器全部完成
│   │   │   ├── player_stats_transformer.py  # 球员统计转换 ✅ 已实现 (480行)
│   │   │   ├── team_stats_transformer.py    # 团队统计转换 ✅ 已实现 (505行)
│   │   │   ├── matchup_stats_transformer.py # 对战统计转换 ✅ 已实现 (573行)
│   │   │   └── stat_utils.py                # 统计工具函数 ✅ 已实现 (420行)
│   │   │
│   │   ├── validators.py          # 数据验证器 ✅ 已实现 (685行)
│   │   ├── cleaners.py            # 数据清洗器 ✅ 已实现 (718行)
│   │   └── quality_checks.py      # 数据质量检查 ✅ 已实现 (1055行)
│   │
│   ├── 📁 load/                   # 加载层 ✅ 已完成，集成到主ETL管道并支持完整的E→T→L流程
│   │   ├── 📁 database/           # 数据库管理 ✅ 已完成
│   │   │   ├── connection_manager.py   # 连接管理 ✅ 已实现 (317行)
│   │   │   ├── session_manager.py      # 会话管理 ✅ 已实现 (418行)
│   │   │   └── models.py               # 数据模型 ✅ 已迁移 (861行)
│   │   │
│   │   ├── 📁 loaders/            # 数据加载器 ✅ 已完成所有17个表的加载器
│   │   │   ├── base_loader.py          # 基础加载器 ✅ 已实现 (450行)
│   │   │   ├── game_loader.py          # 游戏数据加载 ✅ 已实现 (150行)
│   │   │   ├── league_loader.py        # 联盟数据加载 ✅ 已实现 (650行)
│   │   │   ├── team_loader.py          # 团队数据加载 ✅ 已实现 (380行)
│   │   │   ├── player_loader.py        # 球员数据加载 ✅ 已实现 (320行)
│   │   │   ├── roster_loader.py        # 阵容数据加载 ✅ 已实现 (220行)
│   │   │   ├── transaction_loader.py   # 交易数据加载 ✅ 已实现 (420行)
│   │   │   ├── stats_loader.py         # 统计数据加载 ✅ 已实现 (410行)
│   │   │   ├── standings_loader.py     # 排名数据加载 ✅ 已实现 (290行)
│   │   │   ├── matchup_loader.py       # 对战数据加载 ✅ 已实现 (450行)
│   │   │   └── date_loader.py          # 日期维度加载 ✅ 已实现 (150行)
│   │   │
│   │   ├── loader_manager.py      # 加载器管理器 ✅ 已实现 (573行)，支持17个表
│   │   ├── batch_processor.py     # 批量处理器 ✅ 已实现 (180行)
│   │   ├── deduplicator.py        # 去重处理器 ✅ 已实现 (150行)  
│   │   ├── incremental_updater.py # 增量更新器 ✅ 已实现 (200行)
│   │   └── data_quality_enforcer.py # 数据质量强制器 ✅ 已实现 (170行)
│   │
│   ├── 📁 config/                 # 配置管理 ✅ 已完成迁移
│   │   ├── __init__.py            # 模块初始化和统一导出接口
│   │   ├── settings.py            # 统一配置管理器 (整合所有配置)
│   │   ├── database_config.py     # 数据库配置 (迁移自 model.py)
│   │   └── api_config.py          # API配置 (迁移自 yahoo_api_utils.py + app.py)
│   │
│   ├── 📁 monitoring/             # 监控模块 - 待实现
│   │   ├── metrics.py             # 指标收集
│   │   └── logging.py             # 日志管理
│   │
│   ├── 📁 analytics/              # 分析层 - 待实现
│   │   ├── aggregations.py        # 聚合计算
│   │   ├── feature_engineering.py # 特征工程
│   │   └── views.sql              # 分析视图
│   │
│   └── 📁 services/               # 业务服务 - 待实现
│       ├── team_analyzer.py       # 团队分析服务
│       ├── matchup_analyzer.py    # 对战分析服务
│       └── trade_analyzer.py      # 交易分析服务
│
├── 📁 sample_data/                # 样本数据
│   ├── league_info_454_l_53472.json
│   ├── league_players_454_l_53472.json
│   ├── league_settings_454_l_53472.json
│   └── (其他样本文件...)
│
├── 📁 scripts/                    # 旧脚本集 (待迁移后删除)
│   ├── app.py                     # OAuth认证服务器
│   ├── model.py                   # 数据库模型定义
│   ├── yahoo_api_data.py          # 统一数据获取工具
│   ├── yahoo_api_utils.py         # API工具函数
│   ├── database_writer.py         # 数据库写入器
│   └── (其他辅助脚本...)
│
├── 📁 tokens/                     # 认证令牌
│   └── yahoo_token.token          # Yahoo OAuth令牌
│
├── 📁 doc/                        # 项目文档
│   ├── 📁 yahoo-fantasy-sports-API-docs/  # Yahoo API文档
│   ├── database.md                # 数据库架构文档
│   ├── project-structure.md       # 项目结构文档 (本文件)
│   └── (其他文档...)
│
├── 📁 fantasy_etl/
│   └── MIGRATION_GUIDE.md         # 完整ETL迁移指南
│
├── 📁 venv/                       # Python虚拟环境
│
├── .env                           # 环境变量配置
├── .gitattributes                 # Git属性配置
├── README.md                      # 项目说明
└── requirements.txt               # Python依赖包
```



### 📋 已完成
- **Auth层** ✅ - OAuth认证和token管理
- **Extract层** ✅ - 完整的Yahoo Fantasy API数据提取
- **Transform层** ✅ - 数据解析、标准化和验证
- **Load层** ✅ - 完整的数据库加载功能和数据管理
- **ETL主管道** ✅ - 完整的Extract→Transform→Load流程集成
- **数据库覆盖** ✅ - 所有17个表的完整支持

### 📋 待实现
- **Load层工具组件** - 批量处理器、去重器、增量更新器
- **Monitoring层** - 监控和日志系统  
- **Analytics层** - 分析和聚合功能
- **Services层** - 业务服务接口

