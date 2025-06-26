# Fantasy Tool Project Structure

## 📁 项目架构概览

```
📁 FANTASYTOOL/                    # 项目根目录
│
├── 📁 fantasy_etl/                # 新ETL管道架构
│   │
│   ├── main.py                    # ETL主程序入口
│   │
│   ├── 📁 auth/                   # 认证模块 (已创建设计注释)
│   │   ├── __init__.py            # 模块初始化
│   │   ├── oauth_manager.py       # OAuth认证管理 (来自 yahoo_api_utils.py)
│   │   ├── token_storage.py       # 令牌存储管理 (来自 yahoo_api_utils.py)
│   │   └── web_auth_server.py     # Web授权服务器 (来自 app.py)
│   │
│   ├── 📁 extract/                # 提取层 (已创建设计注释)
│   │   ├── yahoo_client.py        # Yahoo API客户端
│   │   ├── rate_limiter.py        # API速率限制器
│   │   ├── base_extractor.py      # 基础提取器类
│   │   ├── api_models.py          # API响应数据模型
│   │   └── extractors/            # 数据提取器集合
│   │       ├── game_extractor.py        # 游戏数据提取 (已创建注释)
│   │       ├── league_extractor.py      # 联盟数据提取 (已创建注释)
│   │       ├── settings_extractor.py    # 联盟设置提取 (待实现)
│   │       ├── stat_categories_extractor.py  # 统计类别提取 (待实现)
│   │       ├── team_extractor.py        # 团队数据提取 (已创建注释)
│   │       ├── player_extractor.py      # 球员数据提取 (已创建注释)
│   │       ├── player_stats_extractor.py # 球员统计提取 (已创建注释)
│   │       ├── roster_extractor.py      # 阵容数据提取 (已创建注释)
│   │       ├── transaction_extractor.py # 交易数据提取 (已创建注释)
│   │       ├── matchup_extractor.py     # 对战数据提取 (已创建注释,包含team_stats)
│   │       └── schedule_extractor.py    # 赛季日程提取 (已创建注释)
│   │
│   ├── 📁 transform/              # 转换层 (已创建设计注释)
│   │   ├── 📁 parsers/            # 数据解析器
│   │   │   ├── game_parser.py           # 游戏数据解析
│   │   │   ├── league_parser.py         # 联盟数据解析
│   │   │   ├── team_parser.py           # 团队数据解析
│   │   │   ├── player_parser.py         # 球员数据解析
│   │   │   ├── transaction_parser.py    # 交易数据解析
│   │   │   ├── matchup_parser.py        # 对战数据解析
│   │   │   └── standings_parser.py      # 排名数据解析
│   │   │
│   │   ├── 📁 normalizers/        # 数据标准化器
│   │   │   ├── player_normalizer.py     # 球员信息标准化
│   │   │   ├── stats_normalizer.py      # 统计数据标准化
│   │   │   └── position_normalizer.py   # 位置数据标准化
│   │   │
│   │   ├── 📁 stats/              # 统计数据转换器
│   │   │   ├── player_stats_transformer.py  # 球员统计转换
│   │   │   ├── team_stats_transformer.py    # 团队统计转换
│   │   │   ├── matchup_stats_transformer.py # 对战统计转换
│   │   │   └── stat_utils.py                # 统计工具函数
│   │   │
│   │   ├── validators.py          # 数据验证器
│   │   ├── cleaners.py            # 数据清洗器
│   │   └── quality_checks.py      # 数据质量检查
│   │
│   ├── 📁 load/                   # 加载层 (已创建设计注释)
│   │   ├── 📁 database/           # 数据库管理
│   │   │   ├── connection_manager.py   # 连接管理
│   │   │   ├── session_manager.py      # 会话管理
│   │   │   └── models.py               # 数据模型 (迁移自scripts/model.py)
│   │   │
│   │   ├── 📁 loaders/            # 数据加载器
│   │   │   ├── base_loader.py          # 基础加载器
│   │   │   ├── game_loader.py          # 游戏数据加载
│   │   │   ├── league_loader.py        # 联盟数据加载
│   │   │   ├── team_loader.py          # 团队数据加载
│   │   │   ├── player_loader.py        # 球员数据加载
│   │   │   ├── roster_loader.py        # 阵容数据加载
│   │   │   ├── transaction_loader.py   # 交易数据加载
│   │   │   ├── stats_loader.py         # 统计数据加载
│   │   │   ├── standings_loader.py     # 排名数据加载
│   │   │   └── matchup_loader.py       # 对战数据加载
│   │   │
│   │   ├── batch_processor.py     # 批量处理器
│   │   ├── deduplicator.py        # 去重处理器
│   │   ├── incremental_updater.py # 增量更新器
│   │   └── data_quality_enforcer.py # 数据质量强制器
│   │
│   ├── 📁 config/                 # 配置管理 (已创建设计注释)
│   │   ├── __init__.py            # 模块初始化
│   │   ├── settings.py            # 统一配置管理 (整合所有配置)
│   │   ├── database_config.py     # 数据库配置 (来自 model.py)
│   │   └── api_config.py          # API配置 (来自 yahoo_api_utils.py)
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

## 🔄 迁移状态

### ✅ 已完成
- **Extract层架构设计** - 11个提取器已创建，9个已有设计注释，2个待实现
- **Transform层架构设计** - 完整的转换层组件已创建，包含详细注释
- **Load层架构设计** - 完整的加载层组件已创建，包含详细注释
- **Auth层架构设计** - 认证模块已创建，包含详细注释
- **Config层架构设计** - 配置管理已创建，包含详细注释
- **完整迁移指南** - 详细的迁移映射和实施计划

### 📊 Extract层详细状态
- **核心业务实体提取器**: game_extractor.py, league_extractor.py, player_extractor.py (已创建注释)
- **团队相关提取器**: team_extractor.py (已创建注释)
- **时间序列数据提取器**: player_stats_extractor.py, roster_extractor.py (已创建注释)
- **交易和对战提取器**: transaction_extractor.py, matchup_extractor.py (已创建注释)
- **辅助数据提取器**: schedule_extractor.py (已创建注释)
- **待实现提取器**: settings_extractor.py, stat_categories_extractor.py (空文件，需要实现)

### 🚧 进行中
- **代码实现阶段** - 所有模块的具体代码实现

### 📋 待实现
- **Monitoring层** - 监控和日志系统
- **Analytics层** - 分析和聚合功能
- **Services层** - 业务服务接口

### 🎯 迁移优先级
1. **🔴 高优先级**: Config → Auth → Load数据库组件
2. **🟡 中优先级**: 
   - 完成Extract层（settings_extractor.py, stat_categories_extractor.py）
   - Transform核心 → 主要数据流ETL
3. **🟢 低优先级**: 高级功能 → 性能优化 → 扩展功能

### 🎯 Extract层具体任务映射
- **game_extractor.py**: 对应 `_fetch_games_data()` ✅ 设计完成
- **league_extractor.py**: 对应 `_fetch_leagues_data(), _fetch_league_settings()` ✅ 设计完成  
- **player_extractor.py**: 对应 `fetch_complete_players_data(), _fetch_all_league_players()` ✅ 设计完成
- **team_extractor.py**: 对应 `fetch_teams_data(), _write_teams_to_db()` ✅ 设计完成
- **player_stats_extractor.py**: 对应 `_fetch_player_season_stats(), _fetch_player_daily_stats_for_range()` ✅ 设计完成
- **roster_extractor.py**: 对应 `fetch_team_rosters(), fetch_team_rosters_for_date_range()` ✅ 设计完成
- **transaction_extractor.py**: 对应 `fetch_complete_transactions_data(), _fetch_all_league_transactions()` ✅ 设计完成
- **matchup_extractor.py**: 对应 `_fetch_team_matchups(), _fetch_and_process_league_standings()` ✅ 设计完成
- **schedule_extractor.py**: 对应 `fetch_season_schedule_data()` ✅ 设计完成
- **settings_extractor.py**: 联盟设置提取 ❌ 待实现
- **stat_categories_extractor.py**: 统计类别提取 ❌ 待实现

## 📊 数据流设计

```
Yahoo API → Extract → Transform → Load → Database
     ↓         ↓         ↓         ↓         ↓
  认证管理   数据提取   格式转换   批量写入   PostgreSQL
  速率控制   分页处理   数据清洗   去重处理   关系约束
  错误重试   结构解析   质量检查   增量更新   索引优化
```

## 🎯 迁移目标

1. **功能完整性** - 保持scripts中所有现有功能
2. **数据一致性** - 确保相同的数据库输出结果  
3. **性能优化** - 维持或改进现有性能表现
4. **代码质量** - 提升代码结构和可维护性
5. **扩展性** - 为未来功能扩展预留空间

## 📝 下一步计划

### 🔴 立即行动（高优先级）
1. **完成Extract层剩余组件** 
   - 实现 `settings_extractor.py` (联盟设置提取)
   - 实现 `stat_categories_extractor.py` (统计类别提取)
2. **实现Config和Auth模块** (其他模块的基础依赖)
3. **实现Load层数据库组件** (数据输出的核心)

### 🟡 后续实施（中优先级）  
4. **实现Extract层核心功能** (完善已有设计注释的提取器)
5. **实现Transform层核心功能** (数据处理基础)
6. **实现主要数据流ETL流程** (联盟、球员、对战等)

### 🟢 最终验证（低优先级）
7. **端到端测试和验证** (确保输出一致性)
8. **性能对比和优化** (保持或提升性能)
9. **完整兼容性测试** (现有脚本继续正常工作)
10. **安全删除scripts目录** (迁移完成后)
