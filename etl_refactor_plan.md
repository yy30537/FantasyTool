# Fantasy Basketball ETL Pipeline 重构计划

## 🎯 重构目标

将现有prototype ETL管道升级为production-ready系统，支持以下Fantasy分析功能：
- Team Analysis (团队分析)
- Matchup Analysis (对战分析) 
- Trade Suggestions (交易建议)
- Add/Drop Recommendations (球员推荐)
- Streaming Plans

## 📊 现状分析

### 当前架构问题
1. **职责混合**: Extract和Transform混合在`yahoo_api_data.py`
2. **缺少数据质量保证**: 无验证、清洗、异常处理
3. **全量更新**: 每次重新获取所有数据，效率低
4. **监控缺失**: 无法跟踪ETL过程健康状况
5. **配置硬编码**: 缺少灵活的配置管理
6. **分析优化不足**: 数据结构未针对Fantasy用例优化

## 🏗️ 新架构设计

### 分层架构
```
📁 fantasy_etl/
├── 📁 extract/              # 提取层
│   ├── yahoo_client.py      # API客户端 + 认证
│   ├── rate_limiter.py      # 速率控制
│   └── extractors/          # 数据提取器
│       ├── league_extractor.py
│       ├── player_extractor.py
│       └── stats_extractor.py
├── 📁 transform/            # 转换层  
│   ├── validators.py        # 数据验证
│   ├── cleaners.py         # 数据清洗
│   ├── transformers.py     # 业务转换
│   └── quality_checks.py   # 质量检查
├── 📁 load/                # 加载层
│   ├── incremental.py      # 增量更新
│   ├── change_detector.py  # 变更检测
│   └── database_loader.py
├── 📁 config/              # 配置管理
│   └── settings.py         # 配置定义
├── 📁 monitoring/          # 监控
│   ├── metrics.py          # 指标收集
│   └── logging.py          # 日志管理
├── 📁 analytics/           # 分析层
│   ├── aggregations.py     # 聚合计算
│   ├── feature_engineering.py
│   └── views.sql           # 分析视图
└── 📁 services/            # 业务服务
    ├── team_analyzer.py
    ├── matchup_analyzer.py
    └── trade_analyzer.py
```

## 🚀 重构路线图 (7周计划)

### Phase 1: 架构分层 (Week 1-2)

**目标**: 分离关注点，建立清晰的层次结构

#### 核心实现
```python
# extract/yahoo_client.py - 统一API客户端
class YahooFantasyClient:
    def __init__(self, config):
        self.auth = OAuthManager(config)
        self.rate_limiter = RateLimiter(requests_per_minute=60)
        self.circuit_breaker = CircuitBreaker()
    
    async def fetch_with_retry(self, url, max_retries=3):
        for attempt in range(max_retries):
            try:
                async with self.rate_limiter:
                    return await self._make_request(url)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)

# transform/validators.py - Pydantic数据验证
class PlayerData(BaseModel):
    player_key: str
    full_name: str
    position: str
    
    @validator('player_key')
    def validate_player_key(cls, v):
        if not v.startswith('nba.p.'):
            raise ValueError('Invalid player_key')
        return v

# load/incremental_loader.py - 增量更新
class IncrementalLoader:
    async def upsert_players(self, players: List[Dict]):
        existing_players = await self.db.get_existing_players()
        changes = self.change_detector.detect_changes(players, existing_players)
        
        async with self.db.transaction():
            if changes.inserts:
                await self.db.bulk_insert('players', changes.inserts)
            if changes.updates:
                await self.db.bulk_update('players', changes.updates)
```

### Phase 2: 数据质量保证 (Week 2-3)

**目标**: 建立数据质量检查和清洗机制

```python
# transform/quality_checks.py - 数据质量框架
class DataQualityChecker:
    def check_completeness(self, data: List[Dict], required_fields: List[str]) -> QualityReport:
        total_records = len(data)
        incomplete_records = sum(1 for record in data 
                               if any(not record.get(field) for field in required_fields))
        
        completeness_rate = 1 - (incomplete_records / total_records)
        
        return QualityReport(
            check_name='completeness',
            passed=completeness_rate >= 0.95,
            score=completeness_rate,
            details=f'{incomplete_records} of {total_records} records incomplete'
        )

# transform/cleaners.py - 数据清洗
class StatsCleaner:
    def clean_player_stats(self, stats: Dict) -> Dict:
        cleaned = stats.copy()
        
        # 处理负值 (除了失误)
        for stat_name, value in stats.items():
            if stat_name != 'turnovers' and value < 0:
                cleaned[stat_name] = 0
        
        # 处理异常高值
        if stats.get('points', 0) > 100:
            cleaned['points'] = None
            
        return cleaned
```

### Phase 3: 增量更新策略 (Week 3-4)

**目标**: 实现智能的增量数据更新

```python
# load/change_detector.py - 智能变更检测
class ChangeDetector:
    def detect_player_changes(self, new_players: List[Dict], 
                            existing_players: Dict[str, Dict]) -> ChangeSet:
        changes = ChangeSet()
        
        for player in new_players:
            player_key = player['player_key']
            existing = existing_players.get(player_key)
            
            if not existing:
                changes.add_insert(player)
            elif self._has_significant_changes(player, existing):
                changes.add_update(player)
        
        return changes
    
    def _has_significant_changes(self, new: Dict, existing: Dict) -> bool:
        important_fields = ['full_name', 'current_team_key', 'status', 'display_position']
        return any(new.get(field) != existing.get(field) for field in important_fields)
```

### Phase 4: 监控和配置 (Week 4-5)

**目标**: 建立ETL过程的可观测性和灵活配置

```python
# monitoring/etl_monitor.py - ETL监控
class ETLMonitor:
    @contextmanager
    def track_stage(self, stage_name: str):
        start_time = time.time()
        try:
            yield StageTracker()
            duration = time.time() - start_time
            self.metrics_collector.record(ETLMetrics(
                stage_name=stage_name,
                duration_seconds=duration,
                errors_count=0
            ))
            self.logger.info(f'{stage_name} completed successfully')
        except Exception as e:
            self.logger.error(f'{stage_name} failed', error=str(e))
            raise

# config/settings.py - 配置管理
class ETLConfig(BaseSettings):
    # Yahoo API配置
    yahoo_client_id: str
    yahoo_client_secret: str
    api_rate_limit: int = 60
    
    # 数据库配置
    database_url: str
    db_pool_size: int = 10
    
    # ETL配置
    batch_size: int = 100
    quality_threshold: float = 0.95
    
    # 数据质量规则
    required_fields: Dict[str, List[str]] = {
        'players': ['player_key', 'full_name', 'position'],
        'teams': ['team_key', 'name', 'league_key'],
        'daily_stats': ['player_key', 'date']
    }
    
    class Config:
        env_file = ".env"
        env_prefix = "FANTASY_ETL_"
```

### Phase 5: 分析优化 (Week 5-6)

**目标**: 为Fantasy分析创建优化的数据结构

```sql
-- analytics/views.sql - 分析视图
-- 球员周表现汇总
CREATE MATERIALIZED VIEW mv_player_weekly_performance AS
SELECT 
    p.player_key,
    p.full_name,
    p.display_position,
    EXTRACT(week FROM pds.date) as week_number,
    AVG(pds.points) as avg_points,
    AVG(pds.rebounds) as avg_rebounds,
    AVG(pds.assists) as avg_assists,
    COUNT(*) as games_played
FROM players p
JOIN player_daily_stats pds ON p.player_key = pds.player_key
WHERE pds.date >= CURRENT_DATE - INTERVAL '8 weeks'
GROUP BY p.player_key, p.full_name, p.display_position, EXTRACT(week FROM pds.date);

-- 团队类别强度排名
CREATE MATERIALIZED VIEW mv_team_category_rankings AS
SELECT 
    t.team_key,
    t.name,
    tw.week,
    RANK() OVER (PARTITION BY tw.week ORDER BY tw.points DESC) as points_rank,
    RANK() OVER (PARTITION BY tw.week ORDER BY tw.rebounds DESC) as rebounds_rank,
    RANK() OVER (PARTITION BY tw.week ORDER BY tw.assists DESC) as assists_rank
FROM teams t
JOIN team_stats_weekly tw ON t.team_key = tw.team_key;
```

```python
# analytics/feature_engineering.py - 特征工程
class FantasyFeatureEngineer:
    def calculate_player_value_metrics(self, player_stats: pd.DataFrame) -> pd.DataFrame:
        df = player_stats.copy()
        
        # 计算Z-Score (标准化得分)
        scoring_categories = ['points', 'rebounds', 'assists', 'steals', 'blocks']
        
        for category in scoring_categories:
            df[f'{category}_zscore'] = zscore(df[category].fillna(0))
        
        # 失误是负向指标
        df['turnovers_zscore'] = -zscore(df['turnovers'].fillna(0))
        
        # 计算综合价值得分
        all_zscore_cols = [f'{cat}_zscore' for cat in scoring_categories] + ['turnovers_zscore']
        df['total_value_score'] = df[all_zscore_cols].sum(axis=1)
        
        return df
```

### Phase 6: 业务服务实现 (Week 6-7)

**目标**: 实现Fantasy分析核心业务逻辑

```python
# services/team_analyzer.py - 团队分析服务
class TeamAnalyzer:
    async def analyze_team(self, team_key: str, weeks: int = 4) -> TeamAnalysis:
        # 获取团队数据
        team_info = await self.db.get_team_info(team_key)
        team_stats = await self.db.get_team_weekly_stats(team_key, weeks)
        league_stats = await self.db.get_league_weekly_stats(team_info.league_key, weeks)
        
        # 计算各类别排名
        category_rankings = self._calculate_category_rankings(team_stats, league_stats)
        
        # 识别优势和劣势
        strengths = [cat for cat, rank in category_rankings.items() if rank <= 3]
        weaknesses = [cat for cat, rank in category_rankings.items() if rank >= 10]
        
        return TeamAnalysis(
            team_key=team_key,
            team_name=team_info.name,
            category_rankings=category_rankings,
            strengths=strengths,
            weaknesses=weaknesses,
            recommendations=self._generate_recommendations(strengths, weaknesses)
        )

# services/matchup_analyzer.py - 对战分析服务
class MatchupAnalyzer:
    async def analyze_matchup(self, team_key: str, opponent_key: str, week: int) -> MatchupAnalysis:
        # 获取双方预期统计
        team_projection = await self._get_team_projection(team_key, week)
        opponent_projection = await self._get_team_projection(opponent_key, week)
        
        # 计算类别优势
        category_advantages = self.fe.calculate_matchup_advantages(team_projection, opponent_projection)
        
        # 统计预计获胜类别
        categories_won = sum(1 for advantage in category_advantages.values() if advantage > 0)
        
        return MatchupAnalysis(
            team_key=team_key,
            opponent_key=opponent_key,
            week=week,
            category_advantages=category_advantages,
            projected_categories_won=categories_won,
            win_probability=categories_won / len(category_advantages),
            recommendations=self._generate_matchup_recommendations(category_advantages)
        )
```

## 📈 成功指标

### 技术指标
- **数据质量**: >95% 数据通过质量检查
- **处理效率**: ETL完整流程 <30分钟
- **系统可用性**: >99% uptime
- **增量更新率**: >80% 数据使用增量更新

### 业务指标
- **分析准确性**: 对战预测准确率 >70%
- **响应性能**: 分析请求响应时间 <3秒
- **用户价值**: 建议采用率 >40%

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

这个重构计划将在7周内将prototype升级为production-ready的ETL管道，为Fantasy Basketball分析工具提供坚实的数据基础。 