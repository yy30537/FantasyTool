
## 关于Yahoo Fantasy API时间序列数据支持的分析

根据你对`Yahoo-Fantasy-Sports-API-Guide.md`的详细查看，以下是支持时间选择的数据类型：

### 1. **Roster数据 - 支持按时间获取**
- **NFL**: 按周获取 - `roster;week=10`
- **MLB/NBA/NHL**: 按日期获取 - `roster;date=2011-05-01`
- **示例URL**: 
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;week=10`
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date=2011-05-01`

### 2. **球员统计数据 - 支持多种时间维度**
- **Season统计**: `stats;type=season` (整赛季数据)
- **Weekly统计** (NFL): `stats;type=week;week=10`  
- **Daily统计** (MLB/NBA/NHL): `stats;type=date;date=2011-07-06`
- **示例URL**:
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/stats;type=season`
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/stats;type=date;date=2011-07-06`

### 3. **团队统计数据 - 支持时间维度**
- 与球员统计类似，支持season、week、date等维度

### 4. **Matchups数据 - 支持按周获取**
- **NFL**: `matchups;weeks=1,5` (可以指定多个周)
- **示例URL**: `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups;weeks=1,5`

### 5. **Players Collection过滤器 - 支持时间排序**
- `sort_type`: season, date, week, lastweek, lastmonth
- `sort_season`: 年份
- `sort_date`: YYYY-MM-DD (棒球、篮球、冰球)
- `sort_week`: 周数 (橄榄球)


1. **我们已经有了完整的数据结构理解**，通过现有的JSON数据和数据库模型
2. **不需要把每天的数据都存储为JSON**，可以直接存入数据库
3. **时间序列数据应该直接入库**，这样更高效且便于查询分析

## 时间序列数据处理建议

### 数据库设计方面：
1. **Roster表**：已经有`coverage_date`字段，可以存储不同日期的roster快照
2. **PlayerStats表**：可以增加时间维度字段（`coverage_type`, `week`, `date`, `season`）
3. **新增时间维度表**：考虑增加专门的时间序列统计表

### 实现策略：
1. **获取历史数据**：使用API的时间参数循环获取不同时期的数据
2. **增量更新**：定期更新最新数据，避免重复获取
3. **数据完整性**：确保时间序列数据的连续性和一致性

这样我们就可以构建一个完整的时间序列数据库，支持历史分析、趋势追踪等高级分析功能！
