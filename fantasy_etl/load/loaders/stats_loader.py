# 统计数据加载器
#
# 迁移来源: @database_writer.py 中的统计相关写入逻辑
# 主要映射:
#   - write_player_season_stat_values() -> StatsLoader.load_player_season_stats()
#   - write_player_daily_stat_values() -> StatsLoader.load_player_daily_stats()
#   - write_team_stat_values() -> StatsLoader.load_team_weekly_stats()
#   - write_team_weekly_stats_from_matchup() -> StatsLoader.load_team_stats_from_matchup()
#   - 各种统计数据模型的写入逻辑
#
# 职责:
#   - 球员赛季统计加载：
#     * PlayerSeasonStats模型写入：11个核心统计项
#     * 复合统计处理：FGM/A、FTM/A的拆分存储
#     * 百分比统计：FG%、FT%的标准化处理
#     * 累计统计：total_points、total_rebounds等字段
#     * 赛季级别聚合：整个赛季的统计汇总
#   - 球员日统计加载：
#     * PlayerDailyStats模型写入：相同的11个核心统计项
#     * 日期维度：date、week字段的处理
#     * 单日统计：points、rebounds（非total_前缀）
#     * 时间序列：按日期排序的统计历史
#   - 团队周统计加载：
#     * TeamStatsWeekly模型写入：团队级别的11个统计项
#     * 周度聚合：team在特定week的统计数据
#     * 从matchup提取：从对战数据中提取团队统计
#     * 团队表现：团队整体的统计表现
#   - 统计数据验证：
#     * 核心统计完整性：11个统计项的存在性检查
#     * 数值合理性：投篮命中不超过尝试等逻辑验证
#     * 百分比一致性：计算百分比与API百分比的对比
#     * 时间一致性：统计日期与赛季的匹配
#   - 数据类型转换：
#     * 安全整数转换：字符串数字的处理
#     * 百分比标准化：统一为0-100范围的decimal值
#     * 复合统计解析：'made/attempted'格式的拆分
#     * NULL值处理：缺失统计项的默认值设置
#   - 关联数据处理：
#     * 外键约束：player_key、team_key、league_key的引用
#     * 时间关联：date与DateDimension的关系
#     * 球员团队一致性：统计数据与球员归属的验证
#   - 去重和更新策略：
#     * 球员赛季统计：(player_key, season)唯一性
#     * 球员日统计：(player_key, date)唯一性
#     * 团队周统计：(team_key, season, week)唯一性
#     * 增量更新：新统计数据vs已有数据的更新
#   - 批量处理优化：
#     * 按类型分批：赛季统计、日统计、团队统计分别处理
#     * 按时间分组：同一时间段的统计数据聚合
#     * 内存管理：大量统计数据的内存优化
#     * 事务边界：统计数据的一致性保证
#   - 业务规则验证：
#     * 统计逻辑：made <= attempted等基本逻辑
#     * 累计一致性：赛季统计与日统计的累计关系
#     * 团队统计：团队统计与球员统计的聚合关系
#   - 11个核心统计项处理：
#     * FGM/FGA：投篮命中/尝试
#     * FG%：投篮命中率
#     * FTM/FTA：罚球命中/尝试
#     * FT%：罚球命中率
#     * 3PTM：三分球命中
#     * PTS：得分
#     * REB：篮板
#     * AST：助攻
#     * ST：抢断
#     * BLK：盖帽
#     * TO：失误
#   - 性能优化：
#     * 批量插入：SQLAlchemy bulk operations
#     * 索引利用：基于时间和球员的查询优化
#     * 缓存策略：频繁访问统计的缓存
#   - 统计和报告：
#     * 处理统计：各类统计数据的加载数量
#     * 时间覆盖：统计数据的时间范围
#     * 完整性报告：缺失统计的识别和报告
#
# 输入: 标准化的统计数据对象 (Dict)，包含11个核心统计项
# 输出: 统计数据加载结果和处理报告 