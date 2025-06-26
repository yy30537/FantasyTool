# 对战数据加载器
#
# 迁移来源: @database_writer.py 中的对战相关写入逻辑
# 主要映射:
#   - write_team_matchup() -> MatchupLoader.load_matchup()
#   - write_team_matchup_from_data() -> MatchupLoader.load_matchup_from_data()
#   - _parse_stat_winners() -> MatchupLoader.parse_stat_winners()
#   - _parse_teams_matchup_data() -> MatchupLoader.parse_teams_data()
#   - TeamMatchups模型写入逻辑
#
# 职责:
#   - 对战基础数据加载：
#     * TeamMatchups模型写入：team_key、week、opponent等
#     * 对战时间信息：week_start、week_end、status
#     * 对战结果：is_winner、is_tied、team_points等
#     * 特殊标记：is_playoffs、is_consolation等
#   - 统计类别获胜情况加载：
#     * 9个核心统计类别的获胜标记：wins_field_goal_pct等
#     * 从stat_winners解析：统计类别ID到获胜状态的映射
#     * 布尔字段设置：各统计类别的wins_*字段
#     * 获胜计算：team在各统计类别中的表现
#   - 比赛场次信息加载：
#     * 团队比赛状态：completed_games、remaining_games、live_games
#     * 对手比赛状态：opponent_*系列字段
#     * 比赛进度：对战双方的比赛完成情况
#     * 实时状态：live_games的实时更新
#   - 对战结果处理：
#     * 胜负判断：基于team_points vs opponent_points
#     * 平局处理：积分相等时的is_tied设置
#     * 获胜者记录：winner_team_key的设置和验证
#     * 积分验证：team_points的合理性检查
#   - 数据验证和清洗：
#     * 必需字段验证：team_key、season、week
#     * 对战逻辑验证：opponent_team_key的存在性
#     * 积分逻辑验证：胜负与积分的一致性
#     * 时间逻辑验证：week在合理范围内
#   - 关联数据处理：
#     * 外键约束：league_key、team_key的引用
#     * 对手关系：opponent_team_key的有效性
#     * 赛季一致性：season与League的匹配
#     * 周数验证：week在联盟赛程范围内
#   - 复杂数据解析：
#     * stat_winners解析：从API的stat_winners数组提取
#     * teams数据解析：从复杂嵌套结构提取对战信息
#     * 积分计算：从team_points字典提取总积分
#     * 比赛状态解析：从team_remaining_games提取
#   - 去重和更新策略：
#     * 唯一约束：(team_key, season, week)的唯一性
#     * 对战更新：对战结果的实时更新
#     * 状态变更：从live到completed的状态转换
#   - 业务规则验证：
#     * 对战配对：确保对战双方的数据一致性
#     * 季后赛规则：playoffs对战的特殊处理
#     * 积分规则：团队积分的计算逻辑验证
#     * 统计获胜：各统计类别获胜的合理性
#   - 批量对战处理：
#     * 周度批量：同一周所有对战的批量处理
#     * 对战关联：确保对战双方数据的同步更新
#     * 事务一致性：对战数据的事务完整性
#   - 统计类别映射：
#     * stat_id映射：API统计ID到数据库字段的映射
#     * 核心统计：9个关键统计类别的处理
#     * FG%(5)、FT%(8)、3PTM(10)、PTS(12)、REB(15)、AST(16)、ST(17)、BLK(18)、TO(19)
#   - 性能优化：
#     * 批量插入：多个对战的高效写入
#     * 索引利用：基于team和week的查询优化
#     * 数据缓存：对战状态的缓存策略
#   - 统计和报告：
#     * 处理统计：对战记录的加载数量
#     * 对战分布：各周对战的分布情况
#     * 胜率统计：团队胜率和对战表现
#     * 数据质量：对战数据的完整性报告
#
# 输入: 标准化的对战数据 (Dict)，包含stat_winners和teams信息
# 输出: 对战数据加载结果和统计信息 