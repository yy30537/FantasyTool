# 团队统计数据转换器
#
# 迁移来源: @database_writer.py 中的团队统计处理逻辑
# 主要映射:
#   - _extract_team_weekly_stats() -> transform_weekly_stats()
#   - write_team_weekly_stats_from_matchup() -> transform_matchup_stats()
#   - 团队统计数据的标准化转换逻辑
#
# 职责:
#   - 团队周统计转换：
#     * 从matchup数据中的team_stats提取统计信息
#     * 处理与球员统计相同的11个核心统计项
#     * 团队级别的复合统计：FGM/A、FTM/A
#     * 团队百分比统计：FG%、FT%转换
#     * 团队累计统计：points、rebounds、assists等
#   - Matchup数据解析：
#     * 从teams数据容器中提取特定团队的统计
#     * 处理复杂嵌套结构：teams["0"]["teams"][i]["team"]
#     * 定位目标团队：通过team_key匹配
#     * 提取team_stats容器：从team[1]["team_stats"]
#   - 统计数组处理：
#     * 解析stats数组：stats[i]["stat"]结构
#     * 构建stat_id到value的映射字典
#     * 应用与球员统计相同的转换逻辑
#   - 数据验证和清洗：
#     * 团队统计合理性验证
#     * 与对手数据的一致性检查
#     * 缺失统计项的处理策略
#   - 聚合计算：
#     * 团队总分计算验证
#     * 效率指标派生：团队投篮效率等
#     * 对战优势统计：各项统计的相对表现
#
# 输入: Matchup数据中的team_stats (Dict)
# 输出: 标准化的团队周统计对象 (Dict) 