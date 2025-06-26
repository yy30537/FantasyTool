# 联盟数据加载器
#
# 迁移来源: @database_writer.py 中的联盟相关写入逻辑
# 主要映射:
#   - write_leagues_data() -> LeagueLoader.load_leagues()
#   - write_league_settings() -> LeagueLoader.load_league_settings()
#   - write_stat_categories() -> LeagueLoader.load_stat_categories()
#   - write_league_roster_positions() -> LeagueLoader.load_roster_positions()
#
# 职责:
#   - 联盟基础数据加载：
#     * League模型数据写入：league_key、name、game_key等
#     * 联盟配置信息：num_teams、scoring_type、draft_status等
#     * 时间信息处理：start_date、end_date、current_week等
#     * 布尔值字段标准化：is_finished、is_pro_league等
#   - 联盟设置数据加载：
#     * LeagueSettings模型写入：draft_type、playoff配置等
#     * 一对一关系维护：League与LeagueSettings的关联
#     * JSON字段处理：roster_positions等复杂配置
#     * 设置完整性验证：playoff、waiver等配置的合理性
#   - 统计类别数据加载：
#     * StatCategory模型写入：stat_id、name、display_name等
#     * 核心统计标记：is_core_stat字段的设置
#     * 统计分组处理：group_name、sort_order等
#     * 统计启用状态：is_enabled、is_only_display_stat等
#   - 阵容位置配置加载：
#     * LeagueRosterPosition模型写入：position、count等
#     * 位置类型分类：position_type、is_starting_position
#     * 阵容规则解析：从JSON配置到结构化数据
#   - 数据关系管理：
#     * 外键关系维护：league_key到Game表的引用
#     * 级联数据处理：联盟相关的所有配置数据
#     * 关系完整性检查：确保所有关联数据的一致性
#   - 去重和更新策略：
#     * 联盟主键冲突处理：league_key唯一性
#     * 设置更新逻辑：已存在联盟的设置更新
#     * 增量配置更新：统计类别和位置配置的变更
#   - 批量处理优化：
#     * 分层批量写入：先联盟、再设置、最后配置
#     * 事务协调：多表写入的事务一致性
#     * 错误恢复：部分失败时的数据回滚
#   - 验证和质量检查：
#     * 联盟配置合理性：team数量、playoff配置等
#     * 统计类别完整性：必需统计项的存在检查
#     * 阵容位置逻辑：位置数量和类型的合理性
#
# 输入: 联盟数据、设置数据、统计类别数据、位置配置数据
# 输出: 联盟数据加载结果和统计信息 