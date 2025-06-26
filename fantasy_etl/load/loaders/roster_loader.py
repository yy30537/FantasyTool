# 阵容数据加载器
#
# 迁移来源: @database_writer.py 中的阵容相关写入逻辑
# 主要映射:
#   - write_roster_daily() -> RosterLoader.load_roster_daily()
#   - RosterDaily模型写入逻辑 -> 阵容数据加载功能
#
# 职责:
#   - 每日阵容数据加载：
#     * RosterDaily模型数据写入：team_key、player_key、date等
#     * 时间维度信息：date、season、week字段
#     * 位置分配信息：selected_position、is_starting等
#     * 球员状态信息：player_status、injury_note等
#   - 阵容位置分类：
#     * 首发状态：is_starting基于selected_position判断
#     * 替补状态：is_bench (selected_position == 'BN')
#     * 伤病名单：is_injured_reserve (position in ['IL', 'IR'])
#     * 位置验证：selected_position的有效性检查
#   - Keeper和Fantasy信息：
#     * Keeper状态：is_keeper、keeper_cost处理
#     * Fantasy设置：is_prescoring、is_editable标记
#     * 成本信息：keeper_cost字符串处理
#   - 时间序列数据管理：
#     * 日期范围处理：单日vs多日阵容数据
#     * 历史数据保持：已有阵容记录的保护
#     * 增量更新：新增日期的阵容数据
#     * 时间戳管理：更新时间的记录
#   - 数据验证和清洗：
#     * 必需字段验证：team_key、player_key、date、league_key
#     * 日期格式验证：date字段的正确性
#     * 位置代码验证：selected_position的有效性
#     * 状态字段验证：各种布尔标记的一致性
#   - 关联数据处理：
#     * 外键约束：team_key、player_key、league_key的引用
#     * 数据完整性：阵容-球员-团队关系的一致性
#     * 赛季一致性：date与season的匹配验证
#   - 去重和冲突处理：
#     * 唯一约束：(team_key, player_key, date)的唯一性
#     * 更新vs插入：已存在阵容记录的更新策略
#     * 冲突解决：同一日期多个位置分配的处理
#   - 批量处理优化：
#     * 按日期分批：同一日期的阵容批量处理
#     * 按团队分组：同一团队的阵容数据聚合
#     * 事务管理：大批量阵容数据的事务边界
#     * 内存管理：大时间范围数据的内存控制
#   - 业务规则验证：
#     * 阵容规则检查：首发位置数量限制
#     * 球员可用性：球员是否属于指定团队
#     * 日期合理性：阵容日期在赛季范围内
#     * 位置合理性：球员是否能打指定位置
#   - 增量和历史数据：
#     * 历史快照：保持每日阵容的完整记录
#     * 变更追踪：阵容变化的识别和记录
#     * 数据回填：补充缺失日期的阵容数据
#   - 统计和报告：
#     * 处理统计：加载的阵容记录数量
#     * 时间覆盖：处理的日期范围统计
#     * 团队分布：各团队的阵容数据量
#     * 位置分布：各位置的使用统计
#
# 输入: 标准化的阵容数据 (Dict或List[Dict])
# 输出: 阵容数据加载结果和统计信息 