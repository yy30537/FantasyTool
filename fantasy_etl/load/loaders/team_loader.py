# 团队数据加载器
#
# 迁移来源: @database_writer.py 中的团队相关写入逻辑
# 主要映射:
#   - write_teams_batch() -> TeamLoader.load_teams()
#   - Team和Manager模型写入逻辑 -> 团队数据加载功能
#   - _write_teams_to_db() -> 批量团队处理逻辑
#
# 职责:
#   - 团队数据加载：
#     * Team模型数据写入：team_key、name、league_key等
#     * 团队基本信息：url、team_logo_url、division_id等
#     * 团队状态信息：waiver_priority、number_of_moves等
#     * 团队成就标记：clinched_playoffs、has_draft_grade等
#   - 管理员数据加载：
#     * Manager模型数据写入：manager_id、team_key、nickname等
#     * 管理员角色信息：is_commissioner、email等
#     * 管理员元数据：guid、image_url、felo_score等
#     * 团队-管理员关系：一对多关系维护
#   - 数据验证和清洗：
#     * 必需字段验证：team_key、team_id、name、league_key
#     * 布尔值标准化：clinched_playoffs、has_draft_grade等
#     * 数值字段处理：number_of_moves、number_of_trades等
#     * Logo URL验证：team_logo_url格式检查
#   - 关联数据处理：
#     * 外键约束：league_key到League表的引用
#     * 管理员关联：team_key到Team表的引用
#     * 数据完整性：团队-管理员关系的一致性
#   - 去重和冲突处理：
#     * 团队主键冲突：team_key唯一性检查
#     * 管理员冲突处理：manager_id + team_key的唯一性
#     * 更新vs插入：已存在团队和管理员的处理
#   - 批量处理优化：
#     * 团队批量插入：高效的多团队写入
#     * 管理员批量处理：团队关联的管理员数据
#     * 分层提交：先团队后管理员的依赖处理
#     * 错误隔离：单个团队失败不影响整批
#   - 业务规则验证：
#     * 团队数量限制：联盟team数量的合理性检查
#     * 管理员数量：每个团队管理员数量的验证
#     * 专员唯一性：每个联盟只能有一个专员
#   - 嵌套数据处理：
#     * roster_adds信息：coverage_value、value字段解析
#     * managers数组：管理员信息的批量处理
#     * team_logos数组：Logo信息的提取和验证
#   - 统计和报告：
#     * 处理统计：新增团队数量、管理员数量
#     * 团队分布：各联盟的团队统计
#     * 加载性能：批量处理的效率指标
#
# 输入: 标准化的团队数据列表 (List[Dict])，包含managers信息
# 输出: 团队和管理员数据加载结果和统计信息 