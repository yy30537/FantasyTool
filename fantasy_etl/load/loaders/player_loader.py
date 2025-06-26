# 球员数据加载器
#
# 迁移来源: @database_writer.py 中的球员相关写入逻辑
# 主要映射:
#   - write_players_batch() -> PlayerLoader.load_players()
#   - write_player_eligible_positions() -> PlayerLoader.load_eligible_positions()
#   - Player和PlayerEligiblePosition模型写入逻辑
#
# 职责:
#   - 球员基础数据加载：
#     * Player模型数据写入：player_key、full_name、league_key等
#     * 球员身份信息：editorial_player_key、player_id等
#     * 球员姓名信息：first_name、last_name、full_name
#     * 球员当前信息：current_team_key、current_team_name等
#   - 球员位置和状态加载：
#     * 位置信息：display_position、primary_position、position_type
#     * 球员状态：status、uniform_number、is_undroppable
#     * 图像信息：image_url、headshot_url
#     * 赛季信息：season、last_updated
#   - 合适位置数据加载：
#     * PlayerEligiblePosition模型写入：player_key、position
#     * 位置列表处理：eligible_positions数组解析
#     * 位置关系维护：球员-位置的多对多关系
#     * 位置数据清洗：删除旧记录后添加新记录
#   - 数据验证和清洗：
#     * 必需字段验证：player_key、player_id、league_key
#     * 姓名字段处理：从name字典中提取各部分
#     * 布尔值标准化：is_undroppable字段处理
#     * 位置数据验证：有效位置代码检查
#   - 关联数据处理：
#     * 外键约束：league_key到League表的引用
#     * 球员-位置关系：PlayerEligiblePosition表维护
#     * 数据完整性：球员信息的一致性保证
#   - 去重和冲突处理：
#     * 球员主键冲突：player_key唯一性检查
#     * 位置关系冲突：player_key + position的处理
#     * 更新vs插入：已存在球员的信息更新
#   - 批量处理优化：
#     * 球员批量插入：高效的多球员写入
#     * 位置批量处理：每个球员的位置数据
#     * 分层提交：先球员后位置的依赖处理
#     * 错误隔离：单个球员失败不影响整批
#   - 业务规则验证：
#     * 球员数量合理性：联盟球员总数检查
#     * 位置数量限制：每个球员的位置数量
#     * 团队归属验证：current_team信息的有效性
#   - 增量更新支持：
#     * 球员信息变更：团队转会、状态变化等
#     * 位置变更处理：新增或删除合适位置
#     * 时间戳管理：last_updated字段维护
#   - 统计和报告：
#     * 处理统计：新增球员数量、位置关系数量
#     * 位置分布：各位置球员的统计
#     * 加载性能：批量处理的效率指标
#
# 输入: 标准化的球员数据列表 (List[Dict])，包含eligible_positions
# 输出: 球员和位置数据加载结果和统计信息 