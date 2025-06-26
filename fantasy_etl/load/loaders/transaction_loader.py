# 交易数据加载器
#
# 迁移来源: @database_writer.py 中的交易相关写入逻辑
# 主要映射:
#   - write_transactions_batch() -> TransactionLoader.load_transactions()
#   - Transaction和TransactionPlayer模型写入逻辑
#   - 交易和球员详情的关联处理
#
# 职责:
#   - 交易记录数据加载：
#     * Transaction模型数据写入：transaction_key、type、status等
#     * 交易基本信息：transaction_id、league_key、timestamp
#     * 交易类型处理：add/drop、trade、waiver等
#     * 交易状态管理：successful、pending等状态
#   - 两队交易数据加载：
#     * 交易双方信息：trader_team_key、tradee_team_key
#     * 团队名称记录：trader_team_name、tradee_team_name
#     * Draft picks处理：picks_data的JSON存储
#     * 复杂交易支持：多球员、多picks的交易
#   - 交易球员详情加载：
#     * TransactionPlayer模型写入：球员交易详细信息
#     * 球员基本信息：player_key、player_name、position等
#     * 交易动作：transaction_type (add/drop/trade)
#     * 来源和目标：source_type、destination_type等
#   - 数据验证和清洗：
#     * 必需字段验证：transaction_key、type、league_key
#     * 时间戳验证：timestamp格式和合理性检查
#     * 交易类型验证：支持的transaction类型
#     * 球员数据验证：涉及球员的完整性
#   - 关联数据处理：
#     * 外键约束：league_key到League表的引用
#     * 交易-球员关系：transaction_key的关联
#     * 团队引用：trader/tradee_team_key的有效性
#     * 球员引用：player_key的存在性检查
#   - 复杂数据结构处理：
#     * players数据解析：嵌套的球员信息提取
#     * transaction_data提取：球员交易详情
#     * JSON字段处理：picks_data、players_data存储
#     * 多层嵌套处理：player[0][info]、player[1][transaction_data]
#   - 去重和冲突处理：
#     * 交易主键冲突：transaction_key唯一性
#     * 球员交易冲突：(transaction_key, player_key)唯一性
#     * 重复交易检测：相同交易的识别和跳过
#   - 批量处理优化：
#     * 交易批量插入：多个交易的高效写入
#     * 球员详情批量处理：关联球员数据的批量写入
#     * 分层提交：先交易记录后球员详情
#     * 事务协调：交易和球员数据的一致性
#   - 业务规则验证：
#     * 交易合法性：参与团队和球员的有效性
#     * 交易时间：在联盟交易期限内
#     * 球员资格：球员是否可交易（非undroppable等）
#     * 工资帽规则：如果适用的salary cap验证
#   - 交易历史管理：
#     * 时间序列：按时间戳排序的交易历史
#     * 球员轨迹：球员在团队间的转移轨迹
#     * 交易统计：团队和球员的交易频率
#   - 统计和报告：
#     * 处理统计：交易数量、涉及球员数量
#     * 交易类型分布：各类交易的统计
#     * 团队活跃度：各团队的交易活动
#     * 时间分布：交易的时间模式分析
#
# 输入: 标准化的交易数据列表 (List[Dict])，包含players详情
# 输出: 交易和球员详情数据加载结果和统计信息 