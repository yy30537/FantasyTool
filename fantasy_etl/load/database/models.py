# 数据库模型定义
#
# 迁移来源: @model.py - 完整迁移所有数据库模型定义
# 主要映射:
#   - 所有SQLAlchemy模型类 -> 直接迁移
#   - 表关系定义 -> 保持不变
#   - 索引和约束 -> 完整保留
#   - 数据库工具函数 -> 适配新的架构
#
# 职责:
#   - 核心数据模型定义：
#     * Game: 游戏基本信息表
#     * League: 联盟信息表
#     * Team: 团队信息表
#     * Player: 球员信息表
#     * Manager: 团队管理员表
#   - 配置和设置模型：
#     * LeagueSettings: 联盟设置表
#     * StatCategory: 统计类别定义表
#     * LeagueRosterPosition: 联盟阵容位置表
#   - 时间序列数据模型：
#     * RosterDaily: 每日名单表
#     * PlayerDailyStats: 球员日统计表
#     * PlayerSeasonStats: 球员赛季统计表
#     * TeamStatsWeekly: 团队周统计表
#   - 交易和对战模型：
#     * Transaction: 交易记录表
#     * TransactionPlayer: 交易球员详情表
#     * TeamMatchups: 团队对战表
#     * LeagueStandings: 联盟排名表
#   - 辅助数据模型：
#     * DateDimension: 日期维度表
#     * PlayerEligiblePosition: 球员合适位置表
#   - 数据库关系定义：
#     * 外键关系：一对多、多对多关系
#     * 反向引用：便于查询的back_populates
#     * 级联操作：删除和更新的级联规则
#   - 索引和性能优化：
#     * 复合索引：多列查询优化
#     * 唯一约束：数据完整性保证
#     * 查询优化索引：常用查询路径优化
#   - 数据库工具函数：
#     * create_database_engine(): 引擎创建
#     * create_tables(): 表结构创建
#     * recreate_tables(): 表重建逻辑
#     * get_session(): 会话获取
#
# 注意:
#   - 完全保持原有表结构和字段定义
#   - 保持所有索引和约束不变
#   - 维护现有的关系映射
#   - 确保向后兼容性
#
# 输入: 无
# 输出: 完整的数据库模型定义和工具函数 