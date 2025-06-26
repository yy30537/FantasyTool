# 对战统计数据转换器
#
# 迁移来源: @database_writer.py 中的对战统计处理逻辑
# 主要映射:
#   - _parse_stat_winners() -> transform_stat_winners()
#   - _parse_teams_matchup_data() -> transform_matchup_details()
#   - 对战结果和统计类别获胜情况的处理逻辑
#
# 职责:
#   - 统计类别获胜分析：
#     * 解析stat_winners数组：每个统计类别的获胜者
#     * 提取stat_id和winner_team_key信息
#     * 构建团队在各统计类别中的获胜映射
#     * 支持9个核心统计类别：FG%、FT%、3PTM、PTS、REB、AST、ST、BLK、TO
#   - 对战详情提取：
#     * 从teams数据中识别目标团队和对手团队
#     * 提取双方team_points：总积分信息
#     * 解析team_remaining_games：比赛场次统计
#     * 计算胜负关系：基于积分比较
#   - 比赛场次统计：
#     * completed_games：已完成比赛数
#     * remaining_games：剩余比赛数  
#     * live_games：进行中比赛数
#     * 双方比赛场次对比分析
#   - 对战结果计算：
#     * 基于team_points确定胜负关系
#     * 处理平局情况：积分相等
#     * 交叉验证：与winner_team_key的一致性
#   - 结构化字段转换：
#     * 将复杂JSON结构转换为数据库的结构化字段
#     * 布尔值字段：wins_field_goal_pct、wins_points等
#     * 整数字段：team_points、completed_games等
#   - 数据完整性验证：
#     * 统计类别获胜数量合理性检查
#     * 比赛场次数据的逻辑一致性
#     * 胜负结果的多重验证
#
# 输入: Matchup数据和团队标识
# 输出: 标准化的对战结果对象 (Dict) 