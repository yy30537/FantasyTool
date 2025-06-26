# 排名数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的排名相关解析逻辑
# 主要映射:
#   - _extract_team_standings_info() -> parse_standings_data()
#   - 联盟排名数据提取逻辑
#
# 职责:
#   - 解析Yahoo API返回的league standings数据结构
#   - 从复杂嵌套结构中递归提取关键字段：
#     * team_key：团队标识符
#     * team_standings：排名和战绩信息
#     * team_points：团队积分信息
#   - 处理team_standings数据：
#     * rank：联盟排名
#     * outcome_totals：总战绩（wins、losses、ties、percentage）
#     * games_back：落后场次
#     * playoff_seed：季后赛种子
#     * divisional_outcome_totals：分区战绩
#   - 数值字段安全转换和验证
#   - 处理特殊值：games_back的"-"表示第一名
#
# 输入: Yahoo API league standings响应 (JSON)
# 输出: 标准化的排名数据列表 (List[Dict]) 