# 对战数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的对战相关解析逻辑
# 主要映射:
#   - _extract_matchup_info() -> parse_matchup_info()
#   - _extract_team_matchup_details() -> parse_team_matchup_details()
#   - _parse_stat_winners() -> 移至 stats/matchup_stats_transformer.py
#   - _parse_teams_matchup_data() -> 移至 stats/matchup_stats_transformer.py
#
# 职责:
#   - 解析Yahoo API返回的matchups数据结构
#   - 提取对战基本信息：week、week_start、week_end、status
#   - 处理对战标记：is_playoffs、is_consolation、is_matchup_of_week
#   - 解析胜负信息：is_tied、winner_team_key
#   - 提取对手信息：opponent_team_key
#   - 处理teams数据容器：从复杂嵌套结构中提取团队信息
#   - 解析team_points：团队得分信息
#   - 处理team_remaining_games：比赛场次统计
#   - 布尔值标准化：将字符串'0'/'1'转换为布尔值
#
# 输入: Yahoo API matchups响应 (JSON)
# 输出: 标准化的对战数据 (Dict)，包含对战详情和团队信息 