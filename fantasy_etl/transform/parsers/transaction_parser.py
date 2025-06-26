# 交易数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的交易相关解析逻辑
# 主要映射:
#   - _extract_transactions_from_data() -> parse_transactions_data()
#   - 交易记录和交易球员解析逻辑
#
# 职责:
#   - 解析Yahoo API返回的transactions数据结构
#   - 提取transaction_key、transaction_id、type、status等核心字段
#   - 处理交易时间戳信息
#   - 解析两队交易信息：trader_team_key、tradee_team_key等
#   - 处理picks_data：draft picks交易数据
#   - 解析players数据：涉及的球员详细信息
#   - 提取transaction_players数据：
#     * player基本信息：player_key、player_name等
#     * transaction_data：transaction_type、source/destination信息
#   - 处理复杂嵌套结构：player[0][info_items]、player[1][transaction_data]
#
# 输入: Yahoo API transactions响应 (JSON)
# 输出: 标准化的交易数据和交易球员数据 (Tuple[List[Dict], List[Dict]]) 