# 球员数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的球员相关解析逻辑
# 主要映射:
#   - _extract_player_info_from_league_data() -> parse_league_players()
#   - _normalize_player_info() -> 移至 normalizers/player_normalizer.py
#   - 球员基本信息提取逻辑
#
# 职责:
#   - 解析Yahoo API返回的players数据结构
#   - 提取player_key、player_id、editorial_player_key等核心字段
#   - 处理球员姓名信息：full_name、first_name、last_name
#   - 解析团队信息：editorial_team_key、editorial_team_full_name等
#   - 处理头像信息：headshot URL提取
#   - 解析位置信息：display_position、position_type
#   - 提取合适位置信息：eligible_positions数组
#   - 处理球员状态和属性字段
#
# 输入: Yahoo API league players响应 (JSON)
# 输出: 标准化的球员数据列表 (List[Dict])，包含eligible_positions 