# 联盟数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的联盟相关解析逻辑
# 主要映射:
#   - _extract_leagues_from_data() -> parse_leagues_data()
#   - 联盟基本信息提取逻辑
#
# 职责:
#   - 解析Yahoo API返回的leagues数据结构
#   - 提取league_key、league_id、name、game_key等核心字段
#   - 处理联盟配置信息：num_teams、scoring_type、draft_status等
#   - 解析联盟时间信息：start_date、end_date、current_week等
#   - 处理布尔值字段的标准化转换
#
# 输入: Yahoo API leagues响应 (JSON)
# 输出: 标准化的联盟数据列表 (List[Dict]) 