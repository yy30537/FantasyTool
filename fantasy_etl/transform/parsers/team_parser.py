# 团队数据解析器
#
# 迁移来源: @yahoo_api_data.py 中的团队相关解析逻辑
# 主要映射:
#   - _extract_team_data_from_api() -> parse_team_data()
#   - _extract_team_keys_from_data() -> parse_team_keys()
#   - 团队基本信息提取逻辑
#
# 职责:
#   - 解析Yahoo API返回的teams数据结构
#   - 提取team_key、team_id、name、url等核心字段
#   - 处理团队Logo信息：team_logos数组解析
#   - 解析roster_adds信息：coverage_value、value字段
#   - 处理managers数据：管理员信息提取
#   - 布尔值字段标准化：clinched_playoffs、has_draft_grade等
#   - 数字字段安全转换：number_of_trades等
#
# 输入: Yahoo API teams响应 (JSON)
# 输出: 标准化的团队数据列表 (List[Dict])，包含managers信息 