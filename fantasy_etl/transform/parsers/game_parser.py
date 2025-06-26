# 游戏数据解析器
# 
# 迁移来源: @yahoo_api_data.py 中的游戏相关解析逻辑
# 主要映射:
#   - _extract_game_keys() -> parse_game_keys()
#   - 游戏数据提取和验证逻辑
#
# 职责:
#   - 解析Yahoo API返回的games数据结构
#   - 提取game_key、game_id、name、code等核心字段
#   - 过滤type='full'的游戏
#   - 数据格式标准化和验证
#
# 输入: Yahoo API games响应 (JSON)
# 输出: 标准化的游戏数据列表 (List[Dict]) 