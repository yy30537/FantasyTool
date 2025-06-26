# 球员数据标准化器
#
# 迁移来源: @yahoo_api_data.py 中的球员标准化逻辑
# 主要映射:
#   - _normalize_player_info() -> normalize_player_data()
#   - 球员信息字段标准化逻辑
#
# 职责:
#   - 标准化球员姓名信息：
#     * 从name字典中提取full、first、last字段
#     * 设置统一的姓名字段格式
#   - 标准化团队信息：
#     * editorial_team_key -> current_team_key
#     * editorial_team_full_name -> current_team_name  
#     * editorial_team_abbr -> current_team_abbr
#   - 标准化头像信息：
#     * 从headshot字典中提取url字段
#     * 设置headshot_url字段
#   - 添加元数据字段：
#     * season：当前赛季标识
#     * last_updated：更新时间戳
#   - 数据类型标准化和字段重命名
#   - 处理缺失值和默认值设置
#
# 输入: 原始球员数据字典 (Dict)
# 输出: 标准化的球员数据字典 (Dict) 