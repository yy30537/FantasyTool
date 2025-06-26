# 球员统计数据转换器
#
# 迁移来源: @database_writer.py 中的球员统计处理逻辑
# 主要映射:
#   - _extract_core_player_season_stats() -> transform_season_stats()
#   - _extract_core_daily_stats() -> transform_daily_stats()
#   - 球员统计数据的标准化转换逻辑
#
# 职责:
#   - 赛季统计转换：
#     * 从统计值字典提取11个核心统计项
#     * 处理复合统计：FGM/A (9004003)、FTM/A (9007006)
#     * 转换百分比统计：FG% (5)、FT% (8)
#     * 提取单项统计：3PTM (10)、PTS (12)、REB (15)等
#     * 累计统计字段映射：total_points、total_rebounds等
#   - 日统计转换：
#     * 相同的11个核心统计项提取
#     * 日期特定的统计字段：points、rebounds（非total_）
#     * 周数信息处理和验证
#   - 统计数据验证：
#     * 数值合理性检查：投篮命中不能超过尝试等
#     * 百分比计算验证：计算值与API值的一致性
#     * 统计项完整性：必需统计项的存在性
#   - 数据类型转换：
#     * 安全整数转换：处理字符串数字
#     * 百分比标准化：统一为0-100范围的decimal值
#     * 复合统计解析：'made/attempted'格式拆分
#   - 特殊值处理：
#     * 无效值标识：'-'、'N/A'等
#     * 缺失统计项的默认值设置
#     * 异常值标记和处理
#
# 输入: 统计值字典 (Dict[str, Any])
# 输出: 标准化的统计数据对象 (Dict) 