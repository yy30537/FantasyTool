# 统计数据工具函数
#
# 迁移来源: @database_writer.py 中的工具方法
# 主要映射:
#   - _safe_int() -> safe_int_conversion()
#   - _safe_float() -> safe_float_conversion()
#   - _parse_percentage() -> parse_percentage_value()
#   - _safe_bool() -> safe_bool_conversion()
#   - 数据类型安全转换的工具函数集合
#
# 职责:
#   - 安全整数转换：
#     * 处理None、空字符串：返回None
#     * 字符串数字转换：先转float再转int处理'1.0'格式
#     * 异常处理：ValueError、TypeError的安全捕获
#     * 浮点数转整数：处理API返回的浮点格式整数
#   - 安全浮点数转换：
#     * 相似的空值和异常处理逻辑
#     * 保持浮点精度：小数位保留策略
#     * 科学记数法支持：处理大数值格式
#   - 百分比解析：
#     * 多格式支持：'50%'、'0.5'、'.500'格式
#     * 百分比标准化：统一转换为0-100范围
#     * 自动识别格式：小数形式vs百分比形式
#     * 特殊值处理：'-'、'N/A'等无效值
#     * 精度控制：保留3位小数
#   - 安全布尔转换：
#     * 多类型支持：str、int、float、bool
#     * 字符串布尔：'1'/'0'、'true'/'false'
#     * 数值布尔：非零为True，零为False
#     * 默认值策略：None或异常时返回False
#   - 复合统计解析：
#     * 'made/attempted'格式拆分
#     * 两个数值的安全提取
#     * 格式验证和错误处理
#   - 数据验证辅助：
#     * 数值范围检查：百分比0-100等
#     * 逻辑关系验证：made <= attempted等
#     * 数据合理性判断：统计值的业务规则
#
# 输入: 各种原始数据类型 (Any)
# 输出: 转换后的目标数据类型，失败时返回None或默认值 