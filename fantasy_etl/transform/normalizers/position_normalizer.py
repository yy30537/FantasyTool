# 位置数据标准化器
#
# 迁移来源: @yahoo_api_data.py 中的位置相关标准化逻辑
# 主要映射:
#   - _extract_position_string() -> normalize_position_data()
#   - 位置信息提取和标准化逻辑
#
# 职责:
#   - 标准化位置数据格式：
#     * 处理字符串类型位置：直接返回
#     * 处理字典类型位置：提取position字段
#     * 处理列表类型位置：提取第一个有效位置
#   - 位置信息验证：
#     * 检查位置代码有效性
#     * 标准化位置缩写：PG、SG、SF、PF、C等
#   - 特殊位置处理：
#     * 首发位置：非BN、IL、IR的位置
#     * 替补位置：BN（Bench）
#     * 伤病名单：IL、IR（Injured List/Reserve）
#   - 位置类型分类：
#     * 确定is_starting、is_bench、is_injured_reserve状态
#   - 处理嵌套位置数据结构
#
# 输入: 原始位置数据 (Any: str/dict/list)
# 输出: 标准化的位置字符串 (Optional[str]) 