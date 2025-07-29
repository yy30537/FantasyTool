# Fantasy ETL - 模块化重构版本

基于命名约定的Yahoo Fantasy Sports ETL系统重构版本。

## 🏗️ 模块架构

```
fantasy_etl/
├── __init__.py                 # 主模块入口
├── api/                        # API数据获取模块
│   ├── __init__.py
│   ├── client.py              # API客户端基础设施
│   └── fetchers.py            # Yahoo API数据获取器 (fetch_*)
├── database/                   # 数据库操作模块
│   ├── __init__.py
│   ├── connection.py          # 数据库连接管理
│   └── queries.py             # 数据库查询操作 (get_*)
├── transformers/               # 数据转换模块
│   ├── __init__.py
│   ├── core.py                # 核心数据转换器 (transform_*)
│   ├── roster.py              # Roster数据转换器
│   ├── team.py                # Team数据转换器
│   ├── player.py              # Player数据转换器
│   └── stats.py               # 统计数据转换器
├── loaders/                    # 数据加载模块
│   ├── __init__.py
│   ├── core.py                # 核心数据加载器 (load_*)
│   ├── batch.py               # 批量数据加载器
│   └── stats.py               # 统计数据专用加载器
├── validators/                 # 验证模块
│   ├── __init__.py
│   └── core.py                # 核心验证器 (verify_*)
└── utils/                      # 工具模块
    ├── __init__.py
    ├── date_utils.py          # 日期处理工具
    └── helpers.py             # 通用辅助函数
```

## 📋 命名约定

- **`fetch_*`**: Yahoo API调用
- **`get_*`**: 数据库查询  
- **`transform_*`**: 数据转换
- **`load_*`**: 数据库写入
- **`verify_*`**: 验证和检查

## 🔄 迁移指导

### 1. API获取器 (fantasy_etl/api/fetchers.py)
从 `archive/yahoo_api_data.py` 迁移所有 `fetch_*` 函数：
- 29个API获取函数
- 包括基础API、团队、球员、交易、统计数据获取
- 保持原有函数签名和功能

### 2. 数据库查询 (fantasy_etl/database/queries.py) 
从多个源文件迁移所有 `get_*` 函数：
- `archive/yahoo_api_data.py`: 4个查询函数
- `archive/database_writer.py`: 3个查询函数
- 保持查询逻辑和返回格式

### 3. 数据转换器 (fantasy_etl/transformers/)
按业务领域分组迁移 `transform_*` 函数：
- **core.py**: 通用转换函数 (6个)
- **roster.py**: Roster相关转换 (1个)  
- **team.py**: Team相关转换 (8个)
- **player.py**: Player相关转换 (2个)
- **stats.py**: 统计数据转换 (11个)

### 4. 数据加载器 (fantasy_etl/loaders/)
按功能类型分组迁移 `load_*` 和写入函数：
- **core.py**: 基础加载函数 (6个)
- **batch.py**: 批量写入函数 (12个)
- **stats.py**: 统计数据写入 (10个)

### 5. 验证器 (fantasy_etl/validators/core.py)
迁移所有 `verify_*` 函数：
- 2个现有验证函数
- 3个新增验证方法

### 6. 工具函数 (fantasy_etl/utils/)
迁移通用工具函数：
- **date_utils.py**: 日期处理 (3个函数)
- **helpers.py**: 安全转换 (6个函数)

## 🚀 使用示例

```python
# 导入模块
from fantasy_etl.api import YahooFantasyFetcher
from fantasy_etl.transformers import RosterTransformers
from fantasy_etl.loaders import CoreLoaders

# 创建实例
fetcher = YahooFantasyFetcher()
transformer = RosterTransformers()  
loader = CoreLoaders()

# ETL流程
raw_data = fetcher.fetch_team_roster("team_key")
transformed_data = transformer.transform_roster_data(raw_data, "team_key")
success = loader.load_roster_data(transformed_data)
```

## 🔧 迁移优先级

### 阶段1: 基础设施 🏗️
1. `utils/` - 工具函数 (无依赖)
2. `database/connection.py` - 数据库连接
3. `api/client.py` - API客户端基础

### 阶段2: 核心功能 🔥  
4. `transformers/core.py` - 核心转换
5. `api/fetchers.py` - API获取
6. `database/queries.py` - 数据库查询

### 阶段3: 专用功能 🎯
7. `transformers/roster.py` - Roster转换
8. `transformers/team.py` - Team转换  
9. `transformers/stats.py` - Stats转换
10. `loaders/core.py` - 核心加载

### 阶段4: 批量和统计 📊
11. `loaders/batch.py` - 批量加载
12. `loaders/stats.py` - 统计加载
13. `validators/core.py` - 验证器

## 📝 注意事项

1. **保持向后兼容**: 原有函数签名和功能不变
2. **依赖关系**: 注意模块间的导入依赖
3. **错误处理**: 统一异常处理和日志记录
4. **类型注解**: 为所有函数添加完整类型注解
5. **文档字符串**: 详细的docstring说明迁移来源

## 🎯 预期收益

- **职责分离**: 每个模块功能明确
- **代码复用**: 转换函数可独立使用
- **测试友好**: 支持独立单元测试
- **维护性**: 降低代码复杂度
- **扩展性**: 支持新功能开发